// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/apps/kvstore/txnserver.cc:
 *   Implementation of key value store transaction server.
 *
 * Copyright 2016 Jialin Li <lijl@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include <iostream>
#include "store/apps/kvstore/txnserver.h"

namespace specpaxos {
namespace store {
namespace kvstore {

using namespace std;

KVTxnServer::KVTxnServer(KVStoreTxnServerArg arg)
    : retryLock(arg.retryLock)
{
    this->store = new KVStore();
    this->lockServer = new LockServer(arg.retryLock);
    this->mode = MODE_NORMAL;

    if (arg.keyPath != nullptr) {
        string key;
        ifstream in;
        in.open(arg.keyPath);
        if (!in) {
            throw "Could not read keys";
        }

        for (unsigned int i = 0; i < arg.nKeys; i++) {
            getline(in, key);

            uint64_t hash = 5381;
            const char* str = key.c_str();
            for (unsigned int j = 0; j < key.length(); j++) {
                hash = ((hash << 5) + hash) + (uint64_t)str[j];
            }

            if (hash % arg.nShards == arg.myShard) {
                this->store->put(key, "null");
            }
        }
        in.close();
    }
}

KVTxnServer::~KVTxnServer()
{
    delete this->store;
    delete this->lockServer;
}

void
KVTxnServer::InvokeTransaction(const string &txn, string &result, txnarg_t *arg, txnret_t *ret)
{
    ASSERT(arg != nullptr);
    ASSERT(ret != nullptr);
    proto::KVTxnMessage message;
    string value;
    bool blocked = false;
    struct kvtxn_t *kvtxn;

    ret->txnid = arg->txnid;
    if (this->pendingTxns.find(arg->txnid) != this->pendingTxns.end()) {
        kvtxn = this->pendingTxns.at(arg->txnid);
    } else {
        kvtxn = new struct kvtxn_t;
    }
    kvtxn->txnarg = *arg;

    /* Certain transactions may have empty request (e.g. COMMIT and ABORT).
     */
    if (arg->type == TXN_PREPARE || arg->type == TXN_INDEP) {
        if (txn.length() > 0) {
            message.ParseFromString(txn);
            for (const auto &read : message.gets()) {
                kvtxn->readSet.insert(read.key());
            }
            for (const auto &write : message.puts()) {
                kvtxn->writeSet.insert(make_pair(write.key(), write.value()));
            }
        }
    }

    if (arg->type == TXN_PREPARE ||
        (arg->type == TXN_INDEP && this->mode == MODE_LOCKING)) {
        // Put into pending transactions (if not pending already)
        this->pendingTxns.insert(make_pair(arg->txnid, kvtxn));

        /* Try to acquire all read/write locks. We might
         * have already acquired some of the locks, but safe
         * to call lock multiple times.
         */
        for (const auto &key : kvtxn->readSet) {
            if (!this->lockServer->lockForRead(key, arg->txnid)) {
                // if client will retry acquiring locks, don't
                // need to track blocked keys (they will eventually
                // acquire all locks or abort)
                if (!this->retryLock) {
                    kvtxn->blockedKeys.insert(key);
                }
                blocked = true;
            }
        }
        for (const auto &kv : kvtxn->writeSet) {
            if (!this->lockServer->lockForWrite(kv.first, arg->txnid)) {
                if (!this->retryLock) {
                    kvtxn->blockedKeys.insert(kv.first);
                }
                blocked = true;
            }
        }
    }

    ret->blocked = blocked;

    if (blocked) {
        // Cannot acquire all locks.
        ret->commit = false;
    } else {
        /* ExecuteTransaction will properly release locks */
        ret->commit = ExecuteTransaction(kvtxn, result, ret->unblocked_txns);
    }

    this->mode = this->pendingTxns.empty() ? MODE_NORMAL : MODE_LOCKING;
    ret->mode = this->mode;
}

bool
KVTxnServer::ExecuteTransaction(struct kvtxn_t *txn, string &result,
                                unordered_set<txnid_t> &unblocked_txns)
{
    ASSERT(txn != nullptr);
    proto::KVTxnReplyMessage reply;
    string value;
    bool status = true;
    ASSERT(txn->blockedKeys.empty());

    /* Both PREPARE and INDEP types do reads */
    if (txn->txnarg.type == TXN_PREPARE || txn->txnarg.type == TXN_INDEP) {
        for (auto &read : txn->readSet) {
            if (!this->store->get(read, value)) {
                status = false;
            }
            proto::GetReply *getReply = reply.add_rgets();
            getReply->set_key(read);
            getReply->set_value(value);
        }
    }

    /* INDEP and COMMIT do writes */
    if (txn->txnarg.type == TXN_INDEP || txn->txnarg.type == TXN_COMMIT) {
        for (auto &write : txn->writeSet) {
            this->store->put(write.first, write.second);
        }
    }

    /* Only PREPARE does not finish in one phase */
    if (txn->txnarg.type != TXN_PREPARE) {
        if (this->pendingTxns.find(txn->txnarg.txnid) != this->pendingTxns.end()) {
            CleanupTxn(txn, unblocked_txns);
        }
    }

    if (status) {
        reply.set_status(proto::KVTxnReplyMessage::SUCCESS);
    } else {
        reply.set_status(proto::KVTxnReplyMessage::FAILED);
    }
    reply.SerializeToString(&result);
    return status;
}

void
KVTxnServer::CleanupTxn(struct kvtxn_t *txn,
                        unordered_set<txnid_t> &unblocked_txns)
{
    ASSERT(txn != nullptr);
    ReleaseLocks(txn, unblocked_txns);
    ASSERT(txn->blockedKeys.empty());
    this->pendingTxns.erase(txn->txnarg.txnid);
    delete txn;
}

void
KVTxnServer::ReleaseLocks(struct kvtxn_t *txn,
                          unordered_set<txnid_t> &unblocked_txns)
{
    ASSERT(txn != nullptr);

    for (const auto &read : txn->readSet) {
        unordered_set<uint64_t> newholders;
        this->lockServer->releaseForRead(read, txn->txnarg.txnid, newholders);
        if (this->retryLock) {
            // In retryLock mode, releasing a lock won't
            // automatically promote waiters to acquire
            // the lock.
            ASSERT(newholders.empty());
        }
        for (const uint64_t holder : newholders) {
            ASSERT(this->pendingTxns.find(holder) != this->pendingTxns.end());
            struct kvtxn_t *kvtxn = this->pendingTxns[holder];
            ASSERT(kvtxn->blockedKeys.find(read) != kvtxn->blockedKeys.end());
            kvtxn->blockedKeys.erase(read);
            if (kvtxn->blockedKeys.empty()) {
                unblocked_txns.insert(holder);
            }
        }
    }
    for (const auto &write : txn->writeSet) {
        unordered_set<uint64_t> newholders;
        this->lockServer->releaseForWrite(write.first, txn->txnarg.txnid, newholders);
        if (this->retryLock) {
            // In retryLock mode, releasing a lock won't
            // automatically promote waiters to acquire
            // the lock.
            ASSERT(newholders.empty());
        }
        for (const uint64_t holder : newholders) {
            ASSERT(this->pendingTxns.find(holder) != this->pendingTxns.end());
            struct kvtxn_t *kvtxn = this->pendingTxns[holder];
            ASSERT(kvtxn->blockedKeys.find(write.first) != kvtxn->blockedKeys.end());
            kvtxn->blockedKeys.erase(write.first);
            if (kvtxn->blockedKeys.empty()) {
                unblocked_txns.insert(holder);
            }
        }
    }
}

} // namespace kvstore
} // namespace store
} // namespace specpaxos
