// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/apps/kvstore/txnserver.h:
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

#ifndef __KVSTORE_TXNSERVER_H__
#define __KVSTORE_TXNSERVER_H__

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/backend/txnserver.h"
#include "store/common/backend/kvstore.h"
#include "store/common/backend/lockserver.h"
#include "store/apps/kvstore/kvstore-proto.pb.h"

namespace specpaxos {
namespace store {
namespace kvstore {

typedef struct {
    const char * keyPath;
    unsigned int nKeys;
    unsigned int myShard;
    unsigned int nShards;
    bool retryLock;
} KVStoreTxnServerArg;

class KVTxnServer : public TxnServer
{
public:
    KVTxnServer(KVStoreTxnServerArg arg);
    ~KVTxnServer();

    void InvokeTransaction(const std::string &txn, std::string &result, txnarg_t *arg, txnret_t *ret) override;

private:
    bool retryLock;
    struct kvtxn_t {
        txnarg_t txnarg;
        std::unordered_set<std::string> readSet;
        std::set<std::pair<std::string, std::string> > writeSet;
        std::unordered_set<std::string> blockedKeys;
    };

    KVStore *store;
    LockServer *lockServer;
    servermode_t mode;
    std::unordered_map<txnid_t, struct kvtxn_t *> pendingTxns;

    /* Returns transaction status */
    bool ExecuteTransaction(struct kvtxn_t *txn, std::string &result,
                            std::unordered_set<txnid_t> &unblocked_txns);
    void CleanupTxn(struct kvtxn_t *txn,
                    std::unordered_set<txnid_t> &unblocked_txns);
    void ReleaseLocks(struct kvtxn_t * txn,
                      std::unordered_set<txnid_t> &unblocked_txns);
};

} // namespace kvstore
} // namespace store
} // namespace specpaxos

#endif /* __KVSTORE_TXNSERVER_H__ */
