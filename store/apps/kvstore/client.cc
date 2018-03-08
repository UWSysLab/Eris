// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/apps/kvstore/client.cc:
 *   A transactional key value store client.
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

#include "store/apps/kvstore/client.h"
#include "lib/udptransport.h"

#include "store/eris/client.h"
#include "store/granola/client.h"
#include "store/unreplicated/client.h"
#include "store/spanner/client.h"
#include "store/tapir/client.h"
#include "store/common/frontend/txnclientcommon.h"

using namespace std;

namespace specpaxos {
namespace store {
namespace kvstore {

KVClient::KVClient(TxnClient *txn_client,
                   uint32_t nshards)
	: txnClient(txn_client), nshards(nshards), transport(nullptr), protoClient(nullptr) { }

// Used by YCSB
KVClient::KVClient(string configPath, int nshards, int mode)
    : nshards(nshards)
{
    ifstream configStream(configPath);
    if (configStream.fail()) {
        Panic("unable to read configuration file: %s", configPath.c_str());
    }

    Configuration config(configStream);
    this->transport = new UDPTransport();
    switch (mode) {
    case 1: {
        this->protoClient = new eris::ErisClient(config, this->transport);
        break;
    }
    case 2: {
        this->protoClient = new granola::GranolaClient(config, this->transport);
        break;
    }
    case 3: {
        this->protoClient = new store::unreplicated::UnreplicatedClient(config, this->transport);
        break;
    }
    case 4: {
        this->protoClient = new spanner::SpannerClient(config, this->transport);
        break;
    }
    case 5: {
        break;
    }
    default:
        Panic("Unknown protocol mode");
    }

    if (mode == 5) {
        this->txnClient = new tapir::TapirClient(config, this->transport);
    } else {
        this->txnClient = new TxnClientCommon(transport, this->protoClient);
    }
}

KVClient::~KVClient()
{
    if (this->txnClient != nullptr) {
        delete this->txnClient;
    }
    if (this->protoClient != nullptr) {
        delete this->protoClient;
    }
    if (this->transport != nullptr) {
        delete this->transport;
    }
}

bool
KVClient::InvokeKVTxn(const vector<KVOp_t> &kvops, map<string, string> &results, bool indep)
{
    map<shardnum_t, string> requests;
    map<shardnum_t, string> replies;
    map<shardnum_t, proto::KVTxnMessage> msgs;
    bool ro = true;

    if (kvops.empty()) {
        return true;
    }

    // Group kv operations according to shards
    for (KVOp_t op : kvops) {
        shardnum_t shard = key_to_shard(op.key, nshards);
        if (requests.find(shard) == requests.end()) {
            proto::KVTxnMessage message;
            msgs.insert(pair<shardnum_t, proto::KVTxnMessage>(shard, message));
        }

        proto::KVTxnMessage &message = msgs.at(shard);
        proto::GetMessage *getMessage;
        proto::PutMessage *putMessage;
        switch (op.opType) {
        case KVOp_t::GET: {
            getMessage = message.add_gets();
            getMessage->set_key(op.key);
            break;
        }
        case KVOp_t::PUT: {
            ro = false;
            putMessage = message.add_puts();
            putMessage->set_key(op.key);
            putMessage->set_value(op.value);
            break;
        }
        default:
            Panic("Wrong KV operation type");
        }
    }

    // Construct transaction request for each shard
    for (auto &shard_txn : msgs) {
        string txn_str;

        shard_txn.second.SerializeToString(&txn_str);
        requests.insert(pair<shardnum_t, string>(shard_txn.first, txn_str));
    }

    if (!this->txnClient->Invoke(requests, replies, indep, ro)) {
        return false;
    }
    ASSERT(requests.size() == replies.size());

    ParseReplies(replies, results);
    return true;
}

bool
KVClient::InvokeGetTxn(const string &key, string &value)
{
    KVOp_t op;
    bool ret;
    op.opType = KVOp_t::GET;
    op.key = key;
    vector<KVOp_t> kvops;
    kvops.push_back(op);
    map<string, string> results;
    ret = InvokeKVTxn(kvops, results, true);
    value = results[key];
    return ret;
}

bool
KVClient::InvokePutTxn(const string &key, const string &value)
{
    KVOp_t op;
    op.opType = KVOp_t::PUT;
    op.key = key;
    op.value = value;
    vector<KVOp_t> kvops;
    kvops.push_back(op);
    map<string, string> results;
    return InvokeKVTxn(kvops, results, true);
}

bool
KVClient::InvokeRMWTxn(const string &key1, const string &key2,
                       const string &value1, const string &value2,
                       bool indep)
{
    vector<KVOp_t> kvops;
    map<string, string> results;
    KVOp_t op;

    op.opType = KVOp_t::GET;
    op.key = key1;
    kvops.push_back(op);
    op.opType = KVOp_t::GET;
    op.key = key2;
    kvops.push_back(op);
    op.opType = KVOp_t::PUT;
    op.key = key1;
    op.value = value1;
    kvops.push_back(op);
    op.opType = KVOp_t::PUT;
    op.key = key2;
    op.value = value2;
    kvops.push_back(op);

    return InvokeKVTxn(kvops, results, indep);
}

void
KVClient::ParseReplies(const map<shardnum_t, string> &replies,
                       map<string, string> &results)
{
    for (auto &reply : replies) {
        proto::KVTxnReplyMessage reply_msg;
        reply_msg.ParseFromString(reply.second);

        for (int i = 0; i < reply_msg.rgets_size(); i++) {
            results[reply_msg.rgets(i).key()] = reply_msg.rgets(i).value();
        }
    }
}

} // namespace kvstore
} // namespace store
} // namespace storeapp
