// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/apps/kvstore/client.h:
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

#ifndef __KVSTORE_CLIENT_H__
#define __KVSTORE_CLIENT_H__

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/configuration.h"

#include "store/common/frontend/txnclient.h"
#include "store/apps/kvstore/kvstore-proto.pb.h"

#include <string>
#include <map>

namespace specpaxos {
namespace store {
namespace kvstore {

typedef struct {
    enum op_t {
        GET,
        PUT
    };
    op_t opType;
    std::string key;
    std::string value;
} KVOp_t;

class KVClient
{
public:
    KVClient(TxnClient *txn_client,
             uint32_t nshards);
    KVClient(std::string configPath, int nshards, int mode); // for YCSB
    ~KVClient();

    bool InvokeKVTxn(const std::vector<KVOp_t> &kvops, std::map<std::string, std::string> &results, bool indep);

    // These 3 methods are used by YCSB
    bool InvokeGetTxn(const std::string &key, std::string &value);
    bool InvokePutTxn(const std::string &key, const std::string &value);
    bool InvokeRMWTxn(const std::string &key1, const std::string &key2,
                      const std::string &value1, const std::string &value2,
                      bool indep);

    /**
        Made public for testing
    */
    shardnum_t key_to_shard(const std::string &key, const uint32_t nshards) {
        uint64_t hash = 5381;
        const char* str = key.c_str();
        for (unsigned int i = 0; i < key.length(); i++) {
            hash = ((hash << 5) + hash) + (uint64_t)str[i];
        }

        return (shardnum_t)(hash % nshards);
    };

private:
    TxnClient *txnClient;
    uint32_t nshards;
    Transport *transport;
    Client *protoClient;

    /* Parse txnclient replies and store all GET results.
     */
    void ParseReplies(const std::map<shardnum_t, std::string> &replies,
                      std::map<std::string, std::string> &results);
};

} // namespace kvstore
} // namespace store
} // namespace specpaxos

#endif /* __KVSTORE_CLIENT_H__ */
