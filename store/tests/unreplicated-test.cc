// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tests/unreplicated-test.cc:
 *   Test cases for unreplicated transaction protocol (using key value store).
 *
 * Copyright 2016 Jialin Li    <lijl@cs.washington.edu>
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

#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/simtransport.h"

#include "store/common/frontend/txnclientcommon.h"
#include "store/unreplicated/client.h"
#include "store/apps/kvstore/client.h"
#include "store/unreplicated/server.h"
#include "store/apps/kvstore/txnserver.h"

#include <stdio.h>
#include <stdlib.h>
#include <gtest/gtest.h>
#include <vector>
#include <utility>
#include <random>

using google::protobuf::Message;
using namespace specpaxos;
using namespace specpaxos::store;
using namespace specpaxos::store::kvstore;
using namespace specpaxos::store::unreplicated;

class UnreplicatedTest : public ::testing::Test
{
protected:
    /* Sharded servers. Each shard has 1 server. */
    std::map<int, TxnServer *> txnServers;
    std::map<int, Replica *> protoServers;

    KVClient *kvClient;
    TxnClient *txnClient;
    Client *protoClient;
    SimulatedTransport *transport;
    Configuration *config;
    int requestNum;
    int nShards;

    virtual void SetUp() {
        // Setup all node addresses
        std::map<int, std::vector<ReplicaAddress> > nodeAddrs =
        {
            {
                0,
                {
                    { "localhost", "12300" }
                }
            },
            {
                1,
                {
                    { "localhost", "12310" }
                }
            },
            {
                2,
                {
                    { "localhost", "12320" }
                }
            }
        };
        nShards = nodeAddrs.size();

        this->config = new Configuration(nShards, 1, 0, nodeAddrs);

        this->transport = new SimulatedTransport(true);

        // Create server replicas for each shard
        for (auto& kv : nodeAddrs) {
            int shardIdx = kv.first;
            KVStoreTxnServerArg arg;
            arg.keyPath = nullptr;
            arg.retryLock = true;

            this->txnServers[shardIdx] = new KVTxnServer(arg);
            this->protoServers[shardIdx] = new UnreplicatedServer(*config, shardIdx, 0, true, transport, this->txnServers[shardIdx]);
        }

        // Create client
        uint64_t client_id = 0;
        while (client_id == 0) {
            std::random_device rd;
            std::mt19937_64 gen(rd());
            std::uniform_int_distribution<uint64_t> dis;
            client_id = dis(gen);
        }

        this->protoClient = new UnreplicatedClient(*config, transport, client_id);
        this->txnClient = new TxnClientCommon(transport, protoClient);
        this->kvClient = new KVClient(txnClient, nShards);
    }

    virtual void TearDown() {
        this->transport->Stop();
        delete this->kvClient;
        delete this->protoClient;

        for (auto &kv : txnServers) {
            delete kv.second;
        }
        txnServers.clear();
        for (auto &kv : protoServers) {
            delete kv.second;
        }
        protoServers.clear();
        delete transport;
        delete config;
    }
};

TEST_F(UnreplicatedTest, SingleShardIndepWriteReadTest) {
    std::vector<KVOp_t> kvops;
    std::map<std::string, std::string> results;
    KVOp_t putOp;
    putOp.opType = KVOp_t::PUT;
    putOp.key = "k1";
    putOp.value = "v1";
    kvops.push_back(putOp);

    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    EXPECT_EQ(results.size(), 0);

    KVOp_t getOp;
    getOp.opType = KVOp_t::GET;
    getOp.key = "k1";
    kvops[0] = getOp;
    results.clear();
    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    EXPECT_EQ(results.size(), 1);
    EXPECT_TRUE(results.find("k1") != results.end());
    EXPECT_EQ(results.at("k1"), "v1");
}

TEST_F(UnreplicatedTest, MultiShardsIndepWriteReadTest) {
    std::vector<KVOp_t> kvops;
    std::map<std::string, std::string> results;

    // First load keys ki with values vi
    KVOp_t putOp;
    KVOp_t getOp;
    putOp.opType = KVOp_t::PUT;
    getOp.opType = KVOp_t::GET;
    char buf [32], buf2 [32];
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        putOp.key = buf;
        sprintf(buf, "v%d", i);
        putOp.value = buf;
        kvops.push_back(putOp);
    }

    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    ASSERT_EQ(results.size(), 0);

    // Get keys ki and also update ki to vvi
    kvops.clear();
    results.clear();
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        getOp.key = buf;
        kvops.push_back(getOp);
        putOp.key = buf;
        sprintf(buf, "vv%d", i);
        putOp.value = buf;
        kvops.push_back(putOp);
    }

    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    ASSERT_EQ(results.size(), 3);
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        EXPECT_TRUE(results.find(buf) != results.end());
        sprintf(buf2, "v%d", i);
        EXPECT_EQ(results.at(buf), buf2);
    }

    // Get keys ki again
    kvops.clear();
    results.clear();
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        getOp.key = buf;
        kvops.push_back(getOp);
    }

    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    ASSERT_EQ(results.size(), 3);
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        EXPECT_TRUE(results.find(buf) != results.end());
        sprintf(buf2, "vv%d", i);
        EXPECT_EQ(results.at(buf), buf2);
    }
}

