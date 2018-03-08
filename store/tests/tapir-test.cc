// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tests/tapir-test.cc:
 *   test cases for Tapir transaction protocol (using key value store).
 *
 * Copyright 2017 Jialin Li    <lijl@cs.washington.edu>
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

#include "store/tapir/client.h"
#include "store/apps/kvstore/client.h"
#include "store/tapir/server.h"
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
using namespace specpaxos::store::tapir;

class TapirTest : public ::testing::Test
{
protected:
    std::map<int, std::vector<TxnServer *> > servers; // Sharded servers. Each shard has 3 replicas.
    std::map<int, std::vector<Replica *> > protoServers; // Sharded servers. Each shard has 3 replicas.
    std::map<TransportReceiver *, int> serverToShard; // Server to shard index map
    std::vector<AppReplica *> appReplicas;
    KVClient *kvClient;
    TxnClient *txnClient;
    SimulatedTransport *transport;
    Configuration *config;
    int requestNum;
    int nShards;

    virtual void SetUp() {
        // Setup all the node addresses
        std::map<int, std::vector<ReplicaAddress> > nodeAddrs =
        {
            {
                0,
                {
                    { "localhost", "12300" },
                    { "localhost", "12301" },
                    { "localhost", "12302" }
                }
            },
            {
                1,
                {
                    { "localhost", "12310" },
                    { "localhost", "12311" },
                    { "localhost", "12312" }
                }
            },
            {
                2,
                {
                    { "localhost", "12320" },
                    { "localhost", "12321" },
                    { "localhost", "12322" }
                }
            }
        };
        nShards = nodeAddrs.size();

        this->config = new Configuration(nShards, 3, 1, nodeAddrs);

        this->transport = new SimulatedTransport(true);

        // Create server replicas for each shard
        for (auto& kv : nodeAddrs) {
            int shardIdx = kv.first;
            this->servers[shardIdx] = std::vector<TxnServer *>();
            this->protoServers[shardIdx] = std::vector<Replica *>();
            for (int i = 0; i < config->n; i++) {
                KVStoreTxnServerArg arg;
                arg.keyPath = nullptr;
                arg.retryLock = true;
                TxnServer *txnserver = new KVTxnServer(arg);
                TapirServer *protoserver = new TapirServer(*config, shardIdx, i, true, transport, txnserver);

                this->servers[shardIdx].push_back(txnserver);
                this->protoServers[shardIdx].push_back(protoserver);
                this->serverToShard[protoserver] = shardIdx;
            }
        }

        this->txnClient = new TapirClient(*this->config, this->transport);
        this->kvClient = new KVClient(txnClient, nShards);
    }

    virtual void TearDown() {
        this->transport->Stop();
        delete this->kvClient;

        for (auto &kv : servers) {
            for (auto server : kv.second) {
                delete server;
            }
        }
        servers.clear();
        for (auto &kv : protoServers) {
            for (auto protoserver : kv.second) {
                delete protoserver;
            }
        }
        protoServers.clear();
        delete transport;
        delete config;
    }
};

/*
 * Txn1: indep, put k1 v1
 * Txn2: indep, get k1
 */
TEST_F(TapirTest, SingleShardWriteReadTest) {
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

/*
 * Txn1: indep, put k1 v1
 *              put k2 v2
 *              put k3 v3
 * Txn2: indep, get k1
 *              get k2
 *              get k3
 *              put k1 vv1
 *              put k2 vv2
 *              put k2 vv3
 * Txn3: indep, get k1
 *              get k2
 *              get k3
 */
TEST_F(TapirTest, MultiShardsWriteReadTest) {
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

TEST_F(TapirTest, StressTest) {
    std::vector<KVOp_t> kvops;
    std::map<std::string, std::string> results;

    // First load keys ki with values vi
    KVOp_t putOp;
    KVOp_t getOp;
    putOp.opType = KVOp_t::PUT;
    getOp.opType = KVOp_t::GET;
    char buf [32];
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        putOp.key = buf;
        sprintf(buf, "v%d", i);
        putOp.value = buf;
        kvops.push_back(putOp);
    }

    for (int i = 0; i < 1000; i++) {
        EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    }
}

