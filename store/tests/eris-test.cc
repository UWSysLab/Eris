// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tests/eris-test.cc:
 *   test cases for Eris transaction protocol (using key value store).
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
#include "store/eris/client.h"
#include "store/apps/kvstore/client.h"
#include "store/eris/server.h"
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
using namespace specpaxos::store::eris;

class ErisTest : public ::testing::Test
{
protected:
    std::map<int, std::vector<TxnServer *> > servers; // Sharded servers. Each shard has 3 replicas.
    std::map<int, std::vector<Replica *> > protoServers; // Sharded servers. Each shard has 3 replicas.
    std::map<TransportReceiver *, int> serverToShard; // Server to shard index map
    std::vector<AppReplica *> appReplicas;
    KVClient *kvClient;
    TxnClient *txnClient;
    Client *protoClient;
    SimulatedTransport *transport;
    Configuration *config;
    Configuration *fcorConfig;
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
        std::map<int, std::vector<ReplicaAddress> > fcorAddrs =
        {
            {
                0,
                {
                    { "localhost", "12400" },
                    { "localhost", "12401" },
                    { "localhost", "12402" }
                }
            }
        };
        nShards = nodeAddrs.size();

        this->config = new Configuration(nShards, 3, 1, nodeAddrs);
        this->fcorConfig = new Configuration(1, 3, 1, fcorAddrs);

        this->transport = new SimulatedTransport(true);

        // Create server replicas for each shard
        for (auto& kv : nodeAddrs) {
            int shardIdx = kv.first;
            this->servers[shardIdx] = std::vector<TxnServer *>();
            this->protoServers[shardIdx] = std::vector<Replica *>();
            for (int i = 0; i < config->n; i++) {
                KVStoreTxnServerArg arg;
                arg.keyPath = nullptr;
                arg.retryLock = false;
                TxnServer *txnserver = new KVTxnServer(arg);
                ErisServer *protoserver = new ErisServer(*config, shardIdx, i, true, transport, txnserver, *this->fcorConfig);

                this->servers[shardIdx].push_back(txnserver);
                this->protoServers[shardIdx].push_back(protoserver);
                this->serverToShard[protoserver] = shardIdx;
            }
        }

        // Create client
        uint64_t client_id = 0;
        while (client_id == 0) {
            std::random_device rd;
            std::mt19937_64 gen(rd());
            std::uniform_int_distribution<uint64_t> dis;
            client_id = dis(gen);
        }

        this->protoClient = new ErisClient(*config, transport, client_id);
        this->txnClient = new TxnClientCommon(transport, protoClient);
        this->kvClient = new KVClient(txnClient, nShards);
    }

    virtual void TearDown() {
        this->transport->Stop();
        delete this->kvClient;
        delete this->protoClient;

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
TEST_F(ErisTest, SingleShardIndepWriteReadTest) {
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
TEST_F(ErisTest, MultiShardsIndepWriteReadTest) {
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

/*
 * Txn1: indep, put k1 v1
 *              put k2 v2
 *              put k3 v3
 * Txn2: coord, get k1
 *              put k1 vv1
 * Txn3: coord, get k2
 *              get k3
 *              put k2 vv2
 *              put k3 vv3
 * Txn4: indep, get k1
 *              get k2
 *              get k3
 */
TEST_F(ErisTest, GeneralTxnTest)
{
    std::vector<KVOp_t> kvops;
    std::map<std::string, std::string> results;

    // Txn1
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

    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    ASSERT_EQ(results.size(), 0);

    // Txn2
    kvops.clear();
    results.clear();
    getOp.key = "k1";
    kvops.push_back(getOp);
    putOp.key = "k1";
    putOp.value = "vv1";
    kvops.push_back(putOp);

    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, false));
    ASSERT_EQ(results.size(), 1);
    EXPECT_TRUE(results.find("k1") != results.end());
    EXPECT_EQ(results["k1"], "v1");

    // Txn3
    kvops.clear();
    results.clear();
    getOp.key = "k2";
    kvops.push_back(getOp);
    getOp.key = "k3";
    kvops.push_back(getOp);
    putOp.key = "k2";
    putOp.value = "vv2";
    kvops.push_back(putOp);
    putOp.key = "k3";
    putOp.value = "vv3";
    kvops.push_back(putOp);

    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, false));
    ASSERT_EQ(results.size(), 2);
    EXPECT_TRUE(results.find("k2") != results.end());
    EXPECT_TRUE(results.find("k3") != results.end());
    EXPECT_EQ(results["k2"], "v2");
    EXPECT_EQ(results["k3"], "v3");

    // Txn4
    kvops.clear();
    results.clear();
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        getOp.key = buf;
        kvops.push_back(getOp);
    }
    EXPECT_TRUE(kvClient->InvokeKVTxn(kvops, results, true));
    ASSERT_EQ(results.size(), 3);
    EXPECT_TRUE(results.find("k1") != results.end());
    EXPECT_TRUE(results.find("k2") != results.end());
    EXPECT_TRUE(results.find("k3") != results.end());
    EXPECT_EQ(results["k1"], "vv1");
    EXPECT_EQ(results["k2"], "vv2");
    EXPECT_EQ(results["k3"], "vv3");
}

/*
TEST_F(NOTxnTest, SingleShardReplicaDropPacketTest)
{
    std::vector<KVOp> kvops;
    std::vector<std::string> results;
    KVTxnReplyMessage reply;
    KVOp putOp, getOp;
    std::map<std::pair<int, int>, int> message_counter;

    // Setup packet drops:
    // Replica 1 of the shard drops the first packet from the client
    transport->AddFilter(1, [&](TransportReceiver *src, int srcIdx,
                                TransportReceiver *dst, int dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx == -1 && dstIdx == 1) {
            int shardidx = server_to_shard[dst];
            if (message_counter.count(std::make_pair(shardidx, dstIdx)) == 0) {
                message_counter[std::make_pair(shardidx, dstIdx)] = 1;
                return false;
            }
        }
        return true;
    });

    putOp.opType = KVOp::PUT;
    putOp.key = "k1";
    putOp.value = "v1";
    kvops.push_back(putOp);

    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rputs_size(), 1);
    EXPECT_EQ(reply.rputs(0).key(), "k1");

    getOp.opType = KVOp::GET;
    getOp.key = "k1";
    kvops[0] = getOp;
    results.clear();
    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rgets_size(), 1);
    EXPECT_EQ(reply.rgets(0).key(), "k1");
    EXPECT_EQ(reply.rgets(0).value(), "v1");
}

TEST_F(NOTxnTest, SingleShardReplicaDropPacketTimeoutTest)
{
    std::vector<KVOp> kvops;
    std::vector<std::string> results;
    KVTxnReplyMessage reply;
    KVOp putOp, getOp;
    std::map<std::pair<int, std::pair<int, int> >, int> message_counter; // <shardid, <src, dst> >

    // Setup packet drops:
    // Non-leader replicas of the shard drop the first packet from the client.
    // Drops the first packet from replica to the leader, to simulate
    // GapRequest timeout.
    transport->AddFilter(1, [&](TransportReceiver *src, int srcIdx,
                                TransportReceiver *dst, int dstIdx,
                                Message &m, uint64_t &delay) {
        if ((srcIdx == -1 && (dstIdx == 1 || dstIdx == 2)) ||
            ((srcIdx == 1 || srcIdx == 2) && dstIdx == 0)) {
            int shardidx = server_to_shard[dst];
            if (message_counter.count(std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))) == 0) {
                message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))] = 1;
                return false;
            }
        }
        return true;
    });


    putOp.opType = KVOp::PUT;
    putOp.key = "k1";
    putOp.value = "v1";
    kvops.push_back(putOp);

    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rputs_size(), 1);
    EXPECT_EQ(reply.rputs(0).key(), "k1");

    getOp.opType = KVOp::GET;
    getOp.key = "k1";
    kvops[0] = getOp;
    results.clear();
    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rgets_size(), 1);
    EXPECT_EQ(reply.rgets(0).key(), "k1");
    EXPECT_EQ(reply.rgets(0).value(), "v1");

    // Add a timer to wait for timeout
    transport->Timer(5000, [&]() {
        transport->CancelAllTimers();
    });
    while (transport->HasTimers()) {
        usleep(1000);
    }
}

TEST_F(NOTxnTest, SingleShardLeaderDropPacketTest)
{
    std::vector<KVOp> kvops;
    std::vector<std::string> results;
    KVTxnReplyMessage reply;
    KVOp putOp, getOp;
    std::map<std::pair<int, std::pair<int, int> >, int> message_counter; // <shardid, <src, dst> >

    // Setup packet drops:
    // Leader replica of the shard drop the first packet from the client.
    transport->AddFilter(1, [&](TransportReceiver *src, int srcIdx,
                                TransportReceiver *dst, int dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx == -1 && dstIdx == 0) {
            int shardidx = server_to_shard[dst];
            if (message_counter.count(std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))) == 0) {
                message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))] = 1;
                return false;
            }
        }
        return true;
    });


    putOp.opType = KVOp::PUT;
    putOp.key = "k1";
    putOp.value = "v1";
    kvops.push_back(putOp);

    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rputs_size(), 1);
    EXPECT_EQ(reply.rputs(0).key(), "k1");

    getOp.opType = KVOp::GET;
    getOp.key = "k1";
    kvops[0] = getOp;
    results.clear();
    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rgets_size(), 1);
    EXPECT_EQ(reply.rgets(0).key(), "k1");
    EXPECT_EQ(reply.rgets(0).value(), "v1");
}

TEST_F(NOTxnTest, SingleShardCommitGapTest)
{
    std::vector<KVOp> kvops;
    std::vector<std::string> results;
    KVTxnReplyMessage reply;
    KVOp putOp, getOp;
    std::map<std::pair<int, std::pair<int, int> >, int> message_counter; // <shardid, <src, dst> >

    // Setup packet drops:
    // Client request 1 is dropped by all replicas of the shard
    transport->AddFilter(1, [&](TransportReceiver *src, int srcIdx,
                                TransportReceiver *dst, int dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx == -1) {
            int shardidx = server_to_shard[dst];
            if (message_counter.count(std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))) == 0) {
                message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))] = 1;
                return false;
            }
        }
        return true;
    });


    putOp.opType = KVOp::PUT;
    putOp.key = "k1";
    putOp.value = "v1";
    kvops.push_back(putOp);

    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rputs_size(), 1);
    EXPECT_EQ(reply.rputs(0).key(), "k1");

    getOp.opType = KVOp::GET;
    getOp.key = "k1";
    kvops[0] = getOp;
    results.clear();
    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rgets_size(), 1);
    EXPECT_EQ(reply.rgets(0).key(), "k1");
    EXPECT_EQ(reply.rgets(0).value(), "v1");
}

TEST_F(NOTxnTest, SingleShardLeaderDropPacketTimeoutTest)
{
    std::vector<KVOp> kvops;
    std::vector<std::string> results;
    KVTxnReplyMessage reply;
    KVOp putOp, getOp;
    std::map<std::pair<int, std::pair<int, int> >, int> message_counter; // <shardid, <src, dst> >

    // Setup packet drops:
    // Leader replica of the shard drop the first packet from the client.
    // Also drop first packets from leader to replicas to simulate timeout.
    transport->AddFilter(1, [&](TransportReceiver *src, int srcIdx,
                                TransportReceiver *dst, int dstIdx,
                                Message &m, uint64_t &delay) {
        if ((srcIdx == -1 && dstIdx == 0) || (srcIdx == 0 && dstIdx != -1)) {
            int shardidx = server_to_shard[dst];
            if (message_counter.count(std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))) == 0) {
                message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))] = 1;
                return false;
            }
        }
        return true;
    });


    putOp.opType = KVOp::PUT;
    putOp.key = "k1";
    putOp.value = "v1";
    kvops.push_back(putOp);

    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rputs_size(), 1);
    EXPECT_EQ(reply.rputs(0).key(), "k1");

    getOp.opType = KVOp::GET;
    getOp.key = "k1";
    kvops[0] = getOp;
    results.clear();
    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rgets_size(), 1);
    EXPECT_EQ(reply.rgets(0).key(), "k1");
    EXPECT_EQ(reply.rgets(0).value(), "v1");
}

TEST_F(NOTxnTest, SingleShardLogRequestTest)
{
    std::vector<KVOp> kvops;
    std::vector<std::string> results;
    KVTxnReplyMessage reply;
    KVOp putOp, getOp;
    std::map<std::pair<int, std::pair<int, int> >, int> message_counter; // <shardid, <src, dst> >

    // Setup packet drops:
    // Drop all packets from client to replica 2 (other than the first one).
    // Also drop packet 3 from client to all replicas.
    transport->AddFilter(1, [&](TransportReceiver *src, int srcIdx,
                                TransportReceiver *dst, int dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx == -1 && dstIdx == 2) {
            int shardidx = server_to_shard[dst];
            if (message_counter.count(std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))) == 0) {
                message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))] = 0;
                return true;
            } else {
                return false;
            }
        }
        if (srcIdx == -1) {
            int shardidx = server_to_shard[dst];
            if (message_counter.count(std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))) == 0) {
                message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))] = 0;
            }
            message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))]++;
            if (message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))] == 4) {
                return false;
            }
        }
        return true;
    });


    putOp.opType = KVOp::PUT;
    putOp.key = "k1";
    putOp.value = "v1";
    kvops.push_back(putOp);

    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rputs_size(), 1);
    EXPECT_EQ(reply.rputs(0).key(), "k1");

    putOp.opType = KVOp::PUT;
    putOp.key = "k1";
    putOp.value = "v2";
    kvops[0] = putOp;
    results.clear();

    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rputs_size(), 1);
    EXPECT_EQ(reply.rputs(0).key(), "k1");

    putOp.opType = KVOp::PUT;
    putOp.key = "k1";
    putOp.value = "v3";
    kvops[0] = putOp;
    results.clear();

    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rputs_size(), 1);
    EXPECT_EQ(reply.rputs(0).key(), "k1");

    getOp.opType = KVOp::GET;
    getOp.key = "k1";
    kvops[0] = getOp;
    results.clear();
    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rgets_size(), 1);
    EXPECT_EQ(reply.rgets(0).key(), "k1");
    EXPECT_EQ(reply.rgets(0).value(), "v3");
}

TEST_F(NOTxnTest, SingleShardLogRequestTimeoutTest)
{
    std::vector<KVOp> kvops;
    std::vector<std::string> results;
    KVTxnReplyMessage reply;
    KVOp putOp, getOp;
    std::map<std::pair<int, std::pair<int, int> >, int> message_counter; // <shardid, <src, dst> >

    // Setup packet drops:
    // Drop all packets from client to replica 2 (other than the first one).
    // Also drop packet 3 from client to all replicas.
    // Drop the second packet from replica 2 to the leader to simulate timeout.
    transport->AddFilter(1, [&](TransportReceiver *src, int srcIdx,
                                TransportReceiver *dst, int dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx == -1 && dstIdx == 2) {
            int shardidx = server_to_shard[dst];
            if (message_counter.count(std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))) == 0) {
                message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))] = 0;
                return true;
            } else {
                return false;
            }
        }
        if (srcIdx == -1) {
            int shardidx = server_to_shard[dst];
            if (message_counter.count(std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))) == 0) {
                message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))] = 0;
            }
            message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))]++;
            if (message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))] == 4) {
                return false;
            }
        }
        if (srcIdx == 2 && dstIdx == 0) {
            int shardidx = server_to_shard[dst];
            if (message_counter.count(std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))) == 0) {
                message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))] = 0;
            }
            message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))]++;
            if (message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))] == 2) {
                return false;
            }
        }
        return true;
    });


    putOp.opType = KVOp::PUT;
    putOp.key = "k1";
    putOp.value = "v1";
    kvops.push_back(putOp);

    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rputs_size(), 1);
    EXPECT_EQ(reply.rputs(0).key(), "k1");

    putOp.opType = KVOp::PUT;
    putOp.key = "k1";
    putOp.value = "v2";
    kvops[0] = putOp;
    results.clear();

    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rputs_size(), 1);
    EXPECT_EQ(reply.rputs(0).key(), "k1");

    putOp.opType = KVOp::PUT;
    putOp.key = "k1";
    putOp.value = "v3";
    kvops[0] = putOp;
    results.clear();

    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rputs_size(), 1);
    EXPECT_EQ(reply.rputs(0).key(), "k1");

    getOp.opType = KVOp::GET;
    getOp.key = "k1";
    kvops[0] = getOp;
    results.clear();
    client->InvokeKVTxn(kvops, results, true);
    EXPECT_EQ(results.size(), 1);
    reply.ParseFromString(results[0]);
    EXPECT_EQ(reply.rgets_size(), 1);
    EXPECT_EQ(reply.rgets(0).key(), "k1");
    EXPECT_EQ(reply.rgets(0).value(), "v3");
}

TEST_F(NOTxnTest, MultiShardsReplicaDropTest) {
    std::vector<KVOp> kvops;
    std::vector<std::string> results;
    KVTxnReplyMessage reply;
    std::map<std::pair<int, std::pair<int, int> >, int> message_counter; // <shardid, <src, dst> >

    // Setup packet drops:
    // First txn: drop client to replica 1 of each shard
    // Second txn: drop client to replica 2 of each shard
    transport->AddFilter(1, [&](TransportReceiver *src, int srcIdx,
                                TransportReceiver *dst, int dstIdx,
                                Message &m, uint64_t &delay) {
        if (srcIdx == -1 && (dstIdx == 1 || dstIdx == 2)) {
            int shardidx = server_to_shard[dst];
            if (message_counter.count(std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))) == 0) {
                message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))] = 0;
            }
            message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))]++;
            if (dstIdx == 1 && message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))] == 1) {
                return false;
            }

            if (dstIdx == 2 && message_counter[std::make_pair(shardidx, std::make_pair(srcIdx, dstIdx))] == 2) {
                return false;
            }
        }
        return true;
    });

    // First load keys ki with values vi
    KVOp putOp;
    KVOp getOp;
    putOp.opType = KVOp::PUT;
    getOp.opType = KVOp::GET;
    char buf [32];
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        putOp.key = buf;
        sprintf(buf, "v%d", i);
        putOp.value = buf;
        kvops.push_back(putOp);
    }

    client->InvokeKVTxn(kvops, results, true);
    ASSERT_EQ(results.size(), 3);
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        int shard = client->key_to_shard(buf, nShards);
        reply.ParseFromString(results[shard]);
        EXPECT_EQ(reply.rputs_size(), 1);
        EXPECT_EQ(reply.rgets_size(), 0);

        EXPECT_EQ(reply.rputs(0).key(), buf);
    }

    // Get keys ki and also update ki to vvi
    kvops.clear();
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        getOp.key = buf;
        kvops.push_back(getOp);
        putOp.key = buf;
        sprintf(buf, "vv%d", i);
        putOp.value = buf;
        kvops.push_back(putOp);
    }

    client->InvokeKVTxn(kvops, results, true);
    ASSERT_EQ(results.size(), 3);
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        int shard = client->key_to_shard(buf, nShards);
        reply.ParseFromString(results[shard]);
        EXPECT_EQ(reply.rgets_size(), 1);
        EXPECT_EQ(reply.rputs_size(), 1);

        EXPECT_EQ(reply.rgets(0).key(), buf);
        EXPECT_EQ(reply.rputs(0).key(), buf);
        sprintf(buf, "v%d", i);
        EXPECT_EQ(reply.rgets(0).value(), buf);
    }

    // Get keys ki again
    kvops.clear();
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        getOp.key = buf;
        kvops.push_back(getOp);
    }

    client->InvokeKVTxn(kvops, results, true);
    ASSERT_EQ(results.size(), 3);
    for (int i = 1; i <= 3; i++) {
        sprintf(buf, "k%d", i);
        int shard = client->key_to_shard(buf, nShards);
        reply.ParseFromString(results[shard]);
        EXPECT_EQ(reply.rgets_size(), 1);
        EXPECT_EQ(reply.rputs_size(), 0);

        EXPECT_EQ(reply.rgets(0).key(), buf);
        sprintf(buf, "vv%d", i);
        EXPECT_EQ(reply.rgets(0).value(), buf);
    }
}
*/
