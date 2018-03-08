// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * eris-protocol-test.cc:
 *   test cases for Eris protocol
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

#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/simtransport.h"

#include "store/eris/client.h"
#include "store/eris/server.h"
#include "store/eris/fcor.h"
#include "vr/replica.h"

#include <stdio.h>
#include <stdlib.h>
#include <gtest/gtest.h>

using google::protobuf::Message;
using namespace specpaxos;
using namespace specpaxos::store;
using namespace specpaxos::store::eris;
using namespace specpaxos::store::eris::proto;
using namespace std;

class ErisTestApp : public AppReplica
{
public:
    ErisTestApp() { };
    ~ErisTestApp() { };

    void ReplicaUpcall(opnum_t opnum, const string &req, string &reply,
                       void *arg = nullptr, void *ret = nullptr) override {
        ops.push_back(req);
        reply = "reply: " + req;
        ASSERT(ret != nullptr);
        ((txnret_t *)ret)->blocked = false;
    }

    vector<string> ops;
};

class ErisTest : public ::testing::Test
{
protected:
    struct TestClient {
        ErisClient *client;
        int clientId;
        int requestNum;

        TestClient()
            : clientId(0), requestNum(0) {}

        TestClient(ErisClient *client, int clientId)
            : client(client), clientId(clientId), requestNum(0) {}

        string RequestOp(int n) {
            ostringstream stream;
            stream << this->clientId << ":" << n;
            return stream.str();
        }

        string LastRequestOp() {
            return RequestOp(requestNum);
        }

        set<shardnum_t> lastRequestShards;

        void SendNext(set<shardnum_t> shards,
                      Client::g_continuation_t upcall) {
            requestNum++;
            lastRequestShards = shards;
            map<shardnum_t, string> requests;
            for (auto &shard : shards) {
                requests[shard] = LastRequestOp();
            }
            clientarg_t arg;
            arg.indep = true;
            client->Invoke(requests, upcall, (void *)&arg);
        }
    };

    map<int, vector<ErisTestApp *> > apps;
    map<int, vector<ErisServer *> > servers;
    vector<Fcor *> fcorApps;
    vector<vr::VRReplica *> fcorReplicas;
    vector<TestClient>  clients;
    SimulatedTransport *transport;
    Configuration *config;
    Configuration *fcorConfig;
    int nShards, nClients;

    set<shardnum_t> GenerateShards(int nshards) {
        int nremove = nShards - (rand() % nshards + 1);
        set<shardnum_t> res;
        for (int i = 0; i < nShards; i++) {
            res.insert(i);
        }
        for (int i = 0; i < nremove; i++) {
            auto it = res.begin();
            int index = rand() % res.size();
            for (int j = 0; j < index; j++) {
                it++;
            }
            res.erase(it);
        }
        return res;
    }

    virtual void SetUp() {
        map<int, vector<ReplicaAddress> > nodeAddrs =
        {
            {
                0,
                {
                    { "localhost", "12300" },
                    { "localhost", "12301" },
                    { "localhost", "12302" },
                }
            },
            {
                1,
                {
                    { "localhost", "12310" },
                    { "localhost", "12311" },
                    { "localhost", "12312" },
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
        map<int, vector<ReplicaAddress> > fcorAddrs =
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
        this->nShards = nodeAddrs.size();
        this->config = new Configuration(nShards, 3, 1, nodeAddrs);
        this->fcorConfig = new Configuration(1, 3, 1, fcorAddrs);

        this->transport = new SimulatedTransport();

        for (auto &kv : nodeAddrs) {
            int shardIdx = kv.first;
            this->servers[shardIdx] = vector<ErisServer *>();
            this->apps[shardIdx] = vector<ErisTestApp *>();
            for (int i = 0; i < config->n; i++) {
                ErisTestApp *app = new ErisTestApp();
                this->apps[shardIdx].push_back(app);
                this->servers[shardIdx].push_back(new ErisServer(*this->config,
                                                                 shardIdx,
                                                                 i,
                                                                 true,
                                                                 this->transport,
                                                                 app,
                                                                 *this->fcorConfig));
            }
        }

        for (int i = 0; i < this->fcorConfig->n; i++) {
            fcorApps.push_back(new Fcor(*this->config, this->transport));
            fcorReplicas.push_back(new vr::VRReplica(*this->fcorConfig, i, true, this->transport, 1, fcorApps[i]));
        }

        this->nClients = 5;
        for (int i = 0; i < this->nClients; i++) {
            this->clients.push_back(TestClient(new ErisClient(*this->config, this->transport, i+1), i));
        }

    }

    virtual void TearDown() {
        this->transport->Stop();

        for (auto client : this->clients) {
            delete client.client;
        }
        this->clients.clear();

        for (auto &kv : this->servers) {
            for (auto server : kv.second) {
                delete server;
            }
        }
        this->servers.clear();
        for (auto &kv : this->apps) {
            for (auto app : kv.second) {
                delete app;
            }
        }
        this->apps.clear();

        delete this->transport;
        delete this->config;
        delete this->fcorConfig;
    }

    void SetupClientCalls(map<int, vector<set<shardnum_t> > > &clientRequests,
                          vector<Client::g_continuation_t> &upcalls,
                          int &numUpcalls,
                          const int num_packets,
                          const int wait_packet_index = -1,
                          const int wait_time = 0,
                          const int epoch_change_packet_index = -1)
    {
        for (int i = 0; i < nClients; i++) {
            upcalls.push_back([&, i, num_packets, wait_packet_index, wait_time, epoch_change_packet_index](const map<shardnum_t, string> & request, const map<shardnum_t, string> & reply, bool commit) {
                EXPECT_EQ(request.size(), clients[i].lastRequestShards.size());
                EXPECT_EQ(reply.size(), clients[i].lastRequestShards.size());
                numUpcalls++;

                for (auto const &kv : request) {
                    EXPECT_EQ(clients[i].lastRequestShards.count(kv.first), 1);
                    EXPECT_EQ(clients[i].LastRequestOp(), kv.second);
                }
                for (auto const &kv : reply) {
                    EXPECT_EQ(clients[i].lastRequestShards.count(kv.first), 1);
                    EXPECT_EQ("reply: " + clients[i].LastRequestOp(), kv.second);
                }
                if (clients[i].requestNum < num_packets) {
                    // Only change session once
                    if (i == 0 && clients[i].requestNum == epoch_change_packet_index) {
                        this->transport->SessionChange();
                    }
                    if (clients[i].requestNum == wait_packet_index) {
                        transport->Timer(wait_time, [&, i]() {
                            set<shardnum_t> shards = GenerateShards(nShards);
                            clients[i].SendNext(shards, upcalls[i]);
                            clientRequests[i].push_back(shards);
                        });
                    } else {
                        set<shardnum_t> shards = GenerateShards(nShards);
                        clients[i].SendNext(shards, upcalls[i]);
                        clientRequests[i].push_back(shards);
                    }
                }
            });
            // First txn should include all shards, so that
            // all replicas can learn the client's address
            set<shardnum_t> shards;
            for (const auto &kv : servers) {
                shards.insert(kv.first);
            }
            clients[i].SendNext(shards, upcalls[i]);
            clientRequests[i].push_back(shards);
        }
    }

    void PrintServerLog()
    {
        for (auto kv : servers) {
            printf("Server log:\n");
            for (int i = 1; kv.second[0]->log.Find(i) != nullptr; ++i) {
                const LogEntry *entry = kv.second[0]->log.Find(i);
                printf("(%lu,%lu) ", entry->request.clientid(), entry->request.clientreqid());
                for (auto it = entry->request.ops().begin();
                     it != entry->request.ops().end();
                     ++it) {
                    printf("%u ", it->shard());
                }
                printf("\n");
            }
        }
    }
};

// Auxiliary checking functions
void CheckConsistency(const map<int, vector<set<shardnum_t> > > &clientRequests,
                      const map<int, vector<ErisServer *> > &servers,
                      const Configuration *config)
{
    map<int, pair<int, bool> > markers; // shard id -> <curent marker, pending check?>
    // Initialize markers
    for (const auto &kv : servers) {
        if (kv.second[0]->log.Find(1) != nullptr) {
            markers[kv.first] = make_pair(1, false);
        }
    }
    unsigned int currShard = markers.begin()->first;

    while (true) {
        ASSERT_FALSE(markers[currShard].second);
        const LogEntry *refEntry = servers.at(currShard)[0]->log.Find(markers[currShard].first);
        ASSERT_NE(refEntry, nullptr);

        if (refEntry->state == LOG_STATE_NOOP) {
            // Dropped txn, can safely skip
            markers[currShard].first++;
            if (servers.at(currShard)[0]->log.Find(markers[currShard].first) == nullptr) {
                markers.erase(currShard);
            }
        } else {
            // Received Txn
            // First, check if the request matches clientRequests
            set<shardnum_t> destShards;
            string request;
            for (auto it = refEntry->request.ops().begin();
                 it != refEntry->request.ops().end();
                 ++it) {
                destShards.insert(it->shard());
                if (it->shard() == currShard) {
                    request = it->op();
                }
            }
            ASSERT_FALSE(request.empty());
            int clientid, clientreqid;
            sscanf(request.c_str(), "%d:%d", &clientid, &clientreqid);
            ASSERT_EQ(clientRequests.at(clientid)[clientreqid-1], destShards);

            // Next, check consistency within and across shards. For each shard
            // in destShards, do the following: if the shard is already removed
            // from markers, a consistency violation is found.
            // Then, if the leader log entry at marker matches the reference
            // request, then mark the shard pending check, and check if all
            // replicas in the shard have the same log entry at the marker.
            // If all shards match the ref request, the check is passed, and advance
            // the marker on each shard, and turn off all pending check flags.
            // If a shard does not match, there are two cases:
            // 1. the shard is pending check, then a consistency violation is found.
            // 2. the shard is not pending check, then repeat the process with this
            // shard. When advancing marker, if currShard runs out of entries,
            // remove the shard from markers, and continue searching with
            // another shard that is not pending check. If all remaining shards
            // are pending check, then a consistency violation is found. The
            // process terminates when all shards are successfully removed.

            bool unmatched = false;
#ifdef DEBUGPRINT
            printf("matching shard %u marker %u (%lu,%lu)\n", currShard, markers[currShard].first, refEntry->request.clientid(), refEntry->request.clientreqid());
#endif
            for (auto shard : destShards) {
                ASSERT_NE(markers.find(shard), markers.end());
                const LogEntry *leaderEntry = servers.at(shard)[0]->log.Find(markers[shard].first);
                ASSERT_NE(leaderEntry, nullptr);

                bool match = (refEntry->request.clientid() == leaderEntry->request.clientid()) && (refEntry->request.clientreqid() == leaderEntry->request.clientreqid());
                if (!match) {
#ifdef DEBUGPRINT
                    printf("shard %u unmatched\n", shard);
#endif
                    unmatched = true;
                    continue;
                }
#ifdef DEBUGPRINT
                printf("shard %u matched\n", shard);
#endif

                // Mark as pending
                markers[shard].second = true;

                // Check all replicas have the same request
                for (int i = 1; i < config->n; i++) {
                    const LogEntry *entry = servers.at(shard)[i]->log.Find(markers[shard].first);
                    ASSERT_NE(entry, nullptr);
                    ASSERT_EQ(entry->request.clientid(), refEntry->request.clientid());
                    ASSERT_EQ(entry->request.clientreqid(), refEntry->request.clientreqid());
                }
            }

            if (unmatched) {
                // Try to find another non-pending shard
                bool shard_found = false;
                for (auto kv : markers) {
                    if (!kv.second.second) {
                        currShard = kv.first;
                        shard_found = true;
                        break;
                    }
                }
                // If no non-pending shard found, consistency is violated
                ASSERT_TRUE(shard_found);
                continue;
            }

            // Move marker to the next log entry, and remove all pending flags
            for (auto shard : destShards) {
                markers[shard].second = false;
                markers[shard].first++;
                if (servers.at(shard)[0]->log.Find(markers[shard].first) == nullptr) {
                    markers.erase(shard);
                }
            }
        }

        if (markers.empty()) {
            break;
        }

        if (markers.find(currShard) == markers.end()) {
            bool found = false;
            for (const auto &kv: markers) {
                if (!kv.second.second) {
                    currShard = kv.first;
                    found = true;
                    break;
                }
            }
            ASSERT_TRUE(found);
        }
    }
}

TEST_F(ErisTest, OneOp)
{
    int numUpcalls = 0;

    auto upcall = [&](const map<shardnum_t, string> & request, const map<shardnum_t, string> & reply, bool commit) {
        EXPECT_EQ(request.size(), clients[0].lastRequestShards.size());
        EXPECT_EQ(reply.size(), clients[0].lastRequestShards.size());
        numUpcalls++;

        for (auto const &kv : request) {
            EXPECT_EQ(clients[0].lastRequestShards.count(kv.first), 1);
            EXPECT_EQ(clients[0].LastRequestOp(), kv.second);
        }
        for (auto const &kv : reply) {
            EXPECT_EQ(clients[0].lastRequestShards.count(kv.first), 1);
            EXPECT_EQ("reply: " + clients[0].LastRequestOp(), kv.second);
        }

        // Leader of each shard should have executed the request
        for (auto const &shard : clients[0].lastRequestShards) {
            EXPECT_EQ(1, apps[shard][0]->ops.size());
            EXPECT_EQ(request.at(shard), apps[shard][0]->ops.back());
        }

        // There should be a quorum of replicas from each shard received this request
        for (auto const &shard : clients[0].lastRequestShards) {
            int numReceived = 0;
            for (int i = 0; i < config->n; i++) {
                const LogEntry *entry = servers.at(shard)[i]->log.Find(1);
                if (entry != nullptr) {
                    numReceived++;
                    string serverOp;
                    for (auto it = entry->request.ops().begin();
                         it != entry->request.ops().end();
                         ++it) {
                        if (it->shard() == shard) {
                            serverOp = it->op();
                            break;
                        }
                    }
                    EXPECT_EQ(clients[0].LastRequestOp(), serverOp);
                    EXPECT_EQ(LOG_STATE_RECEIVED, entry->state);
                }
            }
            EXPECT_GE(numReceived, config->QuorumSize());
        }
    };

    // Without any failures, 200ms should be enough for all replicas to receive the request
    transport->Timer(200, [&]() {
        transport->CancelAllTimers();
    });

    clients[0].SendNext(set<shardnum_t>{0, 1, 2}, upcall);
    transport->Run();

    // By now, all replicas should have received the request
    for (auto const &shard : clients[0].lastRequestShards) {
        for (int i = 0; i < config->n; i++) {
            const LogEntry *entry = servers[shard][i]->log.Find(1);
            ASSERT_NE(entry, nullptr);
            string serverOp;
            for (auto it = entry->request.ops().begin();
                 it != entry->request.ops().end();
                 ++it) {
                if (it->shard() == shard) {
                    serverOp = it->op();
                    break;
                }
            }
            EXPECT_EQ(clients[0].LastRequestOp(), serverOp);
            EXPECT_EQ(LOG_STATE_RECEIVED, entry->state);

            if (i == 0) {
                EXPECT_EQ(1, apps[shard][i]->ops.size());
                EXPECT_EQ(clients[0].LastRequestOp(), apps[shard][i]->ops.back());
            }
        }
    }
    // And client should have made the upcall
    EXPECT_EQ(1, numUpcalls);
}

TEST_F(ErisTest, ManyOps)
{
    const int NUM_PACKETS = 5;
    int numUpcalls = 0;
    vector<Client::g_continuation_t> upcalls;
    map<int, vector<set<shardnum_t> > > clientRequests;

    unsigned int seed = time(NULL);
    Notice("Seed is %u", seed);
    srand(seed);

    SetupClientCalls(clientRequests, upcalls, numUpcalls, NUM_PACKETS);
    // Without any failures, 500ms should be enough for all replicas to receive the request
    transport->Timer(500, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    // By now, all replicas should have received the request
#ifdef DEBUGPRINT
    PrintServerLog();
#endif
    EXPECT_EQ(nClients * NUM_PACKETS, numUpcalls);
    CheckConsistency(clientRequests, servers, config);
}

TEST_F(ErisTest, ReplicaGap)
{
    const int NUM_PACKETS = 10;
    int numUpcalls = 0;
    vector<Client::g_continuation_t> upcalls;
    map<int, vector<set<shardnum_t> > > clientRequests;
    map<pair<shardnum_t, msgnum_t>, int> stamps;
    int msg_counter = 0;

    unsigned int seed = time(NULL);
    Notice("Seed is %u", seed);
    srand(seed);

    // Drop the 12th and 16th txn from the client to 2 of the replicas
    // in each shard. They should be able to recover the txn from the
    // other replica in the local shard.
    eris::proto::RequestMessage requestMessage;
    set<int> drops = {12, 16};
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay,
                                const multistamp_t &stamp) {
        if (srcIdx.second == -1 &&
            m.GetTypeName() == requestMessage.GetTypeName()) {
            auto seqnum = stamp.seqnums.begin();
            if (stamps.find(make_pair(seqnum->first, seqnum->second)) == stamps.end()) {
                msg_counter++;
                for (auto it = stamp.seqnums.begin();
                     it != stamp.seqnums.end();
                     ++it) {
                    stamps[make_pair(it->first, it->second)] = msg_counter;
                }
            }
            if (drops.find(stamps[make_pair(seqnum->first, seqnum->second)]) != drops.end() &&
                dstIdx.second != 1) {
                return false;
            }
        }
        return true;
    });

    SetupClientCalls(clientRequests, upcalls, numUpcalls, NUM_PACKETS);
    // Without any failures, 1000ms should be enough for all replicas to receive the request
    transport->Timer(1000, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    // By now, all replicas should have received the request
    CheckConsistency(clientRequests, servers, config);
    EXPECT_EQ(nClients * NUM_PACKETS, numUpcalls);
}

TEST_F(ErisTest, FCFoundTxnLocal)
{
    const int NUM_PACKETS = 10;
    int numUpcalls = 0;
    vector<Client::g_continuation_t> upcalls;
    map<int, vector<set<shardnum_t> > > clientRequests;
    map<pair<shardnum_t, msgnum_t>, int> stamps;
    int msg_counter = 0;

    unsigned int seed = time(NULL);
    Notice("Seed is %u", seed);
    srand(seed);

    // Drop the 10th packet to the leader, and drop
    // all GapRequest messages from the leader. It
    // will force the leader to contact the FC, which
    // will find the txn from the other replicas in
    // the same group.
    set<int> drops = {10};
    GapRequestMessage gapRequestMessage;
    eris::proto::RequestMessage requestMessage;
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay,
                                const multistamp_t &stamp) {
        if (srcIdx.second == -1 &&
            m.GetTypeName() == requestMessage.GetTypeName()) {
            auto seqnum = stamp.seqnums.begin();
            if (stamps.find(make_pair(seqnum->first, seqnum->second)) == stamps.end()) {
                msg_counter++;
                for (auto it = stamp.seqnums.begin();
                     it != stamp.seqnums.end();
                     ++it) {
                    stamps[make_pair(it->first, it->second)] = msg_counter;
                }
            }
            if (drops.find(stamps[make_pair(seqnum->first, seqnum->second)]) != drops.end() &&
                dstIdx.second == 0) {
                return false;
            }
        } else if (srcIdx.second == 0 && dstIdx.second > 0 &&
                   m.GetTypeName() == gapRequestMessage.GetTypeName()) {
            return false;
        }
        return true;
    });

    SetupClientCalls(clientRequests, upcalls, numUpcalls, NUM_PACKETS);

    // Without any failures, 1000ms should be enough for all replicas to receive the request
    transport->Timer(1000, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    // By now, all replicas should have received all request
    CheckConsistency(clientRequests, servers, config);
    EXPECT_EQ(nClients * NUM_PACKETS, numUpcalls);
}

TEST_F(ErisTest, FCFoundTxnRemote)
{
    const int NUM_PACKETS = 10;
    int numUpcalls = 0;
    vector<Client::g_continuation_t> upcalls;
    map<int, vector<set<shardnum_t> > > clientRequests; // clientid -> [participant shards]
    map<pair<shardnum_t, msgnum_t>, int> stamps;
    int multishard_counter = 0;

    unsigned int seed = time(NULL);
    Notice("Seed is %u", seed);
    srand(seed);

    // Drop the 8rd multishard client request at one of
    // the shards (all replicas). Leader will contact
    // the FC, which will find the txn from some other
    // shards.
    set<int> drops = {8};
    eris::proto::RequestMessage requestMessage;
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay,
                                const multistamp_t &stamp) {
        if (srcIdx.second == -1 &&
            m.GetTypeName() == requestMessage.GetTypeName()) {
            if (stamp.seqnums.size() > 1) {
                // multi-shard request
                auto seqnum = stamp.seqnums.begin();
                if (stamps.find(make_pair(seqnum->first, seqnum->second)) == stamps.end()) {
                    multishard_counter++;
                    for (auto it = stamp.seqnums.begin();
                         it != stamp.seqnums.end();
                         ++it) {
                        stamps[make_pair(it->first, it->second)] = multishard_counter;
                    }
                }
                if (drops.find(stamps[make_pair(seqnum->first, seqnum->second)]) != drops.end() &&
                    dstIdx.first == (int)seqnum->first) {
                    // Only drop at one of the shards
                    return false;
                }
            }
        }
        return true;
    });

    SetupClientCalls(clientRequests, upcalls, numUpcalls, NUM_PACKETS);

    // Without any failures, 1000ms should be enough for all replicas to receive the request
    transport->Timer(1000, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    // By now, all replicas should have received all request
    CheckConsistency(clientRequests, servers, config);
    EXPECT_EQ(nClients * NUM_PACKETS, numUpcalls);
}

TEST_F(ErisTest, FCDropTxn)
{
    const int NUM_PACKETS = 10;
    int numUpcalls = 0;
    vector<Client::g_continuation_t> upcalls;
    map<int, vector<set<shardnum_t> > > clientRequests; // clientid -> [participant shards]
    map<pair<shardnum_t, msgnum_t>, int> stamps;
    int multishard_counter = 0;

    unsigned int seed = time(NULL);
    Notice("Seed is %u", seed);
    srand(seed);

    // Drop the 9th multishard client request at all shards.
    // FC will drop the txn.
    eris::proto::RequestMessage requestMessage;
    set<int> drops = {9};
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay,
                                const multistamp_t &stamp) {
        if (srcIdx.second == -1 &&
            m.GetTypeName() == requestMessage.GetTypeName()) {
            if (stamp.seqnums.size() > 1) {
                // multi-shard request
                auto seqnum = stamp.seqnums.begin();
                if (stamps.find(make_pair(seqnum->first, seqnum->second)) == stamps.end()) {
                    multishard_counter++;
                    for (auto it = stamp.seqnums.begin();
                         it != stamp.seqnums.end();
                         ++it) {
                        stamps[make_pair(it->first, it->second)] = multishard_counter;
                    }
                }
                if (drops.find(stamps[make_pair(seqnum->first, seqnum->second)]) != drops.end()) {
                    return false;
                }
            }
        }
        return true;
    });

    SetupClientCalls(clientRequests, upcalls, numUpcalls, NUM_PACKETS);

    // Without any failures, 1000ms should be enough for all replicas to receive the request
    transport->Timer(1000, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    // By now, all replicas should have received all request
    CheckConsistency(clientRequests, servers, config);
    EXPECT_EQ(nClients * NUM_PACKETS, numUpcalls);
}

TEST_F(ErisTest, ViewChangeNoDrop)
{
    const int NUM_PACKETS = 10;
    int numUpcalls = 0;
    vector<Client::g_continuation_t> upcalls;
    map<int, vector<set<shardnum_t> > > clientRequests; // clientid -> [participant shards]

    unsigned int seed = time(NULL);
    Notice("Seed is %u", seed);
    srand(seed);

    // Drop all SyncPrepare to trigger view change
    eris::proto::SyncPrepareMessage syncPrepareMessage;
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay,
                                const multistamp_t &stamp) {
        if (m.GetTypeName() == syncPrepareMessage.GetTypeName()) {
            return false;
        }
        return true;
    });

    SetupClientCalls(clientRequests, upcalls, numUpcalls, NUM_PACKETS, 5, 2000);

    transport->Timer(2500, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    CheckConsistency(clientRequests, servers, config);
    EXPECT_EQ(nClients * NUM_PACKETS, numUpcalls);
}

TEST_F(ErisTest, ViewChangeStateTransfer)
{
    const int NUM_PACKETS = 10;
    int numUpcalls = 0;
    vector<Client::g_continuation_t> upcalls;
    map<int, vector<set<shardnum_t> > > clientRequests; // clientid -> [participant shards]

    unsigned int seed = time(NULL);
    Notice("Seed is %u", seed);
    srand(seed);

    // Drop all SyncPrepare to trigger view change.
    // Drop the last few txns in the first view at replica 1,
    // so that when it becomes the leader, it will transfer
    // state from other replicas.
    eris::proto::SyncPrepareMessage syncPrepareMessage;
    eris::proto::RequestMessage requestMessage;
    set<pair<uint64_t, uint64_t> > request_counter;
    uint64_t drop_start = 5 * nClients - 5;
    uint64_t drop_end = 5 * nClients;
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay,
                                const multistamp_t &stamp) {
        if (m.GetTypeName() == syncPrepareMessage.GetTypeName()) {
            return false;
        }
        if (m.GetTypeName() == requestMessage.GetTypeName()) {
            requestMessage.CopyFrom(m);
            request_counter.insert(make_pair(requestMessage.request().clientid(),
                                             requestMessage.request().clientreqid()));
            if (request_counter.size() >= drop_start &&
                request_counter.size() <= drop_end &&
                dstIdx.second == 1) {
                return false;
            }
        }
        return true;
    });

    SetupClientCalls(clientRequests, upcalls, numUpcalls, NUM_PACKETS, 5, 2000);

    transport->Timer(2500, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    CheckConsistency(clientRequests, servers, config);
    EXPECT_EQ(nClients * NUM_PACKETS, numUpcalls);
}

TEST_F(ErisTest, ViewChangeWithDrops)
{
    const int NUM_PACKETS = 10;
    int numUpcalls = 0;
    vector<Client::g_continuation_t> upcalls;
    map<int, vector<set<shardnum_t> > > clientRequests; // clientid -> [participant shards]

    unsigned int seed = time(NULL);
    Notice("Seed is %u", seed);
    srand(seed);

    // Drop all SyncPrepare to trigger view change.
    // Drop a few txns in the first view at replica 0 and 2,
    // then drop all FC txn check at replica 1, so they will be
    // decided as perm drops. Replica 1 will merge these perm drops
    // during view change, overwriting existing log entries.
    eris::proto::SyncPrepareMessage syncPrepareMessage;
    eris::proto::RequestMessage requestMessage;
    eris::proto::GapRequestMessage gapRequestMessage;
    eris::proto::FCToErisMessage fcToErisMessage;
    set<pair<uint64_t, uint64_t> > request_counter;
    set<uint64_t> drops = {15, 21};
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay,
                                const multistamp_t &stamp) {
        if (m.GetTypeName() == syncPrepareMessage.GetTypeName()) {
            return false;
        }
        if (m.GetTypeName() == gapRequestMessage.GetTypeName()) {
            return false;
        }
        if (m.GetTypeName() == requestMessage.GetTypeName()) {
            requestMessage.CopyFrom(m);
            request_counter.insert(make_pair(requestMessage.request().clientid(),
                                             requestMessage.request().clientreqid()));
            if (drops.find(request_counter.size()) != drops.end() &&
                dstIdx.second != 1) {
                return false;
            }
        }
        if (m.GetTypeName() == fcToErisMessage.GetTypeName() &&
            dstIdx.second == 1) {
            return false;
        }
        return true;
    });

    SetupClientCalls(clientRequests, upcalls, numUpcalls, NUM_PACKETS, 5, 2000);

    transport->Timer(2500, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    CheckConsistency(clientRequests, servers, config);
    EXPECT_EQ(nClients * NUM_PACKETS, numUpcalls);
}

TEST_F(ErisTest, EpochChangeNoDrop)
{
    const int NUM_PACKETS = 10;
    int numUpcalls = 0;
    vector<Client::g_continuation_t> upcalls;
    map<int, vector<set<shardnum_t> > > clientRequests; // clientid -> [participant shards]

    unsigned int seed = time(NULL);
    Notice("Seed is %u", seed);
    srand(seed);

    // Epoch change when one of the client has sent 5 packets
    SetupClientCalls(clientRequests, upcalls, numUpcalls, NUM_PACKETS, -1, 0, 5);

    transport->Timer(2500, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    CheckConsistency(clientRequests, servers, config);
    EXPECT_EQ(nClients * NUM_PACKETS, numUpcalls);
}

TEST_F(ErisTest, EpochChangeWithDrops)
{
    const int NUM_PACKETS = 10;
    int numUpcalls = 0;
    vector<Client::g_continuation_t> upcalls;
    map<int, vector<set<shardnum_t> > > clientRequests; // clientid -> [participant shards]

    unsigned int seed = time(NULL);
    Notice("Seed is %u", seed);
    srand(seed);

    // Drop a few txns before epoch change
    eris::proto::RequestMessage requestMessage;
    set<pair<uint64_t, uint64_t> > request_counter;
    set<uint64_t> drops = {12, 15};
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay,
                                const multistamp_t &stamp) {
        if (m.GetTypeName() == requestMessage.GetTypeName()) {
            requestMessage.CopyFrom(m);
            request_counter.insert(make_pair(requestMessage.request().clientid(),
                                             requestMessage.request().clientreqid()));
            if (drops.find(request_counter.size()) != drops.end()) {
                return false;
            }
        }
        return true;
    });

    // Epoch change when one of the client has sent 5 packets
    SetupClientCalls(clientRequests, upcalls, numUpcalls, NUM_PACKETS, -1, 0, 5);

    transport->Timer(2500, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    CheckConsistency(clientRequests, servers, config);
    EXPECT_EQ(nClients * NUM_PACKETS, numUpcalls);
}

TEST_F(ErisTest, EpochChangeWithStateTransfer)
{
    const int NUM_PACKETS = 10;
    int numUpcalls = 0;
    vector<Client::g_continuation_t> upcalls;
    map<int, vector<set<shardnum_t> > > clientRequests; // clientid -> [participant shards]

    unsigned int seed = time(NULL);
    Notice("Seed is %u", seed);
    srand(seed);

    // In the first epoch, drop transactions after the 10th transaction
    // at shard 0. During epoch change, shard 0's leader will transfer
    // the missing transactions from other shards.
    // For shard 0, also drop the 9th and the 10th transactions at
    // non-leader replicas, and they will state transfer from
    // the leader after epoch change.
    eris::proto::RequestMessage requestMessage;
    GapRequestMessage gapRequestMessage;
    transport->AddFilter(1, [&](TransportReceiver *src, std::pair<int, int> srcIdx,
                                TransportReceiver *dst, std::pair<int, int> dstIdx,
                                Message &m, uint64_t &delay,
                                const multistamp_t &stamp) {
        if (m.GetTypeName() == gapRequestMessage.GetTypeName()) {
            // Drop all GapRequestMessages, force state transfer after
            // epoch change
            return false;
        }
        if (m.GetTypeName() == requestMessage.GetTypeName()) {
            if (stamp.sessnum > 0) {
                return true;
            }
            if (dstIdx.first == 0 &&
                stamp.seqnums.find(0) != stamp.seqnums.end()) {
                if (dstIdx.second == 0) {
                    // leader
                    if (stamp.seqnums.at(0) > 10) {
                        return false;
                    }
                } else {
                    // non-leader
                    if (stamp.seqnums.at(0) > 8) {
                        return false;
                    }
                }
            }
        }
        return true;
    });

    // Epoch change when client 0 has sent 5 packets.
    // Make 3 clients send transactions to shard 0. The
    // other 2 clients never send transactions to shard 0
    // (so that can make progress with packet drops).
    bool epoch_changed = false;
    for (int i = 0; i < nClients; i++) {
        upcalls.push_back([&, i, NUM_PACKETS](const map<shardnum_t, string> & request, const map<shardnum_t, string> & reply, bool commit) {
            EXPECT_EQ(request.size(), clients[i].lastRequestShards.size());
            EXPECT_EQ(reply.size(), clients[i].lastRequestShards.size());
            numUpcalls++;

            for (auto const &kv : request) {
                EXPECT_EQ(clients[i].lastRequestShards.count(kv.first), 1);
                EXPECT_EQ(clients[i].LastRequestOp(), kv.second);
            }
            for (auto const &kv : reply) {
                EXPECT_EQ(clients[i].lastRequestShards.count(kv.first), 1);
                EXPECT_EQ("reply: " + clients[i].LastRequestOp(), kv.second);
            }
            if (clients[i].requestNum < NUM_PACKETS) {
                // Only change session once
                if (!epoch_changed && clients[i].requestNum == 5) {
                    this->transport->SessionChange();
                    epoch_changed = true;
                }
                set<shardnum_t> shards = GenerateShards(nShards);
                if (i < 3) {
                    shards.insert(0);
                } else {
                    shards.erase(0);
                    if (shards.empty()) {
                        shards.insert(1);
                    }
                }
                clients[i].SendNext(shards, upcalls[i]);
                clientRequests[i].push_back(shards);
            }
        });
        // First txn should include all shards, so that
        // all replicas can learn the client's address
        set<shardnum_t> shards;
        for (const auto &kv : servers) {
            shards.insert(kv.first);
        }
        clients[i].SendNext(shards, upcalls[i]);
        clientRequests[i].push_back(shards);
    }

    transport->Timer(2500, [&]() {
        transport->CancelAllTimers();
    });

    transport->Run();

    CheckConsistency(clientRequests, servers, config);
    EXPECT_EQ(nClients * NUM_PACKETS, numUpcalls);
}
