#include <gtest/gtest.h>
#include "store/apps/kvstore/txnserver.h"

using namespace specpaxos::store;
using namespace specpaxos::store::kvstore;

class KVTxnTest : public ::testing::Test
{
protected:
    virtual void SetUp() override
    {
        KVStoreTxnServerArg arg;
        arg.keyPath = nullptr;
        arg.retryLock = false;
        server = new KVTxnServer(arg);
    }

    virtual void TearDown() override
    {
        delete server;
    }

    KVTxnServer *server;
};

/* Txn1: indep, Get k1
                Put k2 v2
   Txn2: indep, Get k2
*/
TEST_F(KVTxnTest, IndepTxnTest)
{
    proto::KVTxnMessage message;
    proto::KVTxnReplyMessage reply;
    std::string request, result;
    txnarg_t arg;
    txnret_t ret;
    arg.txnid = 1;
    arg.type = TXN_INDEP;
    proto::GetMessage *getm = message.add_gets();
    getm->set_key("k1");
    proto::PutMessage *putm = message.add_puts();
    putm->set_key("k2");
    putm->set_value("v2");
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);

    EXPECT_EQ(ret.txnid, 1);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);

    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 1);
    proto::GetReply greply = reply.rgets(0);
    EXPECT_EQ(greply.key(), "k1");
    EXPECT_EQ(greply.value(), "");

    message.Clear();
    arg.txnid = 2;
    arg.type = TXN_INDEP;
    getm = message.add_gets();
    getm->set_key("k2");
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);

    EXPECT_EQ(ret.txnid, 2);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);

    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 1);
    greply = reply.rgets(0);
    EXPECT_EQ(greply.key(), "k2");
    EXPECT_EQ(greply.value(), "v2");
}

/*
    Txn1: recon, Put k1 v1
                 Put k2 v2
                 Put k3 v3
    Txn2: indep, Get k1
    Txn3: indep, Get k2
                 Get k3
    Txn1: commit
*/

TEST_F(KVTxnTest, BlockedIndepTest)
{
    proto::KVTxnMessage message;
    proto::KVTxnReplyMessage reply;
    proto::PutMessage *putm;
    proto::GetMessage *getm;
    proto::GetReply greply;
    std::string request, result;
    txnarg_t arg;
    txnret_t ret;

    arg.txnid = 1;
    arg.type = TXN_PREPARE;
    putm  = message.add_puts();
    putm->set_key("k1");
    putm->set_value("v1");
    putm = message.add_puts();
    putm->set_key("k2");
    putm->set_value("v2");
    putm = message.add_puts();
    putm->set_key("k3");
    putm->set_value("v3");
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 1);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 0);

    message.Clear();
    result.clear();
    arg.txnid = 2;
    arg.type = TXN_INDEP;
    getm = message.add_gets();
    getm->set_key("k1");
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 2);
    EXPECT_TRUE(ret.blocked);
    EXPECT_FALSE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);
    EXPECT_EQ(result.size(), 0);

    message.Clear();
    result.clear();
    arg.txnid = 3;
    arg.type = TXN_INDEP;
    getm = message.add_gets();
    getm->set_key("k2");
    getm = message.add_gets();
    getm->set_key("k3");
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 3);
    EXPECT_TRUE(ret.blocked);
    EXPECT_FALSE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);
    EXPECT_EQ(result.size(), 0);

    message.Clear();
    result.clear();
    arg.txnid = 1;
    arg.type = TXN_COMMIT;
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 1);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 0);
    EXPECT_EQ(ret.unblocked_txns.size(), 2);
    EXPECT_EQ(ret.unblocked_txns.count(2), 1);
    EXPECT_EQ(ret.unblocked_txns.count(3), 1);
    result.clear();
    arg.txnid = 2;
    arg.type = TXN_INDEP;
    server->InvokeTransaction("", result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 2);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 1);
    greply = reply.rgets(0);
    EXPECT_EQ(greply.key(), "k1");
    EXPECT_EQ(greply.value(), "v1");
    result.clear();
    arg.txnid = 3;
    arg.type = TXN_INDEP;
    server->InvokeTransaction("", result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 3);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 2);
    std::map<std::string, std::string> results;
    greply = reply.rgets(0);
    results[greply.key()] = greply.value();
    greply = reply.rgets(1);
    results[greply.key()] = greply.value();
    EXPECT_EQ(results.size(), 2);
    EXPECT_EQ(results.at("k2"), "v2");
    EXPECT_EQ(results.at("k3"), "v3");
}

/*
    Txn1: recon, Put k1 v1
                 Put k2 v2
                 Put k3 v3
    Txn2: indep, Get k1
    Txn1: commit
    Txn3: indep, Get k2
                 Get k3
*/

TEST_F(KVTxnTest, SlowFastPathTest)
{
    proto::KVTxnMessage message;
    proto::KVTxnReplyMessage reply;
    proto::PutMessage *putm;
    proto::GetMessage *getm;
    proto::GetReply greply;
    std::string request, result;
    txnarg_t arg;
    txnret_t ret;

    arg.txnid = 1;
    arg.type = TXN_PREPARE;
    putm = message.add_puts();
    putm->set_key("k1");
    putm->set_value("v1");
    putm = message.add_puts();
    putm->set_key("k2");
    putm->set_value("v2");
    putm = message.add_puts();
    putm->set_key("k3");
    putm->set_value("v3");
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 1);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 0);

    message.Clear();
    result.clear();
    arg.txnid = 2;
    arg.type = TXN_INDEP;
    getm = message.add_gets();
    getm->set_key("k1");
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 2);
    EXPECT_TRUE(ret.blocked);
    EXPECT_FALSE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);
    EXPECT_EQ(result.size(), 0);

    message.Clear();
    result.clear();
    arg.txnid = 1;
    arg.type = TXN_COMMIT;
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 1);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 0);
    EXPECT_EQ(ret.unblocked_txns.size(), 1);
    EXPECT_EQ(ret.unblocked_txns.count(2), 1);
    result.clear();
    arg.txnid = 2;
    arg.type = TXN_INDEP;
    server->InvokeTransaction("", result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 2);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 1);
    greply = reply.rgets(0);
    EXPECT_EQ(greply.key(), "k1");
    EXPECT_EQ(greply.value(), "v1");

    message.Clear();
    result.clear();
    ret.unblocked_txns.clear();
    arg.txnid = 3;
    arg.type = TXN_INDEP;
    getm = message.add_gets();
    getm->set_key("k2");
    getm = message.add_gets();
    getm->set_key("k3");
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 3);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 2);
    std::map<std::string, std::string> results;
    greply = reply.rgets(0);
    results[greply.key()] = greply.value();
    greply = reply.rgets(1);
    results[greply.key()] = greply.value();
    EXPECT_EQ(results.size(), 2);
    EXPECT_EQ(results.at("k2"), "v2");
    EXPECT_EQ(results.at("k3"), "v3");
}

/*
    Txn1: recon, Put k1 v1
                 Put k2 v2
                 Put k3 v3
    Txn2: indep, Get k1
                 Put k2 V2
    Txn3: recon, Get k2
                 Put k3 V3
    Txn4: indep, Get k3
    Txn5: indep, Get k1
                 Get k2
    Txn1: commit
    Txn3: abort
*/
TEST_F(KVTxnTest, MultiTxnTest)
{
    proto::KVTxnMessage message;
    proto::KVTxnReplyMessage reply;
    proto::PutMessage *putm;
    proto::GetMessage *getm;
    proto::GetReply greply;
    std::string request, result;
    txnarg_t arg;
    txnret_t ret;

    // Txn1 recon
    arg.txnid = 1;
    arg.type = TXN_PREPARE;
    putm  = message.add_puts();
    putm->set_key("k1");
    putm->set_value("v1");
    putm = message.add_puts();
    putm->set_key("k2");
    putm->set_value("v2");
    putm = message.add_puts();
    putm->set_key("k3");
    putm->set_value("v3");
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 1);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 0);

    // Txn2
    message.Clear();
    result.clear();
    arg.txnid = 2;
    arg.type = TXN_INDEP;
    getm = message.add_gets();
    getm->set_key("k1");
    putm = message.add_puts();
    putm->set_key("k2");
    putm->set_value("V2");
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 2);
    EXPECT_TRUE(ret.blocked);
    EXPECT_FALSE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);
    EXPECT_EQ(result.size(), 0);

    // Txn3
    message.Clear();
    result.clear();
    arg.txnid = 3;
    arg.type = TXN_PREPARE;
    getm = message.add_gets();
    getm->set_key("k2");
    putm = message.add_puts();
    putm->set_key("k3");
    putm->set_value("V3");
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 3);
    EXPECT_TRUE(ret.blocked);
    EXPECT_FALSE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);
    EXPECT_EQ(result.size(), 0);

    // Txn4
    message.Clear();
    result.clear();
    arg.txnid = 4;
    arg.type = TXN_INDEP;
    getm = message.add_gets();
    getm->set_key("k3");
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 4);
    EXPECT_TRUE(ret.blocked);
    EXPECT_FALSE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);
    EXPECT_EQ(result.size(), 0);

    // Txn5
    message.Clear();
    result.clear();
    arg.txnid = 5;
    arg.type = TXN_INDEP;
    getm = message.add_gets();
    getm->set_key("k1");
    getm = message.add_gets();
    getm->set_key("k2");
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 5);
    EXPECT_TRUE(ret.blocked);
    EXPECT_FALSE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);
    EXPECT_EQ(result.size(), 0);

    // Txn1 commit
    message.Clear();
    result.clear();
    arg.txnid = 1;
    arg.type = TXN_COMMIT;
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 1);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 0);
    EXPECT_EQ(ret.unblocked_txns.size(), 1);
    EXPECT_EQ(ret.unblocked_txns.count(2), 1);
    result.clear();
    arg.txnid = 2;
    arg.type = TXN_INDEP;
    ret = txnret_t();
    server->InvokeTransaction("", result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 2);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 1);
    greply = reply.rgets(0);
    EXPECT_EQ(greply.key(), "k1");
    EXPECT_EQ(greply.value(), "v1");
    EXPECT_EQ(ret.unblocked_txns.size(), 2);
    EXPECT_EQ(ret.unblocked_txns.count(3), 1);
    EXPECT_EQ(ret.unblocked_txns.count(5), 1);
    result.clear();
    arg.txnid = 3;
    arg.type = TXN_PREPARE;
    ret = txnret_t();
    server->InvokeTransaction("", result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 3);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 1);
    greply = reply.rgets(0);
    EXPECT_EQ(greply.key(), "k2");
    EXPECT_EQ(greply.value(), "V2");
    EXPECT_EQ(ret.unblocked_txns.size(), 0);
    result.clear();
    arg.txnid = 5;
    arg.type = TXN_INDEP;
    ret = txnret_t();
    server->InvokeTransaction("", result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 5);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 2);
    std::map<std::string, std::string> results;
    greply = reply.rgets(0);
    results[greply.key()] = greply.value();
    greply = reply.rgets(1);
    results[greply.key()] = greply.value();
    EXPECT_EQ(results.size(), 2);
    EXPECT_EQ(results.at("k1"), "v1");
    EXPECT_EQ(results.at("k2"), "V2");
    EXPECT_EQ(ret.unblocked_txns.size(), 0);

    // Txn3 abort
    message.Clear();
    result.clear();
    ret = txnret_t();
    arg.txnid = 3;
    arg.type = TXN_ABORT;
    ret = txnret_t();
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 3);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 0);
    EXPECT_EQ(ret.unblocked_txns.size(), 1);
    EXPECT_EQ(ret.unblocked_txns.count(4), 1);
    result.clear();
    arg.txnid = 4;
    arg.type = TXN_INDEP;
    ret = txnret_t();
    server->InvokeTransaction("", result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 4);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 1);
    greply = reply.rgets(0);
    EXPECT_EQ(greply.key(), "k3");
    EXPECT_EQ(greply.value(), "v3");
    EXPECT_EQ(ret.unblocked_txns.size(), 0);
}

/*
    Txn1: recon, Get k1-k4
                 Put k1-k4
    Txn2: recon, Get k1-k4
                 Put k1-k4
    Txn3: recon, Get k1-k4
                 Put k1-k4
    Txn1: commit
    Txn2: commit
    Txn3: commit
*/
TEST_F(KVTxnTest, RWSameTxnTest)
{
    proto::KVTxnMessage message;
    proto::KVTxnReplyMessage reply;
    proto::PutMessage *putm;
    proto::GetMessage *getm;
    proto::GetReply greply;
    std::string request, result;
    txnarg_t arg;
    txnret_t ret;

    // Txn1 recon
    arg.txnid = 1;
    arg.type = TXN_PREPARE;
    getm = message.add_gets();
    getm->set_key("k1");
    getm = message.add_gets();
    getm->set_key("k2");
    getm = message.add_gets();
    getm->set_key("k3");
    getm = message.add_gets();
    getm->set_key("k4");
    putm = message.add_puts();
    putm->set_key("k1");
    putm->set_value("v1");
    putm = message.add_puts();
    putm->set_key("k2");
    putm->set_value("v2");
    putm = message.add_puts();
    putm->set_key("k3");
    putm->set_value("v3");
    putm = message.add_puts();
    putm->set_key("k4");
    putm->set_value("v4");

    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 1);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 4);
    std::map<std::string, std::string> results;
    for (int i = 0; i < 4; i++ ) {
        results[reply.rgets(i).key()] = reply.rgets(i).value();
    }
    EXPECT_EQ(results.at("k1"), "");
    EXPECT_EQ(results.at("k2"), "");
    EXPECT_EQ(results.at("k3"), "");
    EXPECT_EQ(results.at("k4"), "");

    // Txn2 recon
    message.Clear();
    result.clear();
    ret.unblocked_txns.clear();
    arg.txnid= 2;
    arg.type = TXN_PREPARE;
    getm = message.add_gets();
    getm->set_key("k1");
    getm = message.add_gets();
    getm->set_key("k2");
    getm = message.add_gets();
    getm->set_key("k3");
    getm = message.add_gets();
    getm->set_key("k4");
    putm = message.add_puts();
    putm->set_key("k1");
    putm->set_value("v1");
    putm = message.add_puts();
    putm->set_key("k2");
    putm->set_value("v2");
    putm = message.add_puts();
    putm->set_key("k3");
    putm->set_value("v3");
    putm = message.add_puts();
    putm->set_key("k4");
    putm->set_value("v4");

    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 2);
    EXPECT_TRUE(ret.blocked);
    EXPECT_FALSE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);

    // Txn3 recon
    message.Clear();
    result.clear();
    ret.unblocked_txns.clear();
    arg.txnid = 3;
    arg.type = TXN_PREPARE;
    getm = message.add_gets();
    getm->set_key("k1");
    getm = message.add_gets();
    getm->set_key("k2");
    getm = message.add_gets();
    getm->set_key("k3");
    getm = message.add_gets();
    getm->set_key("k4");
    putm = message.add_puts();
    putm->set_key("k1");
    putm->set_value("v1");
    putm = message.add_puts();
    putm->set_key("k2");
    putm->set_value("v2");
    putm = message.add_puts();
    putm->set_key("k3");
    putm->set_value("v3");
    putm = message.add_puts();
    putm->set_key("k4");
    putm->set_value("v4");

    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 3);
    EXPECT_TRUE(ret.blocked);
    EXPECT_FALSE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);

    // Txn1 commit
    message.Clear();
    result.clear();
    ret.unblocked_txns.clear();
    arg.txnid = 1;
    arg.type = TXN_COMMIT;
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 1);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 0);
    EXPECT_EQ(ret.unblocked_txns.size(), 1);
    EXPECT_EQ(ret.unblocked_txns.count(2), 1);
    result.clear();
    arg.txnid = 2;
    arg.type = TXN_PREPARE;
    server->InvokeTransaction("", result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 2);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 4);
    results.clear();
    for (int i = 0; i < 4; i++ ) {
        results[reply.rgets(i).key()] = reply.rgets(i).value();
    }
    EXPECT_EQ(results.at("k1"), "v1");
    EXPECT_EQ(results.at("k2"), "v2");
    EXPECT_EQ(results.at("k3"), "v3");
    EXPECT_EQ(results.at("k4"), "v4");

    // Txn2 commit
    message.Clear();
    result.clear();
    ret.unblocked_txns.clear();
    arg.txnid = 2;
    arg.type = TXN_COMMIT;
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 2);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 1);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 0);
    EXPECT_EQ(ret.unblocked_txns.count(3), 1);
    result.clear();
    arg.txnid = 3;
    arg.type = TXN_PREPARE;
    server->InvokeTransaction("", result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 3);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 4);
    results.clear();
    for (int i = 0; i < 4; i++ ) {
        results[reply.rgets(i).key()] = reply.rgets(i).value();
    }
    EXPECT_EQ(results.at("k1"), "v1");
    EXPECT_EQ(results.at("k2"), "v2");
    EXPECT_EQ(results.at("k3"), "v3");
    EXPECT_EQ(results.at("k4"), "v4");

    // Txn3 commit
    message.Clear();
    result.clear();
    ret.unblocked_txns.clear();
    arg.txnid = 3;
    arg.type = TXN_COMMIT;
    message.SerializeToString(&request);
    server->InvokeTransaction(request, result, &arg, &ret);
    EXPECT_EQ(ret.txnid, 3);
    EXPECT_FALSE(ret.blocked);
    EXPECT_TRUE(ret.commit);
    EXPECT_EQ(ret.unblocked_txns.size(), 0);
    reply.ParseFromString(result);
    EXPECT_EQ(reply.rgets_size(), 0);
}
