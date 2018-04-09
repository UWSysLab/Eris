// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/eris/fcor.h:
 *   Eris failure coordinator server implementation.
 *
 * Copyright 2017 Ellis Michael <emichael@cs.washington.edu>
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


#ifndef __ERIS_FCOR_H__
#define __ERIS_FCOR_H__

#include "common/replica.h"
#include "lib/configuration.h"
#include "lib/configuration.h"
#include "store/eris/eris-proto.pb.h"

#include <map>

namespace specpaxos {
namespace store {
namespace eris {

class TransportSender : public TransportReceiver
{
public:
    TransportSender() { }
    ~TransportSender() { }
    void ReceiveMessage(const TransportAddress &remote,
                        const string &type,
                        const string &data,
                        void *meta_data) override
    {
        // Sender ignores all incoming messages
    }
};

struct TxnID {
    TxnID(int shardNum, int msgNum, int epochNum) :
        shardNum(shardNum), msgNum(msgNum), epochNum(epochNum) {}

    TxnID(const proto::Stamp &stn) :
        shardNum(stn.shard_num()), msgNum(stn.msg_num()), epochNum(stn.sess_num()) {}

    int shardNum;
    uint msgNum;
    uint epochNum;

    bool operator==(const TxnID &o) const {
        return shardNum == o.shardNum &&
               msgNum == o.msgNum &&
               epochNum == o.epochNum;
    }

    bool operator<(const TxnID &o) const {
        return epochNum < o.epochNum ||
               (epochNum == o.epochNum && msgNum < o.msgNum) ||
               (epochNum == o.epochNum && msgNum == o.msgNum && shardNum < o.shardNum);
    }
};

enum TxnFate { DROPPED, FOUND };

struct TxnResult {
    TxnResult(TxnFate fate, proto::RequestMessage txn) :
        fate(fate), txn(txn) {}

    TxnFate fate;
    proto::RequestMessage txn; // TODO: maybe just store a pointer here?
};

struct TxnDropRecord {
    TxnDropRecord(int shardNum, int replicaNum, int viewNum) :
        shardNum(shardNum), replicaNum(replicaNum), viewNum(viewNum) {}

    int shardNum;
    int replicaNum;
    int viewNum;

    bool operator==(const TxnDropRecord &o) const {
        return shardNum == o.shardNum &&
               replicaNum == o.replicaNum &&
               viewNum == o.viewNum;
    }

    bool operator<(const TxnDropRecord &o) const {
        return shardNum < o.shardNum ||
               (shardNum == o.shardNum && viewNum < o.viewNum) ||
               (shardNum == o.shardNum && viewNum == o.viewNum && replicaNum < o.replicaNum);
    }
};

enum FcorStatus {
    STATUS_NORMAL,
    STATUS_EPOCH_CHANGE,
    STATUS_PENDING_STATE_TRANSFER
};

class Fcor : public AppReplica {
public:
    Fcor(const Configuration &config,
         Transport *transport);
    ~Fcor();

    void ReplicaUpcall(opnum_t opnum, const string &request, string &reply,
                       void *arg = nullptr, void *ret = nullptr) override;

private:
    Configuration config;
    Transport *transport;
    TransportSender *sender;

    // State
    std::map<TxnID, TxnResult> txnResults;
    std::map<TxnID, std::map<int /* shard num */, std::set<TxnDropRecord> >> dropRecords;
    std::map<int /* shard num */, std::set<TxnID> /* msg num */> droppedTxns;

    uint epochNum;
    uint lastNormalEpoch;
    FcorStatus status;
    std::map<int /* shard num */, std::set<int /* replica num */>> epochChangeAcks;
    struct LatestMsgRecord {
        LatestMsgRecord() :
            replica_num(0), op_num(0) {}
        LatestMsgRecord(const proto::ErisToFCMessage &msg) {
            replica_num = msg.replica_num();
            op_num = msg.epoch_change_ack().op_num();
            for (auto it = msg.epoch_change_ack().log_info().begin();
                 it != msg.epoch_change_ack().log_info().end();
                 ++it) {
                msg_nums[it->shard_num()] = it->latest_msg_num();
            }
        }
        uint32_t replica_num;
        uint64_t op_num;
        std::map<uint32_t /* shard num */, uint64_t /* msg num */> msg_nums;
    };
    std::map<int /* shard num */, LatestMsgRecord> latestMsgNums;
    std::map<int /* shard num */, int /* view num */> latestViewNums;

    std::map<std::pair<int /* shard num */, int /* replica num */>, proto::FCToErisMessage> pendingStateTransfers;

    std::map<int /* shard num */, proto::StartEpoch> lastStartEpochs;

    // Message handlers
    void HandleTxnInfoReq(const proto::TxnInfoRequest &m);
    void HandleTxnReceived(const proto::TxnReceived &m);
    void HandleTxnTempDropped(const proto::TxnTempDropped &m, int shardNum,
        int replicaNum, int viewNum);

    void HandleEpochChangeReq(const proto::ErisToFCMessage &m);
    void HandleEpochChangeAck(const proto::ErisToFCMessage &m);
    void HandleEpochChangeStateTransferAck(const proto::ErisToFCMessage &m);

    // Helpers
    bool IsDropped(TxnID &id);
    void AddDropRecord(TxnID &id, TxnDropRecord &newPromise);
    void SendResult(TxnID &id);
    void StartEpoch();
};

} // namespace eris
} // namespace store
} // namespace specpaxos

#endif /* __ERIS_FCOR_H__ */
