// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/eris/server.h:
 *   Eris protocol server implementation.
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


#ifndef __ERIS_SERVER_H__
#define __ERIS_SERVER_H__

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/configuration.h"
#include "common/log.h"
#include "common/replica.h"
#include "common/quorumset.h"
#include "vr/client.h"
#include "store/eris/eris-proto.pb.h"

#include <map>
#include <set>
#include <memory>

namespace specpaxos {
namespace store {
namespace eris {

class ErisServer : public Replica
{
public:
    ErisServer(const Configuration &config, int myShard, int myIdx,
               bool initialize, Transport *transport, AppReplica *app,
               const Configuration &fcorConfig);
    ~ErisServer();

    void ReceiveMessage(const TransportAddress &remote,
			const string &type, const string &data,
                        void *meta_data) override;

public:
    /* Log entry additional data */
    struct EntryData {
        txnid_t txnid;
        proto::RequestType type;

        EntryData() : txnid(0), type(proto::UNKNOWN) { }
        EntryData(txnid_t txnid, proto::RequestType type)
            : txnid(txnid), type(type) { }
    };
    Log<EntryData> log;

private:
    /* Replica states */
    opnum_t lastOp;
    view_t view;
    sessnum_t sessnum;
    msgnum_t nextMsgnum;

    sessnum_t lastNormalSessnum;
    opnum_t lastCommittedOp;
    opnum_t lastExecutedOp;

    struct MsgStamp {
        MsgStamp(int shard_num, int msg_num, int sess_num) :
            shard_num(shard_num), msg_num(msg_num), sess_num(sess_num) {}
        MsgStamp(const proto::Stamp &stamp) :
            shard_num(stamp.shard_num()), msg_num(stamp.msg_num()), sess_num(stamp.sess_num()) {}

        shardnum_t shard_num;
        msgnum_t msg_num;
        sessnum_t sess_num;

        bool operator==(const MsgStamp &o) const {
            return shard_num == o.shard_num &&
                msg_num == o.msg_num &&
                sess_num == o.sess_num;
        }

        bool operator<(const MsgStamp &o) const {
            return sess_num < o.sess_num ||
                (sess_num == o.sess_num && msg_num < o.msg_num) ||
                (sess_num == o.sess_num && msg_num == o.msg_num && shard_num < o.shard_num);
        }
    };

    std::map<MsgStamp, opnum_t> txnLookup;
    std::map<shardnum_t, msgnum_t> shardToMsgnum;

    /* Client information */
    std::map<uint64_t, std::unique_ptr<TransportAddress> > clientAddresses;
    struct ClientTableEntry
    {
	uint64_t lastReqId;
        bool replied;
	proto::ReplyMessage reply;
    };
    std::map<uint64_t, ClientTableEntry> clientTable;

    /* Pending requests */
    struct PendingRequest
    {
        sessnum_t sessnum;
        msgnum_t msgnum;
        mutable proto::RequestMessage requestMessage;
    };
    struct __pendreq_key_compare {
        bool operator() (const struct PendingRequest &lhs,
                         const struct PendingRequest &rhs) const
        {
            if (lhs.sessnum < rhs.sessnum) {
                return true;
            }
            return lhs.msgnum < rhs.msgnum;
        }
    };
    std::set<struct PendingRequest, __pendreq_key_compare> pendingRequests;

    /* Failure coordinator */
    specpaxos::vr::VRClient *fcorClient;

    /* Blocked operations due to locking */
    std::unordered_map<txnid_t, opnum_t> blockedTxns;
    std::set<opnum_t> unblockedExecutionQueue; // Execute unblocked transactions in log order (std::set is ordered).

    /* Quorums */
    QuorumSet<opnum_t, proto::GapReplyMessage> gapReplyQuorum;
    QuorumSet<view_t, proto::ViewChangeMessage> viewChangeQuorum;

    /* Gaps */
    std::set<MsgStamp> tempDrops;
    std::set<MsgStamp> permDrops;
    std::set<MsgStamp> unDrops;
    std::map<MsgStamp, proto::RequestMessage> unDropMsgs;

    /* Pending FC queries in view change */
    struct FCQueries {
        std::set<MsgStamp> stamps;
        std::function<void (void)> callback;
    };
    FCQueries pendingFCQueries;

    /* Pending state transfer */
    struct StateTransfer {
        opnum_t begin;
        opnum_t end;
        uint32_t replica_idx;
        std::function<void (void)> callback;
    };
    StateTransfer pendingStateTransfer;

    /* Pending epoch change state transfer */
    struct ECStateTransfer {
        std::map<shardnum_t, std::pair<uint32_t /*replica_idx*/, proto::EpochChangeStateTransferRequest> > requests;
        std::set<MsgStamp> drops;
        std::function<void (void)> callback;
    };
    ECStateTransfer pendingECStateTransfer;

    /* Timeouts */
    Timeout * startGapRequestTimeout;
    const int START_GAP_REQUEST_TIMEOUT = 10;
    Timeout * gapRequestTimeout;
    const int GAP_REQUEST_TIMEOUT = 50;
    Timeout * fcTxnInfoRequestTimeout;
    const int FC_TXN_INFO_REQUEST_TIMEOUT = 50;
    Timeout * syncTimeout;
    const int SYNC_TIMEOUT = 1000;
    Timeout * leaderSyncHeardTimeout;
    const int LEADER_SYNC_HEARD_TIMEOUT = 2000;
    Timeout * viewChangeTimeout;
    const int VIEW_CHANGE_TIMEOUT = 100;
    Timeout * stateTransferTimeout;
    const int STATE_TRANSFER_TIMEOUT = 50;
    Timeout * fcQueriesTimeout;
    const int FC_QUERIES_TIMEOUT = 50;
    Timeout * epochChangeReqTimeout;
    const int EPOCH_CHANGE_REQ_TIMEOUT = 50;
    Timeout * epochChangeAckTimeout;
    const int EPOCH_CHANGE_ACK_TIMEOUT = 50;
    Timeout * ecStateTransferTimeout;
    const int EC_STATE_TRANSFER_TIMEOUT = 50;
    Timeout * ecStateTransferAckTimeout;
    const int EC_STATE_TRANSFER_ACK_TIMEOUT = 50;

    /* Message handlers */
    void HandleClientRequest(const TransportAddress &remote,
			     proto::RequestMessage &msg,
                             const multistamp_t &stamp);
    void HandleGapRequest(const TransportAddress &remote,
                          const proto::GapRequestMessage &msg);
    void HandleGapReply(const TransportAddress &remote,
                        const proto::GapReplyMessage &msg);
    void HandleFCToErisMessage(const TransportAddress &remote,
                               const proto::FCToErisMessage &msg);
    void HandleSyncPrepare(const TransportAddress &remote,
                           const proto::SyncPrepareMessage &msg);
    void HandleViewChangeRequest(const TransportAddress &remote,
                                 const proto::ViewChangeRequestMessage &msg);
    void HandleViewChange(const TransportAddress &remote,
                          const proto::ViewChangeMessage &msg);
    void HandleStartView(const TransportAddress &remote,
                         const proto::StartViewMessage &msg);
    void HandleStateTransferRequest(const TransportAddress &remote,
                                    const proto::StateTransferRequestMessage &msg);
    void HandleStateTransferReply(const TransportAddress &remote,
                                  const proto::StateTransferReplyMessage &msg);
    void HandleEpochChangeStateTransferRequest(const TransportAddress &remote,
                                               const proto::EpochChangeStateTransferRequest &msg);
    void HandleEpochChangeStateTransferReply(const TransportAddress &remote,
                                             const proto::EpochChangeStateTransferReply &msg);

    /* Returns true if the request is processed/ignored.
     * False if the request should be processed later (pending).
     */
    bool TryProcessClientRequest(const proto::RequestMessage &msg,
                                 sessnum_t sessnum, msgnum_t msgnum);
    void ProcessNextOperation(const proto::RequestMessage &msg,
                              const viewstamp_t &vs,
                              const LogEntryState &state);
    void UpdateClientTable(const specpaxos::Request &req);
    void AddPendingRequest(const proto::RequestMessage &msg,
                           sessnum_t sessnum, msgnum_t msgnum);
    void ProcessPendingRequests();
    void ExecuteUptoOp(opnum_t opnum);
    void ExecuteTxn(opnum_t opnum);
    void ExecuteUnblockedTxns();

    void QueryFCForLastTxn();
    void StartViewChange(view_t new_view);
    void EnterView(view_t new_view);
    void DetectedEpochChange(sessnum_t new_sessnum);
    void StartEpochChange(sessnum_t new_sessnum);
    void EnterEpoch(sessnum_t new_sessnum);

    void SendGapRequest();
    void SendFCTxnInfoRequest(const proto::Stamp &stamp);
    void SendSyncPrepare();
    void SendViewChange();
    void SendStateTransferRequest();
    void SendFCQueries();
    void SendEpochChangeReq();
    void SendEpochChangeAck();
    void SendEpochChangeStateTransfer();
    void SendEpochChangeStateTransferAck();

    inline bool AmLeader() const;
    inline bool CheckViewNumAndStatus(proto::ViewNum view_num);
    void InitShardMsgNum();
    void InstallLogEntry(const viewstamp_t &vs, const LogEntryState &state, const proto::RequestMessage &msg);
    void ClearTimeoutAndQuorums();
    void ClearEpochData();
    void InstallTempDrop(const MsgStamp &stamp);
    void InstallUnDrop(const proto::RequestMessage &un_drop);
    void InstallPermDrop(const MsgStamp &stamp);
    void BuildDropTxns(proto::DropTxns &drop_txns) const;
    void MergeDropTxns(const proto::DropTxns &drop_txns);
    void CheckTempDrops() const;
    bool MatchLogWithTempDrops();
    void BuildFCMessage(proto::ErisToFCMessage &msg) const;
    void CompleteFCQuery(const MsgStamp &stamp);
    void CompleteECStateTransfer();
    void RewindLog(opnum_t opnum);
};

typedef Log<struct ErisServer::EntryData>::LogEntry LogEntry;

} // namespace eris
} // namespace store
} // namespace specpaxos

#endif /* __ERIS_SERVER_H__ */
