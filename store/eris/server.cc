// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/eris/server.cc:
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

#include "store/eris/server.h"

#define RDebug(fmt, ...) Debug("[%d %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)

namespace specpaxos {
namespace store {
namespace eris {

using namespace proto;
using namespace std;

ErisServer::ErisServer(const Configuration &config, int myShard, int myIdx,
                       bool initialize, Transport *transport, AppReplica *app,
                       const Configuration &fcorConfig)
    : Replica(config, myShard, myIdx, initialize, transport, app),
    log(false),
    gapReplyQuorum(config.n - 1),
    viewChangeQuorum(config.n - 1)
{
    this->status = STATUS_NORMAL;
    this->sessnum = 0;
    this->view = 0;
    this->lastOp = 0;
    this->nextMsgnum = 1;

    this->lastNormalSessnum = 0;
    this->lastCommittedOp = 0;
    this->lastExecutedOp = 0;

    this->fcorClient = new specpaxos::vr::VRClient(fcorConfig, transport);
    InitShardMsgNum();

    this->gapRequestTimeout = new Timeout(transport,
                                          GAP_REQUEST_TIMEOUT,
                                          [this, myShard, myIdx]() {
                                              RDebug("Gap request timed out!");
                                              if (AmLeader()) {
                                                  // Leader on a timeout contacts
                                                  // the failure coordinator
                                                  QueryFCForLastTxn();
                                              } else {
                                                  // Non-leader replica resends
                                                  // request to the leader
                                                  SendGapRequest();
                                              }
                                          });

    this->startGapRequestTimeout = new Timeout(transport,
                                               START_GAP_REQUEST_TIMEOUT,
                                               [this, myShard, myIdx]() {
                                                   RDebug("Start gap request timeout! op %lu", this->lastOp+1);
                                                   this->startGapRequestTimeout->Stop();
                                                   SendGapRequest();
                                               });

    this->fcTxnInfoRequestTimeout = new Timeout(transport,
                                                FC_TXN_INFO_REQUEST_TIMEOUT,
                                                [this, myShard, myIdx]() {
                                                    RWarning("FC request timed out! Resending fc request");
                                                    QueryFCForLastTxn();
                                                });

    this->syncTimeout = new Timeout(transport,
                                    SYNC_TIMEOUT,
                                    [this, myShard, myIdx]() {
                                        RDebug("Start Synchronization");
                                        SendSyncPrepare();
                                    });
    this->leaderSyncHeardTimeout = new Timeout(transport,
                                               LEADER_SYNC_HEARD_TIMEOUT,
                                               [this, myShard, myIdx]() {
                                                   RWarning("Haven't received consecutive SyncPrepares from the leader, start view change");
                                                   StartViewChange(this->view+1);
                                               });
    this->viewChangeTimeout = new Timeout(transport,
                                          VIEW_CHANGE_TIMEOUT,
                                          [this, myShard, myIdx]() {
                                              RWarning("View change timed out!");
                                              SendViewChange();
                                          });
    this->stateTransferTimeout = new Timeout(transport,
                                             STATE_TRANSFER_TIMEOUT,
                                             [this, myShard, myIdx]() {
                                                 RWarning("State transfer timed out!");
                                                 SendStateTransferRequest();
                                             });
    this->fcQueriesTimeout = new Timeout(transport,
                                         FC_QUERIES_TIMEOUT,
                                         [this, myShard, myIdx]() {
                                             RWarning("FC Query timed out!");
                                             SendFCQueries();
                                         });
    this->epochChangeReqTimeout = new Timeout(transport,
                                              EPOCH_CHANGE_REQ_TIMEOUT,
                                              [this, myShard, myIdx]() {
                                                  RWarning("EpochChangeReq timed out!");
                                                  SendEpochChangeReq();
                                              });
    this->epochChangeAckTimeout = new Timeout(transport,
                                              EPOCH_CHANGE_ACK_TIMEOUT,
                                              [this, myShard, myIdx]() {
                                                  RWarning("EpochChangeAck timed out!");
                                                  SendEpochChangeAck();
                                              });
    this->ecStateTransferTimeout = new Timeout(transport,
                                               EC_STATE_TRANSFER_TIMEOUT,
                                               [this, myShard, myIdx]() {
                                                   RWarning("EpochChangeStateTransferRequest timed out");
                                                   SendEpochChangeStateTransfer();
                                               });
    this->ecStateTransferAckTimeout = new Timeout(transport,
                                                  EC_STATE_TRANSFER_ACK_TIMEOUT,
                                                  [this, myShard, myIdx]() {
                                                      RWarning("EpochChangeStateTransferAck timed out");
                                                      SendEpochChangeStateTransferAck();
                                                  });

    if (AmLeader()) {
        this->syncTimeout->Start();
    } else {
        this->leaderSyncHeardTimeout->Start();
    }
}

ErisServer::~ErisServer()
{
    delete this->fcorClient;
    delete this->gapRequestTimeout;
    delete this->startGapRequestTimeout;
    delete this->fcTxnInfoRequestTimeout;
    delete this->syncTimeout;
    delete this->leaderSyncHeardTimeout;
    delete this->viewChangeTimeout;
    delete this->stateTransferTimeout;
    delete this->fcQueriesTimeout;
    delete this->epochChangeReqTimeout;
    delete this->epochChangeAckTimeout;
    delete this->ecStateTransferTimeout;
    delete this->ecStateTransferAckTimeout;
}

void
ErisServer::ReceiveMessage(const TransportAddress &remote,
                           const string &type, const string &data,
                           void *meta_data)
{
    static RequestMessage requestMessage;
    static GapRequestMessage gapRequestMessage;
    static GapReplyMessage gapReplyMessage;
    static FCToErisMessage fcToErisMessage;
    static SyncPrepareMessage syncPrepareMessage;
    static ViewChangeRequestMessage viewChangeRequestMessage;
    static ViewChangeMessage viewChangeMessage;
    static StartViewMessage startViewMessage;
    static StateTransferRequestMessage stateTransferRequestMessage;
    static StateTransferReplyMessage stateTransferReplyMessage;
    static EpochChangeStateTransferRequest epochChangeStateTransferRequest;
    static EpochChangeStateTransferReply epochChangeStateTransferReply;

    if (type == requestMessage.GetTypeName()) {
        requestMessage.ParseFromString(data);
        HandleClientRequest(remote, requestMessage, *(multistamp_t *)meta_data);
    } else if (type == gapRequestMessage.GetTypeName()) {
        gapRequestMessage.ParseFromString(data);
        HandleGapRequest(remote, gapRequestMessage);
    } else if (type == gapReplyMessage.GetTypeName()) {
        gapReplyMessage.ParseFromString(data);
        HandleGapReply(remote, gapReplyMessage);
    } else if (type == fcToErisMessage.GetTypeName()) {
        fcToErisMessage.ParseFromString(data);
        HandleFCToErisMessage(remote, fcToErisMessage);
    } else if (type == syncPrepareMessage.GetTypeName()) {
        syncPrepareMessage.ParseFromString(data);
        HandleSyncPrepare(remote, syncPrepareMessage);
    } else if (type == viewChangeRequestMessage.GetTypeName()) {
        viewChangeRequestMessage.ParseFromString(data);
        HandleViewChangeRequest(remote, viewChangeRequestMessage);
    } else if (type == viewChangeMessage.GetTypeName()) {
        viewChangeMessage.ParseFromString(data);
        HandleViewChange(remote, viewChangeMessage);
    } else if (type == startViewMessage.GetTypeName()) {
        startViewMessage.ParseFromString(data);
        HandleStartView(remote, startViewMessage);
    } else if (type == stateTransferRequestMessage.GetTypeName()) {
        stateTransferRequestMessage.ParseFromString(data);
        HandleStateTransferRequest(remote, stateTransferRequestMessage);
    } else if (type == stateTransferReplyMessage.GetTypeName()) {
        stateTransferReplyMessage.ParseFromString(data);
        HandleStateTransferReply(remote, stateTransferReplyMessage);
    } else if (type == epochChangeStateTransferRequest.GetTypeName()) {
        epochChangeStateTransferRequest.ParseFromString(data);
        HandleEpochChangeStateTransferRequest(remote, epochChangeStateTransferRequest);
    } else if (type == epochChangeStateTransferReply.GetTypeName()) {
        epochChangeStateTransferReply.ParseFromString(data);
        HandleEpochChangeStateTransferReply(remote, epochChangeStateTransferReply);
    } else {
        Panic("Received unexpected message type in ErisServer proto: %s",
              type.c_str());
    }
    ProcessPendingRequests();
}

void
ErisServer::HandleClientRequest(const TransportAddress &remote,
                                RequestMessage &msg,
                                const multistamp_t &stamp)
{
    // Save client's address if not exist. Assume client
    // addresses never change.
    if (this->clientAddresses.find(msg.request().clientid()) == this->clientAddresses.end()) {
        this->clientAddresses.insert(std::pair<uint64_t, std::unique_ptr<TransportAddress> >(msg.request().clientid(), std::unique_ptr<TransportAddress>(remote.clone())));
    }
    if (stamp.seqnums.find(this->groupIdx) == stamp.seqnums.end()) {
        return;
    }

    // Install sessnum and msgnum into request
    msg.mutable_request()->set_sessnum(stamp.sessnum);
    for (auto it = msg.mutable_request()->mutable_ops()->begin();
         it != msg.mutable_request()->mutable_ops()->end();
         it++) {
        it->set_msgnum(stamp.seqnums.at(it->shard()));
    }
    msgnum_t msgnum = stamp.seqnums.at(this->groupIdx);
    if (!TryProcessClientRequest(msg, stamp.sessnum, msgnum)) {
        AddPendingRequest(msg, stamp.sessnum, msgnum);
    }
}

void
ErisServer::HandleGapRequest(const TransportAddress &remote,
                             const GapRequestMessage &msg)
{
    if (!CheckViewNumAndStatus(msg.view())) {
        return;
    }

    GapReplyMessage reply;
    reply.mutable_view()->set_view_num(this->view);
    reply.mutable_view()->set_sess_num(this->sessnum);
    reply.set_op_num(msg.op_num());
    reply.set_replica_num(this->replicaIdx);

    LogEntry *entry = log.Find(msg.op_num());

    if (entry) {
        if (entry->state == LOG_STATE_RECEIVED) {
            reply.set_isfound(true);
            reply.set_isgap(false);
            reply.mutable_req()->set_txnid(entry->data.txnid);
            reply.mutable_req()->set_type(entry->data.type);
            *(reply.mutable_req()->mutable_request()) = entry->request;
            RDebug("Replying log entry %lu to the gap requester", msg.op_num());
        } else if (entry->state == LOG_STATE_NOOP) {
            reply.set_isfound(true);
            reply.set_isgap(true);
            reply.mutable_req()->set_txnid(0);
            reply.mutable_req()->set_type(proto::UNKNOWN);
            reply.mutable_req()->mutable_request()->set_op("");
            reply.mutable_req()->mutable_request()->set_clientid(0);
            reply.mutable_req()->mutable_request()->set_clientreqid(0);
            RDebug("Replying log entry (gap) %lu to the gap requester", msg.op_num());
        } else {
            NOT_REACHABLE();
        }
    } else {
        // Only reply with message not received when we are
        // actually missing the message (i.e. when we are also
        // requesting GapRequest for this message).
        if (this->gapRequestTimeout->Active() && this->lastOp+1 == msg.op_num()) {
            reply.set_isfound(false);
            reply.set_isgap(false);
            RDebug("Replica also has not received log entry %lu", msg.op_num());
        } else {
            return;
        }
    }
    if (!(transport->SendMessage(this, remote, reply))) {
        RWarning("Failed to send GapReplyMessage");
    }
}

void
ErisServer::HandleGapReply(const TransportAddress &remote,
                           const GapReplyMessage &msg)
{
    if (!CheckViewNumAndStatus(msg.view())) {
        return;
    }

    // We can ignore old GapReplyMessage
    if (msg.op_num() == this->lastOp+1) {
        if (msg.isfound()) {
            viewstamp_t vs(this->view,
                           this->lastOp+1,
                           this->sessnum,
                           this->nextMsgnum,
                           this->groupIdx);
            RequestMessage req;
            if (!msg.isgap()) {
                ASSERT(msg.has_req());
                req = msg.req();
            }
            ProcessNextOperation(req,
                                 vs,
                                 msg.isgap() ? LOG_STATE_NOOP : LOG_STATE_RECEIVED);
        } else {
            if (AmLeader()) {
                // Optimization: if the leader received 'not found' from
                // all other replicas, immediately contact FC
                if (this->gapReplyQuorum.AddAndCheckForQuorum(this->lastOp+1, msg.replica_num(), msg)) {
                    QueryFCForLastTxn();
                }
            }
        }
    }
}

void
ErisServer::HandleFCToErisMessage(const TransportAddress &remote,
                                  const FCToErisMessage &msg)
{
    // Some other shard is requesting for a missing message
    if (msg.has_txn_check()) {
        if (this->status == STATUS_EPOCH_CHANGE) {
            return;
        }
        shardnum_t shard_num = msg.txn_check().txn_num().shard_num();
        msgnum_t msg_num = msg.txn_check().txn_num().msg_num();
        sessnum_t sess_num = msg.txn_check().txn_num().sess_num();

        if (sess_num < this->sessnum) {
            return;
        }
        if (sess_num > this->sessnum) {
            DetectedEpochChange(sess_num);
            return;
        }
        MsgStamp stamp(shard_num, msg_num, sess_num);

        // If FC already decided this message, ignore
        if (this->permDrops.find(stamp) != this->permDrops.end() ||
            this->unDrops.find(stamp) != this->unDrops.end()) {
            return;
        }

        // Try to find the missing transaction in log,
        // by searching the reverse lookup map txnLookup
        const auto& it = this->txnLookup.find(stamp);
        LogEntry *entry = it != this->txnLookup.end() ? log.Find(it->second) : nullptr;

        // If we have promised FC to drop the message, it should
        // not appear in the log.
        if (this->tempDrops.find(stamp) != this->tempDrops.end()) {
            ASSERT(entry == nullptr);
        }

        ErisToFCMessage reply;
        BuildFCMessage(reply);

        // We have received the message
        if (entry) {
            // Entry cannot be a NOOP, otherwise it should appear
            // in permDrops
            ASSERT(entry->state == LOG_STATE_RECEIVED);
            RequestMessage m;
            m.set_txnid(entry->data.txnid);
            m.set_type(entry->data.type);
            *m.mutable_request() = entry->request;
            *(reply.mutable_txn_received()->mutable_txn()) = m;
        }
        // We have not received this message
        else {
            *(reply.mutable_txn_temp_drop()->mutable_txn_num()) = msg.txn_check().txn_num();
            // Promise not to process it until hearing back from FC
            InstallTempDrop(stamp);
        }

        string replyStr;
        reply.SerializeToString(&replyStr);
        this->fcorClient->InvokeAsync(replyStr);
    }
    // Some other shards have received the missing message
    else if (msg.has_txn_found()) {
        if (this->status == STATUS_EPOCH_CHANGE) {
            return;
        }

        RequestMessage un_drop = msg.txn_found().txn();
        if (un_drop.request().sessnum() < this->sessnum) {
            return;
        }
        if (un_drop.request().sessnum() > this->sessnum) {
            DetectedEpochChange(un_drop.request().sessnum());
            return;
        }
        msgnum_t msg_num = 0;
        for (auto iter = un_drop.request().ops().begin();
             iter != un_drop.request().ops().end();
             ++iter) {
            if ((int)iter->shard() == this->groupIdx) {
                msg_num = iter->msgnum();
                break;
            }
        }
        ASSERT(msg_num > 0);
        InstallUnDrop(un_drop);
        // If we are waiting for this message, process it as a RECEIVED
        if (this->status == STATUS_NORMAL && msg_num == this->nextMsgnum) {
            viewstamp_t vs(this->view, this->lastOp+1, this->sessnum, this->nextMsgnum, this->groupIdx);
            ProcessNextOperation(un_drop, vs, LOG_STATE_RECEIVED);
        }
        CompleteFCQuery(MsgStamp(this->groupIdx, msg_num, this->sessnum));
    }
    // FC has decided to permanently drop the message
    else if (msg.has_drop_txn()) {
        if (this->status == STATUS_EPOCH_CHANGE) {
            return;
        }
        shardnum_t shard_num = msg.drop_txn().txn_num().shard_num();
        msgnum_t msg_num = msg.drop_txn().txn_num().msg_num();
        sessnum_t sess_num = msg.drop_txn().txn_num().sess_num();

        if (sess_num < this->sessnum) {
            return;
        }
        if (sess_num > this->sessnum) {
            DetectedEpochChange(sess_num);
            return;
        }

        InstallPermDrop(MsgStamp(shard_num, msg_num, sess_num));
        // If we are waiting for this message, process it as a NOOP
        if (this->status == STATUS_NORMAL &&
            shard_num == (uint32_t)this->groupIdx &&
            msg_num == this->nextMsgnum) {
            viewstamp_t vs(this->view, this->lastOp+1, sess_num, msg_num, shard_num);
            ProcessNextOperation(RequestMessage(), vs, LOG_STATE_NOOP);
        }
        CompleteFCQuery(MsgStamp(shard_num, msg_num, sess_num));
    }
    // epoch change
    else if (msg.has_epoch_change()) {
        if (msg.epoch_change().new_epoch_num() < this->sessnum) {
            // Ignore epoch change msg from previous epochs
            return;
        }

        // We can stop sending epoch change req now
        this->epochChangeReqTimeout->Stop();

        if (msg.epoch_change().new_epoch_num() > this->sessnum) {
            StartEpochChange(msg.epoch_change().new_epoch_num());
        }

        ASSERT(this->sessnum == msg.epoch_change().new_epoch_num());

        if (this->status != STATUS_EPOCH_CHANGE) {
            // Replica has already entered the new epoch
            return;
        }

        // Send EpochChangeAck to FC
        SendEpochChangeAck();
    }
    // state transfer
    else if (msg.has_epoch_change_state_transfer()) {
        const auto &st_msg = msg.epoch_change_state_transfer();
        ASSERT(st_msg.epoch_num() <= this->sessnum);
        if (this->status != STATUS_EPOCH_CHANGE || st_msg.epoch_num() < this->sessnum) {
            return;
        }
        // Can stop sending epoch change ack now
        this->epochChangeAckTimeout->Stop();

        if (!this->pendingECStateTransfer.requests.empty()) {
            // If we are already requesting state transfers,
            // ignore this message.
            return;
        }

        // First, install all perm_drops in last epoch
        for (auto it = st_msg.perm_drops().begin();
             it != st_msg.perm_drops().end();
             ++it) {
            InstallPermDrop(MsgStamp(*it));
        }

        bool state_transfer = false;
        // Second, request missing transactions
        ASSERT(this->lastNormalSessnum == st_msg.state_transfer_epoch_num());
        if (st_msg.latest_msgs_size() > 0) {
            for (auto it = st_msg.latest_msgs().begin();
                 it != st_msg.latest_msgs().end();
                 ++it) {
                if (it->msg_num() < this->nextMsgnum) {
                    continue;
                }
                state_transfer = true;
                EpochChangeStateTransferRequest request;
                request.set_sess_num(this->sessnum);
                request.set_state_transfer_sess_num(st_msg.state_transfer_epoch_num());
                request.set_shard_num(this->groupIdx);
                request.set_begin(this->nextMsgnum);
                request.set_end(it->msg_num()+1);

                this->pendingECStateTransfer.requests[it->shard_num()] = make_pair(it->replica_num(),
                                                                                   request);
            }
            if (state_transfer) {
                SendEpochChangeStateTransfer();
                this->pendingECStateTransfer.callback = [this]() {
                    CompleteECStateTransfer();
                };
                return;
            }
        }

        // We have the complete log now, reply to FC
        CompleteECStateTransfer();
    }
    // Start epoch
    else if (msg.has_start_epoch()) {
        if (msg.start_epoch().new_view().sess_num() < this->sessnum) {
            // Ignore start epoch msg from previous epochs
            return;
        }
        if (msg.start_epoch().new_view().sess_num() == this->sessnum &&
            this->status != STATUS_EPOCH_CHANGE) {
            // We have already entered the new epoch
            return;
        }

        this->epochChangeAckTimeout->Stop();
        ASSERT(msg.start_epoch().new_view().view_num() >= this->view);
        this->view = msg.start_epoch().new_view().view_num();
        this->sessnum = msg.start_epoch().new_view().sess_num();

        if (this->replicaIdx != (int)msg.start_epoch().latest_replica_num()) {
            // First install all perm drops
            for (auto it = msg.start_epoch().perm_drops().begin();
                 it != msg.start_epoch().perm_drops().end();
                 ++it) {
                InstallPermDrop(MsgStamp(*it));
            }

            // Replica does not have the merged log, 4 cases:
            // 1. Replica is lagging behind in epoch number. Rewind log to
            // the last committed op, and transfer from the replica with
            // the merged log.
            // 2. Replica has a shorter log than the merged log. Request
            // state transfer.
            // 3. Replica has a longer log. Rewind log.
            // 4. Replica has equal length log. No action needed.
            // When doing state transfer, make sure replica is in epoch change
            // status.
            bool state_transfer = false;
            if (this->lastNormalSessnum < msg.start_epoch().last_normal_epoch_num()) {
                RewindLog(this->lastCommittedOp);
                state_transfer = true;
            } else {
                ASSERT(this->lastNormalSessnum == msg.start_epoch().last_normal_epoch_num());
                if (this->lastOp < msg.start_epoch().latest_op_num()) {
                    state_transfer = true;
                } else if (this->lastOp > msg.start_epoch().latest_op_num()) {
                    RewindLog(msg.start_epoch().latest_op_num());
                }
            }
            if (state_transfer) {
                ClearTimeoutAndQuorums();
                this->status = STATUS_EPOCH_CHANGE;
                this->pendingStateTransfer.begin = this->lastOp+1;
                this->pendingStateTransfer.end = msg.start_epoch().latest_op_num()+1;
                this->pendingStateTransfer.replica_idx = msg.start_epoch().latest_replica_num();
                this->pendingStateTransfer.callback = [this]() {
                    EnterEpoch(this->sessnum);
                };
                SendStateTransferRequest();
                return;
            }
        }

        // Replica now has the complete log, enter the new epoch
        EnterEpoch(this->sessnum);
    }
}

void
ErisServer::HandleSyncPrepare(const TransportAddress &remote,
                              const SyncPrepareMessage &msg)
{
    if (!CheckViewNumAndStatus(msg.view())) {
        return;
    }

    this->leaderSyncHeardTimeout->Reset();
}

void
ErisServer::HandleViewChangeRequest(const TransportAddress &remote,
                                    const ViewChangeRequestMessage &msg)
{
    CheckViewNumAndStatus(msg.view());
}

void
ErisServer::HandleViewChange(const TransportAddress &remote,
                             const ViewChangeMessage &msg)
{
    if (msg.view().sess_num() < this->sessnum ||
        msg.view().view_num() < this->view) {
        return;
    }

    if (msg.view().sess_num() > this->sessnum) {
        DetectedEpochChange(msg.view().sess_num());
        return;
    }

    if (msg.view().view_num() > this->view) {
        StartViewChange(msg.view().view_num());
    }

    ASSERT(msg.view().sess_num() == this->sessnum &&
           msg.view().view_num() == this->view);

    ASSERT(AmLeader());

    if (this->status != STATUS_VIEW_CHANGE) {
        // Already entered the new view, potentially the
        // requesting replica has not received the
        // StartViewMessage. Resend StartViewMessage.
        StartViewMessage startViewMessage;
        startViewMessage.mutable_view()->set_sess_num(this->sessnum);
        startViewMessage.mutable_view()->set_view_num(this->view);
        startViewMessage.set_op_num(this->lastOp);
        BuildDropTxns(*startViewMessage.mutable_drops());

        if (!this->transport->SendMessage(this, remote, startViewMessage)) {
            RWarning("Failed to send StartViewMessage");
        }
        return;
    }

    if (this->stateTransferTimeout->Active() ||
        this->fcQueriesTimeout->Active()) {
        // If we are in the middle of state transfer or querying
        // the FC, do not proceed. The callbacks will finish the
        // view change.
        return;
    }

    if (auto msgs = this->viewChangeQuorum.AddAndCheckForQuorum(msg.view().view_num(),
                                                                msg.replica_num(),
                                                                msg)) {
        RDebug("Leader doing view change for view %u", msg.view().view_num());
        // Merge logs
        opnum_t latestOp = this->lastOp;
        int latestOpReplicaIdx = -1;

        for (const auto &kv : *msgs) {
            const ViewChangeMessage &m = kv.second;
            if (m.op_num() > latestOp) {
                latestOp = m.op_num();
                latestOpReplicaIdx = m.replica_num();
            }
            // Merge temp_drops, perm_drops and un_drops
            MergeDropTxns(m.drops());
        }
        CheckTempDrops();

        if (latestOpReplicaIdx > -1) {
            // Some other replica has a longer log,
            // request missing log entries from that
            // replica first.
            this->pendingStateTransfer.begin = this->lastOp+1;
            this->pendingStateTransfer.end = latestOp+1;
            this->pendingStateTransfer.replica_idx = latestOpReplicaIdx;
            this->pendingStateTransfer.callback = [this]() {
                if (MatchLogWithTempDrops()) {
                    EnterView(this->view);
                }
            };
            SendStateTransferRequest();
            return;
        }

        // If there is any temp_drop txns matching log entries,
        // query the FC for their outcomes.
        if (MatchLogWithTempDrops()) {
            // We have the complete log and all meta data,
            // start the new view.
            EnterView(this->view);
        }
    }
}

void
ErisServer::HandleStartView(const TransportAddress &remote,
                            const proto::StartViewMessage &msg)
{
    if (msg.view().sess_num() < this->sessnum ||
        msg.view().view_num() < this->view) {
        // Ignore StartView from an older view or epoch
        return;
    }

    if (msg.view().sess_num() > this->sessnum) {
        DetectedEpochChange(msg.view().sess_num());
        return;
    }

    if (msg.view().view_num() == this->view &&
        status != STATUS_VIEW_CHANGE) {
        // We have already entered the new view.
        // XXX potentially in an epoch change, do
        // we start the new view here?
        return;
    }

    ASSERT(configuration.GetLeaderIndex(msg.view().view_num()) != this->replicaIdx);

    if (this->stateTransferTimeout->Active() ||
        this->fcQueriesTimeout->Active()) {
        // If we are in the middle of state transfer or querying
        // the FC, do not proceed. The callbacks will finish the
        // view change.
        return;
    }

    // Merge temp_drops, perm_drops and un_drops
    MergeDropTxns(msg.drops());
    CheckTempDrops();

    // Change status to view change in case we need to do state transfer
    // or fc queries
    this->view = msg.view().view_num();
    this->status = STATUS_VIEW_CHANGE;
    ClearTimeoutAndQuorums();

    if (msg.op_num() > this->lastOp) {
        // Request missing log entreis from the leader,
        // make sure we are in view change status
        this->pendingStateTransfer.begin = this->lastOp+1;
        this->pendingStateTransfer.end = msg.op_num()+1;
        this->pendingStateTransfer.replica_idx = configuration.GetLeaderIndex(msg.view().view_num());
        opnum_t opnum = msg.op_num();
        this->pendingStateTransfer.callback = [this, opnum]() {
            this->lastCommittedOp = opnum;
            if (MatchLogWithTempDrops()) {
                EnterView(this->view);
            }
        };
        SendStateTransferRequest();
        return;
    }

    this->lastCommittedOp = msg.op_num();
    if (MatchLogWithTempDrops()) {
        EnterView(this->view);
    }
}

void
ErisServer::HandleStateTransferRequest(const TransportAddress &remote,
                                       const StateTransferRequestMessage &msg)
{
    if (msg.view().sess_num() != this->sessnum || msg.view().view_num() != this->view) {
        return;
    }

    if (msg.begin() > this->lastOp || msg.end() > this->lastOp + 1) {
        return;
    }

    StateTransferReplyMessage reply;
    reply.mutable_view()->set_sess_num(this->sessnum);
    reply.mutable_view()->set_view_num(this->view);
    reply.set_begin(msg.begin());
    reply.set_end(msg.end());

    // Dump log entries
    for (uint64_t i = msg.begin(); i < msg.end(); i++) {
        LogEntry *log_entry = log.Find(i);
        ASSERT(log_entry != nullptr);
        StateTransferReplyMessage_MsgLogEntry *msg_entry = reply.add_entries();
        msg_entry->set_view(log_entry->viewstamp.view);
        msg_entry->set_op_num(log_entry->viewstamp.opnum);
        msg_entry->set_sess_num(log_entry->viewstamp.sessnum);
        msg_entry->set_msg_num(log_entry->viewstamp.msgnum);
        msg_entry->set_shard_num(log_entry->viewstamp.shardnum);
        msg_entry->set_state(log_entry->state);
        msg_entry->mutable_request()->set_txnid(log_entry->data.txnid);
        msg_entry->mutable_request()->set_type(log_entry->data.type);
        if (log_entry->state == LOG_STATE_NOOP) {
            // Avoid protobuf serialization issue
            msg_entry->mutable_request()->mutable_request()->set_op("");
            msg_entry->mutable_request()->mutable_request()->set_clientid(0);
            msg_entry->mutable_request()->mutable_request()->set_clientreqid(0);
        } else {
            *(msg_entry->mutable_request()->mutable_request()) = log_entry->request;
        }
    }

    if (!(transport->SendMessage(this, remote, reply))) {
        RWarning("Failed to send StateTransferReplyMessage");
    }
}

void
ErisServer::HandleStateTransferReply(const TransportAddress &remote,
                                     const StateTransferReplyMessage &msg)
{
    if (msg.view().sess_num() != this->sessnum || msg.view().view_num() != this->view) {
        return;
    }

    // Ignore if already have all the log entries
    if (msg.end() - 1 <= this->lastOp) {
        return;
    }

    ASSERT(msg.entries(0).op_num() == msg.begin());
    ASSERT(msg.entries(msg.entries_size()-1).op_num() == msg.end()-1);

    // Install log entries
    for (int i = this->lastOp + 1 - msg.begin(); i < msg.entries().size(); i++) {
        const auto &entry = msg.entries(i);
        ASSERT(entry.op_num() == this->lastOp+1);
        ASSERT(entry.msg_num() == this->nextMsgnum);
        viewstamp_t vs(entry.view(), entry.op_num(), entry.sess_num(), entry.msg_num(), entry.shard_num());
        if (this->permDrops.find(MsgStamp(entry.shard_num(), entry.msg_num(), entry.sess_num()))
            != this->permDrops.end()) {
            // this txn is already dropped by FC
            InstallLogEntry(vs, LOG_STATE_NOOP, RequestMessage());
        } else {
            InstallLogEntry(vs, (LogEntryState)entry.state(), entry.request());
        }
    }

    if (this->stateTransferTimeout->Active() &&
        msg.end() >= this->pendingStateTransfer.end) {
        // State transfer is completed
        this->stateTransferTimeout->Stop();
        this->pendingStateTransfer.callback();
    }
}

void
ErisServer::HandleEpochChangeStateTransferRequest(const TransportAddress &remote,
                                                  const proto::EpochChangeStateTransferRequest &msg)
{
    if (msg.sess_num() < this->sessnum || this->status != STATUS_EPOCH_CHANGE) {
        return;
    }

    // This replica should at least have the last Txn
    ASSERT(msg.sess_num() == this->sessnum);
    ASSERT(this->lastNormalSessnum == msg.state_transfer_sess_num());
    ASSERT(this->txnLookup.find(MsgStamp(msg.shard_num(), msg.end()-1, msg.state_transfer_sess_num()))
           != this->txnLookup.end());

    EpochChangeStateTransferReply reply;
    reply.set_sess_num(msg.sess_num());
    reply.set_state_transfer_sess_num(msg.state_transfer_sess_num());
    reply.set_shard_num(this->groupIdx);

    for (uint64_t i = msg.begin(); i < msg.end(); i++) {
        auto it = this->txnLookup.find(MsgStamp(msg.shard_num(), i, msg.state_transfer_sess_num()));
        if (it != this->txnLookup.end()) {
            LogEntry *log_entry = this->log.Find(it->second);
            ASSERT(log_entry != nullptr);
            EpochChangeStateTransferReply_MsgEntry *msg_entry = reply.add_entries();
            msg_entry->set_msg_num(i);
            msg_entry->mutable_request()->set_txnid(log_entry->data.txnid);
            msg_entry->mutable_request()->set_type(log_entry->data.type);
            *(msg_entry->mutable_request()->mutable_request()) = log_entry->request;
        }
    }

    if (!this->transport->SendMessage(this, remote, reply)) {
        RWarning("Failed to send EpochChangeStateTransferReply");
    }
}

void
ErisServer::HandleEpochChangeStateTransferReply(const TransportAddress &remote,
                                                const proto::EpochChangeStateTransferReply &msg)
{
    if (msg.sess_num() != this->sessnum || this->status != STATUS_EPOCH_CHANGE) {
        return;
    }

    ASSERT(msg.sess_num() == this->sessnum);
    ASSERT(msg.state_transfer_sess_num() == this->lastNormalSessnum);

    if (this->pendingECStateTransfer.requests.find(msg.shard_num())
        == this->pendingECStateTransfer.requests.end()) {
        // We have already installed messages from this shard
        return;
    }

    // Install log entries
    for (auto it = msg.entries().begin(); it != msg.entries().end(); ++it) {
        // Ignore txns that are already dropped by FC
        MsgStamp stamp(this->groupIdx, it->msg_num(), this->lastNormalSessnum);
        if (this->permDrops.find(stamp) != this->permDrops.end()) {
            continue;
        }
        if (it->msg_num() < this->nextMsgnum) {
            // We might have temporarily installed this entry as NOOP before,
            // replace it with the actual message.
            opnum_t opnum = this->lastOp + 1 - (this->nextMsgnum - it->msg_num());
            LogEntry *log_entry = this->log.Find(opnum);
            ASSERT(log_entry != nullptr);
            if (log_entry->state == LOG_STATE_NOOP) {
                log_entry->request = it->request().request();
                log_entry->state = LOG_STATE_RECEIVED;
                log_entry->data = EntryData(it->request().txnid(), it->request().type());
                // Remove it from the set of drops that we are sending to FC
                this->pendingECStateTransfer.drops.erase(MsgStamp(this->groupIdx,
                                                                  it->msg_num(),
                                                                  this->lastNormalSessnum));
            }
        } else {
            // If there are gaps between nextMsgnum and this entry,
            // temporarily fill in NOOPs (state transfer from other shards may
            // replace those NOOPs)
            for (msgnum_t i = this->nextMsgnum; i <= it->msg_num(); ++i) {
                this->lastOp += 1;
                this->nextMsgnum += 1;
                viewstamp_t vs(this->view, this->lastOp, this->lastNormalSessnum, i, this->groupIdx);
                if (i < it->msg_num()) {
                    this->log.Append(vs, Request(), LOG_STATE_NOOP, EntryData());
                    // Temporarily add it to the set of drops that we are sending to FC.
                    // (FC might have already dropped these txns, but thats fine)
                    this->pendingECStateTransfer.drops.insert(MsgStamp(this->groupIdx,
                                                                       i,
                                                                       this->lastNormalSessnum));
                } else {
                    this->log.Append(vs, it->request().request(), LOG_STATE_RECEIVED,
                                     EntryData(it->request().txnid(), it->request().type()));
                }
            }
        }
    }

    this->pendingECStateTransfer.requests.erase(msg.shard_num());

    if (this->pendingECStateTransfer.requests.empty()) {
        // We have received all state transfers
        this->pendingECStateTransfer.callback();
    }
}

bool
ErisServer::TryProcessClientRequest(const RequestMessage &msg,
                                    sessnum_t sessnum,
                                    msgnum_t msgnum)
{
    if (sessnum < this->sessnum) {
        return true;
    }

    if (sessnum > this->sessnum) {
        // new session
        DetectedEpochChange(sessnum);
        // Try the request later when we move to
        // the new session.
        return false;
    }

    if (this->status != STATUS_NORMAL) {
        // Replica currently not processing any client
        // requests. Add it to pending requests and try
        // later.
        return false;
    }

    if (msgnum < this->nextMsgnum) {
        return true;
    }

    if (msgnum > this->nextMsgnum) {
        // Detected message gap.
        if (!gapRequestTimeout->Active() &&
            !startGapRequestTimeout->Active() &&
            !fcTxnInfoRequestTimeout->Active()) {
            this->startGapRequestTimeout->Reset();
        }
        return false;
    } else {
        viewstamp_t vs;
        vs.view = this->view;
        vs.opnum = this->lastOp+1;
        vs.sessnum = sessnum;
        vs.msgnum = msgnum;
        vs.shardnum = this->groupIdx;

        ProcessNextOperation(msg, vs, LOG_STATE_RECEIVED);
        return true;
    }
}

void
ErisServer::ProcessNextOperation(const RequestMessage &msg,
                                 const viewstamp_t &vs,
                                 const LogEntryState &state)
{
    ASSERT(vs.opnum == this->lastOp+1);
    ASSERT(vs.msgnum == this->nextMsgnum);
    ASSERT(vs.shardnum == (uint32_t)this->groupIdx);

    LogEntryState entry_state = state;

    // If we have sent a GapRequest, we can cancel the timeout.
    // Either some other replica responds with the missing request,
    // or the replica receives it from the FC/client.
    if (this->startGapRequestTimeout->Active()) {
        this->startGapRequestTimeout->Stop();
    }
    if (this->gapRequestTimeout->Active()) {
        this->gapRequestTimeout->Stop();
        this->gapReplyQuorum.Clear();
    }

    // If we have tentatively agreed to drop this txn, do
    // not process it, and contact the FC again (if not
    // already doing so)
    MsgStamp stamp(vs.shardnum, vs.msgnum, vs.sessnum);
    if (this->tempDrops.find(stamp) != this->tempDrops.end()) {
        if (!this->fcTxnInfoRequestTimeout->Active()) {
            QueryFCForLastTxn();
        }
        return;
    }
    // If there is a matching perm_drop, install NOOP entry
    if (this->permDrops.find(stamp) != this->permDrops.end()) {
        entry_state = LOG_STATE_NOOP;
    }
    // If there is a mathching un_drop, make sure the entry
    // is not NOOP
    if (this->unDrops.find(stamp) != this->unDrops.end()) {
        ASSERT(entry_state == LOG_STATE_RECEIVED);
    }

    // Now safe to cancel the FC timeout
    if (this->fcTxnInfoRequestTimeout->Active()) {
        this->fcTxnInfoRequestTimeout->Stop();
    }

    InstallLogEntry(vs, entry_state, msg);

    if (entry_state == LOG_STATE_NOOP) {
        // Only FC can decide to drop (noop) an operation,
        // so safe to insert into permDrops.
        this->permDrops.insert(stamp);
    } else if (entry_state == LOG_STATE_RECEIVED) {
        // Only the leader execute the request.
        if (this->configuration.GetLeaderIndex(vs.view) == this->replicaIdx) {
            ExecuteUptoOp(vs.opnum);
        } else {
            // Non-leader replica simply reply without execution.
            ReplyMessage reply;
            auto addr = this->clientAddresses.find(msg.request().clientid());
            if (addr != this->clientAddresses.end()) {
                reply.set_clientid(msg.request().clientid());
                reply.set_clientreqid(msg.request().clientreqid());
                reply.set_shard_num(this->groupIdx);
                reply.set_replica_num(this->replicaIdx);
                reply.mutable_view()->set_view_num(vs.view);
                reply.mutable_view()->set_sess_num(vs.sessnum);
                reply.set_op_num(vs.opnum);

                if (!this->transport->SendMessage(this, *(addr->second), reply)) {
                    RWarning("Failed to send reply to client");
                }
            }
        }
    }

    // If the next msg (nextMsgnum) is already decided by
    // FC, process it
    stamp.msg_num = this->nextMsgnum;
    viewstamp_t next_vs(this->view, this->lastOp+1, this->sessnum, this->nextMsgnum, this->groupIdx);

    if (this->permDrops.find(stamp) != this->permDrops.end()) {
        ProcessNextOperation(RequestMessage(), next_vs, LOG_STATE_NOOP);
    } else if (this->unDrops.find(stamp) != this->unDrops.end()) {
        ASSERT(this->unDropMsgs.find(stamp) != this->unDropMsgs.end());
        ProcessNextOperation(this->unDropMsgs[stamp], next_vs, LOG_STATE_RECEIVED);
    }
}

void
ErisServer::UpdateClientTable(const Request &req)
{
    ClientTableEntry &entry = this->clientTable[req.clientid()];

    ASSERT(entry.lastReqId <= req.clientreqid());

    if (entry.lastReqId == req.clientreqid()) {
        // Duplicate request
        return;
    }

    entry.lastReqId = req.clientreqid();
    entry.replied = false;
    entry.reply.Clear();
}

void
ErisServer::AddPendingRequest(const RequestMessage &msg,
                              sessnum_t sessnum, msgnum_t msgnum)
{
    struct PendingRequest pendingRequest = {
        .sessnum = sessnum,
        .msgnum = msgnum,
        .requestMessage = msg
    };
    this->pendingRequests.insert(pendingRequest);
}

void
ErisServer::ProcessPendingRequests()
{
    if (this->status != STATUS_NORMAL) {
        return;
    }

    // Try if we can process pending requests,
    // pending requests are already sorted
    while (!pendingRequests.empty()) {
        if (!TryProcessClientRequest(pendingRequests.begin()->requestMessage,
                                     pendingRequests.begin()->sessnum,
                                     pendingRequests.begin()->msgnum)) {
            // Still pending, since the list is sorted, subsequent
            // pending requests will not be processed
            return;
        }
        // Either successfully processed, or discarded
        pendingRequests.erase(pendingRequests.begin());
    }
}

void
ErisServer::ExecuteUptoOp(opnum_t opnum) {
    if (opnum <= this->lastExecutedOp) {
        // Already executed
        return;
    }

    if (opnum > this->lastOp) {
        RPanic("Executing operation not received yet");
    }

    for (opnum_t op = this->lastExecutedOp + 1; op <= opnum; op++) {
        LogEntry *logEntry = this->log.Find(op);
        ASSERT(op == logEntry->viewstamp.opnum);

        // Only execute the operation if it is not a gap.
        // But always update LastExecutedOp even for gaps.
        this->lastExecutedOp = op;
        if (logEntry->state == LOG_STATE_RECEIVED) {
            // Check client table for duplicates.
            auto kv = this->clientTable.find(logEntry->request.clientid());
            if (kv != this->clientTable.end()) {
                ClientTableEntry &entry = kv->second;
                if (logEntry->request.clientreqid() < entry.lastReqId) {
                    // Ignore stale request
                    continue;
                }
                if (logEntry->request.clientreqid() == entry.lastReqId) {
                    // Duplicate request. Send back the last reply if we have one.
                    // We might not have a reply if the transaction is blocked; in
                    // that case just discard the request.
                    if (entry.replied) {
                        // When we send the reply, update view and opnum in the reply
                        entry.reply.mutable_view()->set_view_num(this->view);
                        entry.reply.mutable_view()->set_sess_num(this->sessnum);
                        entry.reply.set_op_num(op);
                        auto addr = this->clientAddresses.find(logEntry->request.clientid());
                        if (addr != this->clientAddresses.end()) {
                            if (!this->transport->SendMessage(this, *(addr->second), entry.reply)) {
                                RWarning("Failed to send reply to client");
                            }
                        }
                    }
                    continue;
                }
            }

            UpdateClientTable(logEntry->request);
            ExecuteTxn(op);
            ExecuteUnblockedTxns();
        }
    }
}

void
ErisServer::ExecuteTxn(opnum_t opnum) {
    ReplyMessage reply;
    LogEntry *logEntry = this->log.Find(opnum);
    ASSERT(logEntry != nullptr);
    ASSERT(logEntry->viewstamp.opnum == opnum);
    ASSERT(logEntry->state == LOG_STATE_RECEIVED);
    txnid_t txnid = logEntry->data.txnid;
    RequestType type = logEntry->data.type;

    txnarg_t txnarg;
    txnret_t txnret;
    txnarg.txnid = txnid;
    switch (type) {
    case proto::INDEPENDENT: {
        txnarg.type = TXN_INDEP;
        break;
    }
    case proto::PREPARE: {
        txnarg.type = TXN_PREPARE;
        break;
    }
    case proto::COMMIT: {
        txnarg.type = TXN_COMMIT;
        break;
    }
    case proto::ABORT: {
        txnarg.type = TXN_ABORT;
        break;
    }
    default:
        RPanic("Wrong transaction type");
    }
    Execute(logEntry->viewstamp.opnum, logEntry->request, reply, this->groupIdx, &txnarg, &txnret);

    if (txnret.blocked) {
        RDebug("Transaction %lu type %d blocked", txnid, type);
        /* Transaction cannot acquire all locks */
        ASSERT(this->blockedTxns.find(txnid) == this->blockedTxns.end());
        this->blockedTxns[txnid] = logEntry->viewstamp.opnum;
        /* Wait until transaction unblocked to reply back
         * to client.
         */
        return;
    }

    if (!txnret.unblocked_txns.empty()) {
        for (txnid_t txn : txnret.unblocked_txns) {
            ASSERT(this->blockedTxns.find(txn) != this->blockedTxns.end());
            ASSERT(this->unblockedExecutionQueue.find(this->blockedTxns.at(txn)) ==
                   this->unblockedExecutionQueue.end());
            this->unblockedExecutionQueue.insert(this->blockedTxns.at(txn));
            this->blockedTxns.erase(txn);
        }
    }

    // Only reply back to client if the replica is the leader
    // of the view this operation belongs to. Otherwise, it
    // might already responded to the client.
    if (this->configuration.GetLeaderIndex(logEntry->viewstamp.view) == this->replicaIdx) {
        reply.set_clientid(logEntry->request.clientid());
        reply.set_clientreqid(logEntry->request.clientreqid());
        reply.set_shard_num(this->groupIdx);
        reply.set_replica_num(this->replicaIdx);
        reply.mutable_view()->set_view_num(logEntry->viewstamp.view);
        reply.mutable_view()->set_sess_num(logEntry->viewstamp.sessnum);
        reply.set_op_num(logEntry->viewstamp.opnum);
        reply.set_commit(txnret.commit ? 1 : 0);

        /* Update client table with reply */
        ClientTableEntry &cte = this->clientTable[logEntry->request.clientid()];
        ASSERT(cte.lastReqId == logEntry->request.clientreqid());
        cte.replied = true;
        cte.reply = reply;

        auto addr = this->clientAddresses.find(logEntry->request.clientid());
        if (addr != this->clientAddresses.end()) {
            if (!this->transport->SendMessage(this, *(addr->second), reply)) {
                RWarning("Failed to send reply to client");
            }
        }
    }
}

void
ErisServer::ExecuteUnblockedTxns()
{
    /* unblockedExecutionQueue is sorted in log (opnum) order */
    while (!this->unblockedExecutionQueue.empty()) {
        opnum_t opnum = *(this->unblockedExecutionQueue.begin());
        ExecuteTxn(opnum);
        this->unblockedExecutionQueue.erase(opnum);
    }
}

void
ErisServer::QueryFCForLastTxn()
{
    this->startGapRequestTimeout->Stop();
    this->gapRequestTimeout->Stop();

    Stamp stamp;
    stamp.set_shard_num(this->groupIdx);
    stamp.set_msg_num(this->nextMsgnum);
    stamp.set_sess_num(this->sessnum);
    SendFCTxnInfoRequest(stamp);

    this->fcTxnInfoRequestTimeout->Reset();
}

void
ErisServer::StartViewChange(view_t new_view)
{
    RNotice("Starting view change view %lu", new_view);
    ASSERT(new_view > this->view);

    ClearTimeoutAndQuorums();
    this->view = new_view;
    this->status = STATUS_VIEW_CHANGE;

    SendViewChange();
}

void
ErisServer::EnterView(view_t new_view)
{
    ASSERT(new_view >= this->view);
    RNotice("Entering new view %lu", new_view);
    this->view = new_view;
    this->status = STATUS_NORMAL;

    ClearTimeoutAndQuorums();

    if (AmLeader()) {
        // New leader safe to execute all ops
        ExecuteUptoOp(this->lastOp);
        // All log entries are committed
        this->lastCommittedOp = this->lastOp;
        // Send StartViewMessage to all replicas
        StartViewMessage startViewMessage;
        startViewMessage.mutable_view()->set_sess_num(this->sessnum);
        startViewMessage.mutable_view()->set_view_num(this->view);
        startViewMessage.set_op_num(this->lastOp);
        BuildDropTxns(*startViewMessage.mutable_drops());

        if (!this->transport->SendMessageToAll(this, startViewMessage)) {
            RWarning("Failed to broadcast StartViewMessage");
        }
        this->syncTimeout->Start();
    } else {
        // Non-leader replica updates lastCommitedOp in
        // HandleStartView (because it may have more log
        // entries than the leader log).
        this->leaderSyncHeardTimeout->Start();
    }
}

void
ErisServer::DetectedEpochChange(sessnum_t new_sessnum)
{
    ASSERT(new_sessnum > this->sessnum);
    StartEpochChange(new_sessnum);
    SendEpochChangeReq();
}

void
ErisServer::StartEpochChange(sessnum_t new_sessnum)
{
    ASSERT(new_sessnum > this->sessnum);
    RNotice("Starting epoch change epoch %lu", new_sessnum);

    ClearTimeoutAndQuorums();
    this->sessnum = new_sessnum;
    this->status = STATUS_EPOCH_CHANGE;
}

void
ErisServer::EnterEpoch(sessnum_t new_sessnum)
{
    ASSERT(new_sessnum >= this->sessnum);
    RNotice("Entering new epoch %lu", new_sessnum);
    this->sessnum = new_sessnum;
    this->status = STATUS_NORMAL;
    this->lastNormalSessnum = new_sessnum;
    this->nextMsgnum = 1;
    // The merged log is stable: all log entries are committed
    this->lastCommittedOp = this->lastOp;

    ClearTimeoutAndQuorums();
    ClearEpochData();

    if (AmLeader()) {
        // Leader safe to execute all ops
        ExecuteUptoOp(this->lastOp);
        // No need to send StartEpochAck to FC: FC can keep around StartEpoch
        // messages since they are small (only contain metadata).
        this->syncTimeout->Start();
    } else {
        this->leaderSyncHeardTimeout->Start();
    }
}

void
ErisServer::SendGapRequest()
{
    GapRequestMessage gapRequestMessage;
    gapRequestMessage.mutable_view()->set_view_num(this->view);
    gapRequestMessage.mutable_view()->set_sess_num(this->sessnum);
    gapRequestMessage.set_op_num(this->lastOp+1);

    RDebug("Sending GapRequestMessage with opnum %lu", this->lastOp+1);
    if (!AmLeader()) {
        if (!(transport->SendMessageToReplica(this,
                                              configuration.GetLeaderIndex(this->view),
                                              gapRequestMessage))) {
            RWarning("Failed to send GapRequestMessage to leader");
        }
    } else {
        if (!(transport->SendMessageToAll(this, gapRequestMessage))) {
            RWarning("Failed to send GapRequestMessage to replicas");
        }
    }
    this->gapRequestTimeout->Reset();
}

void
ErisServer::SendFCTxnInfoRequest(const Stamp &stamp)
{
    TxnInfoRequest txnInfoRequest;
    *(txnInfoRequest.mutable_txn_num()) = stamp;
    ErisToFCMessage erisToFCMessage;
    BuildFCMessage(erisToFCMessage);
    *(erisToFCMessage.mutable_txn_info_req()) = txnInfoRequest;

    string request;
    erisToFCMessage.SerializeToString(&request);
    this->fcorClient->InvokeAsync(request);
}

void
ErisServer::SendSyncPrepare()
{
    ASSERT(AmLeader());
    SyncPrepareMessage syncPrepareMessage;
    syncPrepareMessage.mutable_view()->set_view_num(this->view);
    syncPrepareMessage.mutable_view()->set_sess_num(this->sessnum);

    if (!this->transport->SendMessageToAll(this, syncPrepareMessage)) {
        RWarning("Failed to send SyncPrepareMessage");
    }
}

void
ErisServer::SendViewChange()
{
    ViewChangeRequestMessage viewChangeRequestMessage;
    viewChangeRequestMessage.mutable_view()->set_view_num(this->view);
    viewChangeRequestMessage.mutable_view()->set_sess_num(this->sessnum);

    if (!transport->SendMessageToAll(this, viewChangeRequestMessage)) {
        RWarning("Failed to send ViewChangeRequestMessage to all replicas");
    }

    // Leader does not send ViewChangeMessage to itself
    if (!AmLeader()) {
        ViewChangeMessage viewChangeMessage;
        viewChangeMessage.mutable_view()->set_view_num(this->view);
        viewChangeMessage.mutable_view()->set_sess_num(this->sessnum);
        viewChangeMessage.set_replica_num(this->replicaIdx);
        viewChangeMessage.set_op_num(this->lastOp);
        BuildDropTxns(*viewChangeMessage.mutable_drops());

        if (!this->transport->SendMessageToReplica(this,
                                                   this->configuration.GetLeaderIndex(this->view),
                                                   viewChangeMessage)) {
            RWarning("Failed to send ViewChangeMessage to leader");
        }
    }
    this->viewChangeTimeout->Reset();
}

void
ErisServer::SendStateTransferRequest()
{
    StateTransferRequestMessage request;
    request.mutable_view()->set_sess_num(this->sessnum);
    request.mutable_view()->set_view_num(this->view);
    request.set_begin(this->pendingStateTransfer.begin);
    request.set_end(this->pendingStateTransfer.end);

    if (!(this->transport->SendMessageToReplica(this,
                                                this->pendingStateTransfer.replica_idx,
                                                request))) {
        RWarning("Failed to send StateTransferRequestMessage");
    }
    this->stateTransferTimeout->Reset();
}

void
ErisServer::SendFCQueries()
{
    ASSERT(this->status == STATUS_VIEW_CHANGE);
    ASSERT(!this->pendingFCQueries.stamps.empty());
    for (const auto &msgstamp : this->pendingFCQueries.stamps) {
        Stamp stamp;
        stamp.set_shard_num(msgstamp.shard_num);
        stamp.set_msg_num(msgstamp.msg_num);
        stamp.set_sess_num(msgstamp.sess_num);
        SendFCTxnInfoRequest(stamp);
    }
    this->fcQueriesTimeout->Reset();
}

void
ErisServer::SendEpochChangeReq()
{
    ErisToFCMessage erisToFCMessage;
    BuildFCMessage(erisToFCMessage);
    erisToFCMessage.mutable_epoch_change_req()->set_new_epoch_num(this->sessnum);

    string msgStr;
    erisToFCMessage.SerializeToString(&msgStr);
    this->fcorClient->InvokeAsync(msgStr);

    this->epochChangeReqTimeout->Reset();
}

void
ErisServer::SendEpochChangeAck()
{
    ErisToFCMessage reply;
    BuildFCMessage(reply);

    reply.mutable_epoch_change_ack()->set_last_normal_epoch(this->lastNormalSessnum);
    reply.mutable_epoch_change_ack()->set_op_num(this->lastOp);

    for (const auto &kv : this->shardToMsgnum) {
        EpochChangeAck::LogInfoPiece *log_info = reply.mutable_epoch_change_ack()->add_log_info();
        log_info->set_shard_num(kv.first);
        log_info->set_latest_msg_num(kv.second);
    }

    string replyStr;
    reply.SerializeToString(&replyStr);
    this->fcorClient->InvokeAsync(replyStr);

    this->epochChangeAckTimeout->Reset();
}

void
ErisServer::SendEpochChangeStateTransfer()
{
    if (this->pendingECStateTransfer.requests.empty()) {
        this->ecStateTransferTimeout->Stop();
        return;
    }

    for (const auto &kv : this->pendingECStateTransfer.requests) {
        if (!this->transport->SendMessageToReplica(this, kv.first, kv.second.first, kv.second.second)) {
            RWarning("Failed to send EpochChangeStateTransferRequest");
        }
    }

    this->ecStateTransferTimeout->Reset();
}

void
ErisServer::SendEpochChangeStateTransferAck()
{
    ErisToFCMessage reply;
    BuildFCMessage(reply);
    reply.mutable_epoch_change_state_transfer_ack()->set_op_num(this->lastOp);
    for (const auto &drop : this->pendingECStateTransfer.drops) {
        Stamp *stamp = reply.mutable_epoch_change_state_transfer_ack()->add_drops();
        stamp->set_shard_num(drop.shard_num);
        stamp->set_msg_num(drop.msg_num);
        stamp->set_sess_num(drop.sess_num);
    }
    string replyStr;
    reply.SerializeToString(&replyStr);
    this->fcorClient->InvokeAsync(replyStr);

    this->ecStateTransferAckTimeout->Reset();
}

inline bool
ErisServer::AmLeader() const
{
    return (this->configuration.GetLeaderIndex(this->view) == this->replicaIdx);
}

inline bool
ErisServer::CheckViewNumAndStatus(ViewNum view_num)
{
    if (view_num.sess_num() < this->sessnum || view_num.view_num() < this->view) {
        return false;
    }

    if (view_num.sess_num() > this->sessnum) {
        DetectedEpochChange(view_num.sess_num());
        return false;
    }
    if (view_num.view_num() > this->view) {
        StartViewChange(view_num.view_num());
        return false;
    }

    if (this->status != STATUS_NORMAL) {
        return false;
    }

    return true;
}

void
ErisServer::InitShardMsgNum()
{
    for (int shard_num = 0; shard_num < this->configuration.g; shard_num++) {
        this->shardToMsgnum[shard_num] = 0;
    }
}

void
ErisServer::InstallLogEntry(const viewstamp_t &vs,
                            const LogEntryState &state,
                            const RequestMessage &msg)
{
    this->lastOp = vs.opnum;
    this->nextMsgnum = vs.msgnum+1;
    this->log.Append(vs, msg.request(), state, EntryData(msg.txnid(), msg.type()));
    if (state == LOG_STATE_RECEIVED) {
        for (auto it = msg.request().ops().begin();
             it != msg.request().ops().end();
             it++) {
            this->txnLookup.insert(make_pair(MsgStamp(it->shard(),
                                                      it->msgnum(),
                                                      msg.request().sessnum()),
                                             vs.opnum));
            ASSERT(it->msgnum() > this->shardToMsgnum[it->shard()]);
            this->shardToMsgnum[it->shard()] = it->msgnum();
        }
    }
}

void
ErisServer::ClearTimeoutAndQuorums()
{
    this->startGapRequestTimeout->Stop();
    this->gapRequestTimeout->Stop();
    this->fcTxnInfoRequestTimeout->Stop();
    this->syncTimeout->Stop();
    this->leaderSyncHeardTimeout->Stop();
    this->viewChangeTimeout->Stop();
    this->stateTransferTimeout->Stop();
    this->fcQueriesTimeout->Stop();
    this->epochChangeReqTimeout->Stop();
    this->epochChangeAckTimeout->Stop();
    this->ecStateTransferTimeout->Stop();
    this->ecStateTransferAckTimeout->Stop();

    this->gapReplyQuorum.Clear();
    this->viewChangeQuorum.Clear();

    this->pendingFCQueries.stamps.clear();
    this->pendingECStateTransfer.requests.clear();
    this->pendingECStateTransfer.drops.clear();
}

void
ErisServer::ClearEpochData()
{
    this->tempDrops.clear();
    this->permDrops.clear();
    this->unDrops.clear();
    this->unDropMsgs.clear();
    this->txnLookup.clear();
    InitShardMsgNum();
}

void
ErisServer::InstallTempDrop(const MsgStamp &stamp)
{
    ASSERT(stamp.sess_num == this->sessnum);
    // Only install the temp_drop if it is not
    // already in perm_drops or un_drops
    if (this->permDrops.find(stamp) == this->permDrops.end() &&
        this->unDrops.find(stamp) == this->unDrops.end()) {
        this->tempDrops.insert(stamp);
    }
}

void
ErisServer::InstallUnDrop(const RequestMessage &un_drop)
{
    ASSERT(un_drop.request().sessnum() == this->sessnum);
    msgnum_t msg_num = 0;
    for (auto iter = un_drop.request().ops().begin();
         iter != un_drop.request().ops().end();
         iter++) {
        MsgStamp stamp(iter->shard(), iter->msgnum(), un_drop.request().sessnum());
        // Panic if FC previous decided this message should be dropped
        ASSERT(this->permDrops.find(stamp) == this->permDrops.end());
        this->unDrops.insert(stamp);
        // If we have promised to drop this message before,
        // safe to remove it
        this->tempDrops.erase(stamp);
        if ((int)iter->shard() == this->groupIdx) {
            msg_num = iter->msgnum();
        }
    }
    // un_drops should only be sent to shards involved in the transaction
    ASSERT(msg_num > 0);
    MsgStamp stamp(this->groupIdx, msg_num, un_drop.request().sessnum());
    this->unDropMsgs[stamp] = un_drop;
}

void
ErisServer::InstallPermDrop(const MsgStamp &stamp)
{
    // FC should not previous decide this is a received message
    ASSERT(this->unDrops.find(stamp) == this->unDrops.end());
    // Add it to the confirmed gaps
    this->permDrops.insert(stamp);
    // If we have promised to drop this message before,
    // safe to remove it
    this->tempDrops.erase(stamp);

    // If we have logged the operation before, mark the operation
    // as NOOP (assert that this operation should not have been executed)
    if (stamp.shard_num == (uint32_t)this->groupIdx) {
        auto it = this->txnLookup.find(stamp);
        if (it != this->txnLookup.end()) {
            LogEntry *entry = this->log.Find(it->second);
            ASSERT(entry != nullptr);
            if (entry->state == LOG_STATE_RECEIVED) {
                ASSERT(it->second > this->lastExecutedOp);
                this->log.SetStatus(it->second, LOG_STATE_NOOP);
                // Remove all relevant entries in txnLookup
                for (auto it = entry->request.ops().begin();
                     it != entry->request.ops().end();
                     ++it) {
                    this->txnLookup.erase(MsgStamp(it->shard(),
                                                   it->msgnum(),
                                                   entry->request.sessnum()));
                }
            }
        }
    }
}

void
ErisServer::MergeDropTxns(const DropTxns &drop_txns)
{
    for (auto it = drop_txns.temp_drops().begin();
         it != drop_txns.temp_drops().end();
         ++it) {
        ;
        InstallTempDrop(MsgStamp(it->shard_num(), it->msg_num(), this->sessnum));
    }
    for (auto it = drop_txns.perm_drops().begin();
         it != drop_txns.perm_drops().end();
         ++it) {
        InstallPermDrop(MsgStamp(it->shard_num(), it->msg_num(), this->sessnum));
    }
    for (auto it = drop_txns.un_drops().begin();
         it != drop_txns.un_drops().end();
         ++it) {
        InstallUnDrop(*it);
    }
}

void
ErisServer::BuildDropTxns(DropTxns &drop_txns) const
{
    for (const auto &txn : this->tempDrops) {
        Stamp stamp;
        stamp.set_shard_num(txn.shard_num);
        stamp.set_msg_num(txn.msg_num);
        *(drop_txns.add_temp_drops()) = stamp;
    }
    for (const auto &txn : this->permDrops) {
        Stamp stamp;
        stamp.set_shard_num(txn.shard_num);
        stamp.set_msg_num(txn.msg_num);
        *(drop_txns.add_perm_drops()) = stamp;
    }
    for (const auto &kv : this->unDropMsgs) {
        ASSERT(kv.first.shard_num == (uint32_t)this->groupIdx);
        *(drop_txns.add_un_drops()) = kv.second;
    }
}

void
ErisServer::CheckTempDrops() const
{
    for (const auto &stamp : this->permDrops) {
        ASSERT(this->tempDrops.find(stamp) == this->tempDrops.end());
    }
    for (const auto &stamp : this->unDrops) {
        ASSERT(this->tempDrops.find(stamp) == this->tempDrops.end());
    }
}

bool
ErisServer::MatchLogWithTempDrops()
{
    ASSERT(this->pendingFCQueries.stamps.empty());
    bool has_unresolved_temp_drops = false;
    for (const auto &stamp : this->tempDrops) {
        auto it = this->txnLookup.find(stamp);
        if (it != this->txnLookup.end()) {
            LogEntry *entry = this->log.Find(it->second);
            ASSERT(entry != nullptr);
            if (entry->state == LOG_STATE_RECEIVED) {
                has_unresolved_temp_drops = true;
                this->pendingFCQueries.stamps.insert(stamp);
            }
        }
    }
    if (has_unresolved_temp_drops) {
        SendFCQueries();
        this->pendingFCQueries.callback = [this]() {
            EnterView(this->view);
        };
        return false;
    } else {
        return true;
    }
}

void
ErisServer::BuildFCMessage(ErisToFCMessage &msg) const
{
    msg.set_shard_num(this->groupIdx);
    msg.set_replica_num(this->replicaIdx);
    msg.mutable_local_view_num()->set_sess_num(this->sessnum);
    msg.mutable_local_view_num()->set_view_num(this->view);
}

void
ErisServer::CompleteFCQuery(const MsgStamp &stamp)
{
    if (this->status == !STATUS_VIEW_CHANGE ||
        !this->fcQueriesTimeout->Active() ||
        this->pendingFCQueries.stamps.empty()) {
        return;
    }

    this->pendingFCQueries.stamps.erase(stamp);
    if (this->pendingFCQueries.stamps.empty()) {
        this->fcQueriesTimeout->Stop();
        this->pendingFCQueries.callback();
    }
}

void
ErisServer::CompleteECStateTransfer()
{
    this->ecStateTransferTimeout->Stop();
    SendEpochChangeStateTransferAck();
}

void
ErisServer::RewindLog(opnum_t opnum)
{
    ASSERT(opnum >= this->lastCommittedOp);
    if (opnum < this->lastExecutedOp) {
        RPanic("Trying to rewind operation that has already executed. However rollback is \
               not implemented yet");
    }
    this->lastOp = opnum;
    this->log.RemoveAfter(opnum);
    this->nextMsgnum = this->log.LastViewstamp().msgnum + 1;
}

} // namespace eris
} // namespace store
} // namespace specpaxos

