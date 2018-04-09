// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/eris/fcor.cc:
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


#include "store/eris/fcor.h"

using namespace specpaxos;
using namespace store;
using namespace eris;
using namespace proto;

Fcor::Fcor(const Configuration &config,
           Transport *transport)
    : config(config), transport(transport),
    epochNum(0), lastNormalEpoch(0), status(STATUS_NORMAL)
{
    this->sender = new TransportSender();
    this->transport->Register(this->sender, config, -1, -1);
}

Fcor::~Fcor()
{
    delete this->sender;
}

void
Fcor::ReplicaUpcall(opnum_t opnum, const string &request, string &reply,
                    void *arg, void *ret)
{
    ASSERT(arg != nullptr);

    // Only the leader should actually send messages
    // XXX TODO: handle this inside of a send function so that followers do things
    bool isLeader = ((UpcallArg *)arg)->isLeader;
    if (!isLeader) {
        return;
    }

    // Parse the message from the Eris replica
    ErisToFCMessage m;
    m.ParseFromString(request);

    // Dispatch the message to the specific handler
    switch(m.msg_case()) {
        case ErisToFCMessage::MsgCase::kTxnInfoReq:
            this->HandleTxnInfoReq(m.txn_info_req());
            break;
        case ErisToFCMessage::MsgCase::kTxnReceived:
            this->HandleTxnReceived(m.txn_received());
            break;
        case ErisToFCMessage::MsgCase::kTxnTempDrop:
            this->HandleTxnTempDropped(m.txn_temp_drop(), m.shard_num(),
                m.replica_num(), m.local_view_num().view_num());
            break;
        case ErisToFCMessage::MsgCase::kEpochChangeReq:
            this->HandleEpochChangeReq(m);
            break;
        case ErisToFCMessage::MsgCase::kEpochChangeAck:
            this->HandleEpochChangeAck(m);
            break;
        case ErisToFCMessage::MsgCase::kEpochChangeStateTransferAck:
            this->HandleEpochChangeStateTransferAck(m);
            break;
        default:
            Panic("Message type not set or not handled.");
            break;
    }
}

void
Fcor::HandleTxnInfoReq(const TxnInfoRequest &m) {
    Stamp txnNum = m.txn_num();
    TxnID id (txnNum);

    if (this->status != STATUS_NORMAL || this->epochNum != id.epochNum) {
        return;
    }

    // If we already know the result, just send it to the replicas
    if (this->txnResults.count(id)) {
        this->SendResult(id);
    }

    // Otherwise, blast out the lookup message to everyone
    TxnCheck mp = TxnCheck();
    *mp.mutable_txn_num() = txnNum;
    FCToErisMessage response = FCToErisMessage();
    *response.mutable_txn_check() = mp;
    this->transport->SendMessageToAllGroups(this->sender, response);
}

void
Fcor::HandleTxnReceived(const TxnReceived &m) {
    RequestMessage txn = m.txn();

    if (this->status != STATUS_NORMAL || this->epochNum != txn.request().sessnum()) {
        return;
    }

    // First, check to see that it wasn't already dropped
    bool shouldDrop = false;
    for (int i = 0; i < txn.request().ops().size(); i++) {
        TxnID id (txn.request().ops(i).shard(), txn.request().ops(i).msgnum(), txn.request().sessnum());

        if (this->txnResults.count(id) && this->txnResults.at(id).fate == DROPPED) {
            shouldDrop = true;
            break;
        }
    }

    TxnResult txnResult (FOUND, txn);

    if (shouldDrop) {
        txnResult.fate = DROPPED;
    }

    // Clear out all the temp drop buffers
    for (int i = 0; i < txn.request().ops().size(); i++) {
        TxnID id (txn.request().ops(i).shard(), txn.request().ops(i).msgnum(), txn.request().sessnum());

        // TODO: does this leak memory???
        this->dropRecords.erase(id);
    }

    // Now, commit the results
    for (int i = 0; i < txn.request().ops().size(); i++) {
        TxnID id (txn.request().ops(i).shard(), txn.request().ops(i).msgnum(), txn.request().sessnum());

        if (!this->txnResults.count(id)) {
            this->txnResults.insert(std::pair<TxnID, TxnResult>(id, txnResult));
            this->SendResult(id);
        }
    }
}

void
Fcor::HandleTxnTempDropped(const TxnTempDropped &m, int shardNum,
        int replicaNum, int viewNum) {

    Stamp txnNum = m.txn_num();
    TxnID id (txnNum);

    if (this->status != STATUS_NORMAL || this->epochNum != id.epochNum) {
        return;
    }

    // If we already have our result, don't need to handle it
    // TODO: check that TxnID indexing into results actually works
    if (this->txnResults.count(id)) {
        return;
    }

    // Add it to the dropped messages
    TxnDropRecord dropRecord (shardNum, replicaNum, viewNum);
    this->AddDropRecord(id, dropRecord);

    if (this->IsDropped(id)) {
        if (this->droppedTxns.find(id.shardNum) == this->droppedTxns.end()) {
            this->droppedTxns[id.shardNum] = std::set<TxnID>();
        }
        this->droppedTxns[id.shardNum].insert(id);
        // TODO: can we really just make a blank Txn like this?
        this->txnResults.insert(std::pair<TxnID, TxnResult>(id, TxnResult(DROPPED, RequestMessage())));
        this->SendResult(id);
    }
}

void
Fcor::AddDropRecord(TxnID &id, TxnDropRecord &newPromise) {
    // Initialize the buffer
    if (!this->dropRecords.count(id)) {
        // TODO: does this work? Is that the right way to initialize, assign to map?
        this->dropRecords[id] = std::map<int, std::set<TxnDropRecord>>();
    }

    if (!this->dropRecords[id].count(newPromise.shardNum)) {
        this->dropRecords[id][newPromise.shardNum] = std::set<TxnDropRecord>();
    }

    auto &oldPromises = this->dropRecords[id][newPromise.shardNum];

    auto it = oldPromises.begin();
    while (it != oldPromises.end()) {
        // Drop promises from older views
        if (it->viewNum < newPromise.viewNum) {
            // TODO: does this cause problems with iteration???
            it = oldPromises.erase(it);
            continue;
        }

        // If this promise has an older viewnum, don't add it
        if (it->viewNum > newPromise.viewNum) {
            return;
        }

        // Delete previous promises from this replica
        if (it->replicaNum == newPromise.replicaNum) {
            oldPromises.erase(it);
            break;
        }

        ++it;
    }

    // Finally, add the new promise to the set
    oldPromises.insert(newPromise);
}

/**
 * Determines if the dropRecords buffer has enough promises to drop a transaction
 */
bool
Fcor::IsDropped(TxnID &id) {
    if (!this->dropRecords.count(id)) {
        return false;
    }

    auto records = this->dropRecords[id];

    // TODO: Jialin, is config.g the number of groups??? For shame....
    for (int shardNum = 0; shardNum < this->config.g; shardNum++) {
        if (!records.count(shardNum)) {
            return false;
        }

        // Check that the quorum size is correct for each shard
        if (records[shardNum].size() < (unsigned) this->config.QuorumSize())  {
            return false;
        }

        // AddDropRecord ensures the view numbers match...

        // Ensure there's one from the designated learner
        bool fromLearner = false;
        for (auto record : records[shardNum]) {
            if (record.replicaNum == this->config.GetLeaderIndex(record.viewNum)) {
                fromLearner = true;
                break;
            }
        }

        if (!fromLearner) {
            return false;
        }
    }

    return true;
}

void
Fcor::SendResult(TxnID &id) {
    TxnResult result = this->txnResults.at(id);

    // TODO: is this right??
    FCToErisMessage response = FCToErisMessage();

    if (result.fate == DROPPED) {
        // Is it okay to just send the result to the shard that asked?
        // Others could preemptively use the information
        // Depends on what Jialin is doing in his code...
        DropTxn m = DropTxn();
        Stamp txnNum = Stamp();
        txnNum.set_shard_num(id.shardNum);
        txnNum.set_sess_num(id.epochNum);
        txnNum.set_msg_num(id.msgNum);
        *m.mutable_txn_num() = txnNum;

        *response.mutable_drop_txn() = m;
    } else {
        TxnFound m = TxnFound();
        *m.mutable_txn() = result.txn;

        *response.mutable_txn_found() = m;
    }

    // Send the message
    this->transport->SendMessageToGroup(this->sender, id.shardNum, response);
}


void
Fcor::HandleEpochChangeReq(const ErisToFCMessage &m) {
    EpochChangeReq req = m.epoch_change_req();
    if (this->epochNum >= req.new_epoch_num()) {
        // epoch change already underway or completed
        if (this->lastNormalEpoch >= req.new_epoch_num()) {
            // Already completed the epoch change,
            // re-send the start epoch message.
            FCToErisMessage response = FCToErisMessage();
            ASSERT(this->lastStartEpochs.find(m.shard_num()) != this->lastStartEpochs.end());
            *response.mutable_start_epoch() = this->lastStartEpochs[m.shard_num()];

            this->transport->SendMessageToReplica(this->sender,
                                                  m.shard_num(),
                                                  m.replica_num(),
                                                  response);
        }
        return;
    }

    Notice("FCOR starting epoch change %d", req.new_epoch_num());
    this->status = STATUS_EPOCH_CHANGE;
    this->epochNum = req.new_epoch_num();
    this->epochChangeAcks.clear();
    this->latestMsgNums.clear();
    this->latestViewNums.clear();
    this->pendingStateTransfers.clear();

    FCToErisMessage response = FCToErisMessage();
    EpochChange epochChange = EpochChange();
    epochChange.set_new_epoch_num(this->epochNum);
    *response.mutable_epoch_change() = epochChange;

    this->transport->SendMessageToAllGroups(this->sender, response);
}

void
Fcor::HandleEpochChangeAck(const ErisToFCMessage &m) {
    const auto &ack = m.epoch_change_ack();
    if (this->status == STATUS_NORMAL || this->epochNum != m.local_view_num().sess_num()) {
        // epoch change over or for previous epoch,
        // re-send the last StartEpoch message
        ASSERT(this->epochNum >= m.local_view_num().sess_num());
        ASSERT(this->lastStartEpochs.find(m.shard_num()) != this->lastStartEpochs.end());

        FCToErisMessage response = FCToErisMessage();
        *response.mutable_start_epoch() = this->lastStartEpochs[m.shard_num()];

        this->transport->SendMessageToReplica(this->sender,
                                              m.shard_num(), m.replica_num(), response);

        return;
    }

    if (this->status == STATUS_PENDING_STATE_TRANSFER) {
        // Resend state transfer messages
        if (this->pendingStateTransfers.find(std::make_pair(m.shard_num(), m.replica_num())) !=
            this->pendingStateTransfers.end()) {
            this->transport->SendMessageToReplica(this->sender,
                                                  m.shard_num(),
                                                  m.replica_num(),
                                                  this->pendingStateTransfers[std::make_pair(m.shard_num(), m.replica_num())]);
        }
        return;
    }

    // Update the latest view for this shard
    int latestView;
    if (!this->latestViewNums.count(m.shard_num())) {
        latestView = -1;
    } else {
        latestView = this->latestViewNums.at(m.shard_num());
    }

    if ((int)m.local_view_num().view_num() > latestView) {
        this->latestViewNums[m.shard_num()] = m.local_view_num().view_num();
    }

    // Update the "front" for each shard.
    // Only do this if the last_normal_epoch is FC's
    // lastStartEpoch.
    // Use the longest log for each shard.
    ASSERT(ack.last_normal_epoch() <= this->lastNormalEpoch);
    if (ack.last_normal_epoch() == this->lastNormalEpoch) {
        if (!this->latestMsgNums.count(m.shard_num())) {
            this->latestMsgNums[m.shard_num()] = LatestMsgRecord();
        }

        if (ack.op_num() > this->latestMsgNums[m.shard_num()].op_num) {
            this->latestMsgNums[m.shard_num()] = LatestMsgRecord(m);
        }
    }

    // Now, add the response to the list
    if (!this->epochChangeAcks.count(m.shard_num())) {
        this->epochChangeAcks.insert(std::pair<int, std::set<int>>(
            m.shard_num(), std::set<int>()));
    }
    this->epochChangeAcks.at(m.shard_num()).insert(m.replica_num());

    // Check if there are enough responses, if so start the epoch
    bool canStartEpoch = true;
    for (int i = 0; i < this->config.g; i++) {
        if (!this->epochChangeAcks.count(i) ||
            this->epochChangeAcks.at(i).size() < (unsigned) this->config.QuorumSize()) {
            canStartEpoch = false;
        }
    }

    if (!canStartEpoch) {
        return;
    }

    for (int i = 0; i < this->config.g; i++) {
        FCToErisMessage msg;
        EpochChangeStateTransfer stateTransfer;
        stateTransfer.set_epoch_num(this->epochNum);
        stateTransfer.set_state_transfer_epoch_num(this->lastNormalEpoch);

        // Add all perm drops for this shard
        if (this->droppedTxns.find(i) != this->droppedTxns.end() &&
            !this->droppedTxns[i].empty()) {
            for (auto txnid : this->droppedTxns[i]) {
                auto *stamp = stateTransfer.add_perm_drops();
                stamp->set_shard_num(txnid.shardNum);
                stamp->set_msg_num(txnid.msgNum);
                stamp->set_sess_num(txnid.epochNum);
            }
        }

        // Check if other shards have received more txns
        ASSERT(this->latestMsgNums.find(i) != this->latestMsgNums.end());
        const LatestMsgRecord &entry = this->latestMsgNums.at(i);
        for (int j = 0; j < this->config.g; j++) {
            if (this->latestMsgNums[j].msg_nums[i] > entry.msg_nums.at(i)) {
                EpochChangeStateTransfer::MsgEntry *msgEntry = stateTransfer.add_latest_msgs();
                msgEntry->set_shard_num(j);
                msgEntry->set_replica_num(this->latestMsgNums[j].replica_num);
                msgEntry->set_msg_num(this->latestMsgNums[j].msg_nums[i]);
            }
        }

        // Only send StateTransfer to the replica with the
        // longest log.
        // XXX Assume these replicas will not fail during epoch change
        if (stateTransfer.perm_drops_size() > 0 || stateTransfer.latest_msgs_size() > 0) {
            *(msg.mutable_epoch_change_state_transfer()) = stateTransfer;
            this->pendingStateTransfers[std::make_pair(i, this->latestMsgNums[i].replica_num)] = msg;
            this->transport->SendMessageToReplica(this->sender,
                                                  i,
                                                  this->latestMsgNums[i].replica_num,
                                                  msg);
            this->status = STATUS_PENDING_STATE_TRANSFER;
        }
    }

    if (this->status == STATUS_PENDING_STATE_TRANSFER) {
        return;
    }

    ASSERT(this->pendingStateTransfers.empty());
    StartEpoch();
}

void
Fcor::HandleEpochChangeStateTransferAck(const proto::ErisToFCMessage &m)
{
    const auto &ack = m.epoch_change_state_transfer_ack();
    if (this->status != STATUS_PENDING_STATE_TRANSFER ||
        m.local_view_num().sess_num() != this->epochNum) {
        return;
    }

    ASSERT(!this->pendingStateTransfers.empty());
    ASSERT(this->latestMsgNums.find(m.shard_num()) != this->latestMsgNums.end());
    ASSERT(this->latestMsgNums[m.shard_num()].replica_num == m.replica_num());
    ASSERT(ack.op_num() >= this->latestMsgNums[m.shard_num()].op_num);

    this->latestMsgNums[m.shard_num()].op_num = ack.op_num();

    for (auto it = ack.drops().begin(); it != ack.drops().end(); ++it) {
        if (this->droppedTxns.find(m.shard_num()) == this->droppedTxns.end()) {
            this->droppedTxns[m.shard_num()] = std::set<TxnID>();
        }
        this->droppedTxns[m.shard_num()].insert(TxnID(*it));
    }
    this->pendingStateTransfers.erase(std::make_pair(m.shard_num(), m.replica_num()));

    if (this->pendingStateTransfers.empty()) {
        // All state transfers done, now can start the new epoch
        StartEpoch();
    }
}

void
Fcor::StartEpoch()
{
    Notice("FCOR entering epoch %d", this->epochNum);
    for (int i = 0; i < this->config.g; i++) {
        FCToErisMessage response = FCToErisMessage();
        proto::StartEpoch startEpoch = proto::StartEpoch();
        startEpoch.mutable_new_view()->set_sess_num(this->epochNum);
        startEpoch.mutable_new_view()->set_view_num(this->latestViewNums[i]);
        startEpoch.set_last_normal_epoch_num(this->lastNormalEpoch);
        if (this->droppedTxns.find(i) != this->droppedTxns.end() &&
            !this->droppedTxns[i].empty()) {
            for (auto txnid : this->droppedTxns[i]) {
                auto *stamp = startEpoch.add_perm_drops();
                stamp->set_shard_num(txnid.shardNum);
                stamp->set_msg_num(txnid.msgNum);
                stamp->set_sess_num(txnid.epochNum);
            }
        }
        startEpoch.set_latest_replica_num(this->latestMsgNums[i].replica_num);
        startEpoch.set_latest_op_num(this->latestMsgNums[i].op_num);
        *response.mutable_start_epoch() = startEpoch;

        this->transport->SendMessageToGroup(this->sender, i, response);
        this->lastStartEpochs[i] = startEpoch;
    }

    this->txnResults.clear();
    this->dropRecords.clear();
    this->droppedTxns.clear();
    this->epochChangeAcks.clear();
    this->lastNormalEpoch = this->epochNum;
    this->status = STATUS_NORMAL;
}
