// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/spanner/server.cc:
 *   Spanner protocol server implementation.
 *
 * Copyright 2017 Jialin Li <lijl@cs.washington.edu>
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

#include "store/spanner/server.h"

#define RDebug(fmt, ...) Debug("[%d, %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)

namespace specpaxos {
namespace store {
namespace spanner {

using namespace std;
using namespace proto;

SpannerServer::SpannerServer(const Configuration&config, int myShard, int myIdx,
                             bool initialize, Transport *transport, AppReplica *app)
    : Replica(config, myShard, myIdx, initialize, transport, app),
    log(false),
    prepareOKQuorum(config.QuorumSize()-1)
{
    this->view = 0;
    this->lastOp = 0;
    this->lastCommitted = 0;

    this->resendPrepareTimeout = new Timeout(transport, RESEND_PREPARE_TIMEOUT, [this, myShard, myIdx]() {
        RWarning("Prepare timeout! Resending Prepare");
        SendPrepare();
    });
}

SpannerServer::~SpannerServer()
{
    delete this->resendPrepareTimeout;
}

void
SpannerServer::ReceiveMessage(const TransportAddress &remote,
                              const string &type, const string &data,
                              void *meta_data)
{
    static RequestMessage requestMessage;
    static PrepareMessage prepareMessage;
    static PrepareOKMessage prepareOKMessage;
    static CommitMessage commitMessage;

    if (type == requestMessage.GetTypeName()) {
        requestMessage.ParseFromString(data);
        HandleClientRequest(remote, requestMessage);
    } else if (type == prepareMessage.GetTypeName()) {
        prepareMessage.ParseFromString(data);
        HandlePrepare(remote, prepareMessage);
    } else if (type == prepareOKMessage.GetTypeName()) {
        prepareOKMessage.ParseFromString(data);
        HandlePrepareOK(remote, prepareOKMessage);
    } else if (type == commitMessage.GetTypeName()) {
        commitMessage.ParseFromString(data);
        HandleCommit(remote, commitMessage);
    } else {
        Panic("Received unexpected message type in SpannerServer proto: %s",
              type.c_str());
    }
}

void
SpannerServer::HandleClientRequest(const TransportAddress &remote,
                                   const RequestMessage &msg)
{
    // Save client's address if not exist. Assume client
    // addresses never change.
    if (this->clientAddresses.find(msg.request().clientid()) == this->clientAddresses.end()) {
        this->clientAddresses.insert(std::pair<uint64_t, std::unique_ptr<TransportAddress> >(msg.request().clientid(), std::unique_ptr<TransportAddress>(remote.clone())));
    }

    // Non-leader replica ignore client requests
    if (!AmLeader()) {
        return;
    }

    // Check the client table to see if this is a duplicate request
    auto kv = this->clientTable.find(msg.request().clientid());
    if (kv != this->clientTable.end()) {
        const ClientTableEntry &entry = kv->second;
        if (msg.request().clientreqid() < entry.lastReqId) {
            RDebug("Ignoring stale request");
            return;
        }
        if (msg.request().clientreqid() == entry.lastReqId) {
            // This is a duplicate request. Resend the reply if we
            // have one. We might not have a reply to resend if we're
            // waiting for the other replicas; in that case, just
            // discard the request.
            if (entry.replied) {
                if (!(this->transport->SendMessage(this, remote,
                                                   entry.reply))) {
                    RWarning("Failed to resend reply to client");
                }
                return;
            } else {
                RDebug("Received duplicate request but no reply available; ignoring");
                return;
            }
        }
    }

    // Update the client table
    UpdateClientTable(msg.request());

    ++this->lastOp;
    viewstamp_t v;
    v.view = this->view;
    v.opnum = this->lastOp;

    // Add the request to my log
    ASSERT(msg.type() != proto::UNKNOWN);
    EntryData entryData(msg.txnid(), msg.type());

    this->log.Append(v, msg.request(), LOG_STATE_PREPARED, entryData);

    // Send PrepareMessage to other replicas
    SendPrepare();
}

void
SpannerServer::HandlePrepare(const TransportAddress &remote,
                             const PrepareMessage &msg)
{
    ASSERT(!AmLeader());

    if (msg.opnum() <= this->lastOp) {
        // Resend the prepareOK message
        PrepareOKMessage prepareOKMessage;
        prepareOKMessage.set_view(msg.view());
        prepareOKMessage.set_opnum(msg.opnum());
        prepareOKMessage.set_replica_num(this->replicaIdx);
        if (!(transport->SendMessageToReplica(this,
                                              configuration.GetLeaderIndex(view),
                                              prepareOKMessage))) {
            RWarning("Failed to send PrepareOK message to leader");
        }
        return;
    }

    /*
    if (msg.opnum() > this->lastOp + 1) {
        Panic("State transfer not implemented yet");
    }

    ASSERT(msg.opnum() == this->lastOp + 1);
    */
    // XXX Hack here to get around state transfer
    while (this->lastOp + 1 < msg.opnum()) {
        this->lastOp++;
        this->log.Append(viewstamp_t(msg.view(), this->lastOp), Request(), LOG_STATE_EXECUTED);
    }

    this->lastOp++;
    ASSERT(msg.type() != proto::UNKNOWN);
    EntryData entryData(msg.txnid(), msg.type());

    this->log.Append(viewstamp_t(msg.view(), this->lastOp), msg.request(), LOG_STATE_PREPARED,
                     entryData);
    UpdateClientTable(msg.request());

    PrepareOKMessage prepareOKMessage;
    prepareOKMessage.set_view(msg.view());
    prepareOKMessage.set_opnum(msg.opnum());
    prepareOKMessage.set_replica_num(this->replicaIdx);
    if (!this->transport->SendMessageToReplica(this,
                                               this->configuration.GetLeaderIndex(view),
                                               prepareOKMessage)) {
        RWarning("Failed to send PrepareOK message to leader");
    }
}

void
SpannerServer::HandlePrepareOK(const TransportAddress &remote,
                               const PrepareOKMessage &msg)
{
    ASSERT(AmLeader());

    viewstamp_t vs = { msg.view(), msg.opnum() };

    if (this->prepareOKQuorum.AddAndCheckForQuorum(vs, msg.replica_num(), msg)) {
        /* CommitUpTo will send Commit message */
        CommitUpTo(msg.opnum());
    }
}

void
SpannerServer::HandleCommit(const TransportAddress &remote,
                            const CommitMessage &msg)
{
    ASSERT(!AmLeader());

    if (msg.opnum() > this->lastOp) {
        // XXX Should do state transfer here
        //Panic("State transfer not implemented yet");
        return;
    }

    if (msg.opnum() <= this->lastCommitted) {
        // Already committed
        return;
    }

    CommitUpTo(msg.opnum());
}

void
SpannerServer::CommitUpTo(opnum_t opnum)
{
    if (this->lastCommitted < opnum && AmLeader()) {
        /* Leader send Commit message */
        CommitMessage commitMessage;
        commitMessage.set_view(this->view);
        commitMessage.set_opnum(opnum);

        if (!this->transport->SendMessageToAll(this,
                                               commitMessage)) {
            RWarning("Failed to send COMMIT message to all replicas");
        }
    }

    if (opnum > this->lastCommitted) {
        this->resendPrepareTimeout->Stop();
    }

    while (this->lastCommitted < opnum) {
        this->lastCommitted++;
        LogEntry *entry = this->log.Find(this->lastCommitted);
        ASSERT(entry != nullptr);
        // XXX Hack to get around state transfer
        if (entry->state != LOG_STATE_EXECUTED) {
            this->log.SetStatus(this->lastCommitted, LOG_STATE_COMMITTED);

            ExecuteTxn(entry);
        }
    }
}

void
SpannerServer::ExecuteTxn(LogEntry *entry)
{
    ASSERT(entry != nullptr);
    ASSERT(entry->state == LOG_STATE_COMMITTED);

    ReplyMessage reply;
    txnarg_t arg;
    txnret_t ret;
    arg.txnid = entry->data.txnid;
    ASSERT(entry->data.type != proto::UNKNOWN);
    arg.type = entry->data.type == proto::PREPARE ? TXN_PREPARE :
        (entry->data.type == proto::COMMIT ? TXN_COMMIT : TXN_ABORT);

    Execute(entry->viewstamp.opnum, entry->request, reply, (void *)&arg, (void *)&ret);

    ASSERT(ret.unblocked_txns.empty());
    if (entry->data.type == proto::COMMIT || entry->data.type == proto::ABORT) {
        ASSERT(ret.commit);
        ASSERT(!ret.blocked);
        reply.set_type(proto::ACK);
    } else {
        // Prepare reply
        reply.set_type(ret.blocked ? proto::RETRY :
                       (ret.commit ? proto::OK : proto::FAIL));
    }

    reply.set_clientreqid(entry->request.clientreqid());
    reply.set_shard_num(this->groupIdx);

    // Update client table
    ClientTableEntry &cte = this->clientTable[entry->request.clientid()];
    // XXX Hack here to work around state transfer
    cte.lastReqId = entry->request.clientreqid();
    //ASSERT(cte.lastReqId == entry->request.clientreqid());
    cte.replied = true;
    cte.reply = reply;

    // Only leader replies to client
    if (this->configuration.GetLeaderIndex(entry->viewstamp.view) == this->replicaIdx) {
        auto iter = this->clientAddresses.find(entry->request.clientid());
        if (iter != this->clientAddresses.end()) {
            if (!this->transport->SendMessage(this, *iter->second, reply)) {
                RWarning("Failed to send ReplyMessage to client");
            }
        }
    }
}

void
SpannerServer::UpdateClientTable(const Request &req)
{
    ClientTableEntry &entry = this->clientTable[req.clientid()];

    ASSERT(entry.lastReqId <= req.clientreqid());

    if (entry.lastReqId == req.clientreqid()) {
        return;
    }

    entry.lastReqId = req.clientreqid();
    entry.replied = false;
    entry.reply.Clear();
}

void
SpannerServer::SendPrepare()
{
    LogEntry *entry = this->log.Find(this->lastOp);
    ASSERT(entry != nullptr);
    PrepareMessage prepareMessage;
    prepareMessage.set_view(entry->viewstamp.view);
    prepareMessage.set_opnum(entry->viewstamp.opnum);
    prepareMessage.set_txnid(entry->data.txnid);
    prepareMessage.set_type(entry->data.type);
    *(prepareMessage.mutable_request()) = entry->request;

    if (!this->transport->SendMessageToAll(this,
                                           prepareMessage)) {
        RWarning("Failed to send Prepare message");
    }

    this->resendPrepareTimeout->Reset();
}

inline bool
SpannerServer::AmLeader()
{
    return (this->configuration.GetLeaderIndex(this->view) == this->replicaIdx);
}

} // namespace granola
} // namespace store
} // namespace specpaxos
