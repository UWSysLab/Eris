// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/unreplicated/server.cc:
 *   Unreplicated transaction server implementation.
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

#include "store/unreplicated/server.h"

#define RDebug(fmt, ...) Debug("[%d, %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d %d] " fmt, this->groupIdx, this->replicaIdx, ##__VA_ARGS__)

namespace specpaxos {
namespace store {
namespace unreplicated {

using namespace std;
using namespace proto;

UnreplicatedServer::UnreplicatedServer(const Configuration&config, int myShard, int myIdx,
                                       bool initialize, Transport *transport, AppReplica *app)
    : Replica(config, myShard, myIdx, initialize, transport, app),
    log(false)
{
    this->lastOp = 0;
}

UnreplicatedServer::~UnreplicatedServer()
{
}

void
UnreplicatedServer::ReceiveMessage(const TransportAddress &remote,
                                   const string &type, const string &data,
                                   void *meta_data)
{
    static RequestMessage requestMessage;

    if (type == requestMessage.GetTypeName()) {
        requestMessage.ParseFromString(data);
        HandleClientRequest(remote, requestMessage);
    } else {
        Panic("Received unexpected message type in UnreplicatedServer proto: %s",
              type.c_str());
    }
}

void
UnreplicatedServer::HandleClientRequest(const TransportAddress &remote,
                                        const RequestMessage &msg)
{
    // Save client's address if not exist. Assume client
    // addresses never change.
    if (this->clientAddresses.find(msg.request().clientid()) == this->clientAddresses.end()) {
        this->clientAddresses.insert(std::pair<uint64_t, std::unique_ptr<TransportAddress> >(msg.request().clientid(), std::unique_ptr<TransportAddress>(remote.clone())));
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
            // This is a duplicate request. Resend the reply
            if (!(this->transport->SendMessage(this, remote,
                                               entry.reply))) {
                RWarning("Failed to resend reply to client");
            }
            return;
        }
    }

    ++this->lastOp;
    viewstamp_t v;
    v.opnum = this->lastOp;

    this->log.Append(v, msg.request(), LOG_STATE_EXECUTED);

    ReplyMessage reply;
    txnarg_t arg;
    txnret_t ret;
    arg.txnid = msg.txnid();
    ASSERT(msg.type() != RequestMessage::UNKNOWN);
    if (msg.type() == RequestMessage::INDEPENDENT) {
        arg.type = TXN_INDEP;
    } else if (msg.type() == RequestMessage::PREPARE) {
        arg.type = TXN_PREPARE;
    } else if (msg.type() == RequestMessage::COMMIT) {
        arg.type = TXN_COMMIT;
    } else {
        arg.type = TXN_ABORT;
    }
    Execute(v.opnum, msg.request(), reply, &arg, &ret);
    reply.set_clientreqid(msg.request().clientreqid());
    reply.set_shard_num(this->groupIdx);
    reply.set_commit(ret.commit ? 1 : 0);

    UpdateClientTable(msg.request(), reply);

    if (!this->transport->SendMessage(this, remote, reply)) {
        RWarning("Failed to send ReplyMessage to client");
    }
}

void
UnreplicatedServer::UpdateClientTable(const Request &req, const ReplyMessage &reply)
{
    ClientTableEntry &entry = this->clientTable[req.clientid()];

    ASSERT(entry.lastReqId <= req.clientreqid());

    if (entry.lastReqId == req.clientreqid()) {
        return;
    }

    entry.lastReqId = req.clientreqid();
    entry.reply = reply;
}

} // namespace unreplicated
} // namespace store
} // namespace specpaxos

