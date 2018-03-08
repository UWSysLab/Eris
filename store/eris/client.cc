// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/eris/client.h:
 *   Eris protocol client implementation.
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

#include "store/eris/client.h"

namespace specpaxos {
namespace store {
namespace eris {

using namespace proto;
using namespace std;

ErisClient::ErisClient(const Configuration &config,
                       Transport *transport,
                       uint64_t clientid)
    : Client(config, transport, clientid)
{
    this->txnid = (this->clientid / 10000) * 10000;
    this->pendingRequest = NULL;
    this->lastReqId = 0;
    this->requestTimeout = new Timeout(this->transport, 100, [this]() {
        Warning("Client timeout; resending request");
        SendRequest();
    });
}

ErisClient::~ErisClient()
{
    if (this->pendingRequest) {
        delete this->pendingRequest;
    }
}

void
ErisClient::Invoke(const std::map<shardnum_t, std::string> &requests,
                   g_continuation_t continuation,
                   void *arg)
{
    ASSERT(arg != nullptr);
    if (this->pendingRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    RequestType txn_type;
    if (((clientarg_t *)arg)->indep) {
        txn_type = proto::INDEPENDENT;
    } else {
        txn_type = proto::PREPARE;
    }

    map<shardnum_t, string> replies;
    for (const auto &kv : requests) {
        replies.insert(make_pair(kv.first, string()));
    }

    InvokeTxn(requests, replies, continuation, txn_type);
}

void
ErisClient::InvokeTxn(const map<shardnum_t, string> &requests,
                      const map<shardnum_t, string> &replies,
                      g_continuation_t continuation,
                      RequestType txn_type)
{
    ASSERT(this->pendingRequest == NULL);
    RequestMessage requestMessage;
    Request request;
    std::vector<int> shards;

    for (auto kv : this->replySet) {
        delete kv.second;
    }
    this->replySet.clear();
    ++this->lastReqId;
    /* COMMIT and ABORT transactions use the same
     * txnid as in the PREPARE phase.
     */
    if (txn_type == proto::PREPARE ||
        txn_type == proto::INDEPENDENT) {
        ++this->txnid;
    }
    requestMessage.set_txnid(this->txnid);
    requestMessage.set_type(txn_type);

    request.set_clientid(this->clientid);
    request.set_clientreqid(this->lastReqId);
    // Set request op to empty string. Replica when receives
    // request will look for the corresponding ShardOp and set
    // op accordingly.
    request.set_op(std::string());
    for (const auto &kv : requests) {
        specpaxos::ShardOp shard_op;
        shard_op.set_shard(kv.first);
        /* COMMIT and ABORT do not have any application level
         * messages.
         */
        if (txn_type == proto::PREPARE ||
            txn_type == proto::INDEPENDENT) {
            shard_op.set_op(kv.second);
        } else {
            shard_op.set_op("");
        }
        *(request.add_ops()) = shard_op;
        shards.push_back(kv.first);
        this->replySet[kv.first] = new QuorumSet<opnum_t, ReplyMessage>(config.QuorumSize());
    }
    *(requestMessage.mutable_request()) = request;

    this->pendingRequest = new PendingRequest(requestMessage, this->lastReqId,
                                              txn_type, requests, replies,
                                              shards, continuation);

    SendRequest();
}

void
ErisClient::Invoke(const string &request,
                   continuation_t continuation)
{
    Warning("ErisClient doesn't support Invoke without specifying shards");
}

void
ErisClient::InvokeUnlogged(int replicaIdx, const string &request,
                           continuation_t continuation,
                           timeout_continuation_t timeoutContinuation,
                           uint32_t timeout)
{
    Warning("ErisClient doesn't support InvokeUnlogged");
}

void
ErisClient::ReceiveMessage(const TransportAddress &remote,
                           const string &type,
                           const string &data,
                           void *meta_data)
{
    static proto::ReplyMessage reply;

    if (type == reply.GetTypeName()) {
        reply.ParseFromString(data);
        HandleReply(remote, reply);
    } else {
        Client::ReceiveMessage(remote, type, data, meta_data);
    }
}

void
ErisClient::HandleReply(const TransportAddress &remote,
                        const proto::ReplyMessage &msg)
{
    if (this->pendingRequest == NULL) {
        Debug("Received reply when no request was pending");
        return;
    }

    if (msg.clientid() != this->clientid) {
        Debug("Received reply for a different client");
        return;
    }

    if (msg.clientreqid() != this->pendingRequest->client_req_id) {
        Debug("Received reply for a different request");
        return;
    }

    ASSERT(this->replySet.find(msg.shard_num()) != this->replySet.end());
    if (!this->pendingRequest->has_replies[msg.shard_num()]) {
        if (auto msgs = this->replySet[msg.shard_num()]->AddAndCheckForQuorum(msg.op_num(),
                                                                              msg.replica_num(),
                                                                              msg)) {
            bool hasLeader = false;
            view_t leaderView = 0;
            int leaderIdx, matching = 0;

            // Find the leader reply in the latest view
            for (auto &kv : *msgs) {
                int replicaIdx = kv.first;
                view_t view = kv.second.view().view_num();
                if (IsLeader(view, replicaIdx) && view >= leaderView) {
                    hasLeader = true;
                    leaderView = view;
                    leaderIdx = replicaIdx;
                }
            }

            if (hasLeader) {
                // Do we have matching replies?
                const proto::ReplyMessage &leaderMessage = msgs->at(leaderIdx);
                ASSERT(leaderMessage.has_commit());

                for (auto &kv : *msgs) {
                    if (kv.second.view().view_num() == leaderMessage.view().view_num() &&
                        kv.second.view().sess_num() == leaderMessage.view().sess_num()) {
                        matching++;
                    }
                }

                if (matching >= this->config.QuorumSize()) {
                    /* All read results of general transactions are
                     * returned in the PREPARE phase. COMMIT or ABORT
                     * does not contain any application level results,
                     * so do not overwrite pendingRequest->replies.
                     */
                    if (this->pendingRequest->txn_type == proto::PREPARE ||
                        this->pendingRequest->txn_type == proto::INDEPENDENT) {
                        this->pendingRequest->replies[msg.shard_num()] = leaderMessage.reply();
                    }
                    /* If any shard decides to ABORT during the PREPARE phase,
                     * abort the transaction. (
                     */
                    if (this->pendingRequest->txn_type == proto::PREPARE &&
                        !leaderMessage.commit()) {
                        this->pendingRequest->commit = false;
                    }
                    this->pendingRequest->has_replies[msg.shard_num()] = true;
                }
            }
        }
    }

    for (const auto &kv : this->pendingRequest->has_replies) {
        if (!kv.second) {
            return;
        }
    }
    CompleteOperation(this->pendingRequest->commit);
}

void
ErisClient::CompleteOperation(bool commit)
{
    this->requestTimeout->Stop();
    for (auto kv : this->replySet) {
        delete kv.second;
    }
    this->replySet.clear();

    PendingRequest *req = this->pendingRequest;
    this->pendingRequest = NULL;

    if (req->txn_type == proto::PREPARE) {
        // Invoke commit/abort phase of the general transaction
        InvokeTxn(req->requests,
                  req->replies,
                  req->continuation,
                  commit ? proto::COMMIT : proto::ABORT);
    } else {
        req->continuation(req->requests, req->replies, req->txn_type == proto::ABORT ? false : true);
    }

    delete req;
}

void
ErisClient::SendRequest()
{
    this->transport->OrderedMulticast(this, this->pendingRequest->shards, this->pendingRequest->request_msg);

    this->requestTimeout->Reset();
}

bool
ErisClient::IsLeader(view_t view, int replicaIdx) {
    return (this->config.GetLeaderIndex(view) == replicaIdx);
}

} // namespace eris
} // namespace store
} // namespace specpaxos
