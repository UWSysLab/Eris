// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/granola/client.cc:
 * Granola protocol client implementation.
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

#include "store/granola/client.h"

namespace specpaxos {
namespace store {
namespace granola {

using namespace std;
using namespace proto;

GranolaClient::GranolaClient(const Configuration &config,
                             Transport *transport,
                             uint64_t clientid)
    : Client(config, transport, clientid),
    replySet(1)
{
    this->txnid = (this->clientid / 10000) * 10000;
    this->pendingRequest = NULL;
    this->lastReqId = 0;
    this->requestTimeout = new Timeout(this->transport, 50, [this]() {
        Warning("Client timeout; resending request");
        SendRequest();
    });
}

GranolaClient::~GranolaClient()
{
    if (this->pendingRequest) {
        delete this->pendingRequest;
    }
}

void
GranolaClient::Invoke(const map<shardnum_t, string> &requests,
                      g_continuation_t continuation,
                      void *arg)
{
    ASSERT(arg != nullptr);
    if (this->pendingRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    map<shardnum_t, string> replies;

    ++this->lastReqId;
    ++this->txnid;

    this->pendingRequest = new PendingRequest(this->txnid, this->lastReqId, requests, replies,
                                              continuation, *(clientarg_t*)arg);

    this->replySet.SetShardRequired(this->pendingRequest->client_req_id, requests.size());
    SendRequest();
}

void
GranolaClient::Invoke(const string &request,
                      continuation_t continuation)
{
    Warning("GranolaClient doesn't support Invoke without specifying shards");
}

void
GranolaClient::InvokeUnlogged(int replicaIdx, const string &request,
                              continuation_t continuation,
                              timeout_continuation_t timeoutContinuation,
                              uint32_t timeout)
{
    Warning("GranolaClient doesn't support InvokeUnlogged");
}

void
GranolaClient::ReceiveMessage(const TransportAddress &remote,
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
GranolaClient::HandleReply(const TransportAddress &remote,
                           const proto::ReplyMessage &msg)
{
    if (this->pendingRequest == NULL) {
        Debug("Received reply when no request was pending");
        return;
    }


    if (msg.clientreqid() != this->pendingRequest->client_req_id) {
        Debug("Received reply for a different request");
        return;
    }

    /* Replica num not relevant here, just use 0 */
    if (auto msgs = this->replySet.AddAndCheckForQuorum(msg.clientreqid(),
                                                        msg.shard_num(),
                                                        0,
                                                        msg)) {
        ASSERT(msgs->size() == this->pendingRequest->requests.size());
        proto::Status status = msgs->begin()->second.at(0).status();
        /* Fill out the replies in pendingRequest (from received messages) */
        for (const auto &kv : *msgs) {
            ASSERT(this->pendingRequest->requests.find(kv.first) != this->pendingRequest->requests.end());
            ASSERT(kv.second.size() == 1);
            ASSERT(kv.second.find(0) != kv.second.end());
            ASSERT(kv.second.at(0).status() == status);
            this->pendingRequest->replies[kv.first] = kv.second.at(0).reply();
        }
        ASSERT(this->pendingRequest->requests.size() == this->pendingRequest->replies.size());
        CompleteOperation(status);
    }
}

void
GranolaClient::CompleteOperation(proto::Status status)
{
    this->requestTimeout->Stop();
    this->replySet.Clear();

    PendingRequest *req = this->pendingRequest;

    if (status == proto::CONFLICT) {
        // Retry transaction
        req->num_retries++;
        if (req->num_retries < MAX_RETRIES) {
            usleep(rand()%RETRY_SLEEP+1);
            RetryTransaction();
            return;
        }
        // Otherwise just abort
        status = proto::ABORT;
    }
    this->pendingRequest = NULL;

    ASSERT(status == proto::COMMIT || status == proto::ABORT);
    req->continuation(req->requests, req->replies, status == proto::COMMIT);

    delete req;
}

void
GranolaClient::RetryTransaction()
{
    ASSERT(this->pendingRequest != NULL);
    this->pendingRequest->replies.clear();
    ++this->lastReqId;
    this->pendingRequest->client_req_id = this->lastReqId;
    // Do not increment txnid
    this->replySet.SetShardRequired(this->lastReqId, this->pendingRequest->requests.size());
    SendRequest();
}

void
GranolaClient::SendRequest()
{
    RequestMessage msg;
    msg.set_txnid(this->pendingRequest->txnid);
    msg.set_indep(this->pendingRequest->arg.indep);
    msg.set_ro(this->pendingRequest->arg.ro);
    Request request;
    request.set_clientid(this->clientid);
    request.set_clientreqid(this->pendingRequest->client_req_id);

    for (auto &kv : this->pendingRequest->requests) {
        ShardOp shardOp;
        shardOp.set_shard(kv.first);
        // We will set op for each shard in the sending loop
        shardOp.set_op("");
        *(request.add_ops()) = shardOp;
    }

    for (auto &kv : this->pendingRequest->requests) {
        request.set_op(kv.second);
        *(msg.mutable_request()) = request;
        if (!this->transport->SendMessageToGroup(this,
                                                 kv.first,
                                                 msg)) {
            Warning("Failed to send request to group %u", kv.first);
        }
    }

    this->requestTimeout->Reset();
}

} // namespace granola
} // namespace store
} // namespace specpaxos
