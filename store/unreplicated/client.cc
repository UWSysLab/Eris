// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/unreplicated/client.cc:
 * Unreplicated transaction client implementation.
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

#include "store/unreplicated/client.h"

namespace specpaxos {
namespace store {
namespace unreplicated {

using namespace std;
using namespace proto;

UnreplicatedClient::UnreplicatedClient(const Configuration &config,
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

UnreplicatedClient::~UnreplicatedClient()
{
    if (this->pendingRequest) {
        delete this->pendingRequest;
    }
}

void
UnreplicatedClient::Invoke(const map<shardnum_t, string> &requests,
                           g_continuation_t continuation,
                           void *arg)
{
    if (this->pendingRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    RequestMessage::RequestType type;
    if (((clientarg_t *)arg)->indep) {
        type = RequestMessage::INDEPENDENT;
    } else {
        type = RequestMessage::PREPARE;
    }

    map<shardnum_t, string> replies;

    /* Construct replies (with empty reply string) */
    for (auto &kv : requests) {
        replies.insert(std::make_pair(kv.first, std::string()));
    }

    InvokeTxn(requests, replies, continuation, type);
}

void
UnreplicatedClient::InvokeTxn(const map<shardnum_t, string> &requests,
                              const map<shardnum_t, string> &replies,
                              g_continuation_t continuation,
                              RequestMessage::RequestType type)
{
    ASSERT(this->pendingRequest == nullptr);
    this->replySet.Clear();
    ++this->lastReqId;
    if (type == RequestMessage::PREPARE ||
        type == RequestMessage::INDEPENDENT) {
        ++this->txnid;
    }
    this->pendingRequest = new PendingRequest(this->lastReqId, this->txnid, requests, replies, type, continuation);
    this->replySet.SetShardRequired(this->pendingRequest->client_req_id, requests.size());
    SendRequest();
}

void
UnreplicatedClient::Invoke(const string &request,
                           continuation_t continuation)
{
    Warning("UnreplicatedClient doesn't support Invoke without specifying shards");
}

void
UnreplicatedClient::InvokeUnlogged(int replicaIdx, const string &request,
                                   continuation_t continuation,
                                   timeout_continuation_t timeoutContinuation,
                                   uint32_t timeout)
{
    Warning("UnreplicatedClient doesn't support InvokeUnlogged");
}

void
UnreplicatedClient::ReceiveMessage(const TransportAddress &remote,
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
UnreplicatedClient::HandleReply(const TransportAddress &remote,
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
        bool commit = true;
        if (this->pendingRequest->type == RequestMessage::PREPARE ||
            this->pendingRequest->type == RequestMessage::INDEPENDENT) {
            /* Fill out the replies in pendingRequest (from received messages) */
            for (auto &kv : this->pendingRequest->replies) {
                auto &shardMessages = msgs->at(kv.first);

                /* Each shard should have only one reply */
                ASSERT(shardMessages.size() == 1);
                for (auto &kv2 : shardMessages) {
                    kv.second = kv2.second.reply();
                    if (!kv2.second.commit()) {
                        commit = false;
                    }
                }
            }
        }
        CompleteOperation(commit);
    }
}

void
UnreplicatedClient::CompleteOperation(bool commit)
{
    this->requestTimeout->Stop();
    this->replySet.Clear();

    PendingRequest *req = this->pendingRequest;
    this->pendingRequest = NULL;

    if (req->type == RequestMessage::PREPARE) {
        InvokeTxn(req->requests,
                  req->replies,
                  req->continuation,
                  commit ? RequestMessage::COMMIT : RequestMessage::ABORT);
    } else {
        req->continuation(req->requests, req->replies, req->type == RequestMessage::ABORT ? false : true);
    }

    delete req;
}

void
UnreplicatedClient::SendRequest()
{
    RequestMessage msg;
    msg.set_txnid(this->pendingRequest->txnid);
    msg.set_type(this->pendingRequest->type);
    Request request;
    request.set_clientid(this->clientid);
    request.set_clientreqid(this->pendingRequest->client_req_id);

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

} // namespace unreplicated
} // namespace store
} // namespace specpaxos
