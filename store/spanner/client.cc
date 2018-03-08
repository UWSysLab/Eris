// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/spanner/client.cc:
 * Spanner protocol client implementation.
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

#include "store/spanner/client.h"

namespace specpaxos {
namespace store {
namespace spanner {

using namespace std;
using namespace proto;

SpannerClient::SpannerClient(const Configuration &config,
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

SpannerClient::~SpannerClient()
{
    if (this->pendingRequest) {
        delete this->pendingRequest;
    }
}

void
SpannerClient::Invoke(const map<shardnum_t, string> &requests,
                      g_continuation_t continuation,
                      void *arg)
{
    ASSERT(arg != nullptr);
    if (this->pendingRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++this->lastReqId;
    ++this->txnid;
    this->pendingRequest = new PendingRequest(this->txnid, this->lastReqId, requests, proto::PREPARE, continuation);
    this->replySet.SetShardRequired(this->lastReqId, requests.size());

    SendRequest();
}

void
SpannerClient::Invoke(const string &request,
                      continuation_t continuation)
{
    Warning("SpannerClient doesn't support Invoke without specifying shards");
}

void
SpannerClient::InvokeUnlogged(int replicaIdx, const string &request,
                              continuation_t continuation,
                              timeout_continuation_t timeoutContinuation,
                              uint32_t timeout)
{
    Warning("SpannerClient doesn't support InvokeUnlogged");
}

void
SpannerClient::ReceiveMessage(const TransportAddress &remote,
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
SpannerClient::HandleReply(const TransportAddress &remote,
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

    if (auto msgs = this->replySet.AddAndCheckForQuorum(msg.clientreqid(),
                                                        msg.shard_num(),
                                                        0,
                                                        msg)) {
        Fate fate = FATE_COMMIT;
        for (const auto &kv : *msgs) {
            ASSERT(kv.second.size() == 1);
            ASSERT(kv.second.find(0) != kv.second.end());
            // An ACK reply is for commit/abort, no
            // need to record the result.
            if (kv.second.at(0).type() == proto::ACK) {
                ASSERT(this->pendingRequest->type == proto::COMMIT ||
                       this->pendingRequest->type == proto::ABORT);
                fate = FATE_ACKED;
            } else {
                ASSERT(this->pendingRequest->type == proto::PREPARE);
                this->pendingRequest->replies[kv.first] = kv.second.at(0).reply();
                if (kv.second.at(0).type() == proto::FAIL) {
                    fate = FATE_ABORT;
                    break;
                } else if (kv.second.at(0).type() == proto::RETRY) {
                    fate = FATE_RETRY;
                    // No break here, some other shard may
                    // decide to abort.
                }
            }
        }
        CompleteOperation(fate);
    }
}

void
SpannerClient::CompleteOperation(Fate fate)
{
    this->requestTimeout->Stop();
    this->replySet.Clear();

    if (fate == FATE_ACKED) {
        // Txn is finished
        ASSERT(this->pendingRequest->type == proto::COMMIT || this->pendingRequest->type == proto::ABORT);
        PendingRequest *req = this->pendingRequest;
        this->pendingRequest = NULL;

        req->continuation(req->requests, req->replies, req->type == proto::COMMIT ? true : false);

        delete req;
    } else {
        ASSERT(this->pendingRequest->type == PREPARE);
        ++this->lastReqId;
        this->pendingRequest->client_req_id = this->lastReqId;
        this->replySet.SetShardRequired(this->lastReqId, this->pendingRequest->requests.size());

        if (fate == FATE_RETRY) {
            // Prepare cannot acquire all locks. Retry prepare.
            this->pendingRequest->num_retries++;
            if (this->pendingRequest->num_retries < MAX_RETRIES) {
                Debug("Cannot acquire all locks... retry number %d", this->pendingRequest->num_retries);
                SendRequest();
                return;
            }
            // We have tried too many times...Just abort.
            fate = FATE_ABORT;
        }

        ASSERT(fate == FATE_COMMIT || fate == FATE_ABORT);
        // Commit or abort the transaction
        this->pendingRequest->type = fate == FATE_COMMIT ? proto::COMMIT : proto::ABORT;
        SendRequest();
    }
}

void
SpannerClient::SendRequest()
{
    RequestMessage msg;
    msg.set_txnid(this->pendingRequest->txnid);
    msg.set_type(this->pendingRequest->type);
    Request request;
    request.set_clientid(this->clientid);
    request.set_clientreqid(this->pendingRequest->client_req_id);

    for (auto &kv : this->pendingRequest->requests) {
        // Only PREPARE contains actual requests
        if (this->pendingRequest->type == proto::PREPARE) {
            request.set_op(kv.second);
        } else {
            request.set_op("");
        }
        *(msg.mutable_request()) = request;
        if (!this->transport->SendMessageToGroup(this,
                                                 kv.first,
                                                 msg)) {
            Warning("Failed to send request to group %u", kv.first);
        }
    }

    this->requestTimeout->Reset();
}

} // namespace spanner
} // namespace store
} // namespace specpaxos

