  // -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
  /***********************************************************************
 *
 * store/tapir/irclient.cc:
 *   Tapir inconsistent replication client
 *
 * Copyright 2013-2015 Dan R. K. Ports  <drkp@cs.washington.edu>
 *                     Irene Zhang Ports  <iyzhang@cs.washington.edu>
 *                     Jialin Li <lijl@cs.washington.edu>
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

#include "store/tapir/irclient.h"
#include <math.h>

namespace specpaxos {
namespace store {
namespace tapir {

using namespace std;
using namespace proto;

IRClient::IRClient(const Configuration &config,
                   Transport *transport,
                   shardnum_t shard,
                   uint64_t clientid)
    : Client(config, transport, clientid),
      myShard(shard),
      view(0),
      lastReqId(0),
      waiting(nullptr),
      blockingPrepare(nullptr)
{

}

IRClient::~IRClient()
{
    for (auto kv : pendingReqs) {
	delete kv.second;
    }
}

void
IRClient::Invoke(const string &request,
                 continuation_t continuation)
{
    InvokeInconsistent(request, continuation);
}

void
IRClient::InvokeUnlogged(int replicaIdx,
                         const string &request,
                         continuation_t continuation,
                         timeout_continuation_t timeoutContinuation,
                         uint32_t timeout)
{
    Warning("IRClient doesn't support InvokeUnlogged");
}

void
IRClient::Invoke(const map<shardnum_t, string> &requests,
                 g_continuation_t continuation,
                 void *arg)
{
    Panic("IRClient has no support for group invoke");
}

void
IRClient::Done()
{
    if (this->blockingPrepare != nullptr) {
        this->blockingPrepare->GetReply();
        delete this->blockingPrepare;
        this->blockingPrepare = nullptr;
    }
}

void
IRClient::Prepare(txnid_t txnid, const string &txn, Promise *promise)
{
    Transaction t;
    t.set_txnid(txnid);
    t.set_op(Transaction::PREPARE);
    t.set_txn(txn);
    string txnstr;
    t.SerializeToString(&txnstr);

    if (this->blockingPrepare != nullptr) {
        this->blockingPrepare->GetReply();
        delete this->blockingPrepare;
        this->blockingPrepare = nullptr;
    }

    this->transport->Timer(0, [=]() {
        this->waiting = promise;
        InvokeConsensus(txnstr,
                        bind(&IRClient::PrepareDecide,
                             this,
                             placeholders::_1),
                        bind(&IRClient::PrepareCallback,
                             this,
                             placeholders::_1,
                             placeholders::_2));
    });
}

void
IRClient::CommitAbort(txnid_t txnid, bool commit)
{
    Transaction t;
    t.set_txnid(txnid);
    t.set_op(commit ? Transaction::COMMIT : Transaction::ABORT);
    t.set_txn("");
    string txnstr;
    t.SerializeToString(&txnstr);

    this->blockingPrepare = new Promise();
    this->transport->Timer(0, [=]() {
        InvokeInconsistent(txnstr,
                           bind(&IRClient::CommitAbortCallback,
                                this,
                                placeholders::_1,
                                placeholders::_2));
    });
}

void
IRClient::InvokeInconsistent(const string &request,
                             continuation_t continuation)
{
    // Bump the request ID
    uint64_t reqId = ++lastReqId;
    // Create new timer
    Timeout *timer = new Timeout(transport, 50, [this, reqId]() {
            ResendInconsistent(reqId);
        });
    PendingInconsistentRequest *req =
	new PendingInconsistentRequest(request,
                                   reqId,
                                   continuation,
                                   timer,
                                   config.QuorumSize());
    pendingReqs[reqId] = req;
    SendInconsistent(req);
}
void
IRClient::SendInconsistent(const PendingInconsistentRequest *req)
{

    proto::ProposeInconsistentMessage reqMsg;
    reqMsg.mutable_req()->set_op(req->request);
    reqMsg.mutable_req()->set_clientid(clientid);
    reqMsg.mutable_req()->set_clientreqid(req->clientReqId);

    if (transport->SendMessageToGroup(this, this->myShard, reqMsg)) {
        req->timer->Reset();
    } else {
        Warning("Could not send inconsistent request to replicas");
        pendingReqs.erase(req->clientReqId);
        delete req;
    }
}

void
IRClient::InvokeConsensus(const string &request,
                          decide_t decide,
                          continuation_t continuation)
{
    uint64_t reqId = ++lastReqId;
    Timeout *timer = new Timeout(transport, 50, [this, reqId]() {
            Warning("Consensus operation timed out! Use slow path");
            ConsensusSlowPath(reqId);
        });

    PendingConsensusRequest *req =
	new PendingConsensusRequest(request,
				    reqId,
				    continuation,
				    timer,
				    config.QuorumSize(),
				    config.QuorumSize() + ceil(0.5 * config.QuorumSize()),
				    decide);

    proto::ProposeConsensusMessage reqMsg;
    reqMsg.mutable_req()->set_op(request);
    reqMsg.mutable_req()->set_clientid(clientid);
    reqMsg.mutable_req()->set_clientreqid(reqId);

    if (transport->SendMessageToGroup(this, this->myShard, reqMsg)) {
        req->timer->Start();
	pendingReqs[reqId] = req;
    } else {
        Warning("Could not send consensus request to replicas");
	delete req;
    }
}

void
IRClient::ResendInconsistent(const uint64_t reqId)
{

    Warning("Client timeout; resending inconsistent request: %lu", reqId);
    SendInconsistent((PendingInconsistentRequest *)pendingReqs[reqId]);
}

void
IRClient::ConsensusSlowPath(const uint64_t reqId)
{
    PendingConsensusRequest *req = static_cast<PendingConsensusRequest *>(pendingReqs[reqId]);
    // Make sure the dynamic cast worked
    ASSERT(req != NULL);

    // Give up on the fast path
    req->timer->Stop();
    delete req->timer;
    // set up a new timer for the slow path
    req->timer = new Timeout(transport, 50, [this, reqId, req]() {
        // In case less than majority replicas received
        // the operation, resend the request too.
        proto::ProposeConsensusMessage reqMsg;
        reqMsg.mutable_req()->set_op(req->request);
        reqMsg.mutable_req()->set_clientid(this->clientid);
        reqMsg.mutable_req()->set_clientreqid(req->clientReqId);
        if (!transport->SendMessageToGroup(this, this->myShard, reqMsg)) {
            Warning("Could not send consensus request to replicas");
        }
        ResendConfirmation(reqId, true);
    });


    Debug("Client timeout; taking consensus slow path: %lu", reqId);

    // get results so far
    viewstamp_t vs = { view, reqId };
    auto msgs = req->consensusReplyQuorum.GetMessages(vs);

    // construct result set
    set<string> results;
    for (auto &msg : msgs) {
        results.insert(msg.second.result());
    }

    // Upcall into the application
    ASSERT(req->decide != NULL);
    string result = req->decide(results);

    // Put the result in the request to store for later retries
    req->decideResult = result;

    // Send finalize message
    proto::FinalizeConsensusMessage response;
    response.mutable_opid()->set_clientid(clientid);
    response.mutable_opid()->set_clientreqid(req->clientReqId);
    response.set_result(result);

    if(transport->SendMessageToGroup(this, this->myShard, response)) {
        req->timer->Start();
    } else {
        Warning("Could not send finalize message to replicas");
	pendingReqs.erase(reqId);
	delete req;
    }
}

void
IRClient::ResendConfirmation(const uint64_t reqId, bool isConsensus)
{
    if (pendingReqs.find(reqId) == pendingReqs.end()) {
        Debug("Received resend request when no request was pending");
        return;
    }

    if (isConsensus) {
	PendingConsensusRequest *req = static_cast<PendingConsensusRequest *>(pendingReqs[reqId]);
	ASSERT(req != NULL);

        proto::FinalizeConsensusMessage response;
        response.mutable_opid()->set_clientid(clientid);
        response.mutable_opid()->set_clientreqid(req->clientReqId);
        response.set_result(req->decideResult);

        if(transport->SendMessageToGroup(this, this->myShard, response)) {
            req->timer->Reset();
        } else {
            Warning("Could not send finalize message to replicas");
	    // give up and clean up
	    pendingReqs.erase(reqId);
	    delete req;
        }
    } else {
	PendingInconsistentRequest *req = static_cast<PendingInconsistentRequest *>(pendingReqs[reqId]);
	ASSERT(req != NULL);

	proto::FinalizeInconsistentMessage response;
        response.mutable_opid()->set_clientid(clientid);
        response.mutable_opid()->set_clientreqid(req->clientReqId);

        if (transport->SendMessageToGroup(this, this->myShard, response)) {
	    req->timer->Reset();
	} else {
            Warning("Could not send finalize message to replicas");
	    pendingReqs.erase(reqId);
	    delete req;
        }

    }

}

void
IRClient::ReceiveMessage(const TransportAddress &remote,
                         const string &type,
                         const string &data,
                         void *meta_data)
{
    proto::ReplyInconsistentMessage replyInconsistent;
    proto::ReplyConsensusMessage replyConsensus;
    proto::ConfirmMessage confirm;

    if (type == replyInconsistent.GetTypeName()) {
        replyInconsistent.ParseFromString(data);
        HandleInconsistentReply(remote, replyInconsistent);
    } else if (type == replyConsensus.GetTypeName()) {
        replyConsensus.ParseFromString(data);
        HandleConsensusReply(remote, replyConsensus);
    } else if (type == confirm.GetTypeName()) {
        confirm.ParseFromString(data);
        HandleConfirm(remote, confirm);
    } else {
        Client::ReceiveMessage(remote, type, data, meta_data);
    }
}

void
IRClient::HandleInconsistentReply(const TransportAddress &remote,
                                  const proto::ReplyInconsistentMessage &msg)
{
    uint64_t reqId = msg.opid().clientreqid();
    auto it = pendingReqs.find(reqId);
    if (it == pendingReqs.end()) {
        Debug("Received reply when no request was pending");
        return;
    }

    PendingInconsistentRequest *req =
        static_cast<PendingInconsistentRequest *>(it->second);
    // Make sure the dynamic cast worked
    ASSERT(req != NULL);

    Debug("Client received reply: %lu %i", reqId,
          req->inconsistentReplyQuorum.NumRequired());

    // Record replies
    viewstamp_t vs = { msg.view(), reqId };
    if (req->inconsistentReplyQuorum.AddAndCheckForQuorum(vs, msg.replicaidx(), msg)) {
        // If all quorum received, then send finalize and return to client
        // Return to client
        if (!req->continuationInvoked) {
            req->timer->Stop();
            delete req->timer;
            req->timer = new Timeout(transport, 50, [this, reqId]() {
                    ResendConfirmation(reqId, false);
                });

            // asynchronously send the finalize message
            proto::FinalizeInconsistentMessage response;
            *(response.mutable_opid()) = msg.opid();

            if (transport->SendMessageToGroup(this, this->myShard, response)) {
                req->timer->Start();
            } else {
                Warning("Could not send finalize message to replicas");
            }

            req->continuation(req->request, "");
            req->continuationInvoked = true;
        }
    }
}

void
IRClient::HandleConsensusReply(const TransportAddress &remote,
                               const proto::ReplyConsensusMessage &msg)
{
    uint64_t reqId = msg.opid().clientreqid();
    auto it = pendingReqs.find(reqId);
    if (it == pendingReqs.end()) {
        Debug("Received reply when no request was pending");
        return;
    }
    PendingConsensusRequest *req = static_cast<PendingConsensusRequest *>(it->second);

    Debug("Client received reply: %lu %i", reqId, req->consensusReplyQuorum.NumRequired());

    // Record replies
    viewstamp_t vs = { msg.view(), reqId };
    if (auto msgs =
        (req->consensusReplyQuorum.AddAndCheckForQuorum(vs, msg.replicaidx(), msg))) {
        // If all quorum received, then check return values

        map<string, int> results;
        // count matching results
        for (auto &m : *msgs) {
            if (results.count(m.second.result()) > 0) {
                results[m.second.result()] = results[m.second.result()] + 1;
            } else {
                results[m.second.result()] = 1;
            }
        }

        // Check that there are a quorum of *matching* results
        bool matching = false;
        for (auto result : results) {
            if (result.second >= req->consensusReplyQuorum.NumRequired()) {
                req->timer->Stop();
                delete req->timer;
                // set up new timeout for finalize phase
                req->timer =
                    new Timeout(transport, 50, [this, reqId]() {
                            ResendConfirmation(reqId, true);
                            });

                // asynchronously send the finalize message
                proto::FinalizeConsensusMessage response;
                *response.mutable_opid() = msg.opid();
                response.set_result(result.first);

                req->decideResult = result.first;

                if(transport->SendMessageToGroup(this, this->myShard, response)) {
                    // Start the timer
                    req->timer->Start();
                } else {
                    Warning("Could not send finalize message to replicas");
                    // give up and clean up
                    pendingReqs.erase(it);
                    delete req;
                }

                // Return to client
                if (!req->continuationInvoked) {
                    req->continuation(req->request, result.first);
                    req->continuationInvoked = true;
                }
                matching = true;
                break;
            }
        }
        if (!matching) {
            // XXX Hack: if we don't having matching replies,
            // just do a slow path now instead of waiting
            // for a timeout.
            ConsensusSlowPath(reqId);
        }
    }
}

void
IRClient::HandleConfirm(const TransportAddress &remote,
                        const proto::ConfirmMessage &msg)
{
    uint64_t reqId = msg.opid().clientreqid();
    auto it = pendingReqs.find(reqId);
    if (it == pendingReqs.end()) {
        // ignore, we weren't waiting for the confirmation
        return;
    }

    PendingRequest *req = it->second;

    viewstamp_t vs = { msg.view(), reqId };
    if (req->confirmQuorum.AddAndCheckForQuorum(vs, msg.replicaidx(), msg)) {
        req->timer->Stop();
        pendingReqs.erase(it);
        if (!req->continuationInvoked) {
            // Return to client
            PendingConsensusRequest *r2 = static_cast<PendingConsensusRequest *>(req);
            r2->continuation(r2->request, r2->decideResult);
        }
        delete req;
    }
}

void
IRClient::PrepareCallback(const string &request, const string &reply)
{
    ReplyMessage replyMessage;
    replyMessage.ParseFromString(reply);

    if (this->waiting != nullptr) {
        Promise *w = this->waiting;
        waiting = nullptr;
        w->Reply(replyMessage.status(), replyMessage.status() == ReplyMessage::OK, replyMessage.reply());
    }
}

void
IRClient::CommitAbortCallback(const string &request, const string &reply)
{
    ASSERT(blockingPrepare != nullptr);
    blockingPrepare->Reply(0, true);
    waiting = nullptr;
}

string
IRClient::PrepareDecide(const set<string> &results)
{
    // If a majority say prepare_ok,
    int ok_count = 0;
    string final_reply_str;
    ReplyMessage final_reply;

    for (string s : results) {
        ReplyMessage reply;
        reply.ParseFromString(s);

	if (reply.status() == ReplyMessage::OK) {
	    ok_count++;
            final_reply.set_reply(reply.reply());
	} else if (reply.status() == ReplyMessage::FAILED) {
	    return s;
	}
    }

    if (ok_count >= config.QuorumSize()) {
	final_reply.set_status(ReplyMessage::OK);
    } else {
        final_reply.set_status(ReplyMessage::RETRY);
    }
    final_reply.SerializeToString(&final_reply_str);
    return final_reply_str;
}

} // namespace tapir
} // namespace store
} // namespace specpaxos

