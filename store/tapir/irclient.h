// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapir/irclient.h:
 *   Tapir inconsistent replication client
 *
 * Copyright 2013-2017 Dan R. K. Ports  <drkp@cs.washington.edu>
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

#ifndef __TAPIR_IR_CLIENT_H__
#define __TAPIR_IR_CLIENT_H__

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "lib/configuration.h"
#include "common/client.h"
#include "common/quorumset.h"
#include "store/common/type.h"
#include "store/common/promise.h"
#include "store/tapir/tapir-proto.pb.h"

#include <functional>
#include <set>
#include <unordered_map>
#include <map>

namespace specpaxos {
namespace store {
namespace tapir {

class IRClient : public Client
{
public:
    typedef std::function<string (const std::set<string> &)> decide_t;

    IRClient(const Configuration &config,
             Transport *transport,
             shardnum_t shard,
             uint64_t clientid = 0);
    virtual ~IRClient();
    virtual void Invoke(const string &request,
                        continuation_t continuation) override;
    virtual void InvokeInconsistent(const string &request,
                                    continuation_t continuation);
    virtual void InvokeConsensus(const string &request,
                                 decide_t decide,
                                 continuation_t continuation);
    virtual void InvokeUnlogged(int replicaIdx,
                                const string &request,
                                continuation_t continuation,
                                timeout_continuation_t timeoutContinuation = nullptr,
                                uint32_t timeout = DEFAULT_UNLOGGED_OP_TIMEOUT);
    virtual void ReceiveMessage(const TransportAddress &remote,
                                const string &type, const string &data,
                                void *meta_data) override;

    virtual void Invoke(const std::map<shardnum_t, std::string> &requests,
                        g_continuation_t continuation,
                        void *arg = nullptr) override;

    virtual void Done();

    void Prepare(txnid_t txnid, const std::string &txn, Promise *promise);
    void CommitAbort(txnid_t txnid, bool commit);

protected:
    struct PendingRequest
    {
        string request;
        uint64_t clientReqId;
	continuation_t continuation;
	bool continuationInvoked = false;
	Timeout *timer;
	QuorumSet<viewstamp_t, proto::ConfirmMessage> confirmQuorum;
    	inline PendingRequest(string request,
			      uint64_t clientReqId,
			      continuation_t continuation,
			      Timeout *timer,
			      int quorumSize) :
	    request(request), clientReqId(clientReqId),
	    continuation(continuation), timer(timer),
	    confirmQuorum(quorumSize) { };
	inline ~PendingRequest() { delete timer; };
    };

    struct PendingInconsistentRequest : public PendingRequest
    {
	QuorumSet<viewstamp_t, proto::ReplyInconsistentMessage> inconsistentReplyQuorum;
    	inline PendingInconsistentRequest(string request,
					  uint64_t clientReqId,
					  continuation_t continuation,
					  Timeout *timer,
					  int quorumSize) :
	    PendingRequest(request, clientReqId,
			   continuation, timer, quorumSize),
	    inconsistentReplyQuorum(quorumSize) { };
    };

    struct PendingConsensusRequest : public PendingRequest
    {
	QuorumSet<viewstamp_t, proto::ReplyConsensusMessage> consensusReplyQuorum;
	decide_t decide;
	string decideResult;
    	inline PendingConsensusRequest(string request,
				       uint64_t clientReqId,
				       continuation_t continuation,
				       Timeout *timer,
				       int quorumSize,
				       int superQuorum,
				       decide_t decide) :
	    PendingRequest(request, clientReqId,
			   continuation, timer, quorumSize),
	    consensusReplyQuorum(superQuorum),
	    decide(decide) { };
    };


    shardnum_t myShard;
    uint64_t view;
    uint64_t lastReqId;
    std::unordered_map<uint64_t, PendingRequest *> pendingReqs;

    Promise *waiting;
    Promise *blockingPrepare;

    void SendInconsistent(const PendingInconsistentRequest *req);
    void ResendInconsistent(const uint64_t reqId);
    void ConsensusSlowPath(const uint64_t reqId);
    void ResendConfirmation(const uint64_t reqId, bool isConsensus);
    void HandleInconsistentReply(const TransportAddress &remote,
                                 const proto::ReplyInconsistentMessage &msg);
    void HandleConsensusReply(const TransportAddress &remote,
                     const proto::ReplyConsensusMessage &msg);
    void HandleConfirm(const TransportAddress &remote,
                       const proto::ConfirmMessage &msg);

    void PrepareCallback(const std::string &request, const string &reply);
    void CommitAbortCallback(const std::string &request, const string &reply);
    std::string PrepareDecide(const std::set<std::string> &results);
};

} // namespace tapir
} // namespace store
} // namespace specpaxos

#endif  /* __TAPIR_IR_CLIENT_H__ */
