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

#ifndef __ERIS_CLIENT_H__
#define __ERIS_CLIENT_H__

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/configuration.h"
#include "common/client.h"
#include "common/quorumset.h"
#include "store/common/type.h"
#include "store/eris/eris-proto.pb.h"

namespace specpaxos {
namespace store {
namespace eris {

class ErisClient : public Client
{
public:
    ErisClient(const Configuration &config,
               Transport *transport,
               uint64_t clientid = 0);
    ~ErisClient();

    void Invoke(const string &request,
		continuation_t continuation) override;
    void InvokeUnlogged(int replicaIdx,
			const string &request,
			continuation_t continuation,
			timeout_continuation_t timeoutContinuation = nullptr,
			uint32_t timeout = DEFAULT_UNLOGGED_OP_TIMEOUT) override;
    void ReceiveMessage(const TransportAddress &remote,
			const string &type,
			const string &data,
                        void *meta_data) override;

    void Invoke(const std::map<shardnum_t, std::string> &requests,
		g_continuation_t continuation,
                void *arg = nullptr) override;

private:
    struct PendingRequest
    {
        proto::RequestMessage request_msg;
	opnum_t client_req_id;
        proto::RequestType txn_type;
        bool commit;
        std::map<shardnum_t, std::string> requests;
	std::map<shardnum_t, std::string> replies;
        std::map<shardnum_t, bool> has_replies;
        std::vector<int> shards;
	g_continuation_t continuation;
	inline PendingRequest(const proto::RequestMessage &request_msg,
                              opnum_t client_req_id,
                              proto::RequestType txn_type,
                              const std::map<shardnum_t, std::string> &requests,
			      const std::map<shardnum_t, std::string> &replies,
                              const std::vector<int> &shards,
			      g_continuation_t continuation)
            : request_msg(request_msg), client_req_id(client_req_id),
            txn_type(txn_type), commit(true), requests(requests),
            replies(replies), shards(shards), continuation(continuation) {
                for (const auto &kv : replies) {
                    has_replies[kv.first] = false;
                }
            }
    };

    txnid_t txnid;
    opnum_t lastReqId;
    PendingRequest *pendingRequest;
    Timeout *requestTimeout;
    std::map<shardnum_t, QuorumSet<opnum_t, proto::ReplyMessage> *> replySet;

    void InvokeTxn(const std::map<shardnum_t, std::string> &requests,
                   const std::map<shardnum_t, std::string> &replies,
                   g_continuation_t continuation,
                   proto::RequestType txn_type);
    void SendRequest();
    void HandleReply(const TransportAddress &remote,
		     const proto::ReplyMessage &msg);
    void CompleteOperation(bool commit);
    bool IsLeader(view_t view, int replicaIdx);
};

} // namespace eris
} // namespace store
} // namespace specpaxos

#endif /* __ERIS_CLIENT_H__ */
