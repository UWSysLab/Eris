// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/unreplicated/client.h:
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

#ifndef __STORE_UNREPLICATED_CLIENT_H__
#define __STORE_UNREPLICATED_CLIENT_H__

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "common/client.h"
#include "common/messageset.h"
#include "store/common/type.h"
#include "store/unreplicated/unreplicated-proto.pb.h"

namespace specpaxos {
namespace store {
namespace unreplicated {

class UnreplicatedClient : public Client
{
public:
    UnreplicatedClient(const Configuration &config,
                       Transport *transport,
                       uint64_t clientid = 0);
    ~UnreplicatedClient();

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
	opnum_t client_req_id;
        txnid_t txnid;
        std::map<shardnum_t, std::string> requests;
	std::map<shardnum_t, std::string> replies;
        proto::RequestMessage::RequestType type;
	g_continuation_t continuation;
	inline PendingRequest(opnum_t client_req_id,
                              txnid_t txnid,
                              std::map<shardnum_t, std::string> requests,
			      std::map<shardnum_t, std::string> replies,
                              proto::RequestMessage::RequestType type,
			      g_continuation_t continuation)
            : client_req_id(client_req_id), txnid(txnid), requests(requests),
            replies(replies), type(type), continuation(continuation) { }
    };
    txnid_t txnid;
    opnum_t lastReqId;
    PendingRequest *pendingRequest;
    Timeout *requestTimeout;
    MessageSet<opnum_t, proto::ReplyMessage> replySet;

    void InvokeTxn(const std::map<shardnum_t, std::string> &requests,
                   const std::map<shardnum_t, std::string> &replies,
                   g_continuation_t continuation,
                   proto::RequestMessage::RequestType type);
    void SendRequest();
    void HandleReply(const TransportAddress &remote,
		     const proto::ReplyMessage &msg);
    void CompleteOperation(bool commit);
};

} // namespace unreplicated
} // namespace store
} // namespace specpaxos

#endif /* __STORE_UNREPLICATED_CLIENT_H__ */
