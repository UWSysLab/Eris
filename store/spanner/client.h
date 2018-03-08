// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/spanner/client.h:
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

#ifndef __SPANNER_CLIENT_H__
#define __SPANNER_CLIENT_H__

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "common/client.h"
#include "common/messageset.h"
#include "store/common/type.h"
#include "store/spanner/spanner-proto.pb.h"

namespace specpaxos {
namespace store {
namespace spanner {

class SpannerClient : public Client
{
public:
    SpannerClient(const Configuration &config,
                  Transport *transport,
                  uint64_t clientid = 0);
    ~SpannerClient();

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
    static const int MAX_RETRIES = 5;
    txnid_t txnid;

    struct PendingRequest
    {
        txnid_t txnid;
	opnum_t client_req_id;
        std::map<shardnum_t, std::string> requests;
        std::map<shardnum_t, std::string> replies;
        proto::RequestType type;
	g_continuation_t continuation;
        int num_retries;
	inline PendingRequest(txnid_t txnid,
                              opnum_t client_req_id,
                              std::map<shardnum_t, std::string> requests,
                              proto::RequestType type,
			      g_continuation_t continuation)
            : txnid(txnid), client_req_id(client_req_id), requests(requests),
            type(type), continuation(continuation), num_retries(0) { }
    };
    opnum_t lastReqId;
    PendingRequest *pendingRequest;
    Timeout *requestTimeout;
    MessageSet<opnum_t, proto::ReplyMessage> replySet;

    void HandleReply(const TransportAddress &remote,
		     const proto::ReplyMessage &msg);
    enum Fate {
        FATE_COMMIT,
        FATE_ABORT,
        FATE_RETRY,
        FATE_ACKED
    };
    void CompleteOperation(Fate fate);
    void SendRequest();
};

} // namespace spanner
} // namespace store
} // namespace specpaxos

#endif /* __SPANNER_CLIENT_H__ */

