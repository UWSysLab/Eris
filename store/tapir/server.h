// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapir/server.h:
 *   Tapir protocol server implementation.
 *
 * Copyright 2017 Dan R. K. Ports  <drkp@cs.washington.edu>
 *                Irene Zhang Ports  <iyzhang@cs.washington.edu>
 *                Jialin Li <lijl@cs.washington.edu>
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

#ifndef __TAPIR_SERVER_H__
#define __TAPIR_SERVER_H__

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/configuration.h"
#include "lib/transport.h"
#include "common/replica.h"
#include "store/common/backend/txnserver.h"
#include "store/tapir/record.h"
#include "store/tapir/tapir-proto.pb.h"

namespace specpaxos {
namespace store {
namespace tapir {

class TapirServer : public Replica
{
private:

    view_t view;

    // record for this replica
    Record record;

public:
    TapirServer(const Configuration &config, int myShard, int myIdx,
                bool initialize, Transport *transport, AppReplica *app);
    ~TapirServer();

    void ReceiveMessage(const TransportAddress &remote,
                        const std::string &type, const std::string &data,
                        void *meta_data) override;

    void HandleMessage(const TransportAddress &remote,
                       const std::string &type, const std::string &data);
    void HandleProposeInconsistent(const TransportAddress &remote,
                                   const proto::ProposeInconsistentMessage &msg);
    void HandleFinalizeInconsistent(const TransportAddress &remote,
                                    const proto::FinalizeInconsistentMessage &msg);
    void HandleProposeConsensus(const TransportAddress &remote,
                                const proto::ProposeConsensusMessage &msg);
    void HandleFinalizeConsensus(const TransportAddress &remote,
                                 const proto::FinalizeConsensusMessage &msg);
};

} // namespace specpaxos::store::tapir
} // namespace specpaxos::store
} // namespace specpaxos

#endif /* __TAPIR_SERVER_H__ */
