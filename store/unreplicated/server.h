// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/unreplicated/server.h:
 *   Unreplicated transaction server implementation.
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

#ifndef __STORE_UNREPLICATED_SERVER_H__
#define __STORE_UNREPLICATED_SERVER_H__

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/configuration.h"
#include "lib/transport.h"
#include "common/replica.h"
#include "store/unreplicated/unreplicated-proto.pb.h"

#include <string>
#include <map>

namespace specpaxos {
namespace store {
namespace unreplicated {

class UnreplicatedServer : public Replica
{
public:
    UnreplicatedServer(const Configuration &config, int myShard, int myIdx,
                       bool initialize, Transport *transport, AppReplica *app);
    ~UnreplicatedServer();

    void ReceiveMessage(const TransportAddress &remote,
                        const std::string &type, const std::string &data,
                        void *meta_data) override;
public:
    Log<int> log;

private:
    /* Server states */
    opnum_t lastOp;

    /* Client information */
    std::map<uint64_t, std::unique_ptr<TransportAddress> > clientAddresses;
    struct ClientTableEntry
    {
	uint64_t lastReqId;
	proto::ReplyMessage reply;
    };
    std::map<uint64_t, ClientTableEntry> clientTable;

    /* Message handlers */
    void HandleClientRequest(const TransportAddress &remote,
			     const proto::RequestMessage &msg);

    void UpdateClientTable(const Request &req, const proto::ReplyMessage &reply);
};

typedef Log<int>::LogEntry LogEntry;

} // namespace unreplicated
} // namespace store
} // namespace specpaxos

#endif /* _STORE_UNREPLICATED_SERVER_H_ */
