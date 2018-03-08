// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapir/client.h:
 * Tapir protocol client implementation.
 *
 * Copyright 2017 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
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

#ifndef __TAPIR_CLIENT_H__
#define __TAPIR_CLIENT_H__

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "store/common/frontend/txnclient.h"
#include "store/common/type.h"
#include "store/tapir/irclient.h"
#include "store/tapir/tapir-proto.pb.h"

namespace specpaxos {
namespace store {
namespace tapir {

class TapirClient : public store::TxnClient
{
public:
    TapirClient(const Configuration &config,
                Transport *transport,
                uint64_t clientid = 0);
    ~TapirClient() override;

    virtual bool Invoke(const std::map<shardnum_t, std::string> &requests,
                        std::map<shardnum_t, std::string> &results,
                        bool indep,
                        bool ro) override;
    virtual void Done() override;

private:
    Transport *transport;
    std::thread *transportThread;

    static const int MAX_RETRIES = 5;
    txnid_t txnid;
    std::vector<IRClient *> irClients;

    void Run();
    int Prepare(const std::map<shardnum_t, std::string> &requests,
                 std::map<shardnum_t, std::string> &results);
};

} // namespace tapir
} // namespace store
} // namespace specpaxos

#endif /* __TAPIR_CLIENT_H__ */


