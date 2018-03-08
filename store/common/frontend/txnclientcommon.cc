// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/frontend/txnclientcommon.cc:
 *   Implementation of frontend client side proxy.
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

#include "store/common/frontend/txnclientcommon.h"

namespace specpaxos {
namespace store {

using namespace std;

TxnClientCommon::TxnClientCommon(Transport *transport,
                                 Client *proto_client)
    : transport(transport), protoClient(proto_client)
{
    proxyTransport = new thread(&TxnClientCommon::RunProxy, this);
}

TxnClientCommon::~TxnClientCommon()
{
    this->transport->Stop();
    this->proxyTransport->join();
}

void
TxnClientCommon::RunProxy()
{
    transport->Run();
}

bool
TxnClientCommon::Invoke(const std::map<shardnum_t, string> &requests,
                        std::map<shardnum_t, string> &results,
                        bool indep,
                        bool ro)
{
    Promise * promise = new Promise();
    clientarg_t arg;
    arg.indep = indep;
    arg.ro = ro;

    this->transport->Timer(0, [ = ]() {
        this->waiting = promise;
        this->protoClient->Invoke(requests,
                                  bind(&TxnClientCommon::InvokeCallback,
                                       this,
                                       placeholders::_1,
                                       placeholders::_2,
                                       placeholders::_3),
                                  (void *)&arg);
    });

    results = promise->GetValues();
    bool commit = promise->GetCommit();

    delete promise;
    return commit;
}

void
TxnClientCommon::Done() { }

void
TxnClientCommon::InvokeCallback(const map<shardnum_t, string> &requests,
                                const map<shardnum_t, string> &replies,
                                bool commit)
{
    if (waiting != NULL) {
        Promise *w = waiting;
        waiting = NULL;
        w->Reply(0, commit, replies);
    }
}

} // namespace store
} // namespace specpaxos
