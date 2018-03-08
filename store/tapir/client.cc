// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapir/client.cc:
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

#include "store/tapir/client.h"

#include <random>

namespace specpaxos {
namespace store {
namespace tapir {

using namespace std;
using namespace proto;

TapirClient::TapirClient(const Configuration &config,
                         Transport *transport,
                         uint64_t clientid)
    : transport(transport)
{
    // Randomly generate a client ID
    // This is surely not the fastest way to get a random 64-bit int,
    // but it should be fine for this purpose.
    while (clientid == 0) {
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> dis;
        clientid = dis(gen);
    }

    this->txnid = (clientid / 10000) * 10000;

    for (int i = 0; i < config.g; i++) {
        this->irClients.push_back(new IRClient(config, transport, i, clientid));
    }

    this->transportThread = new thread(&TapirClient::Run, this);
}

TapirClient::~TapirClient()
{
    this->transport->Stop();
    this->transportThread->join();
    for (auto irclient : this->irClients) {
        delete irclient;
    }
}

void
TapirClient::Run()
{
    this->transport->Run();
}

bool
TapirClient::Invoke(const map<shardnum_t, string> &requests,
                    map<shardnum_t, string> &results,
                    bool indep,
                    bool ro)
{
    ++this->txnid;
    int status;

    // Prepare transaction
    for (int i = 0; i < MAX_RETRIES; i++) {
        results.clear();
        status = Prepare(requests, results);
        if (status == REPLY_RETRY) {
            Debug("Cannot acquire all locks...retry %d", i);
            continue;
        } else {
            break;
        }
    }

    if (status == REPLY_OK) {
        // Commit the transaction
        for (const auto &kv : requests) {
            this->irClients[kv.first]->CommitAbort(this->txnid, true);
        }
        return true;
    }

    // Otherwise abort the transaction
    for (const auto &kv : requests) {
        this->irClients[kv.first]->CommitAbort(this->txnid, false);
    }
    return false;
}

void
TapirClient::Done()
{
    // Make sure all irclients have finished their commit/abort
    for (auto irclient : this->irClients) {
        irclient->Done();
    }
}

int
TapirClient::Prepare(const map<shardnum_t, string> &requests,
                     map<shardnum_t, string> &results)
{
    list<Promise *> promises;
    int status = REPLY_OK;

    for (const auto &kv : requests) {
        promises.push_back(new Promise());
        this->irClients[kv.first]->Prepare(this->txnid, kv.second, promises.back());
    }

    auto participants = requests.begin();
    for (auto p : promises) {
        switch(p->GetReply()) {
        case REPLY_OK:
            results[participants->first] = p->GetValue();
            break;
        case REPLY_FAIL:
            return REPLY_FAIL;
            break;
        case REPLY_RETRY:
            status = REPLY_RETRY;
            break;
        default:
            break;
        }
        delete p;
        participants++;
    }

    return status;
}

} // namespace tapir
} // namespace store
} // namespace specpaxos
