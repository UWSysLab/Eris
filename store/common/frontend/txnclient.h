// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/frontend/txnclient.h:
 * Frontend client side proxy.
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

#ifndef __TXNCLIENT_H__
#define __TXNCLIENT_H__

#include "lib/assert.h"
#include "lib/transport.h"
#include "common/client.h"
#include "store/common/promise.h"

#include <thread>
#include <string>
#include <map>

namespace specpaxos {
namespace store {

class TxnClient
{
public:
    virtual ~TxnClient() {};
    virtual bool Invoke(const std::map<shardnum_t, std::string> &requests,
                        std::map<shardnum_t, std::string> &results,
                        bool indep,
                        bool ro) = 0;
    virtual void Done() = 0;

private:
};

} // namespace store
} // namespace specpaxos

#endif /* __TXNCLIENT_H__ */
