// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/backend/txnserver.h:
 *   Interface of a transaction server.
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

#ifndef __TXNSERVER_H__
#define __TXNSERVER_H__

#include "lib/message.h"
#include "common/replica.h"
#include "store/common/transaction.h"

#include <string>
#include <list>

namespace specpaxos {
namespace store {

class TxnServer : public AppReplica
{
public:
    TxnServer();
    virtual ~TxnServer();

    void ReplicaUpcall(opnum_t opnum, const string &str1, string &str2,
                       void *arg = nullptr, void *ret = nullptr) override;
    virtual void InvokeTransaction(const string &txn, std::string &result,
                                   txnarg_t *arg, txnret_t *ret) = 0;
};

} // namespace store
} // namespace specpaxos

#endif /* __TXNSERVER_H__ */
