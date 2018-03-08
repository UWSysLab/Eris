// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/apps/tpcc/txnserver.h:
 *   Implementation of tpcc transaction server.
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

#ifndef __TPCC_TXNSERVER_H__
#define __TPCC_TXNSERVER_H__

#include "store/common/backend/txnserver.h"
#include "store/apps/tpcc/dbserver.h"

#include <unordered_map>

namespace specpaxos {
namespace store {
namespace tpcc {

typedef struct {
	int total_warehouses;
	int warehouses_per_partition;
	int partition_id;
        bool locking;
} TPCCTxnServerArg;

class Transaction {
public:
    int64_t txnid;
    void *undo;
    std::string result;
    Transaction(int64_t id) : txnid(id), undo(nullptr) {}
};

class TPCCTxnServer : public TxnServer {
public:
	TPCCTxnServer(TPCCTxnServerArg arg);
	~TPCCTxnServer();

	void InvokeTransaction(const std::string &txn, std::string &result, txnarg_t *arg, txnret_t *ret) override;

private:
        bool locking;
	DBServer* srv_;
        std::unordered_map<int64_t, Transaction *> transactions;

	void init(int total_warehouses, int warehouses_per_partition, int partition_id);
};

} // namespace tpcc
} // namespace store
} // namespace specpaxos

#endif /* __TPCC_TXNSERVER_H__ */
