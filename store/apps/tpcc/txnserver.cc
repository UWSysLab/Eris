// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/apps/tpcc/txnserver.cc:
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

#include "store/apps/tpcc/txnserver.h"
#include "store/apps/tpcc/assert.h"
#include "store/apps/tpcc/clock.h"
#include "store/apps/tpcc/randomgenerator.h"
#include "store/apps/tpcc/tpccclient.h"
#include "store/apps/tpcc/tpccgenerator.h"
#include "store/apps/tpcc/dbimpl.h"
#include "store/apps/tpcc/dbserver.h"

namespace specpaxos {
namespace store {
namespace tpcc {

TPCCTxnServer::TPCCTxnServer(TPCCTxnServerArg arg)
    : locking(arg.locking)
{
	init(arg.total_warehouses, arg.warehouses_per_partition, arg.partition_id);
}

TPCCTxnServer::~TPCCTxnServer() {
	delete srv_;
}

void
TPCCTxnServer::init(int total_warehouses,
                    int warehouses_per_partition,
                    int partition_id) {
	if (total_warehouses > Warehouse::MAX_WAREHOUSE_ID) {
		fprintf(stderr, "Number of warehouses must be <= %d (was %d)\n",
		        Warehouse::MAX_WAREHOUSE_ID, total_warehouses);
		exit(1);
	}

	DBImpl* db = new DBImpl();
	SystemClock* clock = new SystemClock();

	// Create a generator for filling the database.
	::tpcc::RealRandomGenerator* random = new ::tpcc::RealRandomGenerator();
	random->seed(0);
	::tpcc::NURandC cLoad = ::tpcc::NURandC::makeRandom(random);
	random->setC(cLoad);

	// Generate the data
	printf("Loading %d of %d warehouses...\n", warehouses_per_partition,
	       total_warehouses);
	fflush(stdout);
	char now[Clock::DATETIME_SIZE + 1];
	clock->getDateTimestamp(now);
	TPCCGenerator generator(random, now, Item::NUM_ITEMS,
	                        District::NUM_PER_WAREHOUSE, Customer::NUM_PER_DISTRICT,
	                        NewOrder::INITIAL_NUM_PER_DISTRICT);
	int64_t begin = clock->getMicroseconds();
	generator.makeItemsTable(db);

	// TODO: Vertical partitioning only needs s_dist_xx and s_data.
	// This copies *all* stock. This means that some partitions have
	// incorrect data (eg. s_quantity), but it is simple.
	printf("Loading stock ...\n");
	for (int i = 0; i < total_warehouses; ++i) {
		generator.makeStock(db, i + 1);
	}

	for (int i = 0; i < warehouses_per_partition; ++i) {
		int start_warehouse = partition_id * warehouses_per_partition + 1;
		printf("Loading warehouse %d on partition %d...\n", start_warehouse + i,
		       partition_id + 1);
		generator.makeWarehouseWithoutStock(db, start_warehouse + i);
	}

	int64_t end = clock->getMicroseconds();
	printf("%lu ms\n", (end - begin + 500) / 1000);
	fflush(stdout);

	srv_ = new DBServer(db);
}

void
TPCCTxnServer::InvokeTransaction(const string &ops, string &result, txnarg_t *arg, txnret_t *ret) {
    ret->txnid = arg->txnid;
    int64_t id = abs(int64_t(arg->txnid));
    if (this->locking) {
        ASSERT(arg->type == TXN_PREPARE || arg->type == TXN_COMMIT || arg->type == TXN_ABORT);
        ASSERT(id > 0);
        if (arg->type == TXN_PREPARE) {
            //Notice("Prepare txn %ld", id);
            // Prepare phase
            if (this->transactions.find(id) != this->transactions.end()) {
                //Notice("txn %ld already prepared", id);
                // this transaction is already prepared, this is a retry...
                // don't rerun the transaction, just copy the result
                ret->blocked = false;
                ret->commit = true;
                result = this->transactions.at(id)->result;
                return;
            }
            Transaction *t = new Transaction(id);
            srv_->startTransaction(id);

            bool prepared = srv_->handle(ops, &result, &(t->undo), id);

            if (prepared) {
                //Notice("Successful prepared %ld", id);
                ASSERT(this->transactions.find(id) == this->transactions.end());
                t->result = result;
                this->transactions.insert(std::make_pair(id, t));
                ret->blocked = false;
                ret->commit = true;
            } else {
                //Notice("Failed to prepare %ld", id);
                ASSERT(t->undo == nullptr);
                srv_->endTransaction(id);
                delete t;
                ret->blocked = true;
                ret->commit = false;
            }
        } else {
            // Commit or abort
            ret->blocked = false;
            ret->commit = true;
            if (this->transactions.find(id) == this->transactions.end()) {
                // We were not able to prepare this transaction before.
                // This must be an abort call, don't need to do anything here.
                //Notice("Cannot find id %ld, should be an abort", id);
            } else {
                if (arg->type == TXN_ABORT) {
                    //Notice("Aborting id %ld", id);
                } else if (arg->type == TXN_COMMIT) {
                    //Notice("Commiting id %ld", id);
                } else {
                    ASSERT(false);
                }
                Transaction *t = this->transactions.at(id);

                if (t->undo != nullptr) {
                    if (arg->type == TXN_COMMIT) {
                        srv_->freeUndo(t->undo);
                    } else {
                        srv_->applyUndo(t->undo);
                    }
                }

                srv_->endTransaction(id);
                this->transactions.erase(id);
                delete t;
            }
        }
    } else {
        // Independent transaction
	srv_->handle(ops, &result);
    }
}

} // namespace tpcc
} // namespace store
} // namespace specpaxos
