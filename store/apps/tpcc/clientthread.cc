// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
#include "store/apps/tpcc/clientthread.h"

#include "store/apps/tpcc/randomgenerator.h"
#include "store/apps/tpcc/dbproxy.h"
#include "store/apps/tpcc/clock.h"

using std::string;
using std::vector;
using std::map;

ClientThread::ClientThread(int total_warehouses, int warehouses_per_partition,
                           int clients_per_warehouse, int client_id,
                           int remote_item_milli_p, specpaxos::store::TxnClient *txnClient)
    : txnClient(txnClient)
{
    tpcc::RealRandomGenerator* random = new tpcc::RealRandomGenerator();
    random->setC(tpcc::NURandC::makeRandom(random));

    DBProxy* client_db = new DBProxy(total_warehouses,
                                     warehouses_per_partition, this);
    client_ = new TPCCClient(new SystemClock(), random, client_db,
                             Item::NUM_ITEMS, total_warehouses, District::NUM_PER_WAREHOUSE,
                             Customer::NUM_PER_DISTRICT);

    // override the fraction of distributed transactions
    if (remote_item_milli_p >= 0) {
        client_->set_remote_item_milli_p(remote_item_milli_p);
        //newOrderOnly_ = true;
        newOrderOnly_ = false; // Remove the new order txn hack
    } else {
        newOrderOnly_ = false;
    }

    // choose warehouse and district for client, numbered starting from 1
    int warehouse_id = (client_id / clients_per_warehouse) + 1;
    int district_id = 0;
    // there will usually be a fixed number of clients per district (usually 1)
    // otherwise have clients issue to random districts (district_id = 0)
    if (clients_per_warehouse % District::NUM_PER_WAREHOUSE == 0) {
        district_id = (client_id % District::NUM_PER_WAREHOUSE) + 1;
    }
    client_->bindWarehouseDistrict(warehouse_id, district_id);

    if (district_id == 0) {
        printf("Starting client %d "
               "(assigned to random district warehouse %d of %d)\n",
               client_id, warehouse_id, total_warehouses);
    } else {
        printf("Starting client %d "
               "(assigned to district %d of %d at warehouse %d of %d)\n",
               client_id, district_id, District::NUM_PER_WAREHOUSE,
               warehouse_id, total_warehouses);
    }
}



int ClientThread::doOps(int num_ops) {
    int num_new_orders = 0;
    for (int i = 0; i < num_ops; i++) {
        if (newOrderOnly_) {
            client_->doNewOrder();
            num_new_orders++;
        } else {
            if (client_->doOne()) {
                num_new_orders++;
            }
        }
    }
    return num_new_orders;
}


void ClientThread::invoke1(shardnum_t partition_id, const string& request,
                           bool ro, string* result) {
    map<shardnum_t, string> txnrequests;
    map<shardnum_t, string> txnresults;

    // run request
    txnrequests[partition_id] = request;
    if (txnClient->Invoke(txnrequests, txnresults, true, ro)) {
        ASSERT(txnresults.size() == 1);
        ASSERT(txnresults.find(partition_id) != txnresults.end());
        *result = txnresults[partition_id];
    }

//  printf("C++: result.limit: %d\n", reslen);
//  printf("C++: result recv [");
//  for (int i = 0; i < reslen; i++) {
//      printf("%02X", creqbuf[i]);
//  }
//  printf("] len = %d\n", reslen);
}


void ClientThread::invokeN(vector<shardnum_t>& partition_ids,
                           vector<string>& requests, bool ro, vector<string>& results) {
    // XXX uncomment this ASSERT after changing to vector
    // ASSERT(partition_ids.size() == requests.size() &&
    //        requests.size() == results.size());

    // run request
//  printf("C++: sending request with lengths:\n");
//  for (int i = 0; i < (int) requests.size(); i++) {
//      printf("\t%d", (int) requests[i].length());
//  }

    ASSERT(partition_ids.size() == requests.size() &&
           requests.size() == results.size());
    map<shardnum_t, string> txnrequests;
    map<shardnum_t, string> txnresults;
    for (unsigned int i = 0; i < partition_ids.size(); i++) {
        txnrequests[partition_ids[i]] = requests[i];
    }
    if (txnClient->Invoke(txnrequests, txnresults, true, ro)) {
        ASSERT(txnresults.size() == txnrequests.size());
        for (unsigned int i = 0; i < partition_ids.size(); i++) {
            results[i] = txnresults[partition_ids[i]];
        }
    }

//  printf("C++: wrote back results with lengths:\n");
//  for (int i = 0; i < (int) results.size(); i++) {
//      printf("\t%d", (int) results[i].length());
//  }
//  printf("C++: result recv [");
//  for (int i = 0; i < reslen; i++) {
//      printf("%02X", creqbuf[i]);
//  }
//  printf("] len = %d\n", reslen);
}
