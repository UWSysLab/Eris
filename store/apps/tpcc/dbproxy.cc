// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "store/apps/tpcc/dbproxy.h"
#include "store/apps/tpcc/serialization.h"

using std::string;
using std::vector;
using std::list;

/*
 * Contains code from tpccclientperf.cc in hstore package. TPCCRemoteDB was known
 * as TPCCClientDB.
 */

DBProxy::DBProxy(int num_warehouses, int warehouses_per_partition,
                 ClientThread* ct) : num_warehouses_(num_warehouses),
	warehouses_per_partition_(warehouses_per_partition), ct_(ct) {
	assert(num_warehouses_ > 0 && warehouses_per_partition_ > 0);
}

DBProxy::~DBProxy() {}

int32_t DBProxy::stockLevel(int32_t warehouse_id, int32_t district_id, int32_t threshold) {
	serializeStockLevel(warehouse_id, district_id, threshold, &request_);
	makeRequest(warehouse_id, true);
        if (result_.size() == 0) {
            return 0;
        }

	int32_t level;
	const char* real_end = result_.data() + result_.size();
	const char* end = serialization::deserialize(&level, result_.data(), real_end);
	ASSERT(real_end == end);
	assert(level >= 0);
	return level;
}

void DBProxy::orderStatus(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
                          OrderStatusOutput* output) {
	serializeOrderStatus(warehouse_id, district_id, customer_id, &request_);
	makeRequest(warehouse_id, true);
        if (result_.size() == 0) {
            return;
        }
	deserializeOrderStatusOutput(result_, output);
}

void DBProxy::orderStatus(int32_t warehouse_id, int32_t district_id, const char* c_last,
                          OrderStatusOutput* output) {
	serializeOrderStatus(warehouse_id, district_id, c_last, &request_);
	makeRequest(warehouse_id, true);
        if (result_.size() == 0) {
            return;
        }
	deserializeOrderStatusOutput(result_, output);
}

bool DBProxy::newOrder(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
                       const std::vector<NewOrderItem>& items, const char* now,
                       NewOrderOutput* output, TPCCUndo** undo) {
	assert(undo == NULL);
	// run as a distributed transaction if more than one partition
	for (size_t i = 0; i < items.size(); ++i) {
		if (partition(items[i].ol_supply_w_id) != partition(warehouse_id)) {
			return distNewOrder(warehouse_id, district_id, customer_id, items,
			                    now, output);
		}
	}

	serializeNewOrder(NEW_ORDER, warehouse_id, district_id, customer_id,
	                  items, now, &request_);
	makeRequest(warehouse_id, false);
        if (result_.size() == 0) {
            return false;
        }
	deserializeNewOrderOutput(result_, output);

	// HACK (see explanation in dbserver.cc):
	// We're always returning commit at the application level, and reserving
	// aborts for lock conflicts, so need to extract abort status. See comments
	// elsewhere
	return (strcmp(output->status,
	               NewOrderOutput::INVALID_ITEM_STATUS) != 0);
}

bool DBProxy::distNewOrder(int32_t warehouse_id, int32_t district_id,
                           int32_t customer_id, const std::vector<NewOrderItem>& items,
                           const char* now, NewOrderOutput* output) {
	// Serialize the home partition request
	string home_request;
	serializeNewOrder(NEW_ORDER_HOME, warehouse_id, district_id, customer_id,
	                  items, now, &home_request);
	int home_partition = partition(warehouse_id);

	// Serialize the remote request
	string remote_request;
	serializeNewOrderRemote(warehouse_id, items, &remote_request);

	// Figure out which remote partitions we need to talk to
	std::tr1::unordered_set<int> remote_partitions;
	WarehouseSet warehouses = newOrderRemoteWarehouses(warehouse_id, items);
	assert(warehouses.size() > 0);
	for (WarehouseSet::const_iterator i = warehouses.begin();
	        i != warehouses.end(); ++i) {
		// send a request to each partition once
		int p = partition(*i);
		if (p != home_partition) {
			remote_partitions.insert(p);
		}
	}
	assert(remote_partitions.size() > 0);

	// Aggregate requests
	vector<rid_t> partition_ids;
	vector<string> requests;
	vector<string> results;
	partition_ids.push_back((rid_t)home_partition);
	requests.push_back(home_request);
	// TODO do i need to use new here?
	results.push_back(string());
	for (std::tr1::unordered_set<int>::const_iterator i =
	            remote_partitions.begin();
	        i != remote_partitions.end(); ++i) {
		partition_ids.push_back((rid_t)*i);
		requests.push_back(remote_request);
		// TODO this has got to be wrong...
		results.push_back(string());
	}

	// Send the request
	makeRequest(partition_ids, requests, false, results);

	// Find the main response
	// there's always a response from the home partition, even if aborted
	for (size_t i = 0; i < partition_ids.size(); ++i) {
		if (partition_ids[i] == (rid_t)home_partition) {
                        if (results[i].size() == 0) {
                            return false;
                        }
			deserializeNewOrderOutput(results[i], output);
			break;
		}
	}

	// HACK (see explanation in dbserver.cc):
	// The server responds with a fake commit vote for all distributed NewOrder
	// requests, to ensure that the home partition responds with the message
	// above, as required by the TPC-C client. We check the return status here
	// to determine whether it was meant to be an application abort.
	bool committed = (strcmp(output->status,
	                         NewOrderOutput::INVALID_ITEM_STATUS) != 0);

	if (committed) {
		// Combine all other responses
		vector<int32_t> quantities;
		for (size_t i = 0; i < partition_ids.size(); ++i) {
			if (partition_ids[i] != (rid_t)home_partition) {
				deserializeQuantities(results[i], &quantities);
				newOrderCombine(quantities, output);
			}
		}
	}
	return committed;
}

bool DBProxy::newOrderHome(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
                           const std::vector<NewOrderItem>& items, const char* now,
                           NewOrderOutput* output, TPCCUndo** undo) {
	CHECK(false);
	return false;
}

bool DBProxy::newOrderRemote(int32_t home_warehouse, int32_t remote_warehouse,
                             const std::vector<NewOrderItem>& items, std::vector<int32_t>* out_quantities,
                             TPCCUndo** undo) {
	CHECK(false);
	return false;
}

void DBProxy::payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                      int32_t c_district_id, int32_t customer_id, float h_amount, const char* now,
                      PaymentOutput* output, TPCCUndo** undo) {
	assert(undo == NULL);
	internalPayment(warehouse_id, district_id, c_warehouse_id, c_district_id, customer_id, NULL,
	                h_amount, now, output);
}

void DBProxy::payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                      int32_t c_district_id, const char* c_last, float h_amount, const char* now,
                      PaymentOutput* output, TPCCUndo** undo) {
	assert(undo == NULL);
	internalPayment(warehouse_id, district_id, c_warehouse_id, c_district_id, 0, c_last,
	                h_amount, now, output);
}

void DBProxy::paymentHome(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                          int32_t c_district_id, int32_t c_id, float h_amount, const char* now,
                          PaymentOutput* output, TPCCUndo** undo) {
	CHECK(false);
}
void DBProxy::paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                            int32_t c_district_id, int32_t c_id, float h_amount, PaymentOutput* output,
                            TPCCUndo** undo) {
	CHECK(false);
}
void DBProxy::paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                            int32_t c_district_id, const char* c_last, float h_amount, PaymentOutput* output,
                            TPCCUndo** undo) {
	CHECK(false);
}

void DBProxy::delivery(int32_t warehouse_id, int32_t carrier_id, const char* now,
                       std::vector<DeliveryOrderInfo>* orders, TPCCUndo** undo) {
	assert(undo == NULL);
	serializeDelivery(warehouse_id, carrier_id, now, &request_);
	makeRequest(warehouse_id, false);
        if (result_.size() == 0) {
            return;
        }
	deserializeDeliveryOutput(result_, orders);
}

void DBProxy::internalPayment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                              int32_t c_district_id, int32_t customer_id, const char* c_last, float h_amount,
                              const char* now, PaymentOutput* output) {
	// run as distributed transaction if more than one partition
	if (partition(warehouse_id) != partition(c_warehouse_id)) {
		distInternalPayment(warehouse_id, district_id, c_warehouse_id,
		                    c_district_id, customer_id, c_last,	h_amount, now, output);
		return;
	}

	serializePayment(warehouse_id, district_id, c_warehouse_id, c_district_id,
	                 customer_id, c_last, h_amount, now, &request_);
	makeRequest(warehouse_id, false);
        if (result_.size() == 0) {
            return;
        }
	deserializePaymentOutput(result_, output);
}

void DBProxy::distInternalPayment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                                  int32_t c_district_id, int32_t customer_id, const char* c_last, float h_amount,
                                  const char* now, PaymentOutput* output) {
	// Serialize the home partition request
	string home_request;
	serializePayment(warehouse_id, district_id, c_warehouse_id, c_district_id,
	                 customer_id, c_last, h_amount, now, &home_request);
	// change msg type since there's no serializePaymentHome
	const_cast<string&>(home_request)[0] = PAYMENT_HOME;
	int home_partition = partition(warehouse_id);

	// Serialize the remote request
	string remote_request;
	serializePayment(warehouse_id, district_id, c_warehouse_id, c_district_id,
	                 customer_id, c_last, h_amount, now, &remote_request);
	if (customer_id == 0) {
		const_cast<string&>(remote_request)[0] = PAYMENT_REMOTE_BY_NAME;
	} else {
		const_cast<string&>(remote_request)[0] = PAYMENT_REMOTE_BY_ID;
	}
	int remote_partition = partition(c_warehouse_id);
	ASSERT(remote_partition != home_partition);

	// Aggregate requests
	vector<rid_t> partition_ids;
	vector<string> requests;
	vector<string> results;
	partition_ids.push_back((rid_t)home_partition);
	requests.push_back(home_request);
	// TODO do i need to use new here?
	results.push_back(string());
	partition_ids.push_back((rid_t)remote_partition);
	requests.push_back(remote_request);
	// TODO do i need to use new here?
	results.push_back(string());

	// Send the request
	makeRequest(partition_ids, requests, false, results);
        if (results[0].size() == 0 || results[1].size() == 0) {
            return;
        }

	// Deserialize and combine responses
	deserializePaymentOutput(results[0], output);
	PaymentOutput other;
	deserializePaymentOutput(results[1], &other);
	paymentCombine(other, output);
}

// The partition function assumes that warehouses are evenly distributed
int DBProxy::partition(int32_t warehouse_id) {
	ASSERT(warehouse_id <= num_warehouses_);
	return (warehouse_id - 1) / warehouses_per_partition_;
}

void DBProxy::makeRequest(int32_t warehouse_id, bool ro) {
	ASSERT(warehouse_id > 0);
//	printf("invoke: warehouse %d on partition %d\n", warehouse_id,
//			partition(warehouse_id));
	result_.clear();
	ct_->invoke1(partition(warehouse_id), request_, ro, &result_);
	request_.clear();
}

void DBProxy::makeRequest(vector<rid_t>& partition_ids,
                          vector<string>& requests, bool ro, vector<string>& results) {
	ASSERT(partition_ids.size() > 1);
	ASSERT(partition_ids.size() == requests.size() &&
	       requests.size() == results.size());
//	printf("invoke: partitions ");
//	for (int i = 0; i < partition_ids.size(); ++i) {
//		printf("%d, ", (int) partition_ids[i]);
//	}
//	printf("\n");

	// TODO make sure this isn't passing string copies

	ct_->invokeN(partition_ids, requests, ro, results);
	ASSERT(results.size() == partition_ids.size());
}
