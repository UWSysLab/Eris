// TODO remove a lot of this code and integrate it into tpccserver.cc

// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "store/apps/tpcc/dbserver.h"

#include <cstring>
#include <vector>

#include "store/apps/tpcc/cast.h"
#include "store/apps/tpcc/serialization.h"

using serialization::deserialize;
using serialization::serialize;

using std::string;
using std::vector;


static bool callNewOrder(const std::string& input, TPCCDB* db, std::string* out,
		void** undo) {
    int32_t warehouse_id;
    int32_t district_id;
    int32_t customer_id;
    vector<NewOrderItem> items;
    string now;
    deserializeNewOrder(input, NEW_ORDER, &warehouse_id, &district_id,
            &customer_id, &items, &now);

    NewOrderOutput output;
    bool commit = db->newOrder(warehouse_id, district_id, customer_id, items,
    		now.c_str(), &output, reinterpret_cast<TPCCUndo**>(undo));
    serializeNewOrderOutput(output, out);

    // TODO: fix this?
    // HACK:
    // Always sending back commit votes on the application level, which will
    // be translated into aborts where applicable at the client (by looking at
    // the response code). Aborts are being reserved for lock conflicts in the
    // locking version. See older comments in callNewOrderHome etc.
    commit = true;

    return commit;
}

static bool callNewOrderHome(const std::string& input, TPCCDB* db,
		std::string* out, void** undo) {
    int32_t warehouse_id;
    int32_t district_id;
    int32_t customer_id;
    vector<NewOrderItem> items;
    string now;
    deserializeNewOrder(input, NEW_ORDER_HOME, &warehouse_id, &district_id,
            &customer_id, &items, &now);

    NewOrderOutput output;
    bool commit = db->newOrderHome(warehouse_id, district_id, customer_id,
    		items, now.c_str(), &output, reinterpret_cast<TPCCUndo**>(undo));
    if (!commit) {
        serializeNewOrderOutput(output, out);
        // TODO: fix this?
        // HACK:
        // The TPC-C client expects a response from the home partition, even if
        // there's an abort. However, if a remote partition votes abort before
        // the home partition receives the transaction, granola might
        // discard the transaction before it gets to the home partition.
        // To make life simple, we're just going to vote commit on all
        // distributed NewOrder transactions. Then the client can check if the
        // return status is NewOrderOutput::INVALID_ITEM_STATUS, and interpret
        // it as an application-level abort appropriately.
        //return false;
        return true;
    }

    // Call newOrderRemote for any local warehouses
    vector<int32_t> quantities;
    TPCCDB::WarehouseSet remote_warehouses = TPCCDB::newOrderRemoteWarehouses(warehouse_id, items);
    for (TPCCDB::WarehouseSet::const_iterator i = remote_warehouses.begin(); i != remote_warehouses.end(); ++i) {
        if (db->hasWarehouse(*i)) {
            bool commit = db->newOrderRemote(warehouse_id, *i, items, &quantities, NULL);
            ASSERT(commit);
            TPCCDB::newOrderCombine(quantities, &output);
        }
    }
    serializeNewOrderOutput(output, out);

    return true;
}

static bool callNewOrderRemote(const std::string& input, TPCCDB* db,
		std::string* out, void** undo) {
    const char* const end = input.data() + input.size();
    int32_t code;
    int32_t warehouse_id;
    vector<NewOrderItem> items;
    const char* next = deserialize(&code, input.data(), end);
    assert(code == NEW_ORDER_REMOTE);
    next = deserialize(&warehouse_id, next, end);
    next = vectorDeserialize(&items, MemcpyDeserializer(), next, end);
    assert(next == end);

    // The (int32_t) cast avoids the template from taking a reference, which requires that we put
    // INVALID_QUANTITY in a .cc file.
    vector<int32_t> final_quantities(items.size(), (int32_t) TPCCDB::INVALID_QUANTITY);
    vector<int32_t> temp_quantities(items.size(), (int32_t) TPCCDB::INVALID_QUANTITY);
    TPCCDB::WarehouseSet remote_warehouses = TPCCDB::newOrderRemoteWarehouses(warehouse_id, items);
    for (TPCCDB::WarehouseSet::const_iterator i = remote_warehouses.begin(); i != remote_warehouses.end(); ++i) {
        if (db->hasWarehouse(*i)) {
            bool commit = db->newOrderRemote(warehouse_id, *i, items,
            		&temp_quantities, reinterpret_cast<TPCCUndo**>(undo));
            if (!commit) {
                // TODO: fix this?
                // HACK:
                // The TPC-C client expects a response from the home partition, even if
                // there's an abort. However, if a remote partition votes abort before
                // the home partition receives the transaction, granola might
                // discard the transaction before it gets to the home partition.
                // To make life simple, we're just going to vote commit on all
                // distributed NewOrder transactions. Then the client can check if the
                // return status is NewOrderOutput::INVALID_ITEM_STATUS, and interpret
                // it as an application-level abort appropriately.
                //return false;
                return true;
            }

            db->newOrderRemote(warehouse_id, *i, items, &temp_quantities, NULL);
            TPCCDB::newOrderCombine(temp_quantities, &final_quantities);
        }
    }
    vectorSerialize(final_quantities, MemcpySerializer(), out);

    //~ printf("new order remote %d\n", out->size());

    return true;
}


static bool callStockLevel(const std::string& input, TPCCDB* db,
		std::string* out) {
    int32_t warehouse_id;
    int32_t district_id;
    int32_t threshold;
    const char* const end = input.data() + input.size();
    const char* next = deserializeThreeInts(input, STOCK_LEVEL, &warehouse_id,
            &district_id, &threshold);
    ASSERT(next == end);

    int32_t count = db->stockLevel(warehouse_id, district_id, threshold);
    serialize(count, out);

    return true;
}

static bool callOrderStatusById(const std::string& input, TPCCDB* db,
		std::string* out) {
    int32_t warehouse_id;
    int32_t district_id;
    int32_t customer_id;
    const char* const end = input.data() + input.size();
    const char* next = deserializeThreeInts(input, ORDER_STATUS_BY_ID, &warehouse_id,
            &district_id, &customer_id);
    ASSERT(next == end);

    OrderStatusOutput output;
    db->orderStatus(warehouse_id, district_id, customer_id, &output);
    serializeOrderStatusOutput(output, out);
    return true;
}

static bool callOrderStatusByName(const std::string& input, TPCCDB* db,
		std::string* out) {
    int32_t warehouse_id;
    int32_t district_id;
    string c_last;
    const char* const end = input.data() + input.size();
    const char* next = deserializeTwoInts(input, ORDER_STATUS_BY_NAME, &warehouse_id,
            &district_id);
    next = deserialize(&c_last, next, end);
    assert(next == end);

    OrderStatusOutput output;
    db->orderStatus(warehouse_id, district_id, c_last.c_str(), &output);
    serializeOrderStatusOutput(output, out);
    return true;
}


static bool callPaymentById(const std::string& input, TPCCDB* db,
		std::string* out, void** undo) {
    int32_t warehouse_id;
    int32_t district_id;
    int32_t c_warehouse_id;
    int32_t c_district_id;
    int32_t customer_id;
    string c_last;
    float h_amount;
    string now;
    deserializePayment(input, PAYMENT_BY_ID, &warehouse_id,
            &district_id, &c_warehouse_id, &c_district_id, &customer_id, &c_last, &h_amount, &now);
    assert(customer_id != 0 && c_last.empty());

    PaymentOutput output;
    db->payment(warehouse_id, district_id, c_warehouse_id, c_district_id,
    		customer_id, h_amount, now.c_str(), &output,
    		reinterpret_cast<TPCCUndo**>(undo));
    serializePaymentOutput(output, out);
    return true;
}

static bool callPaymentHome(const std::string& input, TPCCDB* db,
		std::string* out, void** undo) {
    int32_t warehouse_id;
    int32_t district_id;
    int32_t c_warehouse_id;
    int32_t c_district_id;
    int32_t customer_id;
    string c_last;
    float h_amount;
    string now;
    deserializePayment(input, PAYMENT_HOME, &warehouse_id,
            &district_id, &c_warehouse_id, &c_district_id, &customer_id, &c_last, &h_amount, &now);

    PaymentOutput output;
    db->paymentHome(warehouse_id, district_id, c_warehouse_id, c_district_id,
    		customer_id, h_amount, now.c_str(), &output,
    		reinterpret_cast<TPCCUndo**>(undo));
    serializePaymentOutput(output, out);
    return true;
}

static bool callPaymentRemoteById(const std::string& input, TPCCDB* db,
		std::string* out, void** undo) {
    int32_t warehouse_id;
    int32_t district_id;
    int32_t c_warehouse_id;
    int32_t c_district_id;
    int32_t customer_id;
    string c_last;
    float h_amount;
    string now;
    deserializePayment(input, PAYMENT_REMOTE_BY_ID, &warehouse_id,
            &district_id, &c_warehouse_id, &c_district_id, &customer_id, &c_last, &h_amount, &now);
    assert(customer_id != 0 && c_last.empty());

    PaymentOutput output;
    db->paymentRemote(warehouse_id, district_id, c_warehouse_id, c_district_id,
    		customer_id, h_amount, &output, reinterpret_cast<TPCCUndo**>(undo));
    serializePaymentOutput(output, out);
    return true;
}

static bool callPaymentRemoteByName(const std::string& input, TPCCDB* db,
		std::string* out, void** undo) {
    int32_t warehouse_id;
    int32_t district_id;
    int32_t c_warehouse_id;
    int32_t c_district_id;
    int32_t customer_id;
    string c_last;
    float h_amount;
    string now;
    deserializePayment(input, PAYMENT_REMOTE_BY_NAME, &warehouse_id,
            &district_id, &c_warehouse_id, &c_district_id, &customer_id, &c_last, &h_amount, &now);
    assert(customer_id == 0 && !c_last.empty());

    PaymentOutput output;
    db->paymentRemote(warehouse_id, district_id, c_warehouse_id, c_district_id,
    		c_last.c_str(), h_amount, &output,
    		reinterpret_cast<TPCCUndo**>(undo));
    serializePaymentOutput(output, out);
    return true;
}

static bool callPaymentByName(const std::string& input, TPCCDB* db,
		std::string* out, void** undo) {
    int32_t warehouse_id;
    int32_t district_id;
    int32_t c_warehouse_id;
    int32_t c_district_id;
    int32_t customer_id;
    string c_last;
    float h_amount;
    string now;
    deserializePayment(input, PAYMENT_BY_NAME, &warehouse_id,
            &district_id, &c_warehouse_id, &c_district_id, &customer_id, &c_last, &h_amount, &now);
    assert(customer_id == 0 && !c_last.empty());

    PaymentOutput output;
    db->payment(warehouse_id, district_id, c_warehouse_id, c_district_id,
    		c_last.c_str(), h_amount, now.c_str(), &output,
    		reinterpret_cast<TPCCUndo**>(undo));
    serializePaymentOutput(output, out);
    return true;
}

static bool callDelivery(const std::string& input, TPCCDB* db, std::string* out,
		void** undo) {
    int32_t warehouse_id;
    int32_t carrier_id;
    string now;
    const char* const end = input.data() + input.size();
    const char* next = deserializeTwoInts(input, DELIVERY, &warehouse_id,
            &carrier_id);
    next = deserialize(&now, next, end);
    assert(next == end);

    vector<DeliveryOrderInfo> output;
    db->delivery(warehouse_id, carrier_id, now.c_str(), &output,
    		reinterpret_cast<TPCCUndo**>(undo));
    vectorSerialize(output, MemcpySerializer(), out);
    return true;
}

DBServer::~DBServer() {
    delete db_;
}

bool DBServer::handle(const string& work_unit, string* output,
		void** undo, int64_t tid) {
    assert(output->empty());
    
    // Deserialize the type
    int32_t type;
    deserialize(&type, work_unit.data(), work_unit.data() + work_unit.size());

    // either we're not locking, or we have a valid (uninit'd) undo structure
    assert(tid == -1 || (undo != NULL && *undo == NULL));

    try {
		switch (type) {
			// these should all return true, unless throwing an exception
			// (since we changed them to have no application level aborts)
			case NEW_ORDER:
				return callNewOrder(work_unit, db_, output, undo);
				break;
			case STOCK_LEVEL:
				return callStockLevel(work_unit, db_, output);
				break;
			case ORDER_STATUS_BY_ID:
				return callOrderStatusById(work_unit, db_, output);
				break;
			case ORDER_STATUS_BY_NAME:
				return callOrderStatusByName(work_unit, db_, output);
				break;
			case PAYMENT_BY_ID:
				return callPaymentById(work_unit, db_, output, undo);
				break;
			case PAYMENT_BY_NAME:
				return callPaymentByName(work_unit, db_, output, undo);
				break;
			case DELIVERY:
				return callDelivery(work_unit, db_, output, undo);
				break;
			case NEW_ORDER_HOME:
				return callNewOrderHome(work_unit, db_, output, undo);
				break;
			case NEW_ORDER_REMOTE:
				return callNewOrderRemote(work_unit, db_, output, undo);
				break;
			case PAYMENT_HOME:
				return callPaymentHome(work_unit, db_, output, undo);
				break;
			case PAYMENT_REMOTE_BY_ID:
				return callPaymentRemoteById(work_unit, db_, output, undo);
				break;
			case PAYMENT_REMOTE_BY_NAME:
				// TODO: Test
				return callPaymentRemoteByName(work_unit, db_, output, undo);
				break;
			case RESET:
				return false; // we don't implement reset
				break;
			default:
				CHECK(false);
		}
    } catch (LockBlockedException& e) {
    	// the transaction has conflicted. apply the undo record and return
    	// false. we still might undo later if someone else voted abort
        if (*undo != NULL) {
            applyUndo(*undo);
            *undo = NULL;
        }
        assert(output->empty());
        return false;
    }
}

void DBServer::applyUndo(void* undo) {
    db_->applyUndo(reinterpret_cast<TPCCUndo*>(undo));
}

void DBServer::freeUndo(void* undo) {
    db_->freeUndo(reinterpret_cast<TPCCUndo*>(undo));
}

void DBServer::startTransaction(int64_t tid) {
	db_->setTransaction(tid);
}

void DBServer::endTransaction(int64_t tid) {
	db_->endTransaction(tid);
}

