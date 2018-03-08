// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef SERIALIZATION_H__
#define SERIALIZATION_H__

#include <stdint.h>

#include <string>
#include <vector>

#include "store/apps/tpcc/tpccdb.h"
#include "store/apps/tpcc/assert.h"
#include "store/apps/tpcc/cast.h"

namespace serialization {

// TODO: Overloading sucks due to implicit conversions. Make these explicit names?
void serialize(bool value, std::string* out);
void serialize(int8_t value, std::string* out);
void serialize(int16_t value, std::string* out);  // NOT DEFINED (yet)
void serialize(int32_t value, std::string* out);
void serialize(int64_t value, std::string* out);
void serialize(float value, std::string* out);
void serialize(double value, std::string* out);  // NOT DEFINED (yet)

/** NOT DEFINED: this prevents implicit conversions. */
void serialize(const char* value, std::string* out);

/** Serializes a blob of binary data. This deserializes as a std::string. */
void serialize(const void* bytes, size_t length, std::string* out);

/** Serializes a std::string containing arbitrary binary data. */
inline void serialize(const std::string& value, std::string* out) {
	serialize(value.data(), value.size(), out);
}

const char* deserialize(bool* value, const char* start, const char* end);
//~ const char* deserialize(char* value, const char* start, const char* end);
const char* deserialize(int8_t* value, const char* start, const char* end);
//~ const char* deserialize(int16_t* value, const char* start, const char* end);
const char* deserialize(int32_t* value, const char* start, const char* end);
const char* deserialize(int64_t* value, const char* start, const char* end);
const char* deserialize(float* value, const char* start, const char* end);
//~ const char* deserialize(double* value, const char* start, const char* end);
const char* deserialize(std::string* value, const char* start, const char* end);

#ifndef __sun__
// On Linux, char* and int8_t* are distinct types. On Solaris, they are equivalent.
inline const char* deserialize(char* value, const char* start, const char* end) {
	return deserialize(reinterpret_cast<int8_t*>(value), start, end);
}

inline void serialize(char value, std::string* out) {
	serialize(static_cast<int8_t>(value), out);
}
#endif

}

// TODO integrate this stuff better. just dumped it here out of tpccengine.cc
// TODO probably add this to the serialization namespace

static const int32_t NEW_ORDER = 0;
static const int32_t STOCK_LEVEL = 1;
static const int32_t ORDER_STATUS_BY_ID = 2;
static const int32_t ORDER_STATUS_BY_NAME = 3;
static const int32_t PAYMENT_BY_ID = 4;
static const int32_t PAYMENT_BY_NAME = 5;
static const int32_t DELIVERY = 6;

static const int32_t NEW_ORDER_HOME = 7;
static const int32_t NEW_ORDER_REMOTE = 8;
static const int32_t PAYMENT_HOME = 9;
static const int32_t PAYMENT_REMOTE_BY_ID = 10;
static const int32_t PAYMENT_REMOTE_BY_NAME = 11;

static const int32_t RESET = 127;


void serializeDelivery(int32_t warehouse_id, int32_t carrier_id, const char* now,
                       std::string* out);
void deserializeDeliveryOutput(const std::string& in, std::vector<DeliveryOrderInfo>* out);


void serializeStockLevel(int32_t warehouse_id, int32_t district_id, int32_t threshold,
                         std::string* out);

void serializeOrderStatus(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
                          std::string* out);
void serializeOrderStatus(int32_t warehouse_id, int32_t district_id, const char* c_last,
                          std::string* out);
void deserializeOrderStatusOutput(const std::string& in, OrderStatusOutput* output);

// One version: either customer_id is zero or c_last should be NULL.
void serializePayment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                      int32_t c_district_id, int32_t customer_id, const char* c_last, float h_amount,
                      const char* now, std::string* out);

void serializeNewOrder(int32_t code, int32_t warehouse_id, int32_t district_id,
                       int32_t customer_id, const std::vector<NewOrderItem>& items, const char* now, std::string* out);
void deserializeNewOrder(const std::string& in,
                         int32_t code, int32_t* warehouse_id, int32_t* district_id, int32_t* customer_id,
                         std::vector<NewOrderItem>* items, std::string* now);
void serializeNewOrderRemote(int32_t warehouse_id,
                             const std::vector<NewOrderItem>& items, std::string* out);
void deserializeNewOrderOutput(const std::string& in, NewOrderOutput* output);
void deserializeQuantities(const std::string& in, std::vector<int32_t>* quantities);
void serializeNewOrderOutput(const NewOrderOutput& in, std::string* out);

void serializePaymentOutput(const PaymentOutput& in, std::string* out);
void deserializePaymentOutput(const std::string& in, PaymentOutput* output);

void serializeReset(std::string* out);


class MemcpySerializer {
public:
	template <typename T>
	void operator()(const T& value, std::string* out) const {
		out->append(reinterpret_cast<const char*>(&value), sizeof(value));
	}
};


static inline const char* memcpyDeserialize(void* output, size_t size, const char* start, const char* end) {
	const char* next = start + size;
	assert(next <= end);
	memcpy(output, start, size);
	return next;
}

// TODO was static inline
template <typename T>
static inline const char* memcpyDeserialize(T* v, const char* start, const char* end) {
	const char* next = start + sizeof(*v);
	assert(next <= end);
	memcpy(v, start, sizeof(*v));
	return next;
}


class MemcpyDeserializer {
public:
	template <typename T>
	const char* operator()(T* out, const char* start, const char* end) const {
		return memcpyDeserialize(out, sizeof(*out), start, end);
	}
};

template <typename T, typename Serializer>
void vectorSerialize(const std::vector<T>& values, Serializer s, std::string* out) {
	int32_t size = assert_range_cast<int32_t>(values.size());
	serialization::serialize(size, out);
	for (size_t i = 0; i < values.size(); ++i) {
		s(values[i], out);
	}
}

template <typename T, typename Deserializer>
const char* vectorDeserialize(std::vector<T>* out, Deserializer d, const char* start, const char* end) {
	assert(start < end);
	int32_t size;
	start = serialization::deserialize(&size, start, end);
	assert(size >= 0);
	out->resize(size);
	for (int i = 0; i < size; ++i) {
		assert(start < end);
		start = d(&(*out)[i], start, end);
	}
	return start;
}

void serializeTwoInts(int32_t code, int32_t one, int32_t two, std::string* out);
void serializeThreeInts(int32_t code, int32_t one, int32_t two, int32_t three, std::string* out);
const char* deserializeTwoInts(const std::string& in, int32_t expected_code, int32_t* one,
                               int32_t* two);

const char* deserializeThreeInts(const std::string& in, int32_t expected_code, int32_t* one,
                                 int32_t* two, int32_t* three);

void serializeOrderStatusOutput(const OrderStatusOutput& output, std::string* out);

void deserializePayment(const std::string& input, int32_t code,
                        int32_t* warehouse_id, int32_t* district_id, int32_t* c_warehouse_id,
                        int32_t* c_district_id, int32_t* customer_id, std::string* c_last, float* h_amount, std::string* now);

void cStringSerialize(const char* c_string, std::string* out);

const char* strcpyDeserialize(char* destination, const char* start, const char* end);

class OrderLineSubsetSerializer {
public:
	void operator()(const OrderStatusOutput::OrderLineSubset& value, std::string* out) {
		serialization::serialize(value.ol_i_id, out);
		serialization::serialize(value.ol_supply_w_id, out);
		serialization::serialize(value.ol_quantity, out);
		serialization::serialize(value.ol_amount, out);
		cStringSerialize(value.ol_delivery_d, out);
	}
};

class OrderLineSubsetDeserializer {
public:
	const char* operator()(OrderStatusOutput::OrderLineSubset* value, const char* start,
	                       const char* end) {
		const char* next = serialization::deserialize(&value->ol_i_id, start, end);
		next = serialization::deserialize(&value->ol_supply_w_id, next, end);
		next = serialization::deserialize(&value->ol_quantity, next, end);
		next = serialization::deserialize(&value->ol_amount, next, end);
		return strcpyDeserialize(value->ol_delivery_d, next, end);
	}
};

#endif
