// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "store/apps/tpcc/serialization.h"
#include "store/apps/tpcc/assert.h"
#include "store/apps/tpcc/cast.h"

#include <cassert>
#include <cstring>
#include <vector>

using std::string;
using std::vector;

using serialization::deserialize;
using serialization::serialize;

namespace serialization {

void serialize(bool value, string* out) {
	// Normalizes value: it could be some weird bit pattern
	int8_t byte = 0;
	if (value) byte = 1;
	serialize(byte, out);
}

void serialize(int8_t value, std::string* out) {
	out->push_back(value);
}

template <typename T>
static inline void memcpySerialize(const T& v, string* out) {
	out->append(reinterpret_cast<const char*>(&v), sizeof(v));
}

void serialize(int32_t value, string* out) {
	memcpySerialize(value, out);
}

void serialize(int64_t value, string* out) {
	memcpySerialize(value, out);
}

void serialize(float value, string* out) {
	memcpySerialize(value, out);
}

void serialize(const void* bytes, size_t length, string* out) {
	int32_t size = static_cast<int32_t>(length);
	assert(0 <= size && static_cast<size_t>(size) == length);
	serialize(size, out);
	out->append(reinterpret_cast<const char*>(bytes), length);
}

const char* deserialize(bool* value, const char* start, const char* end) {
	int8_t byte;
	const char* copy_end = memcpyDeserialize(&byte, start, end);
	*value = byte;
	return copy_end;
}

const char* deserialize(int8_t* value, const char* start, const char* end) {
	assert(start <= end);
	*value = *start;
	start += 1;
	return start;
}

const char* deserialize(int32_t* value, const char* start, const char* end) {
	return memcpyDeserialize(value, start, end);
}

const char* deserialize(int64_t* value, const char* start, const char* end) {
	return memcpyDeserialize(value, start, end);
}

const char* deserialize(float* value, const char* start, const char* end) {
	return memcpyDeserialize(value, start, end);
}

const char* deserialize(string* value, const char* start, const char* end) {
	int32_t size;
	start = deserialize(&size, start, end);

	const char* copy_end = start + size;
	assert(copy_end <= end);
	value->assign(start, size);
	return copy_end;
}

}  // namespace serialization

void serializeDelivery(int32_t warehouse_id, int32_t carrier_id, const char* now, string* out) {
	serializeTwoInts(DELIVERY, warehouse_id, carrier_id, out);
	cStringSerialize(now, out);
}

void deserializeDeliveryOutput(const std::string& in, std::vector<DeliveryOrderInfo>* out) {
	const char* end = in.data() + in.size();
	const char* next = vectorDeserialize(out, MemcpyDeserializer(), in.data(), end);
	ASSERT(next == end);
}

void deserializeOrderStatusOutput(const std::string& in, OrderStatusOutput* output) {
	const char* next = in.data();
	const char* end = in.data() + in.size();
	next = deserialize(&output->c_id, next, end);
	next = deserialize(&output->c_balance, next, end);
	next = deserialize(&output->o_id, next, end);
	next = deserialize(&output->o_carrier_id, next, end);
	next = vectorDeserialize(&output->lines, OrderLineSubsetDeserializer(), next, end);
	// TODO: This makes copies, which is gross, but avoids touching uninitialized data
	next = strcpyDeserialize(output->c_first, next, end);
	next = strcpyDeserialize(output->c_middle, next, end);
	next = strcpyDeserialize(output->c_last, next, end);
	next = strcpyDeserialize(output->o_entry_d, next, end);
	assert(next == end);
}

void serializePayment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
                      int32_t c_district_id, int32_t customer_id, const char* c_last, float h_amount,
                      const char* now, std::string* out) {
	assert(customer_id == 0 || c_last == NULL);
	static const char EMPTY[] = "";
	if (c_last == NULL) {
		c_last = EMPTY;
	} else {
		assert(strlen(c_last) > 0);
	}
	assert(customer_id == 0 || strlen(c_last) == 0);
	int32_t type = PAYMENT_BY_ID;
	if (customer_id == 0) type = PAYMENT_BY_NAME;
	serializeThreeInts(type, warehouse_id, district_id, c_warehouse_id,
	                   out);
	serialize(c_district_id, out);
	serialize(customer_id, out);
	cStringSerialize(c_last, out);
	serialize(h_amount, out);
	cStringSerialize(now, out);
}

void serializeOrderStatus(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
                          std::string* out) {
	serializeThreeInts(ORDER_STATUS_BY_ID, warehouse_id, district_id, customer_id,
	                   out);
}

void serializeOrderStatus(int32_t warehouse_id, int32_t district_id, const char* c_last,
                          std::string* out) {
	serializeTwoInts(ORDER_STATUS_BY_NAME, warehouse_id, district_id, out);
	cStringSerialize(c_last, out);
}

void serializeStockLevel(int32_t warehouse_id, int32_t district_id, int32_t threshold,
                         string* out) {
	serializeThreeInts(STOCK_LEVEL, warehouse_id, district_id, threshold, out);
}



void cStringSerialize(const char* c_string, string* out) {
	serialize(c_string, strlen(c_string), out);
}

class ItemInfoSerializer {
public:
	void operator()(const NewOrderOutput::ItemInfo& value, string* out) const {
		serialize(value.s_quantity, out);
		serialize(value.i_price, out);
		serialize(value.ol_amount, out);
		serialize(value.brand_generic, out);
		cStringSerialize(value.i_name, out);
	}
};

const char* strcpyDeserialize(char* destination, const char* start, const char* end) {
	string out;
	const char* next = deserialize(&out, start, end);
	strcpy(destination, out.c_str());
	return next;
}

class ItemInfoDeserializer {
public:
	const char* operator()(NewOrderOutput::ItemInfo* value, const char* start, const char* end) const {
		const char* next = deserialize(&value->s_quantity, start, end);
		next = deserialize(&value->i_price, next, end);
		next = deserialize(&value->ol_amount, next, end);
		next = deserialize(&value->brand_generic, next, end);
		return strcpyDeserialize(value->i_name, next, end);
	}
};

void serializeTwoInts(int32_t code, int32_t one, int32_t two, string* out) {
	serialize(code, out);
	serialize(one, out);
	serialize(two, out);
}

void serializeThreeInts(int32_t code, int32_t one, int32_t two, int32_t three, string* out) {
	serializeTwoInts(code, one, two, out);
	serialize(three, out);
}

const char* deserializeTwoInts(const string& in, int32_t expected_code, int32_t* one,
                               int32_t* two) {
	int32_t temp;
	const char* real_end = in.data() + in.size();
	const char* next = deserialize(&temp, in.data(), real_end);
	assert(temp == expected_code);

	next = deserialize(one, next, real_end);
	return deserialize(two, next, real_end);
}

const char* deserializeThreeInts(const string& in, int32_t expected_code, int32_t* one,
                                 int32_t* two, int32_t* three) {
	const char* real_end = in.data() + in.size();
	const char* next = deserializeTwoInts(in, expected_code, one, two);
	return deserialize(three, next, real_end);
}

void serializeNewOrder(int32_t code, int32_t warehouse_id, int32_t district_id, int32_t customer_id,
                       const std::vector<NewOrderItem>& items, const char* now, std::string* out) {
	assert(code == NEW_ORDER || code == NEW_ORDER_HOME);
	serializeThreeInts(code, warehouse_id, district_id, customer_id, out);
	vectorSerialize(items, MemcpySerializer(), out);
	cStringSerialize(now, out);
}

void deserializeNewOrder(const string& input, int32_t code, int32_t* warehouse_id,
                         int32_t* district_id, int32_t* customer_id, vector<NewOrderItem>* items,
                         string* now) {
	assert(code == NEW_ORDER || code == NEW_ORDER_HOME);
	const char* const end = input.data() + input.size();
	const char* next = deserializeThreeInts(input, code, warehouse_id,
	                                        district_id, customer_id);
	next = vectorDeserialize(items, MemcpyDeserializer(), next, end);
	next = deserialize(now, next, end);
	assert(next == end);
}

void serializeNewOrderRemote(int32_t warehouse_id,
                             const std::vector<NewOrderItem>& items, std::string* out) {
	serialize(NEW_ORDER_REMOTE, out);
	serialize(warehouse_id, out);
	vectorSerialize(items, MemcpySerializer(), out);
}


void serializeOrderStatusOutput(const OrderStatusOutput& output, string* out) {
	serialize(output.c_id, out);
	serialize(output.c_balance, out);
	serialize(output.o_id, out);
	serialize(output.o_carrier_id, out);
	vectorSerialize(output.lines, OrderLineSubsetSerializer(), out);
	cStringSerialize(output.c_first, out);
	cStringSerialize(output.c_middle, out);
	cStringSerialize(output.c_last, out);
	cStringSerialize(output.o_entry_d, out);
}

void serializePaymentOutput(const PaymentOutput& output, string* out) {
#define SERIALIZE_ADDRESS(src, prefix, output) \
    cStringSerialize(src.prefix ## street_1, output); \
    cStringSerialize(src.prefix ## street_2, output); \
    cStringSerialize(src.prefix ## city, output); \
    cStringSerialize(src.prefix ## state, output); \
    cStringSerialize(src.prefix ## zip, output)

	SERIALIZE_ADDRESS(output, w_, out);
	SERIALIZE_ADDRESS(output, d_, out);

	serialize(output.c_credit_lim, out);
	serialize(output.c_discount, out);
	serialize(output.c_balance, out);
	cStringSerialize(output.c_first, out);
	cStringSerialize(output.c_middle, out);
	cStringSerialize(output.c_last, out);
	SERIALIZE_ADDRESS(output, c_, out);
	cStringSerialize(output.c_phone, out);
	cStringSerialize(output.c_since, out);
	cStringSerialize(output.c_credit, out);
	cStringSerialize(output.c_data, out);

#undef SERIALIZE_ADDRESS
}

void deserializePayment(const std::string& input, int32_t code,
                        int32_t* warehouse_id, int32_t* district_id, int32_t* c_warehouse_id,
                        int32_t* c_district_id, int32_t* customer_id, string* c_last, float* h_amount, string* now) {
	const char* const end = input.data() + input.size();
	const char* next = deserializeThreeInts(input, code, warehouse_id, district_id, c_warehouse_id);
	next = deserialize(c_district_id, next, end);
	next = deserialize(customer_id, next, end);
	next = deserialize(c_last, next, end);
	next = deserialize(h_amount, next, end);
	next = deserialize(now, next, end);
	assert(next == end);
	assert(*customer_id == 0 || c_last->empty());
}

void deserializePaymentOutput(const std::string& in, PaymentOutput* output) {
#define DESERIALIZE_ADDRESS(dest, prefix) \
    next = strcpyDeserialize(dest->prefix ## street_1, next, end); \
    next = strcpyDeserialize(dest->prefix ## street_2, next, end); \
    next = strcpyDeserialize(dest->prefix ## city, next, end); \
    next = strcpyDeserialize(dest->prefix ## state, next, end); \
    next = strcpyDeserialize(dest->prefix ## zip, next, end)

	const char* end = in.data() + in.size();
	const char* next = in.data();
	DESERIALIZE_ADDRESS(output, w_);
	DESERIALIZE_ADDRESS(output, d_);

	next = deserialize(&output->c_credit_lim, next, end);
	next = deserialize(&output->c_discount, next, end);
	next = deserialize(&output->c_balance, next, end);
	next = strcpyDeserialize(output->c_first, next, end);
	next = strcpyDeserialize(output->c_middle, next, end);
	next = strcpyDeserialize(output->c_last, next, end);
	DESERIALIZE_ADDRESS(output, c_);
	next = strcpyDeserialize(output->c_phone, next, end);
	next = strcpyDeserialize(output->c_since, next, end);
	next = strcpyDeserialize(output->c_credit, next, end);
	next = strcpyDeserialize(output->c_data, next, end);

#undef DESERIALIZE_ADDRESS
}

void serializeNewOrderOutput(const NewOrderOutput& in, std::string* out) {
	serialize(in.w_tax, out);
	serialize(in.d_tax, out);
	serialize(in.o_id, out);
	serialize(in.c_discount, out);
	serialize(in.total, out);
	vectorSerialize(in.items, ItemInfoSerializer(), out);
	cStringSerialize(in.c_last, out);
	cStringSerialize(in.c_credit, out);
	cStringSerialize(in.status, out);
}

void deserializeNewOrderOutput(const std::string& in, NewOrderOutput* output) {
	const char* next = in.data();
	const char* end = in.data() + in.size();
	next = deserialize(&output->w_tax, next, end);
	next = deserialize(&output->d_tax, next, end);
	next = deserialize(&output->o_id, next, end);
	next = deserialize(&output->c_discount, next, end);
	next = deserialize(&output->total, next, end);
	next = vectorDeserialize(&output->items, ItemInfoDeserializer(), next, end);
	// TODO: This makes copies, which is gross, but avoids touching uninitialized data
	next = strcpyDeserialize(output->c_last, next, end);
	next = strcpyDeserialize(output->c_credit, next, end);
	next = strcpyDeserialize(output->status, next, end);
	assert(next == end);
}

void deserializeQuantities(const std::string& in, std::vector<int32_t>* quantities) {
	const char* end = in.data() + in.size();
	const char* next = vectorDeserialize(quantities, MemcpyDeserializer(), in.data(), end);
	ASSERT(next == end);
}

void serializeReset(string* out) {
	serialize(RESET, out);
}
