#include "store/apps/tpcc/tpccdb.h"

#include <cassert>

#include "store/apps/tpcc/stlutil.h"

void Address::copy(char* street1, char* street2, char* city, char* state, char* zip,
        const char* src_street1, const char* src_street2, const char* src_city,
        const char* src_state, const char* src_zip) {
    // TODO: Just do one copy since all the fields should be contiguous?
    memcpy(street1, src_street1, MAX_STREET+1);
    memcpy(street2, src_street2, MAX_STREET+1);
    memcpy(city, src_city, MAX_CITY+1);
    memcpy(state, src_state, STATE+1);
    memcpy(zip, src_zip, ZIP+1);
}

// Non-integral constants must be defined in a .cc file. Needed for Mac OS X.
// http://www.research.att.com/~bs/bs_faq2.html#in-class
constexpr const float Item::MIN_PRICE;
constexpr const float Item::MAX_PRICE;
constexpr const float Warehouse::MIN_TAX;
constexpr const float Warehouse::MAX_TAX;
constexpr const float Warehouse::INITIAL_YTD;
constexpr const float District::MIN_TAX;
constexpr const float District::MAX_TAX;
constexpr const float District::INITIAL_YTD;  // different from Warehouse
constexpr const float Customer::MIN_DISCOUNT;
constexpr const float Customer::MAX_DISCOUNT;
constexpr const float Customer::INITIAL_BALANCE;
constexpr const float Customer::INITIAL_CREDIT_LIM;
constexpr const float Customer::INITIAL_YTD_PAYMENT;
constexpr const char Customer::GOOD_CREDIT[] = "GC";
constexpr const char Customer::BAD_CREDIT[] = "BC";
constexpr const float OrderLine::MIN_AMOUNT;
constexpr const float OrderLine::MAX_AMOUNT;
constexpr const char NewOrderOutput::INVALID_ITEM_STATUS[] = "Item number is not valid";
constexpr const float History::INITIAL_AMOUNT;

TPCCUndo::~TPCCUndo() {
    STLDeleteValues(&modified_warehouses_);
    STLDeleteValues(&modified_districts_);
    STLDeleteValues(&modified_customers_);
    STLDeleteValues(&modified_stock_);
    STLDeleteValues(&modified_orders_);
    STLDeleteValues(&modified_order_lines_);
    STLDeleteElements(&deleted_new_orders_);
}

template <typename T>
static void copyIfNeeded(typename std::tr1::unordered_map<T*, T*>* map, T* source) {
    typedef typename std::tr1::unordered_map<T*, T*> MapType;
    std::pair<typename MapType::iterator, bool> result = map->insert(
            typename MapType::value_type(source, NULL));
    if (result.second) {
        // we did the insert: copy the value
        assert(result.first->second == NULL);
        result.first->second = new T(*source);
    } else {
        assert(result.first->second != NULL);
    }    
}

void TPCCUndo::save(Warehouse* w) {
    copyIfNeeded(&modified_warehouses_, w);
}
void TPCCUndo::save(District* d) {
    copyIfNeeded(&modified_districts_, d);
}
void TPCCUndo::save(Customer* c) {
    copyIfNeeded(&modified_customers_, c);
}
void TPCCUndo::save(Stock* s) {
    copyIfNeeded(&modified_stock_, s);
}
void TPCCUndo::save(Order* o) {
    copyIfNeeded(&modified_orders_, o);
}
void TPCCUndo::save(OrderLine* ol) {
    copyIfNeeded(&modified_order_lines_, ol);
}

void TPCCUndo::inserted(const Order* o) {
    assert(inserted_orders_.find(o) == inserted_orders_.end());
    inserted_orders_.insert(o);
}
void TPCCUndo::inserted(const OrderLine* ol) {
    assert(inserted_order_lines_.find(ol) == inserted_order_lines_.end());
    inserted_order_lines_.insert(ol);
}
void TPCCUndo::inserted(const NewOrder* no) {
    assert(inserted_new_orders_.find(no) == inserted_new_orders_.end());
    inserted_new_orders_.insert(no);
}
void TPCCUndo::inserted(const History* h) {
    assert(inserted_history_.find(h) == inserted_history_.end());
    inserted_history_.insert(h);
}
void TPCCUndo::deleted(NewOrder* no) {
    assert(deleted_new_orders_.find(no) == deleted_new_orders_.end());
    deleted_new_orders_.insert(no);
}

void TPCCUndo::applied() {
    deleted_new_orders_.clear();
}

TPCCDB::WarehouseSet TPCCDB::newOrderRemoteWarehouses(int32_t home_warehouse,
        const std::vector<NewOrderItem>& items) {
    WarehouseSet out;
    for (size_t i = 0; i < items.size(); ++i) {
        if (items[i].ol_supply_w_id != home_warehouse) {
            out.insert(items[i].ol_supply_w_id);
        }
    }
    return out;
}

void TPCCDB::newOrderCombine(const std::vector<int32_t>& remote_quantities,
        NewOrderOutput* output) {
    assert(remote_quantities.size() == output->items.size());
    for (size_t i = 0; i < remote_quantities.size(); ++i) {
        if (remote_quantities[i] != INVALID_QUANTITY) {
            assert(output->items[i].s_quantity == 0);
            output->items[i].s_quantity = remote_quantities[i];
        }
    }
}

void TPCCDB::newOrderCombine(const std::vector<int32_t>& remote_quantities,
        std::vector<int32_t>* output) {
    assert(remote_quantities.size() == output->size());
    for (size_t i = 0; i < remote_quantities.size(); ++i) {
        if (remote_quantities[i] != INVALID_QUANTITY) {
            assert((*output)[i] == INVALID_QUANTITY);
            (*output)[i] = remote_quantities[i];
        }
    }
}

// TODO: These macros are copied from tpcctables.cc. Is there a way to share them?
#define COPY_ADDRESS(src, dest, prefix) \
    Address::copy( \
            dest->prefix ## street_1, dest->prefix ## street_2, dest->prefix ## city, \
            dest->prefix ## state, dest->prefix ## zip,\
            src.prefix ## street_1, src.prefix ## street_2, src.prefix ## city, \
            src.prefix ## state, src.prefix ## zip)

#define COPY_STRING(dest, src, field) memcpy(dest->field, src.field, sizeof(src.field))

// Copy the customer fields from remote into local
void TPCCDB::paymentCombine(const PaymentOutput& remote, PaymentOutput* home) {
    home->c_credit_lim = remote.c_credit_lim;
    home->c_discount = remote.c_discount;
    home->c_balance = remote.c_balance;
    COPY_STRING(home, remote, c_first);
    COPY_STRING(home, remote, c_middle);
    COPY_STRING(home, remote, c_last);
    COPY_ADDRESS(remote, home, c_);
    COPY_STRING(home, remote, c_phone);
    COPY_STRING(home, remote, c_since);
    COPY_STRING(home, remote, c_credit);
    COPY_STRING(home, remote, c_data);
}

#undef COPY_STRING
#undef COPY_ADDRESS
