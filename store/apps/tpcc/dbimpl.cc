#include "store/apps/tpcc/dbimpl.h"

#include <algorithm>
#include <cstdio>
#include <limits>
#include <vector>

#include "assert.h"
#include "stlutil.h"

using std::vector;
using std::set;

bool CustomerByNameOrdering::operator()(const Customer* a, const Customer* b) {
    if (a->c_w_id < b->c_w_id) return true;
    if (a->c_w_id > b->c_w_id) return false;
    assert(a->c_w_id == b->c_w_id);

    if (a->c_d_id < b->c_d_id) return true;
    if (a->c_d_id > b->c_d_id) return false;
    assert(a->c_d_id == b->c_d_id);

    int diff = strcmp(a->c_last, b->c_last);
    if (diff < 0) return true;
    if (diff > 0) return false;
    assert(diff == 0);

    // Finally delegate to c_first
    return strcmp(a->c_first, b->c_first) < 0;
}

template <typename KeyType, typename ValueType>
static void deleteBTreeValues(BPlusTree<KeyType, ValueType*, DBImpl::KEYS_PER_INTERNAL, DBImpl::KEYS_PER_LEAF>* btree) {
    KeyType key = std::numeric_limits<KeyType>::max();
    ValueType* value = NULL;
    while (btree->findLastLessThan(key, &value, &key)) {
        assert(value != NULL);
        delete value;
    }
}

DBImpl::~DBImpl() {
    // Clean up the b-trees with this gross hack
    deleteBTreeValues(&warehouses_);
    deleteBTreeValues(&stock_);
    deleteBTreeValues(&districts_);
    deleteBTreeValues(&orders_);
    deleteBTreeValues(&orderlines_);

    STLDeleteValues(&neworders_);
    STLDeleteElements(&customers_by_name_);
    STLDeleteElements(&history_);

    STLDeleteValues(&lock_table_);
}

int32_t DBImpl::stockLevel(int32_t warehouse_id, int32_t district_id, int32_t threshold) {
    /* EXEC SQL SELECT d_next_o_id INTO :o_id FROM district
        WHERE d_w_id=:w_id AND d_id=:d_id; */
    //~ printf("stock level %d %d %d\n", warehouse_id, district_id, threshold);
    District* d = findDistrict(warehouse_id, district_id);
    // TODO: This read lock prevents inserting new orders. This is not really "correct" but it works
    tryRead(d);
    int32_t o_id = d->d_next_o_id;

    /* EXEC SQL SELECT COUNT(DISTINCT (s_i_id)) INTO :stock_count FROM order_line, stock
        WHERE ol_w_id=:w_id AND ol_d_id=:d_id AND ol_o_id<:o_id AND ol_o_id>=:o_id-20
            AND s_w_id=:w_id AND s_i_id=ol_i_id AND s_quantity < :threshold;*/


    // retrieve up to 300 tuples from order line, using ( [o_id-20, o_id), d_id, w_id, [1, 15])
    //   and for each retrieved tuple, read the corresponding stock tuple using (ol_i_id, w_id)
    // NOTE: This is a cheat because it hard codes the maximum number of orders.
    // We really should use the ordered b-tree index to find (0, o_id-20, d_id, w_id) then iterate
    // until the end. This will also do less work (wasted finds). Since this is only 4%, it probably
    // doesn't matter much

    // TODO: Test the performance more carefully. I tried: std::set, std::hash_set, std::vector
    // with linear search, and std::vector with binary search using std::lower_bound. The best
    // seemed to be to simply save all the s_i_ids, then sort and eliminate duplicates at the end.
    std::vector<int32_t> s_i_ids;
    // Average size is more like ~30.
    s_i_ids.reserve(300);

    // Iterate over [o_id-20, o_id)
    for (int order_id = o_id - STOCK_LEVEL_ORDERS; order_id < o_id; ++order_id) {
        // HACK: We shouldn't rely on MAX_OL_CNT. See comment above.
        for (int line_number = 1; line_number <= Order::MAX_OL_CNT; ++line_number) {
            OrderLine* line = findOrderLine(warehouse_id, district_id, order_id, line_number);
            if (line == NULL) {
                // We can break since we have reached the end of the lines for this order.
                // TODO: A btree iterate in (w_id, d_id, o_id) order would be a clean way to do this
#ifndef NDEBUG
                for (int test_line_number = line_number + 1; line_number < Order::MAX_OL_CNT; ++line_number) {
                    assert(findOrderLine(warehouse_id, district_id, order_id, test_line_number) == NULL);
                }
#endif
                break;
            }

            // Check if s_quantity < threshold
            Stock* stock = findStock(warehouse_id, line->ol_i_id);
            tryRead(stock);
            if (stock->s_quantity < threshold) {
                s_i_ids.push_back(line->ol_i_id);
            }
        }
    }

    // Filter out duplicate s_i_id: multiple order lines can have the same item
    std::sort(s_i_ids.begin(), s_i_ids.end());
    int num_distinct = 0;
    int32_t last = -1;  // NOTE: This relies on -1 being an invalid s_i_id
    for (size_t i = 0; i < s_i_ids.size(); ++i) {
        if (s_i_ids[i] != last) {
            last = s_i_ids[i];
            num_distinct += 1;
        }
    }

    return num_distinct;
}

void DBImpl::orderStatus(int32_t warehouse_id, int32_t district_id, int32_t customer_id, OrderStatusOutput* output) {
    //~ printf("order status %d %d %d\n", warehouse_id, district_id, customer_id);
    internalOrderStatus(findCustomer(warehouse_id, district_id, customer_id), output);
}

void DBImpl::orderStatus(int32_t warehouse_id, int32_t district_id, const char* c_last, OrderStatusOutput* output) {
    //~ printf("order status %d %d %s\n", warehouse_id, district_id, c_last);
    Customer* customer = findCustomerByName(warehouse_id, district_id, c_last);
    internalOrderStatus(customer, output);
}

void DBImpl::internalOrderStatus(Customer* customer, OrderStatusOutput* output) {
    tryRead(customer);
    output->c_id = customer->c_id;
    // retrieve from customer: balance, first, middle, last
    output->c_balance = customer->c_balance;
    strcpy(output->c_first, customer->c_first);
    strcpy(output->c_middle, customer->c_middle);
    strcpy(output->c_last, customer->c_last);

    // Find the row in the order table with largest o_id
    // HACK: We read lock the district instead of locking the index
    if (tid_ != -1) {
        District* d = findDistrict(customer->c_w_id, customer->c_d_id);
        tryRead(d);
    }
    Order* order = findLastOrderByCustomer(customer->c_w_id, customer->c_d_id, customer->c_id);
    tryRead(order);
    output->o_id = order->o_id;
    output->o_carrier_id = order->o_carrier_id;
    strcpy(output->o_entry_d, order->o_entry_d);

    output->lines.resize(order->o_ol_cnt);
    for (int32_t line_number = 1; line_number <= order->o_ol_cnt; ++line_number) {
        OrderLine* line = findOrderLine(customer->c_w_id, customer->c_d_id, order->o_id, line_number);
        tryRead(line);
        output->lines[line_number-1].ol_i_id = line->ol_i_id;
        output->lines[line_number-1].ol_supply_w_id = line->ol_supply_w_id;
        output->lines[line_number-1].ol_quantity = line->ol_quantity;
        output->lines[line_number-1].ol_amount = line->ol_amount;
        strcpy(output->lines[line_number-1].ol_delivery_d, line->ol_delivery_d);
    }
#ifndef NDEBUG
    // Verify that none of the other OrderLines exist.
    for (int32_t line_number = order->o_ol_cnt+1; line_number <= Order::MAX_OL_CNT; ++line_number) {
        assert(findOrderLine(customer->c_w_id, customer->c_d_id, order->o_id, line_number) == NULL);
    }
#endif
}

bool DBImpl::newOrder(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
        const std::vector<NewOrderItem>& items, const char* now, NewOrderOutput* output,
        TPCCUndo** undo) {
    // perform the home part
    bool result = newOrderHome(warehouse_id, district_id, customer_id, items, now, output, undo);
    if (!result) {
        return false;
    }

    // Process all remote warehouses
    WarehouseSet warehouses = newOrderRemoteWarehouses(warehouse_id, items);
    for (WarehouseSet::const_iterator i = warehouses.begin(); i != warehouses.end(); ++i) {
        vector<int32_t> quantities;
        result = newOrderRemote(warehouse_id, *i, items, &quantities, undo);
        assert(result);
        newOrderCombine(quantities, output);
    }

    return true;
}

bool DBImpl::newOrderHome(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
        const vector<NewOrderItem>& items, const char* now,
        NewOrderOutput* output, TPCCUndo** undo) {
    assert(tid_ == -1 || undo != NULL);
    //~ printf("new order %d %d %d %d %s\n", warehouse_id, district_id, customer_id, items.size(), now);
    // 2.4.3.4. requires that we display c_last, c_credit, and o_id for rolled back transactions:
    // read those values first
    District* d = findDistrict(warehouse_id, district_id);
    tryWrite(d);  // SELECT FOR UPDATE: we will update this soon; avoid upgrade deadlocks
    output->d_tax = d->d_tax;
    output->o_id = d->d_next_o_id;
    assert(findOrder(warehouse_id, district_id, output->o_id) == NULL);

    Customer* c = findCustomer(warehouse_id, district_id, customer_id);
    tryRead(c);
    assert(sizeof(output->c_last) == sizeof(c->c_last));
    memcpy(output->c_last, c->c_last, sizeof(output->c_last));
    memcpy(output->c_credit, c->c_credit, sizeof(output->c_credit));
    output->c_discount = c->c_discount;

    // CHEAT: Validate all items to see if we will need to abort
    vector<Item*> item_tuples(items.size());
    if (!findAndValidateItems(items, &item_tuples)) {
        strcpy(output->status, NewOrderOutput::INVALID_ITEM_STATUS);
        return false;
    }

    // Check if this is an all local transaction
    // TODO: This loops through items *again* which is slightly inefficient
    bool all_local = true;
    for (int i = 0; i < items.size(); ++i) {
        if (items[i].ol_supply_w_id != warehouse_id) {
            all_local = false;
            break;
        }
    }

    // We will not abort: update the status and the database state, allocate an undo buffer
    output->status[0] = '\0';
    allocateUndo(undo);

    // Modify the order id to assign it
    // tryWrite(d);  NOTE: already locked for write above
    if (undo != NULL) {
        (*undo)->save(d);
    }
    d->d_next_o_id += 1;

    Warehouse* w = findWarehouse(warehouse_id);
    tryRead(w);
    output->w_tax = w->w_tax;

    Order order;
    order.o_w_id = warehouse_id;
    order.o_d_id = district_id;
    order.o_id = output->o_id;
    order.o_c_id = customer_id;
    order.o_carrier_id = Order::NULL_CARRIER_ID;
    order.o_ol_cnt = static_cast<int32_t>(items.size());
    order.o_all_local = all_local ? 1 : 0;
    strcpy(order.o_entry_d, now);
    assert(strlen(order.o_entry_d) == DATETIME_SIZE);
    Order* o = insertOrder(order);
    NewOrder* no = insertNewOrder(warehouse_id, district_id, output->o_id);
    if (undo != NULL) {
        (*undo)->inserted(o);
        (*undo)->inserted(no);
    }
    tryWrite(o);
    tryWrite(no);

    OrderLine line;
    line.ol_o_id = output->o_id;
    line.ol_d_id = district_id;
    line.ol_w_id = warehouse_id;
    memset(line.ol_delivery_d, 0, DATETIME_SIZE+1);

    output->items.resize(items.size());
    output->total = 0;
    for (int i = 0; i < items.size(); ++i) {
        line.ol_number = i+1;
        line.ol_i_id = items[i].i_id;
        line.ol_supply_w_id = items[i].ol_supply_w_id;
        line.ol_quantity = items[i].ol_quantity;

        // Vertical Partitioning HACK: We read s_dist_xx from our local replica, assuming that
        // these columns are replicated everywhere.
        // TODO: I think this is unrealistic, since it will occupy ~23 MB per warehouse on all
        // replicas. Try the "two round" version in the future.
        Stock* stock = findStock(items[i].ol_supply_w_id, items[i].i_id);
        tryWrite(stock);  // SELECT FOR UPDATE: we may update this; avoid upgrade deadlocks
        tryRead(item_tuples[i]);
        assert(sizeof(line.ol_dist_info) == sizeof(stock->s_dist[district_id]));
        memcpy(line.ol_dist_info, stock->s_dist[district_id], sizeof(line.ol_dist_info));
        // Since we *need* to replicate s_dist_xx columns, might as well replicate s_data
        // Makes it 290 bytes per tuple, or ~28 MB per warehouse.
        bool stock_is_original = (strstr(stock->s_data, "ORIGINAL") != NULL);
        if (stock_is_original && strstr(item_tuples[i]->i_data, "ORIGINAL") != NULL) {
            output->items[i].brand_generic = NewOrderOutput::ItemInfo::BRAND;
        } else {
            output->items[i].brand_generic = NewOrderOutput::ItemInfo::GENERIC;
        }

        assert(sizeof(output->items[i].i_name) == sizeof(item_tuples[i]->i_name));
        memcpy(output->items[i].i_name, item_tuples[i]->i_name, sizeof(output->items[i].i_name));
        output->items[i].i_price = item_tuples[i]->i_price;
        output->items[i].ol_amount =
                static_cast<float>(items[i].ol_quantity) * item_tuples[i]->i_price;
        line.ol_amount = output->items[i].ol_amount;
        output->total += output->items[i].ol_amount;
        OrderLine* ol = insertOrderLine(line);
        if (undo != NULL) {
            (*undo)->inserted(ol);
        }
        tryWrite(ol);
    }

    // Perform the "remote" part for this warehouse
    // TODO: It might be more efficient to merge this into the loop above, but this is simpler.
    vector<int32_t> quantities;
    bool result = newOrderRemote(warehouse_id, warehouse_id, items, &quantities, undo);
    ASSERT(result);
    newOrderCombine(quantities, output);

    return true;
}

bool DBImpl::newOrderRemote(int32_t home_warehouse, int32_t remote_warehouse,
        const vector<NewOrderItem>& items, std::vector<int32_t>* out_quantities, TPCCUndo** undo) {
    // Validate all the items: needed so that we don't need to undo in order to execute this
    // TODO: item_tuples is unused. Remove?
    vector<Item*> item_tuples;
    if (!findAndValidateItems(items, &item_tuples)) {
        return false;
    }

    // We will not abort: allocate an undo buffer
    allocateUndo(undo);

    out_quantities->resize(items.size());
    for (int i = 0; i < items.size(); ++i) {
        // Skip items that don't belong to remote warehouse
        if (items[i].ol_supply_w_id != remote_warehouse) {
            (*out_quantities)[i] = INVALID_QUANTITY;
            continue;
        }

        // update stock
        Stock* stock = findStock(items[i].ol_supply_w_id, items[i].i_id);
        tryWrite(stock);
        if (undo != NULL) {
            (*undo)->save(stock);
        }
        if (stock->s_quantity >= items[i].ol_quantity + 10) {
            stock->s_quantity -= items[i].ol_quantity;
        } else {
            stock->s_quantity = stock->s_quantity - items[i].ol_quantity + 91;
        }
        (*out_quantities)[i] = stock->s_quantity;
        stock->s_ytd += items[i].ol_quantity;
        stock->s_order_cnt += 1;
        // newOrderHome calls newOrderRemote, so this is needed
        if (items[i].ol_supply_w_id != home_warehouse) {
            // remote order
            stock->s_remote_cnt += 1;
        }
    }

    return true;
}

bool DBImpl::findAndValidateItems(const vector<NewOrderItem>& items,
        vector<Item*>* item_tuples) {
    // CHEAT: Validate all items to see if we will need to abort
    // read lock the entire items table: protects from phantoms if someone does an insert. A real
    // implementation would need to do this.
    tryRead(&items_);
    item_tuples->resize(items.size());
    for (int i = 0; i < items.size(); ++i) {
        (*item_tuples)[i] = findItem(items[i].i_id);
        if ((*item_tuples)[i] == NULL) {
            return false;
        }
    }
    return true;
}


void DBImpl::payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
        int32_t c_district_id, int32_t customer_id, float h_amount, const char* now,
        PaymentOutput* output, TPCCUndo** undo) {
    //~ printf("payment %d %d %d %d %d %f %s\n", warehouse_id, district_id, c_warehouse_id, c_district_id, customer_id, h_amount, now);
    Customer* customer = findCustomer(c_warehouse_id, c_district_id, customer_id);
    paymentHome(warehouse_id, district_id, c_warehouse_id, c_district_id, customer_id, h_amount,
            now, output, undo);
    internalPaymentRemote(warehouse_id, district_id, customer, h_amount, output, undo);
}

void DBImpl::payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
        int32_t c_district_id, const char* c_last, float h_amount, const char* now,
        PaymentOutput* output, TPCCUndo** undo) {
    //~ printf("payment %d %d %d %d %s %f %s\n", warehouse_id, district_id, c_warehouse_id, c_district_id, c_last, h_amount, now);
    Customer* customer = findCustomerByName(c_warehouse_id, c_district_id, c_last);
    paymentHome(warehouse_id, district_id, c_warehouse_id, c_district_id, customer->c_id, h_amount,
            now, output, undo);
    internalPaymentRemote(warehouse_id, district_id, customer, h_amount, output, undo);
}

#define COPY_ADDRESS(src, dest, prefix) \
    Address::copy( \
            dest->prefix ## street_1, dest->prefix ## street_2, dest->prefix ## city, \
            dest->prefix ## state, dest->prefix ## zip,\
            src->prefix ## street_1, src->prefix ## street_2, src->prefix ## city, \
            src->prefix ## state, src->prefix ## zip)

#define ZERO_ADDRESS(output, prefix) \
    output->prefix ## street_1[0] = '\0'; \
    output->prefix ## street_2[0] = '\0'; \
    output->prefix ## city[0] = '\0'; \
    output->prefix ## state[0] = '\0'; \
    output->prefix ## zip[0] = '\0'

static void zeroWarehouseDistrict(PaymentOutput* output) {
    // Zero the warehouse and district data
    // TODO: I should split this structure, but I'm lazy
    ZERO_ADDRESS(output, w_);
    ZERO_ADDRESS(output, d_);
}

void DBImpl::paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
        int32_t c_district_id, int32_t c_id, float h_amount, PaymentOutput* output,
        TPCCUndo** undo) {
    Customer* customer = findCustomer(c_warehouse_id, c_district_id, c_id);
    internalPaymentRemote(warehouse_id, district_id, customer, h_amount, output, undo);
    zeroWarehouseDistrict(output);
}
void DBImpl::paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
        int32_t c_district_id, const char* c_last, float h_amount, PaymentOutput* output,
        TPCCUndo** undo) {
    Customer* customer = findCustomerByName(c_warehouse_id, c_district_id, c_last);
    internalPaymentRemote(warehouse_id, district_id, customer, h_amount, output, undo);
    zeroWarehouseDistrict(output);
}

void DBImpl::paymentHome(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
        int32_t c_district_id, int32_t customer_id, float h_amount, const char* now,
        PaymentOutput* output, TPCCUndo** undo) {
    Warehouse* w = findWarehouse(warehouse_id);
    tryWrite(w);
    if (undo != NULL) {
        allocateUndo(undo);
        (*undo)->save(w);
    }
    w->w_ytd += h_amount;
    COPY_ADDRESS(w, output, w_);

    District* d = findDistrict(warehouse_id, district_id);
    tryWrite(d);
    if (undo != NULL) {
        (*undo)->save(d);
    }
    d->d_ytd += h_amount;
    COPY_ADDRESS(d, output, d_);

    // Insert the line into the history table
    History h;
    h.h_w_id = warehouse_id;
    h.h_d_id = district_id;
    h.h_c_w_id = c_warehouse_id;
    h.h_c_d_id = c_district_id;
    h.h_c_id = customer_id;
    h.h_amount = h_amount;
    strcpy(h.h_date, now);
    strcpy(h.h_data, w->w_name);
    strcat(h.h_data, "    ");
    strcat(h.h_data, d->d_name);
    History* history = insertHistory(h);
    if (undo != NULL) {
        (*undo)->inserted(history);
    }
    tryWrite(history);

    // Zero all the customer fields: avoid uninitialized data for serialization
    output->c_credit_lim = 0;
    output->c_discount = 0;
    output->c_balance = 0;
    output->c_first[0] = '\0';
    output->c_middle[0] = '\0';
    output->c_last[0] = '\0';
    ZERO_ADDRESS(output, c_);
    output->c_phone[0] = '\0';
    output->c_since[0] = '\0';
    output->c_credit[0] = '\0';
    output->c_data[0] = '\0';
}

void DBImpl::internalPaymentRemote(int32_t warehouse_id, int32_t district_id, Customer* c,
        float h_amount, PaymentOutput* output, TPCCUndo** undo) {
    tryWrite(c);
    if (undo != NULL) {
        allocateUndo(undo);
        (*undo)->save(c);
    }
    c->c_balance -= h_amount;
    c->c_ytd_payment += h_amount;
    c->c_payment_cnt += 1;
    if (strcmp(c->c_credit, Customer::BAD_CREDIT) == 0) {
        // Bad credit: insert history into c_data
        static const int HISTORY_SIZE = Customer::MAX_DATA+1;
        char history[HISTORY_SIZE];
        int characters = snprintf(history, HISTORY_SIZE, "(%d, %d, %d, %d, %d, %.2f)\n",
                c->c_id, c->c_d_id, c->c_w_id, district_id, warehouse_id, h_amount);
        assert(characters < HISTORY_SIZE);

        // Perform the insert with a move and copy
        int current_keep = static_cast<int>(strlen(c->c_data));
        if (current_keep + characters > Customer::MAX_DATA) {
            current_keep = Customer::MAX_DATA - characters;
        }
        assert(current_keep + characters <= Customer::MAX_DATA);
        memmove(c->c_data+characters, c->c_data, current_keep);
        memcpy(c->c_data, history, characters);
        c->c_data[characters + current_keep] = '\0';
        assert(strlen(c->c_data) == characters + current_keep);
    }

    output->c_credit_lim = c->c_credit_lim;
    output->c_discount = c->c_discount;
    output->c_balance = c->c_balance;
#define COPY_STRING(dest, src, field) memcpy(dest->field, src->field, sizeof(src->field))
    COPY_STRING(output, c, c_first);
    COPY_STRING(output, c, c_middle);
    COPY_STRING(output, c, c_last);
    COPY_ADDRESS(c, output, c_);
    COPY_STRING(output, c, c_phone);
    COPY_STRING(output, c, c_since);
    COPY_STRING(output, c, c_credit);
    COPY_STRING(output, c, c_data);
#undef COPY_STRING
}

#undef ZERO_ADDRESS
#undef COPY_ADDRESS

// forward declaration for delivery
static int64_t makeNewOrderKey(int32_t w_id, int32_t d_id, int32_t o_id);

void DBImpl::delivery(int32_t warehouse_id, int32_t carrier_id, const char* now,
        std::vector<DeliveryOrderInfo>* orders, TPCCUndo** undo) {
    //~ printf("delivery %d %d %s\n", warehouse_id, carrier_id, now);
    allocateUndo(undo);
    orders->clear();
    for (int32_t d_id = 1; d_id <= District::NUM_PER_WAREHOUSE; ++d_id) {
        // HACK: Read lock the district instead of locking the index
        if (tid_ != -1) {
            District* d = findDistrict(warehouse_id, d_id);
            tryRead(d);
        }

        // Find and remove the lowest numbered order for the district
        int64_t key = makeNewOrderKey(warehouse_id, d_id, 1);
        NewOrderMap::iterator iterator = neworders_.lower_bound(key);
        NewOrder* neworder = NULL;
        if (iterator != neworders_.end()) {
            neworder = iterator->second;
            assert(neworder != NULL);
        }
        if (neworder == NULL || neworder->no_d_id != d_id || neworder->no_w_id != warehouse_id) {
            // No orders for this district
            // TODO: 2.7.4.2: If this occurs in max(1%, 1) of transactions, report it (???)
            continue;
        }
        assert(neworder->no_d_id == d_id && neworder->no_w_id == warehouse_id);
        int32_t o_id = neworder->no_o_id;
        neworders_.erase(iterator);
        if (undo != NULL) {
            (*undo)->deleted(neworder);
        } else {
            delete neworder;
        }
        tryWrite(neworder);

        DeliveryOrderInfo order;
        order.d_id = d_id;
        order.o_id = o_id;
        orders->push_back(order);

        Order* o = findOrder(warehouse_id, d_id, o_id);
        tryWrite(o);
        assert(o->o_carrier_id == Order::NULL_CARRIER_ID);
        if (undo != NULL) {
            (*undo)->save(o);
        }
        o->o_carrier_id = carrier_id;

        float total = 0;
        // TODO: Select based on (w_id, d_id, o_id) rather than using ol_number?
        for (int32_t i = 1; i <= o->o_ol_cnt; ++i) {
            OrderLine* line = findOrderLine(warehouse_id, d_id, o_id, i);
            tryWrite(line);
            if (undo != NULL) {
                (*undo)->save(line);
            }
            assert(0 == strlen(line->ol_delivery_d));
            strcpy(line->ol_delivery_d, now);
            assert(strlen(line->ol_delivery_d) == DATETIME_SIZE);
            total += line->ol_amount;
        }

        Customer* c = findCustomer(warehouse_id, d_id, o->o_c_id);
        tryWrite(c);
        if (undo != NULL) {
            (*undo)->save(c);
        }
        c->c_balance += total;
        c->c_delivery_cnt += 1;
    }
}

template <typename T>
static void restoreFromMap(const T& map) {
    for (typename T::const_iterator i = map.begin(); i != map.end(); ++i) {
        // Copy the original data back
        *(i->first) = *(i->second);
    }
}

template <typename T>
static void eraseTuple(const T& set, DBImpl* tables,
        void (DBImpl::*eraseFPtr)(typename T::value_type)) {
    for (typename T::const_iterator i = set.begin(); i != set.end(); ++i) {
        // Invoke eraseFPtr on each value
        (tables->*eraseFPtr)(*i);
    }
}

// Used by both applyUndo and insertNewOrder to put allocated NewOrder tuples in the map.
static NewOrder* insertNewOrderObject(std::map<int64_t, NewOrder*>* map, NewOrder* neworder) {
    int64_t key = makeNewOrderKey(neworder->no_w_id, neworder->no_d_id, neworder->no_o_id);
    assert(map->find(key) == map->end());
    map->insert(std::make_pair(key, neworder));
    return neworder;
}

void DBImpl::applyUndo(TPCCUndo* undo) {
    restoreFromMap(undo->modified_warehouses());
    restoreFromMap(undo->modified_districts());
    restoreFromMap(undo->modified_customers());
    restoreFromMap(undo->modified_stock());
    restoreFromMap(undo->modified_orders());
    restoreFromMap(undo->modified_order_lines());

    eraseTuple(undo->inserted_orders(), this, &DBImpl::eraseOrder);
    eraseTuple(undo->inserted_order_lines(), this, &DBImpl::eraseOrderLine);
    eraseTuple(undo->inserted_new_orders(), this, &DBImpl::eraseNewOrder);
    eraseTuple(undo->inserted_history(), this, &DBImpl::eraseHistory);

    // Transfer deleted new orders back to the database
    for (TPCCUndo::NewOrderDeletedSet::const_iterator i = undo->deleted_new_orders().begin();
            i != undo->deleted_new_orders().end();
            ++i) {
        NewOrder* neworder = *i;
        insertNewOrderObject(&neworders_, neworder);
    }

    undo->applied();
    delete undo;
}

template <typename T>
static T* insert(BPlusTree<int32_t, T*, DBImpl::KEYS_PER_INTERNAL, DBImpl::KEYS_PER_LEAF>* tree, int32_t key, const T& item) {
    assert(!tree->find(key));
    T* copy = new T(item);
    tree->insert(key, copy);
    return copy;
}

template <typename T>
static T* find(const BPlusTree<int32_t, T*, DBImpl::KEYS_PER_INTERNAL, DBImpl::KEYS_PER_LEAF>& tree, int32_t key) {
    T* output = NULL;
    if (tree.find(key, &output)) {
        return output;
    }
    return NULL;
}

template <typename T, typename KeyType>
static void erase(BPlusTree<KeyType, T*, DBImpl::KEYS_PER_INTERNAL, DBImpl::KEYS_PER_LEAF>* tree,
        KeyType key, const T* value) {
    T* out = NULL;
    ASSERT(tree->find(key, &out));
    ASSERT(out == value);
    bool result = tree->del(key);
    ASSERT(result);
    ASSERT(!tree->find(key));
}

void DBImpl::insertItem(const Item& item) {
    assert(item.i_id == items_.size() + 1);
    items_.push_back(item);
}
Item* DBImpl::findItem(int32_t id) {
    assert(1 <= id);
    id -= 1;
    if (id >= items_.size()) return NULL;
    return &items_[id];
}

void DBImpl::insertWarehouse(const Warehouse& w) {
    insert(&warehouses_, w.w_id, w);
}
Warehouse* DBImpl::findWarehouse(int32_t id) {
    return find(warehouses_, id);
}

static int32_t makeStockKey(int32_t w_id, int32_t s_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= s_id && s_id <= Stock::NUM_STOCK_PER_WAREHOUSE);
    int32_t id = s_id + (w_id * Stock::NUM_STOCK_PER_WAREHOUSE);
    assert(id >= 0);
    return id;
}

void DBImpl::insertStock(const Stock& stock) {
    insert(&stock_, makeStockKey(stock.s_w_id, stock.s_i_id), stock);
}
Stock* DBImpl::findStock(int32_t w_id, int32_t s_id) {
    return find(stock_, makeStockKey(w_id, s_id));
}

static int32_t makeDistrictKey(int32_t w_id, int32_t d_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    int32_t id = d_id + (w_id * District::NUM_PER_WAREHOUSE);
    assert(id >= 0);
    return id;
}

void DBImpl::insertDistrict(const District& district) {
    insert(&districts_, makeDistrictKey(district.d_w_id, district.d_id), district);
}
District* DBImpl::findDistrict(int32_t w_id, int32_t d_id) {
    return find(districts_, makeDistrictKey(w_id, d_id));
}

static int32_t makeCustomerKey(int32_t w_id, int32_t d_id, int32_t c_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= c_id && c_id <= Customer::NUM_PER_DISTRICT);
    int32_t id = (w_id * District::NUM_PER_WAREHOUSE + d_id)
            * Customer::NUM_PER_DISTRICT + c_id;
    assert(id >= 0);
    return id;
}

void DBImpl::insertCustomer(const Customer& customer) {
    Customer* c = insert(&customers_, makeCustomerKey(customer.c_w_id, customer.c_d_id, customer.c_id), customer);
    assert(customers_by_name_.find(c) == customers_by_name_.end());
    customers_by_name_.insert(c);
}
Customer* DBImpl::findCustomer(int32_t w_id, int32_t d_id, int32_t c_id) {
    return find(customers_, makeCustomerKey(w_id, d_id, c_id));
}

Customer* DBImpl::findCustomerByName(int32_t w_id, int32_t d_id, const char* c_last) {
    // select (w_id, d_id, *, c_last) order by c_first
    Customer c;
    c.c_w_id = w_id;
    c.c_d_id = d_id;
    strcpy(c.c_last, c_last);
    c.c_first[0] = '\0';
    CustomerByNameSet::const_iterator it = customers_by_name_.lower_bound(&c);
    assert(it != customers_by_name_.end());
    assert((*it)->c_w_id == w_id && (*it)->c_d_id == d_id && strcmp((*it)->c_last, c_last) == 0);

    // go to the "next" c_last
    // TODO: This is a GROSS hack. Can we do better?
    int length = static_cast<int>(strlen(c_last));
    if (length == Customer::MAX_LAST) {
        c.c_last[length-1] = static_cast<char>(c.c_last[length-1] + 1);
    } else {
        c.c_last[length] = 'A';
        c.c_last[length+1] = '\0';
    }
    CustomerByNameSet::const_iterator stop = customers_by_name_.lower_bound(&c);

    Customer* customer = NULL;
    // Choose position n/2 rounded up (1 based addressing) = floor((n-1)/2)
    if (it != stop) {
        CustomerByNameSet::const_iterator middle = it;
        ++it;
        int i = 0;
        while (it != stop) {
            // Increment the middle iterator on every second iteration
            if (i % 2 == 1) {
                ++middle;
            }
            tryRead(*it);
            assert(strcmp((*it)->c_last, c_last) == 0);
            ++it;
            ++i;
        }
        // There were i+1 matching last names
        customer = *middle;
    }

    assert(customer->c_w_id == w_id && customer->c_d_id == d_id &&
            strcmp(customer->c_last, c_last) == 0);
    return customer;
}

static int32_t makeOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
    // TODO: This is bad for locality since o_id is in the most significant position. Larger keys?
    int32_t id = (o_id * District::NUM_PER_WAREHOUSE + d_id)
            * Warehouse::MAX_WAREHOUSE_ID + w_id;
    assert(id >= 0);
    return id;
}

static int64_t makeOrderByCustomerKey(int32_t w_id, int32_t d_id, int32_t c_id, int32_t o_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= c_id && c_id <= Customer::NUM_PER_DISTRICT);
    assert(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
    int32_t top_id = (w_id * District::NUM_PER_WAREHOUSE + d_id) * Customer::NUM_PER_DISTRICT
            + c_id;
    assert(top_id >= 0);
    int64_t id = (((int64_t) top_id) << 32) | o_id;
    assert(id > 0);
    return id;
}

Order* DBImpl::insertOrder(const Order& order) {
    Order* tuple = insert(&orders_, makeOrderKey(order.o_w_id, order.o_d_id, order.o_id), order);
    // Secondary index based on customer id
    int64_t key = makeOrderByCustomerKey(order.o_w_id, order.o_d_id, order.o_c_id, order.o_id);
    assert(!orders_by_customer_.find(key));
    orders_by_customer_.insert(key, tuple);
    return tuple;
}
Order* DBImpl::findOrder(int32_t w_id, int32_t d_id, int32_t o_id) {
    return find(orders_, makeOrderKey(w_id, d_id, o_id));
}
Order* DBImpl::findLastOrderByCustomer(const int32_t w_id, const int32_t d_id, const int32_t c_id) {
    Order* order = NULL;

    // Increment the (w_id, d_id, c_id) tuple
    int64_t key = makeOrderByCustomerKey(w_id, d_id, c_id, 1);
    key += ((int64_t)1) << 32;
    ASSERT(key > 0);

    bool found = orders_by_customer_.findLastLessThan(key, &order);
    ASSERT(!found || (order->o_w_id == w_id && order->o_d_id == d_id && order->o_c_id == c_id));
    return order;
}
void DBImpl::eraseOrder(const Order* order) {
    int32_t primary = makeOrderKey(order->o_w_id, order->o_d_id, order->o_id);
    erase(&orders_, primary, order);

    // Secondary index based on customer id
    int64_t secondary =
            makeOrderByCustomerKey(order->o_w_id, order->o_d_id, order->o_c_id, order->o_id);
    erase(&orders_by_customer_, secondary, order);
    delete order;
}

static int32_t makeOrderLineKey(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
    assert(1 <= number && number <= Order::MAX_OL_CNT);
    // TODO: This may be bad for locality since o_id is in the most significant position. However,
    // Order status fetches all rows for one (w_id, d_id, o_id) tuple, so it may be fine,
    // but stock level fetches order lines for a range of (w_id, d_id, o_id) values
    int32_t id = ((o_id * District::NUM_PER_WAREHOUSE + d_id)
            * Warehouse::MAX_WAREHOUSE_ID + w_id) * Order::MAX_OL_CNT + number;
    assert(id >= 0);
    return id;
}

OrderLine* DBImpl::insertOrderLine(const OrderLine& orderline) {
    int32_t key = makeOrderLineKey(
            orderline.ol_w_id, orderline.ol_d_id, orderline.ol_o_id, orderline.ol_number);
    return insert(&orderlines_, key, orderline);
}
OrderLine* DBImpl::findOrderLine(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number) {
    return find(orderlines_, makeOrderLineKey(w_id, d_id, o_id, number));
}
void DBImpl::eraseOrderLine(const OrderLine* order_line) {
    int32_t key = makeOrderLineKey(
            order_line->ol_w_id, order_line->ol_d_id, order_line->ol_o_id, order_line->ol_number);
    erase(&orderlines_, key, order_line);
    delete order_line;
}

static int64_t makeNewOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
    int32_t upper_id = w_id * Warehouse::MAX_WAREHOUSE_ID + d_id;
    assert(upper_id > 0);
    int64_t id = static_cast<int64_t>(upper_id) << 32 | o_id;
    assert(id > 0);
    return id;
}

NewOrder* DBImpl::insertNewOrder(int32_t w_id, int32_t d_id, int32_t o_id) {
    NewOrder* neworder = new NewOrder();
    neworder->no_w_id = w_id;
    neworder->no_d_id = d_id;
    neworder->no_o_id = o_id;

    return insertNewOrderObject(&neworders_, neworder);
}

NewOrder* DBImpl::findNewOrder(int32_t w_id, int32_t d_id, int32_t o_id) {
    NewOrderMap::const_iterator it = neworders_.find(makeNewOrderKey(w_id, d_id, o_id));
    if (it == neworders_.end()) return NULL;
    assert(it->second != NULL);
    return it->second;
}
void DBImpl::eraseNewOrder(const NewOrder* new_order) {
    NewOrderMap::iterator it = neworders_.find(
            makeNewOrderKey(new_order->no_w_id, new_order->no_d_id, new_order->no_o_id));
    assert(it != neworders_.end());
    assert(it->second == new_order);
    neworders_.erase(it);
    delete new_order;
}

History* DBImpl::insertHistory(const History& history) {
    History* h = new History(history);
    history_.push_back(h);
    return h;
}
void DBImpl::eraseHistory(const History* history) {
    // Search backwards to find the history: it likely was inserted recently (or last)
    bool found = false;
    for (int i = static_cast<int>(history_.size())-1; i >= 0; --i) {
        if (history == history_[i]) {
            if (i != history_.size() - 1) {
                // erase not at end: move the last element here
                history_[i] = history_[history_.size() - 1];
            }
            found = true;
            break;
        }
    }
    assert(found);
    // Remove the last element
    history_.pop_back();
    delete history;
}

void DBImpl::setTransaction(int64_t tid) {
	tid_ = tid;
	//assert(tid_locks_.find(tid) == tid_locks_.end());
	// TODO (HACK) not acquiring locks for tid 0, see granola_tpcc_twopc_server
	if (tid > 0) {
		tid_locks_.insert(TIDLocksType::value_type(tid, new set<void*>()));
	}
}


// releases locks and deletes locksets for this transaction
void DBImpl::endTransaction(int64_t tid) {
	assert(tid > 0);
	assert(tid_locks_.find(tid) != tid_locks_.end());
	set<void*>* locked_tuples = tid_locks_.find(tid)->second;

	// decrement reference count on each lock held, and delete if empty
	for (set<void*>::iterator it = locked_tuples->begin();
			it != locked_tuples->end(); it++) {
		void* tuple = *it;
		//assert(lock_table_.find(tuple) != lock_table_.end());
		Lock* l = lock_table_.find(tuple)->second;
		assert(l->refcnt >= 1);
		l->refcnt--;
		// TODO should we be garbage-collecting locks lazily instead?
		if (l->refcnt == 0) {
			lock_table_.erase(tuple);
			delete l;
		}
	}
	tid_locks_.erase(tid);
	delete locked_tuples;
}


void DBImpl::tryLock(void* tuple, bool exclusive) {
	// TODO (HACK) not acquiring locks for tid 0, see granola_tpcc_twopc_server
	if (tid_ == 0) {
		LockTableType::iterator it = lock_table_.find(tuple);
		if (it != lock_table_.end()) { // lock already exists
			Lock* l = it->second;
			assert(l->refcnt >= 1);
			if (l->exclusive || exclusive) {
				throw LockBlockedException();
			}
		}

	} else {
		// TODO it might be more efficient if we just map from tid to lock objects
		// then when we delete we just decrement reference count, and garbage
		// collect later. this will avoid repeatedly creating the same lock
		assert(tid_locks_.find(tid_) != tid_locks_.end());
		set<void*>* locked_tuples = tid_locks_.find(tid_)->second;

		std::pair<LockTableType::iterator, bool> result = lock_table_.insert(
				LockTableType::value_type(tuple, NULL));

		if (result.second) { // no lock exists
			result.first->second = new Lock(exclusive, 1);
			locked_tuples->insert(tuple);
		} else {

			Lock* l = result.first->second;
			assert(l->refcnt >= 1);
			if (locked_tuples->find(tuple) != locked_tuples->end()) { // already have it
				if (l->refcnt == 1) {
					l->exclusive = l->exclusive || exclusive; // upgrade if necessary
				} else if (exclusive) {
					throw LockBlockedException();
				} else {
					assert(!l->exclusive);
				}
			} else {
				if (l->exclusive || exclusive) {
					throw LockBlockedException();
				} else {
					l->refcnt++;
					locked_tuples->insert(tuple);
				}
			}
		}
	}
}

void DBImpl::tryRead(void* tuple) {
    assert(tuple != NULL);
    if (tid_ != -1) tryLock(tuple, false);
}

void DBImpl::tryWrite(void* tuple) {
    assert(tuple != NULL);
    if (tid_ != -1) tryLock(tuple, true);
}

