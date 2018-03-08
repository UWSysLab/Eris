#include "store/apps/tpcc/tpccgenerator.h"

#include <set>

#include "store/apps/tpcc/randomgenerator.h"
#include "store/apps/tpcc/assert.h"
#include "store/apps/tpcc/dbimpl.h"

using std::set;

TPCCGenerator::TPCCGenerator(tpcc::RandomGenerator* random, const char* now, int num_items,
        int districts_per_warehouse, int customers_per_district, int new_orders_per_district) :
        random_(random),
        num_items_(num_items),
        districts_per_warehouse_(districts_per_warehouse),
        customers_per_district_(customers_per_district),
        new_orders_per_district_(new_orders_per_district) {
    assert(strlen(now) == DATETIME_SIZE);
    strcpy(now_, now);

    assert(1 <= num_items_ && num_items_ <= Item::NUM_ITEMS);
    assert(1 <= districts_per_warehouse_ &&
            districts_per_warehouse_ <= District::NUM_PER_WAREHOUSE);
    assert(1 <= customers_per_district_ &&
                customers_per_district_ <= Customer::NUM_PER_DISTRICT);
    assert(1 <= new_orders_per_district_ &&
            new_orders_per_district_ <= NewOrder::INITIAL_NUM_PER_DISTRICT);
    assert(new_orders_per_district_ <= customers_per_district_);
}

TPCCGenerator::~TPCCGenerator() {
    delete random_;
}

static void setOriginal(tpcc::RandomGenerator* random, char* s) {
    int length = static_cast<int>(strlen(s));
    int position = random->number(0, length-8);
    memcpy(s + position, "ORIGINAL", 8);
}

void TPCCGenerator::generateItem(int32_t id, bool original, Item* item) {
    assert(1 <= id && id <= num_items_);
    item->i_id = id;
    item->i_im_id = random_->number(Item::MIN_IM, Item::MAX_IM);
    item->i_price = random_->fixedPoint(2, Item::MIN_PRICE, Item::MAX_PRICE);
    random_->astring(item->i_name, Item::MIN_NAME, Item::MAX_NAME);
    random_->astring(item->i_data, Item::MIN_DATA, Item::MAX_DATA);

    if (original) {
        setOriginal(random_, item->i_data);
    }
}

static set<int> selectUniqueIds(tpcc::RandomGenerator* random, int num_unique, int lower_id,
        int upper_id) {
    set<int> rows;
    for (int i = 0; i < num_unique; ++i) {
        int index = -1;
        do {
            index = random->number(lower_id, upper_id);
        } while (rows.find(index) != rows.end());
        rows.insert(index);
    }
    assert(rows.size() == num_unique);
    return rows;
}

// Generates num_items items and inserts them into tables.
void TPCCGenerator::makeItemsTable(DBImpl* tables) {
    tables->reserveItems(num_items_);

    // Select 10% of the rows to be marked "original"
    set<int> original_rows = selectUniqueIds(random_, num_items_/10, 1, num_items_);

    for (int i = 1; i <= num_items_; ++i) {
        Item item;
        bool is_original = original_rows.find(i) != original_rows.end();
        generateItem(i, is_original, &item);
        tables->insertItem(item);
    }
}

static float makeTax(tpcc::RandomGenerator* random) {
    assert(Warehouse::MIN_TAX == District::MIN_TAX);
    assert(Warehouse::MAX_TAX == District::MAX_TAX);
    return random->fixedPoint(4, Warehouse::MIN_TAX, Warehouse::MAX_TAX);
}

static void makeZip(tpcc::RandomGenerator* random, char* zip) {
    random->nstring(zip, 4, 4);
    memcpy(zip+4, "11111", 6);
}

void TPCCGenerator::generateWarehouse(int32_t id, Warehouse* warehouse) {
    assert(1 <= id && id <= Warehouse::MAX_WAREHOUSE_ID);
    warehouse->w_id = id;
    warehouse->w_tax = makeTax(random_);
    warehouse->w_ytd = Warehouse::INITIAL_YTD;
    random_->astring(warehouse->w_name, Warehouse::MIN_NAME, Warehouse::MAX_NAME);
    random_->astring(warehouse->w_street_1, Address::MIN_STREET, Address::MAX_STREET);
    random_->astring(warehouse->w_street_2, Address::MIN_STREET, Address::MAX_STREET);
    random_->astring(warehouse->w_city, Address::MIN_CITY, Address::MAX_CITY);
    random_->astring(warehouse->w_state, Address::STATE, Address::STATE);
    makeZip(random_, warehouse->w_zip);
}

void TPCCGenerator::generateStock(int32_t id, int32_t w_id, bool original, Stock* stock) {
    assert(1 <= id && id <= num_items_);
    stock->s_i_id = id;
    stock->s_w_id = w_id;
    stock->s_quantity = random_->number(Stock::MIN_QUANTITY, Stock::MAX_QUANTITY);
    stock->s_ytd = 0;
    stock->s_order_cnt = 0;
    stock->s_remote_cnt = 0;
    for (int i = 0; i < District::NUM_PER_WAREHOUSE; ++i) {
        random_->astring(stock->s_dist[i], sizeof(stock->s_dist[i])-1, sizeof(stock->s_dist[i])-1);
    }
    random_->astring(stock->s_data, Stock::MIN_DATA, Stock::MAX_DATA);

    if (original) {
        setOriginal(random_, stock->s_data);
    }
}

void TPCCGenerator::generateDistrict(int32_t id, int32_t w_id, District* district) {
    assert(1 <= id && id <= districts_per_warehouse_);
    district->d_id = id;
    district->d_w_id = w_id;
    district->d_tax = makeTax(random_);
    district->d_ytd = District::INITIAL_YTD;
    district->d_next_o_id = customers_per_district_ + 1;
    random_->astring(district->d_name, District::MIN_NAME, District::MAX_NAME);
    random_->astring(district->d_street_1, Address::MIN_STREET, Address::MAX_STREET);
    random_->astring(district->d_street_2, Address::MIN_STREET, Address::MAX_STREET);
    random_->astring(district->d_city, Address::MIN_CITY, Address::MAX_CITY);
    random_->astring(district->d_state, Address::STATE, Address::STATE);
    makeZip(random_, district->d_zip);
}

void TPCCGenerator::generateCustomer(int32_t id, int32_t d_id, int32_t w_id, bool bad_credit,
        Customer* customer) {
    assert(1 <= id && id <= customers_per_district_);
    customer->c_id = id;
    customer->c_d_id = d_id;
    customer->c_w_id = w_id;
    customer->c_credit_lim = Customer::INITIAL_CREDIT_LIM;
    customer->c_discount = random_->fixedPoint(4, Customer::MIN_DISCOUNT, Customer::MAX_DISCOUNT);
    customer->c_balance = Customer::INITIAL_BALANCE;
    customer->c_ytd_payment = Customer::INITIAL_YTD_PAYMENT;
    customer->c_payment_cnt = Customer::INITIAL_PAYMENT_CNT;
    customer->c_delivery_cnt = Customer::INITIAL_DELIVERY_CNT;
    random_->astring(customer->c_first, Customer::MIN_FIRST, Customer::MAX_FIRST);
    strcpy(customer->c_middle, "OE");

    if (id <= 1000) {
        tpcc::makeLastName(id-1, customer->c_last);
    } else {
        random_->lastName(customer->c_last, customers_per_district_);
    }

    random_->astring(customer->c_street_1, Address::MIN_STREET, Address::MAX_STREET);
    random_->astring(customer->c_street_2, Address::MIN_STREET, Address::MAX_STREET);
    random_->astring(customer->c_city, Address::MIN_CITY, Address::MAX_CITY);
    random_->astring(customer->c_state, Address::STATE, Address::STATE);
    makeZip(random_, customer->c_zip);
    random_->nstring(customer->c_phone, Customer::PHONE, Customer::PHONE);
    strcpy(customer->c_since, now_);
    assert(strlen(customer->c_since) == DATETIME_SIZE);
    if (bad_credit) {
        strcpy(customer->c_credit, Customer::BAD_CREDIT);
    } else {
        strcpy(customer->c_credit, Customer::GOOD_CREDIT);
    }
    random_->astring(customer->c_data, Customer::MIN_DATA, Customer::MAX_DATA);
}

void TPCCGenerator::generateOrder(int32_t id, int32_t c_id, int32_t d_id, int32_t w_id,
        bool new_order, Order* order) {
    order->o_id = id;
    order->o_c_id = c_id;
    order->o_d_id = d_id;
    order->o_w_id = w_id;
    if (!new_order) {
        order->o_carrier_id = random_->number(Order::MIN_CARRIER_ID, Order::MAX_CARRIER_ID);
    } else {
        order->o_carrier_id = Order::NULL_CARRIER_ID;
    }
    order->o_ol_cnt = random_->number(Order::MIN_OL_CNT, Order::MAX_OL_CNT);
    order->o_all_local = Order::INITIAL_ALL_LOCAL;
    strcpy(order->o_entry_d, now_);
    assert(strlen(order->o_entry_d) == DATETIME_SIZE);
}

void TPCCGenerator::generateOrderLine(int32_t number, int32_t o_id, int32_t d_id, int32_t w_id,
        bool new_order, OrderLine* orderline) {
    orderline->ol_o_id = o_id;
    orderline->ol_d_id = d_id;
    orderline->ol_w_id = w_id;
    orderline->ol_number = number;
    orderline->ol_i_id = random_->number(OrderLine::MIN_I_ID, OrderLine::MAX_I_ID);
    orderline->ol_supply_w_id = w_id;
    orderline->ol_quantity = OrderLine::INITIAL_QUANTITY;
    if (!new_order) {
        orderline->ol_amount = 0.00;
        strcpy(orderline->ol_delivery_d, now_);
    } else {
        orderline->ol_amount = random_->fixedPoint(2, OrderLine::MIN_AMOUNT, OrderLine::MAX_AMOUNT);
        // HACK: Empty delivery date == null
        orderline->ol_delivery_d[0] = '\0';
    }
    random_->astring(orderline->ol_dist_info, sizeof(orderline->ol_dist_info)-1,
            sizeof(orderline->ol_dist_info)-1);
}

void TPCCGenerator::generateHistory(int32_t c_id, int32_t d_id, int32_t w_id, History* history) {
    history->h_c_id = c_id;
    history->h_c_d_id = d_id;
    history->h_d_id = d_id;
    history->h_c_w_id = w_id;
    history->h_w_id = w_id;
    history->h_amount = History::INITIAL_AMOUNT;
    strcpy(history->h_date, now_);
    assert(strlen(history->h_date) == DATETIME_SIZE);
    random_->astring(history->h_data, History::MIN_DATA, History::MAX_DATA);
}

void TPCCGenerator::makeStock(DBImpl* tables, int32_t w_id) {
    // Select 10% of the stock to be marked "original"
    set<int> selected_rows = selectUniqueIds(random_, num_items_/10, 1, num_items_);

    for (int i = 1; i <= num_items_; ++i) {
        Stock s;
        bool is_original = selected_rows.find(i) != selected_rows.end();
        generateStock(i, w_id, is_original, &s);
        tables->insertStock(s);
    }
}

void TPCCGenerator::makeWarehouse(DBImpl* tables, int32_t w_id) {
    makeStock(tables, w_id);
    makeWarehouseWithoutStock(tables, w_id);
}

void TPCCGenerator::makeWarehouseWithoutStock(DBImpl* tables, int32_t w_id) {
    Warehouse w;
    generateWarehouse(w_id, &w);
    tables->insertWarehouse(w);

    for (int d_id = 1; d_id <= districts_per_warehouse_; ++d_id) {
        District d;
        generateDistrict(d_id, w_id, &d);
        tables->insertDistrict(d);

        // Select 10% of the customers to have bad credit
        set<int> selected_rows = selectUniqueIds(random_, customers_per_district_/10, 1,
                customers_per_district_);
        for (int c_id = 1; c_id <= customers_per_district_; ++c_id) {
            Customer c;
            bool bad_credit = selected_rows.find(c_id) != selected_rows.end();
            generateCustomer(c_id, d_id, w_id, bad_credit, &c);
            tables->insertCustomer(c);

            History h;
            generateHistory(c_id, d_id, w_id, &h);
            tables->insertHistory(h);
        }

        // TODO: TPC-C 4.3.3.1. says that this should be a permutation of [1, 3000]. But since it is
        // for a c_id field, it seems to make sense to have it be a permutation of the customers.
        // For the "real" thing this will be equivalent
        int* permutation = random_->makePermutation(1, customers_per_district_);
        for (int o_id = 1; o_id <= customers_per_district_; ++o_id) {
            // The last new_orders_per_district_ orders are new
            bool new_order = customers_per_district_ - new_orders_per_district_ < o_id;
            Order o;
            generateOrder(o_id, permutation[o_id-1], d_id, w_id, new_order, &o);
            tables->insertOrder(o);

            // Generate each OrderLine for the order
            for (int ol_number = 1; ol_number <= o.o_ol_cnt; ++ol_number) {
                OrderLine line;
                generateOrderLine(ol_number, o_id, d_id, w_id, new_order, &line);
                tables->insertOrderLine(line);
            }

            if (new_order) {
                // This is a new order: make one for it
                tables->insertNewOrder(w_id, d_id, o_id);
            }
        }
        delete[] permutation;
    }
}

