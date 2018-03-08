#ifndef TPCCGENERATOR_H__
#define TPCCGENERATOR_H__

//~ #include <cstdint>
#include <stdint.h>

#include "store/apps/tpcc/tpccdb.h"

namespace tpcc {
class RandomGenerator;
}

class DBImpl;

class TPCCGenerator {
public:
    // Owns generator
    TPCCGenerator(tpcc::RandomGenerator* random, const char* now, int num_items,
            int districts_per_warehouse, int customers_per_district, int new_orders_per_district);

    ~TPCCGenerator();

    // Fills item with random data according to the TPC-C specification 4.3.3.1.
    void generateItem(int32_t id, bool original, Item* item);

    // Generates num_items items and inserts them into tables.
    void makeItemsTable(DBImpl* tables);

    // Fills warehouse with random data according to the TPC-C specification 4.3.3.1.
    void generateWarehouse(int32_t id, Warehouse* warehouse);

    // Fills stock with random data according to the TPC-C specification 4.3.3.1.
    void generateStock(int32_t id, int32_t w_id, bool original, Stock* stock);

    // Fills district with random data according to the TPC-C specification 4.3.3.1.
    void generateDistrict(int32_t id, int32_t w_id, District* district);

    void generateCustomer(int32_t id, int32_t d_id, int32_t w_id, bool bad_credit,
            Customer* customer);

    void generateOrder(int32_t id, int32_t c_id, int32_t d_id, int32_t w_id, bool new_order,
            Order* order);

    void generateOrderLine(int32_t number, int32_t o_id, int32_t d_id, int32_t w_id, bool new_order,
            OrderLine* orderline);

    void generateHistory(int32_t c_id, int32_t d_id, int32_t w_id, History* history);

    // Generates stock rows for w_id.
    void makeStock(DBImpl* tables, int32_t w_id);

    // Generates one warehouse and all related rows.
    void makeWarehouse(DBImpl* tables, int32_t w_id);

    // Generates one warehouse and related rows, except stock.
    // TODO: This exists to support partitioning. Does this make sense?
    void makeWarehouseWithoutStock(DBImpl* tables, int32_t w_id);

private:
    tpcc::RandomGenerator* random_;
    char now_[DATETIME_SIZE+1];

    int num_items_;
    int districts_per_warehouse_;
    int customers_per_district_;
    int new_orders_per_district_;
};

#endif
