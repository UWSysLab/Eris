#ifndef TPCCTABLES_H__
#define TPCCTABLES_H__

#include <map>
#include <set>
#include <vector>
#include <tr1/unordered_map>

#include "store/apps/tpcc/btree.h"
#include "store/apps/tpcc/tpccdb.h"
#include "store/apps/tpcc/assert.h"

class CustomerByNameOrdering {
public:
    bool operator()(const Customer* a, const Customer* b);
};


class Lock {
public:
	bool exclusive; // write locks are exclusive, read locks aren't
	int refcnt; // number of transactions holding lock
	Lock(bool exclusive, int refcnt) : exclusive(exclusive), refcnt(refcnt) {}
};

// Stores all the tables in TPC-C
class DBImpl : public TPCCDB {
public:
	DBImpl() : tid_(-1) {}
    virtual ~DBImpl();

    virtual int32_t stockLevel(int32_t warehouse_id, int32_t district_id, int32_t threshold);
    virtual void orderStatus(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
            OrderStatusOutput* output);
    virtual void orderStatus(int32_t warehouse_id, int32_t district_id, const char* c_last,
            OrderStatusOutput* output);
    virtual bool newOrder(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
            const std::vector<NewOrderItem>& items, const char* now,
            NewOrderOutput* output, TPCCUndo** undo);
    virtual bool newOrderHome(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
            const std::vector<NewOrderItem>& items, const char* now,
            NewOrderOutput* output, TPCCUndo** undo);
    virtual bool newOrderRemote(int32_t home_warehouse, int32_t remote_warehouse,
            const std::vector<NewOrderItem>& items, std::vector<int32_t>* out_quantities,
            TPCCUndo** undo);
    virtual void payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
            int32_t c_district_id, int32_t customer_id, float h_amount, const char* now,
            PaymentOutput* output, TPCCUndo** undo);
    virtual void payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
            int32_t c_district_id, const char* c_last, float h_amount, const char* now,
            PaymentOutput* output, TPCCUndo** undo);
    virtual void paymentHome(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
            int32_t c_district_id, int32_t c_id, float h_amount, const char* now,
            PaymentOutput* output, TPCCUndo** undo);
    virtual void paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
            int32_t c_district_id, int32_t c_id, float h_amount, PaymentOutput* output,
            TPCCUndo** undo);
    virtual void paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
            int32_t c_district_id, const char* c_last, float h_amount, PaymentOutput* output,
            TPCCUndo** undo);
    virtual void delivery(int32_t warehouse_id, int32_t carrier_id, const char* now,
            std::vector<DeliveryOrderInfo>* orders, TPCCUndo** undo);
    virtual bool hasWarehouse(int32_t warehouse_id) { return findWarehouse(warehouse_id) != NULL; }
    virtual void applyUndo(TPCCUndo* undo);
    virtual void freeUndo(TPCCUndo* undo) { assert(undo != NULL); delete undo; }
    
    void reserveItems(int size) { items_.reserve(size); }
    // Copies item into the item table.
    void insertItem(const Item& item);
    Item* findItem(int32_t i_id);

    void insertWarehouse(const Warehouse& warehouse);
    Warehouse* findWarehouse(int32_t w_id);

    void insertStock(const Stock& stock);
    Stock* findStock(int32_t w_id, int32_t s_id);

    void insertDistrict(const District& district);
    District* findDistrict(int32_t w_id, int32_t d_id);

    void insertCustomer(const Customer& customer);
    Customer* findCustomer(int32_t w_id, int32_t d_id, int32_t c_id);
    // Finds all customers that match (w_id, d_id, *, c_last), taking the n/2th one (rounded up).
    Customer* findCustomerByName(int32_t w_id, int32_t d_id, const char* c_last);

    // Stores order in the database. Returns a pointer to the database's tuple.
    Order* insertOrder(const Order& order);
    Order* findOrder(int32_t w_id, int32_t d_id, int32_t o_id);
    Order* findLastOrderByCustomer(int32_t w_id, int32_t d_id, int32_t c_id);

    // Stores orderline in the database. Returns a pointer to the database's tuple.
    OrderLine* insertOrderLine(const OrderLine& orderline);
    OrderLine* findOrderLine(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number);

    // Creates a new order in the database. Returns a pointer to the database's tuple.
    NewOrder* insertNewOrder(int32_t w_id, int32_t d_id, int32_t o_id);
    NewOrder* findNewOrder(int32_t w_id, int32_t d_id, int32_t o_id);

    const std::vector<const History*>& history() const { return history_; }
    // Stores order in the database. Returns a pointer to the database's tuple.
    History* insertHistory(const History& history);

    static const int KEYS_PER_INTERNAL = 8;
    static const int KEYS_PER_LEAF = 8;

    // Sets the transaction that is currently executing. Used to acquire locks. This is a bit of a
    // hack, but seems easier than passing the transaction id around in every method.
    virtual void setTransaction(int64_t tid);

    // we use this to release the locks and delete structures for this trans
    // this wasn't required in evan's code since it was handled through the
    // transaction object
    void endTransaction(int64_t tid);

private:
    static const int STOCK_LEVEL_ORDERS = 20;

    // Loads each item from the items table. Returns true if they are all found.
    bool findAndValidateItems(const std::vector<NewOrderItem>& items,
            std::vector<Item*>* item_tuples);

    // Implements order status transaction after the customer tuple has been located.
    void internalOrderStatus(Customer* customer, OrderStatusOutput* output);

    // Implements payment transaction after the customer tuple has been located.
    void internalPaymentRemote(int32_t warehouse_id, int32_t district_id, Customer* c,
            float h_amount, PaymentOutput* output, TPCCUndo** undo);

    // Erases order from the database. NOTE: This is only for undoing transactions.
    void eraseOrder(const Order* order);
    // Erases order_line from the database. NOTE: This is only for undoing transactions.
    void eraseOrderLine(const OrderLine* order_line);
    // Erases new_order from the database. NOTE: This is only for undoing transactions.
    void eraseNewOrder(const NewOrder* new_order);
    // Erases history from the database. NOTE: This is only for undoing transactions.
    void eraseHistory(const History* history);

    // Allocates an undo buffer if needed, storing the pointer in *undo.
    void allocateUndo(TPCCUndo** undo) {
        if (undo != NULL && *undo == NULL) {
            *undo = new TPCCUndo();
        }
    }

    // Try to acquire lock on tuple.Throws LockBlockedException if it fails.
    void tryLock(void* tuple, bool exclusive);
    void tryRead(void* tuple);
    void tryWrite(void* tuple);

    // TODO: Use a data structure that supports deletes, appends, and sparse ranges.
    // Using a vector instead of a BPlusTree reduced the new order run time by 3.65us. This was an
    // improvement of 12%. It also saved 4141kB of RSS.
    std::vector<Item> items_;

    BPlusTree<int32_t, Warehouse*, KEYS_PER_INTERNAL, KEYS_PER_LEAF> warehouses_;
    BPlusTree<int32_t, Stock*, KEYS_PER_INTERNAL, KEYS_PER_LEAF> stock_;
    BPlusTree<int32_t, District*, KEYS_PER_INTERNAL, KEYS_PER_LEAF> districts_;
    BPlusTree<int32_t, Customer*, KEYS_PER_INTERNAL, KEYS_PER_LEAF> customers_;
    typedef std::set<Customer*, CustomerByNameOrdering> CustomerByNameSet;
    CustomerByNameSet customers_by_name_;
    BPlusTree<int32_t, Order*, KEYS_PER_INTERNAL, KEYS_PER_LEAF> orders_;
    // TODO: Tune the size of this tree for the bigger keys?
    BPlusTree<int64_t, Order*, KEYS_PER_INTERNAL, KEYS_PER_LEAF> orders_by_customer_;
    BPlusTree<int32_t, OrderLine*, KEYS_PER_INTERNAL, KEYS_PER_LEAF> orderlines_;
    // TODO: Implement btree lower_bound?
    typedef std::map<int64_t, NewOrder*> NewOrderMap;
    NewOrderMap neworders_;
    std::vector<const History*> history_;

    // if tid_ != -1 then we're going to do locking, otherwise none
    int64_t tid_; // id of the currently-running transaction

    // mapping from tid to the tuples locked for that transaction
    typedef std::tr1::unordered_map<int64_t, std::set<void*>* > TIDLocksType;
    TIDLocksType tid_locks_;

    // mapping from tuple to lock
    typedef std::tr1::unordered_map<void*, Lock*> LockTableType;
    LockTableType lock_table_;
};

#endif
