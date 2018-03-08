#ifndef DBPROXY_H__
#define DBPROXY_H__

#include <vector>
#include <string>

#include "store/apps/tpcc/tpccdb.h"
#include "store/apps/tpcc/assert.h"

#include "store/apps/tpcc/clientthread.h"

class DBProxy : public TPCCDB {
public:
	DBProxy(int num_warehouses, int warehouses_per_partition_,
	        ClientThread* ct);
	virtual ~DBProxy();

	virtual int32_t stockLevel(int32_t warehouse_id, int32_t district_id,
	                           int32_t threshold);
	virtual void orderStatus(int32_t warehouse_id, int32_t district_id,
	                         int32_t customer_id, OrderStatusOutput* output);
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
	virtual bool hasWarehouse(int32_t warehouse_id) { CHECK(false); return false; }
	virtual void applyUndo(TPCCUndo* undo) { CHECK(false); }
	virtual void freeUndo(TPCCUndo* undo) { CHECK(false); }
	virtual void setTransaction(int64_t tid) { CHECK(false); }

private:
	bool distNewOrder(int32_t warehouse_id, int32_t district_id,
	                  int32_t customer_id, const std::vector<NewOrderItem>& items,
	                  const char* now, NewOrderOutput* output);

	void internalPayment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
	                     int32_t c_district_id, int32_t customer_id, const char* c_last, float h_amount,
	                     const char* now, PaymentOutput* output);
	void distInternalPayment(int32_t warehouse_id, int32_t district_id,
	                         int32_t c_warehouse_id, int32_t c_district_id, int32_t customer_id,
	                         const char* c_last, float h_amount, const char* now,
	                         PaymentOutput* output);

	int partition(int32_t warehouse_id);

	void makeRequest(int32_t warehouse_id, bool ro);
	void makeRequest(std::vector<rid_t>& partition_ids,
	                 std::vector<std::string>& requests, bool ro,
	                 std::vector<std::string>& results);

	int num_warehouses_;
	int warehouses_per_partition_;

	std::string request_;
	std::string result_;

	// pointer to the client thread, so we can issue invoke upcalls
	ClientThread* ct_;
};

#endif
