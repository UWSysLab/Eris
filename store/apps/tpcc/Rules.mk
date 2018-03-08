d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), clock.cc dbimpl.cc dbserver.cc randomgenerator.cc\
	serialization.cc tpccdb.cc tpccgenerator.cc txnserver.cc\
	clientthread.cc dbproxy.cc tpccclient.cc)

tpcc-common := $(o)clock.o $(o)randomgenerator.o $(o)serialization.o $(o)tpccdb.o

OBJS-tpcc-client := $(LIB-store-frontend) $(tpcc-common) $(o)clientthread.o \
    $(o)dbproxy.o $(o)tpccclient.o

OBJS-tpcc-txnserver := $(LIB-store-backend) $(tpcc-common) $(o)dbimpl.o $(o)dbserver.o $(o)tpccgenerator.o $(o)txnserver.o
