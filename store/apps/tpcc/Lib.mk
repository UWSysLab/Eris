d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), clock.cc dbimpl.cc dbserver.cc randomgenerator.cc\
						  serialization.cc tpccdb.cc tpccgenerator.cc txnserver.cc)

LIB-tpcc := $(o)clock.o $(o)randomgenerator.o $(o)serialization.o $(o)tpccdb.o

LIB-tpccserver := $(LIB-tpcc) $(o)dbimpl.o $(o)dbserver.o $(o)tpccgenerator.o $(o)txnserver.o 
