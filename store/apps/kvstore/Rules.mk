d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc txnserver.cc)

PROTOS += $(addprefix $(d), kvstore-proto.proto)

OBJS-kvstore-client := $(LIB-store-frontend) $(OBJS-eris-client) $(OBJS-granola-client) $(OBJS-store-unreplicated-client) $(OBJS-spanner-client) $(OBJS-tapir-client) $(o)kvstore-proto.o $(o)client.o

OBJS-kvstore-txnserver := $(LIB-store-backend) $(o)kvstore-proto.o $(o)txnserver.o

include $(d)tests/Rules.mk
