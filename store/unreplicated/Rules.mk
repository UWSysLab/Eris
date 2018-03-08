d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	client.cc server.cc)

PROTOS += $(addprefix $(d), unreplicated-proto.proto)

OBJS-store-unreplicated-client := $(o)client.o $(o)unreplicated-proto.o \
    $(OBJS-client) $(LIB-message) $(LIB-configuration)

OBJS-store-unreplicated-server := $(o)server.o $(o)unreplicated-proto.o \
    $(OBJS-replica) $(LIB-message) \
    $(LIB-configuration) $(LIB-latency)
