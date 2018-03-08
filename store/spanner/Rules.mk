d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	client.cc server.cc)

PROTOS += $(addprefix $(d), spanner-proto.proto)

OBJS-spanner-client := $(o)client.o $(o)spanner-proto.o \
    $(OBJS-client) $(LIB-message) $(LIB-configuration)

OBJS-spanner-server := $(o)server.o $(o)spanner-proto.o \
    $(OBJS-replica) $(LIB-message) \
    $(LIB-configuration) $(LIB-latency)
