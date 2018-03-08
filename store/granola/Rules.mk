d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	client.cc server.cc)

PROTOS += $(addprefix $(d), granola-proto.proto)

OBJS-granola-client := $(o)client.o $(o)granola-proto.o \
    $(OBJS-client) $(LIB-message) $(LIB-configuration)

OBJS-granola-server := $(o)server.o $(o)granola-proto.o \
    $(OBJS-replica) $(LIB-message) \
    $(LIB-configuration) $(LIB-latency)
