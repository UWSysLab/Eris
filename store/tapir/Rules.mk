d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
	client.cc irclient.cc record.cc server.cc)

PROTOS += $(addprefix $(d), tapir-proto.proto)

OBJS-tapir-client := $(o)tapir-proto.o $(o)irclient.o $(o)client.o \
    $(OBJS-client) $(LIB-message) $(LIB-configuration)

OBJS-tapir-server := $(o)tapir-proto.o $(o)record.o $(o)server.o \
    $(OBJS-replica) $(LIB-message) $(LIB-configuration)
