d := $(dir $(lastword $(MAKEFILE_LIST)))

GTEST_SRCS += $(d)eris-test.cc $(d)granola-test.cc $(d)unreplicated-test.cc \
	      $(d)spanner-test.cc $(d)tapir-test.cc

COMMON-OBJS := $(OBJS-kvstore-client) $(OBJS-kvstore-txnserver) $(LIB-simtransport) $(GTEST_MAIN)

$(d)eris-test: $(o)eris-test.o \
    $(COMMON-OBJS) \
    $(OBJS-eris-client) \
    $(OBJS-eris-server)

$(d)granola-test: $(o)granola-test.o \
    $(COMMON-OBJS) \
    $(OBJS-granola-client) \
    $(OBJS-granola-server)

$(d)unreplicated-test: $(o)unreplicated-test.o \
    $(COMMON-OBJS) \
    $(OBJS-store-unreplicated-client) \
    $(OBJS-store-unreplicated-server)

$(d)spanner-test: $(o)spanner-test.o \
    $(COMMON-OBJS) \
    $(OBJS-spanner-client) \
    $(OBJS-spanner-server)

$(d)tapir-test: $(o)tapir-test.o \
    $(COMMON-OBJS) \
    $(OBJS-tapir-client) \
    $(OBJS-tapir-server)

TEST_BINS += $(d)eris-test $(d)granola-test $(d)unreplicated-test $(d)spanner-test $(d)tapir-test
