d := $(dir $(lastword $(MAKEFILE_LIST)))

GTEST_SRCS += $(d)eris-protocol-test.cc

$(d)eris-protocol-test: $(o)eris-protocol-test.o \
    $(OBJS-eris-client) $(OBJS-eris-server) $(OBJS-eris-fcor) \
    $(OBJS-vr-replica) $(LIB-simtransport) $(GTEST_MAIN)

TEST_BINS += $(d)eris-protocol-test
