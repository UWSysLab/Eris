d := $(dir $(lastword $(MAKEFILE_LIST)))

GTEST_SRCS += $(addprefix $(d), kvtxn-test.cc)

$(d)kvtxn-test: $(o)kvtxn-test.o \
	$(OBJS-kvstore-txnserver) \
	$(LIB-message) \
	$(LIB-store-common) \
	$(GTEST_MAIN)

TEST_BINS += $(d)kvtxn-test
