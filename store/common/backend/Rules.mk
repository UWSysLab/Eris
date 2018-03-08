d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), \
				kvstore.cc lockserver.cc txnstore.cc versionstore.cc txnserver.cc)

LIB-store-backend := $(LIB-store-common) $(o)kvstore.o $(o)lockserver.o $(o)txnstore.o $(o)versionstore.o $(o)txnserver.o

include $(d)tests/Rules.mk
