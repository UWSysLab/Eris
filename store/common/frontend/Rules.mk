d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), txnclientcommon.cc)

LIB-store-frontend := $(o)txnclientcommon.o $(LIB-store-common)
