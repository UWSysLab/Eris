d := $(dir $(lastword $(MAKEFILE_LIST)))

include $(d)kvstore/Rules.mk $(d)tpcc/Rules.mk
