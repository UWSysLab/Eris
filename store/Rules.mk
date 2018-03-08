d := $(dir $(lastword $(MAKEFILE_LIST)))

include $(d)common/Rules.mk $(d)eris/Rules.mk $(d)granola/Rules.mk $(d)unreplicated/Rules.mk \
        $(d)spanner/Rules.mk $(d)tapir/Rules.mk $(d)apps/Rules.mk $(d)benchmark/Rules.mk $(d)tests/Rules.mk
