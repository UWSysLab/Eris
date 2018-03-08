#ifndef __STORE_HEADER_H__
#define __STORE_HEADER_H__

enum phase_t {
    WARMUP,
    MEASURE,
    COOLDOWN
};

enum protomode_t {
    PROTO_UNKNOWN,
    PROTO_ERIS,
    PROTO_GRANOLA,
    PROTO_UNREPLICATED,
    PROTO_SPANNER,
    PROTO_TAPIR
};

enum app_t {
    APP_UNKNOWN,
    APP_KVSTORE,
    APP_TPCC
};

#endif /* __STORE_HEADER_H__ */
