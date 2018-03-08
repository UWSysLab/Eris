// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/server.cc:
 *   Entry point of transaction server.
 *
 * Copyright 2016 Jialin Li <lijl@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include <sched.h>
#include "lib/udptransport.h"
#include "store/eris/server.h"
#include "store/granola/server.h"
#include "store/unreplicated/server.h"
#include "store/spanner/server.h"
#include "store/tapir/server.h"
#include "store/apps/kvstore/txnserver.h"
#include "store/apps/tpcc/txnserver.h"
#include "store/benchmark/header.h"

using namespace std;
using namespace specpaxos;
using namespace specpaxos::store;

int
main(int argc, char **argv)
{
    int replica_num = -1, shard_num = -1, nkeys=100, nshards = 1,
        warehouses_per_partition = 1, partition_id = 0, coreid=-1;
    bool locking = false;
    const char *configPath = nullptr;
    const char *fcorConfigPath = nullptr;
    const char *keyPath = nullptr;
    store::tpcc::TPCCTxnServerArg tpccArg;
    kvstore::KVStoreTxnServerArg kvArg;
    TxnServer *txnServer = nullptr;
    app_t app = APP_UNKNOWN;
    protomode_t mode = PROTO_UNKNOWN;
    float dropRate = 0.0;

    // Parse arguments
    int opt;
    while ((opt = getopt(argc, argv, "c:i:a:m:n:N:w:p:k:f:r:o:d:l")) != -1) {
        switch (opt) {
        case 'c':
            configPath = optarg;
            break;

        case 'i':
        {
            char *strtolPtr;
            replica_num = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (replica_num < 0))
            {
                fprintf(stderr, "option -i requires a numeric arg\n");
            }
            break;
        }

        case 'a':
        {
            if (strcasecmp(optarg, "kv") == 0) {
                app = APP_KVSTORE;
            } else if (strcasecmp(optarg, "tpcc") == 0) {
                app = APP_TPCC;
            } else {
                fprintf(stderr, "unknown app mode '%s'\n", optarg);
            }
            break;
        }

        case 'm':
        {
            if (strcasecmp(optarg, "eris") == 0) {
                mode = PROTO_ERIS;
            } else if (strcasecmp(optarg, "granola") == 0) {
                mode = PROTO_GRANOLA;
            } else if (strcasecmp(optarg, "unreplicated") == 0) {
                mode = PROTO_UNREPLICATED;
            } else if (strcasecmp(optarg, "spanner") == 0) {
                mode = PROTO_SPANNER;
            } else if (strcasecmp(optarg, "tapir") == 0) {
                mode = PROTO_TAPIR;
            } else {
                fprintf(stderr, "unknown protocol mode '%s'\n", optarg);
            }
            break;
        }

        case 'n':
        {
            char *strtolPtr;
            shard_num = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (shard_num < 0))
            {
                fprintf(stderr, "option -n requires a numeric arg\n");
            }
            break;
        }

        case 'N':
        {
            char *strtolPtr;
            nshards = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (nshards <= 0))
            {
                fprintf(stderr, "option -N requires a numeric arg > 0\n");
            }
            break;
        }

        case 'w':
        {
            char *strtolPtr;
            warehouses_per_partition = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (warehouses_per_partition <= 0))
            {
                fprintf(stderr, "option -w requires a numeric arg > 0\n");
            }
            break;
        }

        case 'p':
        {
            char *strtolPtr;
            partition_id = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (partition_id < 0))
            {
                fprintf(stderr, "option -p requires a numeric arg >= 0\n");
            }
            break;
        }

        case 'k':
        {
            char *strtolPtr;
            nkeys = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr, "option -e requires a numeric arg\n");
            }
            break;
        }

        case 'f':   // Load keys from file
        {
            keyPath = optarg;
            break;
        }

        case 'r':
        {
            char *strtolPtr;
            coreid = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (coreid < 0))
            {
                fprintf(stderr, "option -r requires a numeric arg >= 0\n");
            }
            break;
        }

        case 'o':
        {
            fcorConfigPath = optarg;
            break;
        }

        case 'd':
        {
            char *strtofPtr;
            dropRate = strtof(optarg, &strtofPtr);
            if ((*optarg == '\0') || (*strtofPtr != '\0') || dropRate < 0 || dropRate > 1)
            {
                fprintf(stderr,
                        "option -r requires a numeric arg between 0 and 1\n");
            }
            break;
        }

        case 'l':
        {
            locking = true;
            break;
        }

        default:
            fprintf(stderr, "Unknown argument %s\n", argv[optind]);
        }
    }

    if (!configPath) {
        fprintf(stderr, "option -c is required\n");
        exit(1);
    }

    if (replica_num == -1) {
        fprintf(stderr, "option -i is required\n");
        exit(1);
    }

    if (shard_num == -1) {
        fprintf(stderr, "option -n is required\n");
        exit(1);
    }

    if (app == APP_UNKNOWN) {
        fprintf(stderr, "option -a is required\n");
        exit(1);
    }

    if (mode == PROTO_UNKNOWN) {
        fprintf(stderr, "option -m is required\n");
        exit(1);
    }

    switch (app) {
    case APP_KVSTORE: {
        kvArg.keyPath = keyPath;
        kvArg.nKeys = nkeys;
        kvArg.myShard = shard_num;
        kvArg.nShards = nshards;
        if (mode == PROTO_ERIS) {
            kvArg.retryLock = false;
        } else {
            kvArg.retryLock = true;
        }
        txnServer = new kvstore::KVTxnServer(kvArg);
        break;
    }
    case APP_TPCC: {
        tpccArg.total_warehouses = nshards * warehouses_per_partition;
        tpccArg.warehouses_per_partition = warehouses_per_partition;
        tpccArg.partition_id = shard_num;
        if (mode == PROTO_ERIS || mode == PROTO_GRANOLA || mode == PROTO_UNREPLICATED) {
            tpccArg.locking = false;
        } else {
            tpccArg.locking = true;
        }
        txnServer = new store::tpcc::TPCCTxnServer(tpccArg);
        break;
    }
    default:
        fprintf(stderr, "Unknown application mode\n");
        exit(1);
    }

    if (coreid >= 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(coreid, &cpuset);
        if (sched_setaffinity(0, sizeof(cpuset), &cpuset) < 0) {
            fprintf(stderr, "Failed to pin process to core %u\n", coreid);
            exit(1);
        }
    }

    // Load configuration
    ifstream configStream(configPath);
    if (configStream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n", configPath);
        exit(1);
    }
    Configuration config(configStream);

    if (replica_num >= config.n) {
        fprintf(stderr, "replica index %d is out of bounds; "
                "only %d replicas defined\n", replica_num, config.n);
        exit(1);
    }

    if (shard_num >= config.g) {
        fprintf(stderr, "shard index %d is out of bounds; "
                "only %d shards defined\n", shard_num, config.g);
        exit(1);
    }

    UDPTransport *transport = new UDPTransport(dropRate);
    ASSERT(txnServer != nullptr);
    Replica *protoServer;

    switch (mode) {
    case PROTO_ERIS: {
        if (!fcorConfigPath) {
            fprintf(stderr, "eris requires fcor configuration file using -o\n");
            exit(1);
        }
        ifstream fcorConfigStream(fcorConfigPath);
        if (fcorConfigStream.fail()) {
            fprintf(stderr, "unable to read configuration file: %s\n", fcorConfigPath);
            exit(1);
        }
        Configuration fcorConfig(fcorConfigStream);
        protoServer = new eris::ErisServer(config, shard_num, replica_num,
                                           true, transport, txnServer, fcorConfig);
        break;
    }
    case PROTO_GRANOLA: {
        protoServer = new granola::GranolaServer(config, shard_num, replica_num,
                                                 true, transport, txnServer, locking);
        break;
    }
    case PROTO_UNREPLICATED: {
        protoServer = new store::unreplicated::UnreplicatedServer(config, shard_num, replica_num,
                                                                  true, transport, txnServer);
        break;
    }
    case PROTO_SPANNER: {
        protoServer = new spanner::SpannerServer(config, shard_num, replica_num,
                                                 true, transport, txnServer);
        break;
    }
    case PROTO_TAPIR: {
        protoServer = new tapir::TapirServer(config, shard_num, replica_num,
                                             true, transport, txnServer);
        break;
    }
    default:
        Panic("Unknown protocol mode");
    }

    transport->Run();

    delete protoServer;
    delete txnServer;
    delete transport;
    return 0;
}

// namespace specpaxos
