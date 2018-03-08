// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * bench/terminal.cc:
 *   A terminal client for a distributed transactional store.
 *
 **********************************************************************/

#include <random>
#include "lib/udptransport.h"
#include "store/common/frontend/txnclientcommon.h"
#include "store/apps/kvstore/client.h"
#include "store/eris/client.h"
#include "store/granola/client.h"
#include "store/unreplicated/client.h"
#include "store/benchmark/header.h"

using namespace std;
using namespace specpaxos;
using namespace specpaxos::store;
using namespace specpaxos::store::kvstore;

int
main(int argc, char **argv)
{
    const char *configPath = nullptr;

    int nshards = 1;

    KVClient *kvClient;
    TxnClient *txnClient;
    Client *protoClient;

    protomode_t mode = PROTO_UNKNOWN;

    int opt;
    while ((opt = getopt(argc, argv, "c:N:m:")) != -1) {
        switch (opt) {
        case 'c': // Configuration path
        {
            configPath = optarg;
            break;
        }

        case 'N': // Number of shards.
        {
            char *strtolPtr;
            nshards = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (nshards <= 0)) {
                fprintf(stderr, "option -n requires a numeric arg\n");
            }
            break;
        }

        case 'm': // Mode to run in [occ/lock/...]
        {
            if (strcasecmp(optarg, "eris") == 0) {
                mode = PROTO_ERIS;
            } else if (strcasecmp(optarg, "granola") == 0) {
                mode = PROTO_GRANOLA;
            } else if (strcasecmp(optarg, "unreplicated") == 0) {
                mode = PROTO_UNREPLICATED;
            } else {
                fprintf(stderr, "Unknown protocol mode %s\n", optarg);
            }
            break;
        }

        default:
            fprintf(stderr, "Unknown argument %s\n", argv[optind]);
            break;
        }
    }

    if (configPath == nullptr) {
        fprintf(stderr, "option -c required\n");
        exit(1);
    }
    // Load configuration
    ifstream configStream(configPath);
    if (configStream.fail()) {
        Panic("unable to read configuratino file: %s\n",
              configPath);
    }
    Configuration config(configStream);
    UDPTransport *transport = new UDPTransport();
    switch (mode) {
    case PROTO_ERIS: {
        protoClient = new eris::ErisClient(config, transport);
        break;
    }
    case PROTO_GRANOLA: {
        protoClient = new granola::GranolaClient(config, transport);
        break;
    }
    case PROTO_UNREPLICATED: {
        protoClient = new unreplicated::UnreplicatedClient(config, transport);
        break;
    }
    default:
        Panic("Unknwon protocol mode");
    }
    txnClient = new TxnClientCommon(transport, protoClient);
    kvClient = new KVClient(txnClient, nshards);

    char c, cmd[2048], *tok;
    int clen;
    vector<KVOp_t> kvops;
    map<string, string> results;

    while (1) {
        printf(">> ");
        fflush(stdout);

        clen = 0;
        while ((c = getchar()) != '\n')
            cmd[clen++] = c;
        cmd[clen] = '\0';

        if (clen == 0) continue;

        tok = strtok(cmd, " ,.-");

        if (strcasecmp(tok, "exit") == 0 || strcasecmp(tok, "q") == 0) {
            printf("Exiting..\n");
            break;
        } else if (strcasecmp(tok, "get") == 0) {
            tok = strtok(NULL, " ,.-");
            KVOp_t op;
            op.opType = KVOp_t::GET;
            op.key = string(tok);
            kvops.push_back(op);
        } else if (strcasecmp(tok, "put") == 0) {
            tok = strtok(NULL, " ,.-");
            string key = string(tok);
            tok = strtok(NULL, " ,.-");
            string value = string(tok);

            KVOp_t op;
            op.opType = KVOp_t::PUT;
            op.key = key;
            op.value = value;
            kvops.push_back(op);
        } else if (strcasecmp(tok, "invoke") == 0) {
            tok = strtok(NULL, " ,.-");
            results.clear();
            bool success = kvClient->InvokeKVTxn(kvops, results, true);
            kvops.clear();
            printf("%s\n", success ? "SUCCESS" : "FAILED");
            for (auto &kv : results) {
                printf("GET %s value %s\n", kv.first.c_str(), kv.second.c_str());
            }
        } else {
            printf("Unknown command.. Try again!\n");
        }
        fflush(stdout);
    }

    delete kvClient;
    delete txnClient;
    delete protoClient;
    delete transport;

    return 0;
}
