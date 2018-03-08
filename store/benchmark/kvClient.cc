// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/kvClient.cc:
 *   Benchmarking client for a distributed transactional kv-store.
 *
 **********************************************************************/

#include <vector>
#include <map>
#include <set>
#include <algorithm>

#include "lib/latency.h"
#include "lib/timeval.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "store/common/frontend/txnclientcommon.h"
#include "store/apps/kvstore/client.h"
#include "store/eris/client.h"
#include "store/granola/client.h"
#include "store/unreplicated/client.h"
#include "store/spanner/client.h"
#include "store/tapir/client.h"
#include "store/benchmark/header.h"

using namespace std;
using namespace specpaxos;
using namespace specpaxos::store;
using namespace specpaxos::store::kvstore;

DEFINE_LATENCY(op);

// Function to pick a random key according to some distribution.
int rand_key();

bool ready = false;
double alpha = -1;
double *zipf;

vector<string> keys;
int nKeys = 100;

uint64_t key_to_shard(const std::string &key, uint64_t nshards);

int
main(int argc, char **argv)
{
    const char *configPath = nullptr;
    const char *keysPath = nullptr;
    int duration = 10;
    int nShards = 1;
    int tLen = 10;
    int gLen = 2;
    int wPer = 50; // Out of 100
    int mptxnPer = 0; // percentage of multi-phase transactions (/100)
    struct timeval startTime, endTime, initialTime, currTime, lastInterval;
    struct Latency_t latency;
    vector<uint64_t> latencies;
    map<uint64_t, uint64_t> remote_txn_hgram;
    set<uint64_t> partition_count;
    phase_t phase = WARMUP;
    int tputInterval = 0;

    KVClient *kvClient;
    TxnClient *txnClient;
    Client *protoClient = nullptr;

    protomode_t mode = PROTO_UNKNOWN;

    int opt;
    while ((opt = getopt(argc, argv, "c:d:N:l:w:k:f:m:z:p:g:i:")) != -1) {
        switch (opt) {
        case 'c': // Configuration path
        {
            configPath = optarg;
            break;
        }

        case 'd': // duration to run
        {
            char *strtolPtr;
            duration = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                    (duration <= 0)) {
                fprintf(stderr, "option -d requires a numeric arg > 0\n");
            }
            break;
        }

        case 'N': // Number of shards.
        {
            char *strtolPtr;
            nShards = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                    (nShards <= 0)) {
                fprintf(stderr, "option -n requires a numeric arg\n");
            }
            break;
        }

        case 'l': // Length of each transaction (deterministic!)
        {
            char *strtolPtr;
            tLen = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                    (tLen <= 0)) {
                fprintf(stderr, "option -l requires a numeric arg\n");
            }
            break;
        }

        case 'w': // Percentage of writes (out of 100)
        {
            char *strtolPtr;
            wPer = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                    (wPer < 0 || wPer > 100)) {
                fprintf(stderr, "option -w requires a arg b/w 0-100\n");
            }
            break;
        }

        case 'k': // Number of keys to operate on.
        {
            char *strtolPtr;
            nKeys = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                    (nKeys <= 0)) {
                fprintf(stderr, "option -k requires a numeric arg\n");
            }
            break;
        }

        case 'f': // Generated keys path
        {
            keysPath = optarg;
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
            } else if (strcasecmp(optarg, "spanner") == 0) {
                mode = PROTO_SPANNER;
            } else if (strcasecmp(optarg, "tapir") == 0) {
                mode = PROTO_TAPIR;
            } else {
                fprintf(stderr, "Unknown protocol mode %s\n", optarg);
            }
            break;
        }

        case 'z': // Zipf coefficient for key selection.
        {
            char *strtolPtr;
            alpha = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -z requires a numeric arg\n");
            }
            break;
        }

        case 'p': // Percentage of multi-phase transactions
        {
            char *strtolPtr;
            mptxnPer = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || mptxnPer < 0 || mptxnPer > 100)
            {
                fprintf(stderr,
                        "option -p requires a numeric arg between 0 and 100\n");
            }
            break;
        }

        case 'g': // Length of multi-phase transactions
        {
            char *strtolPtr;
            gLen = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || gLen < 0)
            {
                fprintf(stderr,
                        "option -g requires a numeric arg > 0\n");
            }
            break;
        }

        case 'i': // Throughput measurement interval
        {
            char *strtolPtr;
            tputInterval = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || tputInterval <= 0)
            {
                fprintf(stderr,
                        "option -i requires a numeric arg > 0\n");
            }
            break;
        }

        default:
            fprintf(stderr, "Unknown argument %s\n", argv[optind]);
            break;
        }
    }

    if (configPath == nullptr) {
        Panic("option -c required");
    }

    if (mode == PROTO_UNKNOWN) {
        Panic("option -m required");
    }

    // Initialize random number seed
    struct timeval tv;
    gettimeofday(&tv, NULL);
    srand(tv.tv_usec);

    latencies.reserve(duration * 10000);
    _Latency_Init(&latency, "op");

    ifstream configStream(configPath);
    if (configStream.fail()) {
        Panic("unable to read configuration file: %s", configPath);
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
        protoClient = new store::unreplicated::UnreplicatedClient(config, transport);
        break;
    }
    case PROTO_SPANNER: {
        protoClient = new spanner::SpannerClient(config, transport);
        break;
    }
    case PROTO_TAPIR: {
        break;
    }
    default:
        Panic("Unknown protocol mode");
    }
    if (mode == PROTO_TAPIR) {
        txnClient = new tapir::TapirClient(config, transport);
    } else {
        txnClient = new TxnClientCommon(transport, protoClient);
    }
    kvClient = new KVClient(txnClient, nShards);

    // Read in the keys from a file.
    string key, value;
    ifstream in;
    in.open(keysPath);
    if (!in) {
        fprintf(stderr, "Could not read keys from: %s\n", keysPath);
        exit(0);
    }
    for (int i = 0; i < nKeys; i++) {
        getline(in, key);
        keys.push_back(key);
    }
    in.close();

    uint64_t commit_transactions = 0;
    uint64_t last_interval_txns = 0;
    gettimeofday(&initialTime, NULL);
    while (true) {
        gettimeofday(&currTime, NULL);
        uint64_t time_elapsed = currTime.tv_sec - initialTime.tv_sec;

        if (phase == MEASURE) {
            int time_since_interval = (currTime.tv_sec - lastInterval.tv_sec)*1000 + (currTime.tv_usec - lastInterval.tv_usec)/1000;

            if (tputInterval > 0 && time_since_interval >= tputInterval) {
                Notice("Completed %lu transactions at %lu ms", commit_transactions-last_interval_txns,
                       ((currTime.tv_sec*1000+currTime.tv_usec/1000)/tputInterval)*tputInterval);
                lastInterval = currTime;
                last_interval_txns = commit_transactions;
            }
        }

        if (phase == WARMUP) {
            if (time_elapsed >= (uint64_t)duration / 3) {
                phase = MEASURE;
                startTime = currTime;
                gettimeofday(&lastInterval, NULL);
            }
        } else if (phase == MEASURE) {
            if (time_elapsed >= (uint64_t)duration * 2 / 3) {
                phase = COOLDOWN;
                endTime = currTime;
            }
        } else if (phase == COOLDOWN) {
            if (time_elapsed >= (uint64_t)duration) {
                break;
            }
        }

        Latency_Start(&latency);
        vector<KVOp_t> ops;
        map<string, string> results;
        KVOp_t readOp, writeOp;

        bool indep = true;
        if (mptxnPer > 0) {
            if (rand() % 100 < mptxnPer) {
                indep = false;
            }
        }
        int nkeys = tLen;
        if (!indep) {
            nkeys = gLen; // General transactions
        }
        partition_count.clear();
        for (int j = 0; j < nkeys; j++) {
            string key = keys[rand_key()];
            partition_count.insert(key_to_shard(key, nShards));

            if (rand() % 100 < wPer) {
                writeOp.opType = KVOp_t::PUT;
                writeOp.key = key;
                writeOp.value = key;
                ops.push_back(writeOp);
            } else {
                readOp.opType = KVOp_t::GET;
                readOp.key = key;
                ops.push_back(readOp);
            }
        }

        if (kvClient->InvokeKVTxn(ops, results, indep) && phase == MEASURE) {
            commit_transactions++;
        }
        uint64_t ns = Latency_End(&latency);

        if (phase == MEASURE) {
            latencies.push_back(ns);
            uint32_t num_partitions = partition_count.size();
            if (remote_txn_hgram.count(num_partitions) == 0) {
                remote_txn_hgram[num_partitions] = 0;
            }
            remote_txn_hgram[num_partitions] += 1;
        }
    }
    txnClient->Done();

    struct timeval diff = timeval_sub(endTime, startTime);
    uint64_t total_transactions = latencies.size();

    Notice("Completed %lu transactions in " FMT_TIMEVAL_DIFF " seconds",
           commit_transactions, VA_TIMEVAL_DIFF(diff));
    Notice("Commit rate %.3f", (double)commit_transactions / total_transactions);

    char buf[1024];
    std::sort(latencies.begin(), latencies.end());

    uint64_t ns  = latencies[total_transactions / 2];
    LatencyFmtNS(ns, buf);
    Notice("Median latency is %ld ns (%s)", ns, buf);

    ns = latencies[total_transactions * 90 / 100];
    LatencyFmtNS(ns, buf);
    Notice("90th percentile latency is %ld ns (%s)", ns, buf);

    ns = latencies[total_transactions * 95 / 100];
    LatencyFmtNS(ns, buf);
    Notice("95th percentile latency is %ld ns (%s)", ns, buf);

    ns = latencies[total_transactions * 99 / 100];
    LatencyFmtNS(ns, buf);
    Notice("99th percentile latency is %ld ns (%s)", ns, buf);

    for (const pair<uint64_t, uint64_t> &kv : remote_txn_hgram) {
        Notice("Percentage transactions span %lu partitions is %f", kv.first, (float)kv.second / total_transactions);
    }

    delete kvClient;
    // destructor of kvClient will deallocate txnClient
    if (protoClient) {
        delete protoClient;
    }
    delete transport;

    return 0;
}

int rand_key()
{
    if (alpha < 0) {
        // Uniform selection of keys.
        return (rand() % nKeys);
    } else {
        // Zipf-like selection of keys.
        if (!ready) {
            zipf = new double[nKeys];

            double c = 0.0;
            for (int i = 1; i <= nKeys; i++) {
                c = c + (1.0 / pow((double) i, alpha));
            }
            c = 1.0 / c;

            double sum = 0.0;
            for (int i = 1; i <= nKeys; i++) {
                sum += (c / pow((double) i, alpha));
                zipf[i - 1] = sum;
            }
            ready = true;
        }

        double random = 0.0;
        while (random == 0.0 || random == 1.0) {
            random = (1.0 + rand()) / RAND_MAX;
        }

        // binary search to find key;
        int l = 0, r = nKeys, mid;
        while (l < r) {
            mid = (l + r) / 2;
            if (random > zipf[mid]) {
                l = mid + 1;
            } else if (random < zipf[mid]) {
                r = mid - 1;
            } else {
                break;
            }
        }
        return mid;
    }
}

uint64_t key_to_shard(const std::string &key, uint64_t nshards) {
    uint64_t hash = 5381;
    const char* str = key.c_str();
    for (unsigned int i = 0; i < key.length(); i++) {
        hash = ((hash << 5) + hash) + (uint64_t)str[i];
    }

    return (hash % nshards);
};
