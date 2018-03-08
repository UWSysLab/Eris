// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/retwisClient.cc:
 *   Retwis benchmarking client for a distributed transactional store.
 *
 **********************************************************************/

#include "lib/latency.h"
#include "lib/timeval.h"
#include "store/common/truetime.h"
#include "store/common/frontend/client.h"
#include "store/notxn/client.h"
#include "store/apps/kvstore/client.h"
#include <algorithm>

using namespace std;
using namespace storeapp;

DEFINE_LATENCY(op)

// Function to pick a random key according to some distribution.
int rand_key();

bool ready = false;
double alpha = -1;
double *zipf;

vector<string> keys;
int nKeys = 100;

enum phase_t {
    WARMUP,
    MEASURE,
    COOLDOWN
};

int
main(int argc, char **argv)
{
    vector<const char *> configPaths;
    const char *keysPath = NULL;
    int duration = 10;
    int nShards = 1;
    int skew = 0; // difference between real clock and TrueTime
    int error = 0; // error bars
    struct timeval startTime, endTime, initialTime, currTime;
    struct Latency_t latency;
    vector<uint64_t> latencies;
    phase_t phase = WARMUP;

    kvstore::KVClient *client;

    // Mode for strongstore.

    int opt;
    while ((opt = getopt(argc, argv, "c:d:N:k:f:e:s:z:")) != -1) {
        switch (opt) {
        case 'c': // Configuration path
        {
            configPaths.push_back(optarg);
            break;
        }

        case 'f': // Generated keys path
        {
            keysPath = optarg;
            break;
        }

        case 'N': // Number of shards.
        {
            char *strtolPtr;
            nShards = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (nShards <= 0)) {
                fprintf(stderr, "option -N requires a numeric arg\n");
            }
            break;
        }

        case 'd': // Duration in seconds to run.
        {
            char *strtolPtr;
            duration = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (duration <= 0)) {
                fprintf(stderr, "option -d requires a numeric arg\n");
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

        case 's': // Simulated clock skew.
        {
            char *strtolPtr;
            skew = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (skew < 0))
            {
                fprintf(stderr,
                        "option -s requires a numeric arg\n");
            }
            break;
        }

        case 'e': // Simulated clock error.
        {
            char *strtolPtr;
            error = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (error < 0))
            {
                fprintf(stderr,
                        "option -e requires a numeric arg\n");
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

        default:
            fprintf(stderr, "Unknown argument %s\n", argv[optind]);
            break;
        }
    }

    if (configPaths.empty()) {
        Panic("Requires at least 1 config file.");
    }

    map<int, specpaxos::Configuration *> configs;
    for (unsigned int i = 0; i < configPaths.size(); i++) {
        ifstream configStream(configPaths[i]);
        if (configStream.fail()) {
            Panic("unable to read configuration file: %s",
                  configPaths[i]);
        }

        specpaxos::Configuration *config = new specpaxos::Configuration(configStream);
        configs.insert(make_pair(i, config));
    }

    UDPTransport *transport = new UDPTransport(0, 0, 0, 0);
    client = new kvstore::KVClient(configs, transport, nShards);

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

    struct timeval t0;
    int ttype; // Transaction type.
    vector<int> keyIdx;

    gettimeofday(&t0, NULL);
    srand(t0.tv_sec + t0.tv_usec);

    gettimeofday(&initialTime, NULL);
    while (1) {
        gettimeofday(&currTime, NULL);
        uint64_t time_elapsed = currTime.tv_sec - initialTime.tv_sec;

        if (phase == WARMUP) {
            if (time_elapsed >= (uint64_t)duration / 3) {
                phase = MEASURE;
                startTime = currTime;
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
        vector<kvstore::KVOp> ops;
        vector<string> results;
        kvstore::KVOp readOp, writeOp;
        readOp.opType = kvstore::KVOp::GET;
        writeOp.opType = kvstore::KVOp::PUT;

        keyIdx.clear();

        // Decide which type of retwis transaction it is going to be.
        ttype = rand() % 100;

        if (ttype < 5) {
            // 5% - Add user transaction. 1,3
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            sort(keyIdx.begin(), keyIdx.end());

            readOp.key = keys[keyIdx[0]];
            ops.push_back(readOp);

            for (int i = 0; i < 3; i++) {
                writeOp.key = keys[keyIdx[i]];
                writeOp.value = keys[keyIdx[i]];
                ops.push_back(writeOp);
            }
            ttype = 1;
        } else if (ttype < 20) {
            // 15% - Follow/Unfollow transaction. 2,2
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            sort(keyIdx.begin(), keyIdx.end());

            for (int i = 0; i < 2; i++) {
                readOp.key = keys[keyIdx[i]];
                ops.push_back(readOp);

                writeOp.key = keys[keyIdx[i]];
                writeOp.value = keys[keyIdx[i]];
                ops.push_back(writeOp);
            }
            ttype = 2;
        } else if (ttype < 50) {
            // 30% - Post tweet transaction. 3,5
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            sort(keyIdx.begin(), keyIdx.end());

            for (int i = 0; i < 3; i++) {
                readOp.key = keys[keyIdx[i]];
                ops.push_back(readOp);

                writeOp.key = keys[keyIdx[i]];
                writeOp.value = keys[keyIdx[i]];
                ops.push_back(writeOp);
            }
            for (int i = 0; i < 2; i++) {
                writeOp.key = keys[keyIdx[i+3]];
                writeOp.value = keys[keyIdx[i+3]];
                ops.push_back(writeOp);
            }
            ttype = 3;
        } else {
            // 50% - Get followers/timeline transaction. rand(1,10),0
            int nGets = 1 + rand() % 10;
            for (int i = 0; i < nGets; i++) {
                keyIdx.push_back(rand_key());
            }

            sort(keyIdx.begin(), keyIdx.end());
            for (int i = 0; i < nGets; i++) {
                readOp.key = keys[keyIdx[i]];
                ops.push_back(readOp);
            }
            ttype = 4;
        }

        bool indep = true;
        client->InvokeKVTxn(ops, results, indep);
        uint64_t ns = Latency_End(&latency);

        /*
        long latency = (t2.tv_sec - t1.tv_sec) * 1000000 + (t2.tv_usec - t1.tv_usec);
        int retries = (client->Stats())[0];

        fprintf(stderr, "%d %ld.%06ld %ld.%06ld %ld %d %d %d", ++nTransactions, t1.tv_sec,
                t1.tv_usec, t2.tv_sec, t2.tv_usec, latency, status?1:0, ttype, retries);
        fprintf(stderr, "\n");

        if ( ((t2.tv_sec-t0.tv_sec)*1000000 + (t2.tv_usec-t0.tv_usec)) > duration*1000000)
            break;
            */
        if (phase == MEASURE) {
            latencies.push_back(ns);
        }
    }

    struct timeval diff = timeval_sub(endTime, startTime);
    uint64_t total_transactions = latencies.size();
    Notice("Completed %lu transactions in " FMT_TIMEVAL_DIFF " seconds",
           total_transactions, VA_TIMEVAL_DIFF(diff));

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

    delete client;
    fprintf(stderr, "# Client exiting..\n");
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
                zipf[i-1] = sum;
            }
            ready = true;
        }

        double random = 0.0;
        while (random == 0.0 || random == 1.0) {
            random = (1.0 + rand())/RAND_MAX;
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
