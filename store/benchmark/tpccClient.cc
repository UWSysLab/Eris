// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/tpccClient.cc:
 *   Benchmarking client for tpcc.
 *
 **********************************************************************/

#include "lib/latency.h"
#include "lib/timeval.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "store/apps/tpcc/clientthread.h"
#include "store/benchmark/header.h"
#include "store/common/frontend/txnclientcommon.h"
#include "store/apps/kvstore/client.h"
#include "store/eris/client.h"
#include "store/granola/client.h"
#include "store/unreplicated/client.h"
#include "store/spanner/client.h"
#include "store/tapir/client.h"

#include <vector>
#include <algorithm>

using namespace std;
using namespace specpaxos;
using namespace specpaxos::store;

DEFINE_LATENCY(op);

int
main(int argc, char **argv) {
	const char * configPath = nullptr;
	int duration = 10;
	int nshards = 1;
	int warehouse_per_shard = 1;
	int clients_per_warehouse = 1;
	int client_id = 0;
	int remote_item_milli_p = -1;
	int total_warehouses;
	int ops_per_iteration = 1;
	struct timeval startTime, endTime, initialTime, currTime;
	struct Latency_t latency;
	vector<uint64_t> latencies;
	phase_t phase = WARMUP;

        Client *protoClient = nullptr;
        TxnClient *txnClient;
	ClientThread *tpccClient;

        protomode_t mode = PROTO_UNKNOWN;

	int opt;
	while ((opt = getopt(argc, argv, "c:d:s:w:p:i:r:o:m:")) != -1) {
		switch (opt) {
		case 'c':
		{
			configPath = optarg;
			break;
		}
		case 'd':
		{
			char *strtolPtr;
			duration = strtoul(optarg, &strtolPtr, 10);
			if ((*optarg == '\0' || (*strtolPtr != '\0') || (duration <= 0))) {
				fprintf(stderr, "option -d requires a numeric arg > 0\n");
			}
			break;
		}
		case 's':
		{
			char *strtolPtr;
			nshards = strtoul(optarg, &strtolPtr, 10);
			if ((*optarg == '\0' || (*strtolPtr != '\0') || (nshards <= 0))) {
				fprintf(stderr, "option -s requires a numeric arg > 0\n");
			}
			break;
		}
		case 'w':
		{
			char *strtolPtr;
			warehouse_per_shard = strtoul(optarg, &strtolPtr, 10);
			if ((*optarg == '\0' || *strtolPtr != '\0' || warehouse_per_shard <= 0)) {
				fprintf(stderr, "option -w requires a numeric arg > 0\n");
			}
			break;
		}
		case 'p':
		{
			char *strtolPtr;
			clients_per_warehouse = strtoul(optarg, &strtolPtr, 10);
			if ((*optarg == '\0' || *strtolPtr != '\0' || clients_per_warehouse <= 0)) {
				fprintf(stderr, "option -p requires a numeric arg > 0\n");
			}
			break;
		}
		case 'i':
		{
			char *strtolPtr;
			client_id = strtoul(optarg, &strtolPtr, 10);
			if ((*optarg == '\0' || *strtolPtr != '\0' || client_id < 0)) {
				fprintf(stderr, "option -i requires a numeric arg >= 0");
			}
			break;
		}
		case 'r':
		{
			char *strtolPtr;
			remote_item_milli_p = strtoul(optarg, &strtolPtr, 10);
			if ((*optarg == '\0' || *strtolPtr != '\0' || remote_item_milli_p < -1)) {
				fprintf(stderr, "option -r requires a numeric arg >= -1");
			}
			break;
		}
		case 'o':
		{
			char *strtolPtr;
			ops_per_iteration = strtoul(optarg, &strtolPtr, 10);
			if ((*optarg == '\0' || *strtolPtr != '\0' || ops_per_iteration <= 0)) {
				fprintf(stderr, "option -o requires a numeric arg > 0");
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
                        fprintf(stderr, "Unknown protocol mode %s\n", optarg);
                    }
                    break;
                }

		default:
			fprintf(stderr, "Unkown argument %s\n", argv[optind]);
			break;
		}
	}

	if (configPath == nullptr) {
		Panic("tpccClient requires -c option\n");
	}

        if (mode == PROTO_UNKNOWN) {
            Panic("tpccClient requires -m option\n");
        }

        ifstream configStream(configPath);
        if (configStream.fail()) {
            Panic("unable to read configuration file: %s", configPath);
        }

        specpaxos::Configuration config(configStream);

	total_warehouses = nshards * warehouse_per_shard;
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
	tpccClient = new ClientThread(total_warehouses,
	                          warehouse_per_shard,
	                          clients_per_warehouse,
	                          client_id,
	                          remote_item_milli_p,
                                  txnClient);

	latencies.reserve(duration * 10000);
	_Latency_Init(&latency, "op");


	gettimeofday(&initialTime, NULL);
	while (true) {
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
		int num_new_orders = tpccClient->doOps(ops_per_iteration);
		uint64_t ns = Latency_End(&latency);

		if (phase == MEASURE) {
                    if (num_new_orders > 0) {
                        // Only report new order txns
			latencies.push_back(ns);
                    }
		}
	}
        txnClient->Done();

	struct timeval diff = timeval_sub(endTime, startTime);

	Notice("Completed %lu transactions in " FMT_TIMEVAL_DIFF " seconds",
	       latencies.size(), VA_TIMEVAL_DIFF(diff));

	char buf[1024];
	std::sort(latencies.begin(), latencies.end());

	uint64_t ns  = latencies[latencies.size() / 2];
	LatencyFmtNS(ns, buf);
	Notice("Median latency is %ld ns (%s)", ns, buf);

	ns = latencies[latencies.size() * 90 / 100];
	LatencyFmtNS(ns, buf);
	Notice("90th percentile latency is %ld ns (%s)", ns, buf);

	ns = latencies[latencies.size() * 95 / 100];
	LatencyFmtNS(ns, buf);
	Notice("95th percentile latency is %ld ns (%s)", ns, buf);

	ns = latencies[latencies.size() * 99 / 100];
	LatencyFmtNS(ns, buf);
	Notice("99th percentile latency is %ld ns (%s)", ns, buf);

        delete tpccClient;
        delete txnClient;
        if (protoClient) {
            delete protoClient;
        }
	return 0;
}
