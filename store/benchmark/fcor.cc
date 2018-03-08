// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/fcor.cc:
 *   Entry point of eris failure coordinator.
 *
 * Copyright 2017 Jialin Li <lijl@cs.washington.edu>
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

#include "lib/udptransport.h"
#include "vr/replica.h"
#include "store/eris/fcor.h"

using namespace std;
using namespace specpaxos::vr;
using namespace specpaxos::store::eris;

static void
Usage(const char *prog)
{
        fprintf(stderr, "usage: %s -c conf-file -e eris-config-fiel -i replica-index\n", prog);
        exit(1);
}

int
main(int argc, char **argv)
{
    int index = -1;
    const char *configPath = nullptr;
    const char *erisConfigPath = nullptr;

    int opt;
    while ((opt = getopt(argc, argv, "c:e:i:")) != -1) {
        switch (opt) {
        case 'c':
        {
            configPath = optarg;
            break;
        }

        case 'e':
        {
            erisConfigPath = optarg;
            break;
        }

        case 'i':
        {
            char *strtolPtr;
            index = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (index < 0))
            {
                fprintf(stderr,
                        "option -i requires a numeric arg\n");
                Usage(argv[0]);
            }
            break;
        }

        default:
            fprintf(stderr, "Unknown argument %s\n", argv[optind]);
            Usage(argv[0]);
            break;
        }
    }

    if (!configPath) {
        fprintf(stderr, "option -c is required\n");
        Usage(argv[0]);
    }
    if (!erisConfigPath) {
        fprintf(stderr, "option -e is required\n");
        Usage(argv[0]);
    }
    if (index < 0) {
        fprintf(stderr, "option -i is required\n");
        Usage(argv[0]);
    }

    // Load configuration
    ifstream configStream(configPath);
    if (configStream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n",
                configPath);
        Usage(argv[0]);
    }
    specpaxos::Configuration config(configStream);

    ifstream erisConfigStream(erisConfigPath);
    if (erisConfigStream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n",
                erisConfigPath);
        Usage(argv[0]);
    }
    specpaxos::Configuration erisConfig(erisConfigStream);

    if (index >= config.n) {
        fprintf(stderr, "replica index %d is out of bounds; "
                "only %d replicas defined\n", index, config.n);
        Usage(argv[0]);
    }

    UDPTransport *transport = new UDPTransport();
    Fcor *fcor = new Fcor(erisConfig, transport);
    VRReplica *replica = new VRReplica(config, index, true, transport, 1, fcor);

    transport->Run();

    delete replica;
    delete fcor;
    delete transport;
}
