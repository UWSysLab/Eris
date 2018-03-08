// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * sequencer/sequencer.cc:
 *   End-host network sequencer implementation.
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

#include <iostream>
#include <fstream>
#include "lib/message.h"
#include "sequencer/sequencer.h"

using namespace std;

namespace sequencer {

Sequencer::Sequencer(uint64_t sequencer_id) : sequencer_id(sequencer_id) { }

Sequencer::~Sequencer() { }

uint64_t
Sequencer::Increment(uint32_t groupIdx) {
    if (this->counters.find(groupIdx) == this->counters.end()) {
        this->counters.insert(make_pair(groupIdx, 0));
    }

    return ++this->counters[groupIdx];
}

Configuration::Configuration(ifstream &file) {
    while (!file.eof()) {
        string line;
        getline(file, line);

        // Ignore comments
        if ((line.size() == 0) || (line[0] == '#')) {
            continue;
        }

        char *cmd = strtok(&line[0], " \t");

        if (strcasecmp(cmd, "interface") == 0) {
            char *arg = strtok(nullptr, " \t");
            if (!arg) {
                Panic("'interface' configuration line requires an argument");
            }

            char *iface = strtok(arg, "");

            if (!iface) {
                Panic("Configuration line format: 'interface name'");
            }

            this->interface = string(iface);
        } else if (strcasecmp(cmd, "groupaddr") == 0) {
            char *arg = strtok(nullptr, " \t");
            if (!arg) {
                Panic("'groupaddr' configuration line requires an argument");
            }

            char *gaddr = strtok(arg, "");

            if (!gaddr) {
                Panic("Configuration line format: 'groupaddr addr;");
            }
            this->groupAddr = string(gaddr);
        } else {
            Panic("Unknown configuration directive: %s", cmd);
        }
    }
}

Configuration::~Configuration() { }

string
Configuration::GetInterface() {
    return this->interface;
}

string
Configuration::GetGroupAddr() {
    return this->groupAddr;
}

Transport::Transport(Sequencer *sequencer, Configuration *config)
    : sequencer(sequencer), config(config), sockfd(-1)
{
    struct ifreq ifopts;
    struct sock_fprog filter;
    struct sockaddr_ll sll;
    int sockopt = 1;
    char tcpdumpCommand [1024];
    sprintf(tcpdumpCommand, "tcpdump \"ip and udp and dst %s\" -ddd", config->GetGroupAddr().c_str());;
    FILE *tcpdumpOutput;
    if ((tcpdumpOutput = popen(tcpdumpCommand, "r")) == NULL) {
        PPanic("Failed to compile BPF filter using tcpdump");
    }
    int lineCount;
    if (fscanf(tcpdumpOutput, "%d\n", &lineCount) < 1) {
        PPanic("Failed to read filter line count");
    }

    filter.filter = (struct sock_filter *)calloc(sizeof(struct sock_filter)*lineCount, 1);
    filter.len = lineCount;
    for (int i = 0; i < lineCount; i++) {
        if (fscanf(tcpdumpOutput, "%hu %hhu %hhu %u\n", &(filter.filter[i].code), &(filter.filter[i].jt), &(filter.filter[i].jf), &(filter.filter[i].k)) < 4) {
            PPanic("Failed to read filter code line %d\n", i+1);
        }
    }
    pclose(tcpdumpOutput);

    if ((this->sockfd = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL))) == -1) {
        Panic("Failed to open socket");
    }

    memset(&ifopts, 0, sizeof(ifopts));
    strncpy(ifopts.ifr_name, config->GetInterface().c_str(), IFNAMSIZ-1);
    if (ioctl(this->sockfd, SIOCGIFINDEX, &ifopts) < 0) {
        Panic("Failed to set ioctl option SIOCGIFINDEX");
    }

    if (setsockopt(this->sockfd, SOL_SOCKET, SO_ATTACH_FILTER, &filter, sizeof(filter)) < 0) {
        Panic("Failed to attach BPF filter");
    }

    if (setsockopt(this->sockfd, SOL_SOCKET, SO_REUSEADDR, &sockopt, sizeof(sockopt)) == -1) {
        Panic("Failed to set socket option SO_REUSEADDR");
    }

    bzero(&sll, sizeof(sll));
    sll.sll_family = AF_PACKET;
    sll.sll_ifindex = ifopts.ifr_ifindex;

    if (bind(this->sockfd, (struct sockaddr *)&sll, sizeof(sll)) == -1) {
        Panic("Failed to bind socket");
    }

    /* Sequencer sends out packets using multicast */
    this->destSockAddr.sll_ifindex = ifopts.ifr_ifindex;
    this->destSockAddr.sll_halen = ETH_ALEN;
    for (int i = 0; i < ETH_ALEN; i++) {
        this->destSockAddr.sll_addr[i] = 0xFF;
    }
}

Transport::~Transport() {
    if (sockfd != -1) {
        close(sockfd);
    }
}

void
Transport::Run() {
    int n;
    uint8_t buffer[BUFFER_SIZE];

    if (this->sockfd == -1) {
        Warning("Transport not registered yet");
        return;
    }

    while (true) {
        n = recvfrom(this->sockfd, buffer, BUFFER_SIZE, 0, nullptr, nullptr);

        if (n <= 0) {
            break;
        }

        if (ProcessPacket(buffer, n)) {
            if (sendto(this->sockfd, buffer, n, 0,
                       (struct sockaddr*)&this->destSockAddr,
                       sizeof(struct sockaddr_ll)) <= 0) {
                Warning("Failed to send packet");
            }
        }
    }
}

bool
Transport::ProcessPacket(uint8_t *packet, size_t len)
{
    struct udphdr *udph;
    uint8_t *datagram, ngroups;
    uint16_t group_bitmap, host_bitmap;

    if (len < sizeof(struct ether_header) + sizeof(struct iphdr) + sizeof(struct udphdr)) {
        return false;
    }

    udph = (struct udphdr *)(packet
                             + sizeof(struct ether_header)
                             + sizeof(struct iphdr));
    datagram = (uint8_t *)(packet
                           + sizeof(struct ether_header)
                           + sizeof(struct iphdr)
                           + sizeof(struct udphdr));

    /* All network ordered messages are multicast.
     * Check ethernet destination is FF:FF:FF:FF:FF:FF,
     * and IP destination is the group multicast address.
     */

    /* Network ordered packet header format:
     * FRAG_MAGIC(32) | header data len (32) | original udp src (16) |
     * session ID (64) | number of groups (32) |
     * group1 ID (32) | group1 sequence number (64) |
     * group2 ID (32) | group2 sequence number (64) |
     * ...
     */

    if (*(uint32_t *)datagram != NONFRAG_MAGIC) {
        // Only sequence the packet if it is not
        // fragmented.
        return false;
    }

    datagram += sizeof(uint32_t) + sizeof(uint32_t); // now points to udp src
    /* Write the original udp src into header */
    *(uint16_t *)datagram = udph->source;

    datagram += sizeof(uint16_t); // now points to session ID
    *(uint64_t *)datagram = this->sequencer->GetSequencerID();

    datagram += sizeof(uint64_t); // now points to number of groups
    ngroups = *(uint32_t *)datagram;

    datagram += sizeof(uint32_t); // now points to group1 ID
    group_bitmap = 0;
    host_bitmap = 0;
    for (int i = 0; i < ngroups; i++) {
        uint32_t groupid = *(uint32_t *)datagram;
        datagram += sizeof(uint32_t);
        *(uint64_t *)datagram = this->sequencer->Increment(groupid);
        datagram += sizeof(uint64_t);
        group_bitmap |= (1 << groupid);
        host_bitmap |= (1 << (groupid/NGROUP_PER_HOST));
    }

    /* Update udp header src field with the group bitmap.
     * Switches use this bitmap to perform group cast.
     */
    udph->source = htons(group_bitmap);
    udph->dest = htons(host_bitmap);
    udph->check = 0; // disable udp checksum
    return true;
}

} // namespace sequencer

int main(int argc, char *argv[]) {
    const char *config_path = nullptr;
    int opt;
    int coreid = -1;

    while ((opt = getopt(argc, argv, "c:r:")) != -1) {
        switch (opt) {
        case 'c':
            config_path = optarg;
            break;

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

        default:
            fprintf(stderr, "Unknown argument %s\n", argv[optind]);
            break;
        }
    }

    if (config_path == nullptr) {
        fprintf(stderr, "option -c is required\n");
        return 1;
    }

    ifstream config_stream(config_path);
    if (config_stream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n",
                config_path);
        return 1;
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

    sequencer::Configuration config(config_stream);
    sequencer::Sequencer sequencer(0);
    sequencer::Transport transport(&sequencer, &config);
    transport.Run();

    return 0;
}
