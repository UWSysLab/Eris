# Eris

This is an implementation of the Eris protocol, as described in the paper
["Eris: Coordination-Free Consistent Transactions Using In-Network Concurrency Control"](https://homes.cs.washington.edu/~lijl/papers/eris-sosp17.pdf)
from SOSP 2017.

Eris is a new system for high-performance distributed transaction processing.
It moves a core piece of concurrency control functionality into the datacenter
network itself. This network primitive takes on the responsibility for
consistently ordering transactions, and a new lightweight transaction protocol
ensures atomicity.

In the normal case, Eris avoids both replication and transaction coordination
overhead. It can process a large class of distributed transactions in a
single round-trip from the client to the storage system without any explicit
coordination between shards or replicas.

## Contents

This repository contains implementations of 5 distributed transaction protocols:

1. Eris, including independent transaction processing, view change,
   epoch change, and general transaction processing protocols.

2. Lock-Store, a standard distributed transaction protocol that uses two phase
   commit, two phase locking, and Multi-Paxos.

3. TAPIR, as described in the paper
   ["Building Consistent Transactions with Inconsistent Replication"](https://homes.cs.washington.edu/~arvind/papers/tapir.pdf).

4. Granola, as described in the paper
   ["Granola: Low-Overhead Distributed Transaction Coordination"](https://www.usenix.org/system/files/conference/atc12/atc12-final118.pdf).

5. A nontransactional, unreplicated system that provides neither consistency nor
   fault tolerance guarantees.

...as well as an endhost implementation of the sequencer.

## Building and Running

Eris and can be built using `make`. It has been tested on Ubuntu 14.04,
16.04 and Debian 8. Regression tests can be run with `make check`

Dependencies include (Debian/Ubuntu packages):
  protobuf-compiler pkg-config libunwind-dev libssl-dev libprotobuf-dev libevent-dev libgtest-dev

You will need to create a configuration file with the following
syntax:

```
f <number of failures tolerated>
group
replica <hostname>:<port>
replica <hostname>:<port>
...
group
replica <hostname>:<port>
replica <hostname>:<port>
...
multicast <multicast addr>:<port>
```

Each group is a replicated shard, and should contain `2f+1` replicas. Multicast
address is optional. However, the multi-sequenced groupcast implementation
uses the multicast address as the groupcast address.

In order to run Eris, you need to configure the network to route multi-sequenced
groupcast packets first to the sequencer, and then groupcast to all receivers.
The easiest way is to use OpenFlow and install rules that match on the multicast
address. (You don't need to do this for the other protocols)

Our multi-sequenced groupcast implementation expects a custom header at the beginning
of the UDP data, which contains the epoch number and the multi-stamp described in the
paper. If you need to write your own sequencer, please refer to `sequencer/sequencer.cc`
for the custom header format. If you need to modify the header format, you will have
to modify `ProcessPacket` and `DecodePacket` in `lib/udptransport.cc`.

You can then start a simple kvstore replica server with
`./store/benchmark/txnServer -c <path to config file> -i <replica index> -n
<shard index> -N <total number of shards> -k <number of keys> -f <keys file> -a
kv -m <mode>`, where `keys file` is a text file with one key per line, and
`mode` is either:
  - `spanner` for Lock-Store
  - `tapir` for TAPIR
  - `granola` for Granola
  - `unreplicated` for NT-UR
  - `eris` for Eris

If `eris` mode is used, you additionally need to specify the Failure Coordinator
configuration file using `-o <path to FC config file>`. The FC configuration file
uses a similar syntax:

```
f <number of failures tolerated>
replica <hostname>:<port>
replica <hostname>:<port>
...
```

To start a Failure Coordinator replica, run `./store/benchmark/fcor
-e <path to transaction server config file> -c <path to FC config file>
-i <replica index>`.

To run a single kvstore client, use `./store/benchmark/kvClient -c <path to
config file> -N <total number of shards> -l <transaction length> -k <number of
keys> -f <keys file> -m <mode> -d <duration of test (in seconds)>`, where
transaction length specifies how many keys each transaction queries.

For performance measurements, you will likely want to add `-DNASSERT`
and `-O2` to the `CFLAGS` in the Makefile.

## Contact

Eris is a product of the
[UW Systems Lab](http://syslab.cs.washington.edu/). Please email Jialin
Li at lijl@cs.washington.edu with any questions.
