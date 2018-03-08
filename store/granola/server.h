// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/granola/server.h:
 *   Granola protocol server implementation.
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

#ifndef __GRANOLA_SERVER_H__
#define __GRANOLA_SERVER_H__

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/configuration.h"
#include "lib/transport.h"
#include "common/replica.h"
#include "common/quorumset.h"
#include "common/messageset.h"
#include "store/granola/granola-proto.pb.h"

#include <string>
#include <map>

namespace specpaxos {
namespace store {
namespace granola {

class GranolaServer : public Replica
{
public:
    GranolaServer(const Configuration &config, int myShard, int myIdx,
                  bool initialize, Transport *transport, AppReplica *app, bool locking=false);
    ~GranolaServer();

    void ReceiveMessage(const TransportAddress &remote,
                        const std::string &type, const std::string &data,
                        void *meta_data) override;

    void SetMode(bool locking) { this->locking = locking; }

private:
    /* Log entry additional data */
    typedef uint64_t timestamp_t;
    struct EntryData {
        txnid_t txnid;
        bool indep;
        bool ro;
        timestamp_t proposed_ts;
        timestamp_t final_ts;
        bool ts_decided;
        proto::Status status;

        EntryData()
            : txnid(0), indep(false), ro(false),
            proposed_ts(0), final_ts(0), ts_decided(false) { }
        EntryData(txnid_t txnid,
                  bool indep,
                  bool ro,
                  timestamp_t proposed_ts,
                  timestamp_t final_ts,
                  bool ts_decided)
            : txnid(txnid), indep(indep), ro(ro),
            proposed_ts(proposed_ts), final_ts(final_ts), ts_decided(ts_decided) { }
    };

public:
    Log<EntryData> log;

private:
    typedef Log<EntryData>::LogEntry LogEntry;
    /* Replica states */
    view_t view;
    opnum_t lastOp;
    opnum_t lastCommitted;

    /* Locking mode */
    bool locking;

    /* Client information */
    std::map<uint64_t, std::unique_ptr<TransportAddress> > clientAddresses;
    struct ClientTableEntry
    {
	uint64_t lastReqId;
        bool replied;
	proto::ReplyMessage reply;
    };
    std::map<uint64_t, ClientTableEntry> clientTable;


    /* Timestamps */
    timestamp_t localClock;

    /* On-going transactions (waiting for votes) */
    struct __ptxn_key_compare {
        bool operator() (const std::pair<opnum_t, timestamp_t> &lhs,
                         const std::pair<opnum_t, timestamp_t> &rhs) const
        {
            return lhs.second < rhs.second;
        }
    };
    // PendingTransactions sorted in ascending timestamp order
    std::set<std::pair<opnum_t, timestamp_t>, __ptxn_key_compare> pendingTransactions;

    /* Quorums */
    QuorumSet<viewstamp_t, proto::PrepareOKMessage> prepareOKQuorum;
    MessageSet<std::pair<uint64_t, uint64_t>, proto::VoteMessage> voteQuorum;

    /* Timeouts */
    Timeout *resendPrepareTimeout;
    const int RESEND_PREPARE_TIMEOUT = 10;
    std::map<opnum_t, Timeout *> voteTimeouts;
    const int VOTE_TIMEOUT = 10;

    /* Message handlers */
    void HandleClientRequest(const TransportAddress &remote,
			     const proto::RequestMessage &msg);
    void HandlePrepare(const TransportAddress &remote,
                       const proto::PrepareMessage &msg);
    void HandlePrepareOK(const TransportAddress &remote,
                         const proto::PrepareOKMessage &msg);
    void HandleCommit(const TransportAddress &remote,
                      const proto::CommitMessage &msg);
    void HandleVote(const TransportAddress &remote,
                    const proto::VoteMessage &msg);
    void HandleVoteRequest(const TransportAddress &remote,
                           const proto::VoteRequestMessage &msg);
    void HandleFinalTimestamp(const TransportAddress &remote,
                              const proto::FinalTimestampMessage &msg);

    void CommitUpTo(opnum_t opnum);
    void CheckVoteQuorum(LogEntry *entry);
    void ExecuteTxn(LogEntry *entry);
    void ExecuteTxns();
    void UpdateClientTable(const Request &req);
    void SendPrepare();
    void SendVoteRequest(LogEntry *entry);
    inline bool AmLeader();
};

} // namespace granola
} // namespace store
} // namespace specpaxos

#endif /* _GRANOLA_SERVER_H_ */
