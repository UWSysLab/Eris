// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/client.h:
 *   Interface for a multiple shard transactional client.
 *
 **********************************************************************/

#ifndef _CLIENT_API_H_
#define _CLIENT_API_H_

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/transaction.h"
#include <string>
#include <vector>

class Client
{
public:
    Client() {};
    virtual ~Client() {};

    // Issues a transaction to set of shards (can be single shard)
    virtual void InvokeTransaction(std::vector<rid_t> &rids, std::vector<op_t> &ops, std::string & result) = 0;
    // Returns statistics (vector of integers) about most recent transaction.
    // virtual std::vector<int> Stats() = 0;

    // Sharding logic: Given key, generates a number b/w 0 to nshards-1
    uint64_t key_to_shard(const std::string &key, uint64_t nshards) {
        uint64_t hash = 5381;
        const char* str = key.c_str();
        for (unsigned int i = 0; i < key.length(); i++) {
            hash = ((hash << 5) + hash) + (uint64_t)str[i];
        }

        return (hash % nshards);
    };
};

#endif /* _CLIENT_API_H_ */
