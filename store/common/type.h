// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/type.h:
 *   All type defines related to transactions.
 *
 **********************************************************************/

#ifndef TRANSACTION_TYPE_H
#define TRANSACTION_TYPE_H

#include <unordered_set>
#include <stdint.h>
#include <string>

namespace specpaxos {
namespace store {

typedef uint64_t txnid_t;       // Transaction id

enum servermode_t {
    MODE_NORMAL,
    MODE_LOCKING
};

enum txntype_t {
    TXN_PREPARE,
    TXN_COMMIT,
    TXN_ABORT,
    TXN_INDEP
};

typedef struct {
    txnid_t txnid;
    txntype_t type;
} txnarg_t;

typedef struct {
    txnid_t txnid;
    bool blocked;
    bool commit;
    /* unblocked transactions */
    std::unordered_set<txnid_t> unblocked_txns;
    servermode_t mode;
} txnret_t;

typedef struct {
    bool indep;
    bool ro;
} clientarg_t;

} // namespace store
} // namespace specpaxos

#endif /* TRANSACTION_TYPE_H */
