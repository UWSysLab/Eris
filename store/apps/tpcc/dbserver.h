// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DBSERVER_H__
#define DBSERVER_H__

#include <string>
#include <stdint.h>

#include "store/apps/tpcc/dbimpl.h"
#include "store/apps/tpcc/assert.h"

class DBServer  {
public:
    // Owns db
    DBServer(DBImpl* db) : db_(db) {
        assert(db_ != NULL);
    }
    virtual ~DBServer();

    // tids and undo records are only used in the 2PC (sinfonia) version
    bool handle(const std::string& work_unit, std::string* output,
    		void** undo = NULL, int64_t tid = -1);

    virtual void applyUndo(void* undo);
    virtual void freeUndo(void* undo);

    // call this before starting a transaction, so we know which locks to acquire
    void startTransaction(int64_t tid);

    // have to call this when every transaction is complete, to release locks etc
    void endTransaction(int64_t tid);

private:
    DBImpl* db_;
};

#endif
