// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/backend/lockserver.h:
 *   Simple multi-reader, single-writer lock server.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *           2016 Jialin Li <lijl@cs.washington.edu>
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

#ifndef _LOCK_SERVER_H_
#define _LOCK_SERVER_H_

#include "lib/assert.h"
#include "lib/message.h"
#include <sys/time.h>
#include <map>
#include <queue>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#define LOCK_WAIT_TIMEOUT 5000

class LockServer
{

public:
    LockServer(bool retryLock);
    ~LockServer();

    bool lockForRead(const std::string &lock, uint64_t requester);
    bool lockForWrite(const std::string &lock, uint64_t requester);
    void releaseForRead(const std::string &lock, uint64_t holder,
                        std::unordered_set<uint64_t> &newholders);
    void releaseForWrite(const std::string &lock, uint64_t holder,
                         std::unordered_set<uint64_t> &newholders);

private:
    bool retryLock;
    enum LockState {
        UNLOCKED,
        LOCKED_FOR_READ,
        LOCKED_FOR_WRITE,
        LOCKED_FOR_READ_WRITE
    };

    struct Waiter {
        bool read;
        bool write;

        Waiter() {
            read = true;
            write = false;
        }

        Waiter(bool w) {
            read = !w;
            write = w;
        }
    };

    struct Lock {
        LockState state;
        std::unordered_set<uint64_t> holders;
        std::queue<uint64_t> waitQ;
        std::map<uint64_t, Waiter> waiters;

        Lock() {
            state = UNLOCKED;
        };
        void waitForLock(uint64_t requester, bool write);
        bool tryAcquireLock(uint64_t requester, bool write);
        bool isWriteNext();
    };

    /* Global store which keep key -> (timestamp, value) list. */
    std::unordered_map<std::string, Lock> locks;

    uint64_t readers;
    uint64_t writers;

    void TryAcquireForWaiters(const std::string &lock, std::unordered_set<uint64_t> &newholders);
};

#endif /* _LOCK_SERVER_H_ */
