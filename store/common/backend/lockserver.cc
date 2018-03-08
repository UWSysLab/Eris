// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/backend/lockserver.cc:
 *   Simple multi-reader, single-writer lock server
 *
 **********************************************************************/

#include "lockserver.h"

using namespace std;

LockServer::LockServer(bool retryLock)
    : retryLock(retryLock)
{
    readers = 0;
    writers = 0;
}

LockServer::~LockServer() { }

void
LockServer::Lock::waitForLock(uint64_t requester, bool write)
{
    if (waiters.find(requester) != waiters.end()) {
        // Already waiting
        // Updates read write lock if necessary
        if (write) {
            waiters[requester].write = true;
        } else {
            waiters[requester].read = true;
        }
        return;
    }

    Debug("[%lu] Adding me to the queue ...", requester);
    // Otherwise
    waiters[requester] = Waiter(write);
    waitQ.push(requester);
}

bool
LockServer::Lock::tryAcquireLock(uint64_t requester, bool write)
{
    if (waitQ.size() == 0) {
        return true;
    }

    Debug("[%lu] Trying to get lock for %d", requester, (int)write);

    if (waitQ.front() == requester) {
        // this lock is being reserved for the requester
        waitQ.pop();
        ASSERT(waiters.find(requester) != waiters.end());
        waiters.erase(requester);
        return true;
    } else {
        // otherwise, add me to the list
        waitForLock(requester, write);
        return false;
    }
}

bool
LockServer::Lock::isWriteNext()
{
    if (waitQ.size() == 0) return false;

    ASSERT(waiters.find(waitQ.front()) != waiters.end());
    return waiters[waitQ.front()].write;
}

bool
LockServer::lockForRead(const string &lock, uint64_t requester)
{
    Lock &l = locks[lock];
    Debug("Lock for Read: %s [%lu %lu %lu %lu]", lock.c_str(),
          readers, writers, l.holders.size(), l.waiters.size());

    switch (l.state) {
    case UNLOCKED:
        // if you are next in the queue
        if (l.tryAcquireLock(requester, false)) {
            Debug("[%lu] I have acquired the read lock!", requester);
            l.state = LOCKED_FOR_READ;
            ASSERT(l.holders.size() == 0);
            l.holders.insert(requester);
            readers++;
            return true;
        }
        return false;
    case LOCKED_FOR_READ:
        // if you already hold this lock
        if (l.holders.find(requester) != l.holders.end()) {
            return true;
        }

        // There is a write waiting, let's give up the lock
        if (l.isWriteNext()) {
            Debug("[%lu] Waiting on lock because there is a pending write request", requester);
            l.waitForLock(requester, false);
            return false;
        }

        l.holders.insert(requester);
        readers++;
        return true;
    case LOCKED_FOR_WRITE:
    case LOCKED_FOR_READ_WRITE:
        if (l.holders.count(requester) > 0) {
            if (l.state == LOCKED_FOR_WRITE) {
                // If already holding read write lock,
                // no need to increment reader count.
                readers++;
            }
            l.state = LOCKED_FOR_READ_WRITE;
            return true;
        }
        ASSERT(l.holders.size() == 1);
        Debug("Locked for write, held by %lu", *(l.holders.begin()));
        l.waitForLock(requester, false);
        return false;
    }
    NOT_REACHABLE();
    return false;
}

bool
LockServer::lockForWrite(const string &lock, uint64_t requester)
{
    Lock &l = locks[lock];

    Debug("Lock for Write: %s [%lu %lu %lu %lu]", lock.c_str(),
          readers, writers, l.holders.size(), l.waiters.size());

    switch (l.state) {
    case UNLOCKED:
        // Got it!
        if (l.tryAcquireLock(requester, true)) {
            Debug("[%lu] I have acquired the write lock!", requester);
            l.state = LOCKED_FOR_WRITE;
            ASSERT(l.holders.size() == 0);
            l.holders.insert(requester);
            writers++;
            return true;
        }
        return false;
    case LOCKED_FOR_READ:
        if (l.holders.size() == 1 && l.holders.count(requester) > 0) {
            // if there is one holder of this read lock and it is the
            // requester, then upgrade the lock
            l.state = LOCKED_FOR_READ_WRITE;
            writers++;
            return true;
        }

        Debug("Locked for read by%s%lu other people", l.holders.count(requester) > 0 ? "you" : "", l.holders.size());
        l.waitForLock(requester, true);
        return false;
    case LOCKED_FOR_WRITE:
    case LOCKED_FOR_READ_WRITE:
        ASSERT(l.holders.size() == 1);
        if (l.holders.count(requester) > 0) {
            return true;
        }

        Debug("Held by %lu for %s", *(l.holders.begin()), (l.state == LOCKED_FOR_WRITE) ? "write" : "read-write" );
        l.waitForLock(requester, true);
        return false;
    }
    NOT_REACHABLE();
    return false;
}

void
LockServer::releaseForRead(const string &lock, uint64_t holder,
                           unordered_set<uint64_t> &newholders)
{
    if (locks.find(lock) == locks.end()) {
        return;
    }

    Lock &l = locks[lock];

    if (l.holders.count(holder) == 0) {
        Debug("[%ld] Releasing unheld read lock: %s", holder, lock.c_str());
        return;
    }

    switch (l.state) {
    case UNLOCKED:
    case LOCKED_FOR_WRITE:
        return;
    case LOCKED_FOR_READ:
        readers--;
        if (l.holders.erase(holder) < 1) {
            Debug("[%ld] Releasing unheld read lock: %s", holder, lock.c_str());
        }
        if (l.holders.empty()) {
            l.state = UNLOCKED;
            break;
        }
        return;
    case LOCKED_FOR_READ_WRITE:
        readers--;
        l.state = LOCKED_FOR_WRITE;
        return;
    }

    ASSERT(l.state == UNLOCKED);
    if (!this->retryLock) {
        TryAcquireForWaiters(lock, newholders);
    }
}

void
LockServer::releaseForWrite(const string &lock, uint64_t holder,
                            unordered_set<uint64_t> &newholders)
{
    if (locks.find(lock) == locks.end()) {
        return;
    }

    Lock &l = locks[lock];

    if (l.holders.count(holder) == 0) {
        Debug("[%ld] Releasing unheld write lock: %s", holder, lock.c_str());
        return;
    }

    switch (l.state) {
    case UNLOCKED:
    case LOCKED_FOR_READ:
        return;
    case LOCKED_FOR_WRITE:
        writers--;
        l.holders.erase(holder);
        ASSERT(l.holders.size() == 0);
        l.state = UNLOCKED;
        break;
    case LOCKED_FOR_READ_WRITE:
        writers--;
        l.state = LOCKED_FOR_READ;
        ASSERT(l.holders.size() == 1);
        break;
    }

    ASSERT(l.state == UNLOCKED || l.state == LOCKED_FOR_READ);
    if (!this->retryLock) {
        TryAcquireForWaiters(lock, newholders);
    }
}

void
LockServer::TryAcquireForWaiters(const string &lock, unordered_set<uint64_t> &newholders)
{
    Lock &l = locks[lock];
    while (!l.waitQ.empty()) {
        // Now the lock is free, acquire the lock for waiters in the queue
        uint64_t waiter = l.waitQ.front();
        ASSERT(l.waiters.count(waiter) > 0);
        bool acquired = true;
        bool isWrite = l.waiters.at(waiter).write;
        bool isRead = l.waiters.at(waiter).read;
        if (isWrite) {
            acquired &= lockForWrite(lock, waiter);
        }
        if (isRead) {
            acquired &= lockForRead(lock, waiter);
        }
        if (acquired) {
            newholders.insert(waiter);
            if (l.waitQ.front() == waiter) {
                // remove from waitQ
                l.waitQ.pop();
                ASSERT(l.waiters.count(waiter)> 0);
                l.waiters.erase(waiter);
            }
        } else {
            // Head of the waitQ is blocked
            break;
        }
    }
}
