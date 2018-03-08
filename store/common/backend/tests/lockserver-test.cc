// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/backend/tests/lockserver-test.cc:
 *   test cases for simple lock server class
 *
 * Copyright 2015 Irene Zhang  <iyzhang@cs.washington.edu>
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

#include "store/common/backend/lockserver.h"

#include <gtest/gtest.h>

class LockServerTest : public ::testing::Test
{
protected:
    virtual void SetUp() override {
        lockserver_ = new LockServer(false);
    }
    virtual void TearDown() override {
        delete lockserver_;
    }

    LockServer *lockserver_;
};

TEST_F(LockServerTest, SimpleReadLockTest)
{
    EXPECT_TRUE(lockserver_->lockForRead("x", 1));
    EXPECT_TRUE(lockserver_->lockForRead("x", 2));
    EXPECT_FALSE(lockserver_->lockForWrite("x", 3));
}

TEST_F(LockServerTest, SimpleWriteLockTest)
{
    EXPECT_TRUE(lockserver_->lockForWrite("x", 1));
    EXPECT_FALSE(lockserver_->lockForRead("x", 2));
    EXPECT_FALSE(lockserver_->lockForWrite("x", 3));
}

TEST_F(LockServerTest, WaitForReadLockTest)
{
    EXPECT_TRUE(lockserver_->lockForWrite("x", 1));
    EXPECT_FALSE(lockserver_->lockForRead("x", 2));
    EXPECT_FALSE(lockserver_->lockForRead("x", 3));

    std::unordered_set<uint64_t> holders;
    lockserver_->releaseForWrite("x", 1, holders);
    EXPECT_EQ(holders.size(), 2);
    EXPECT_EQ(holders.count(2), 1);
    EXPECT_EQ(holders.count(3), 1);
    EXPECT_EQ(holders.count(1), 0);
    EXPECT_TRUE(lockserver_->lockForRead("x", 2));
    EXPECT_TRUE(lockserver_->lockForRead("x", 3));
}

TEST_F(LockServerTest, WaitForWriteLockTest)
{
    EXPECT_TRUE(lockserver_->lockForRead("x", 1));
    EXPECT_TRUE(lockserver_->lockForRead("x", 2));
    EXPECT_FALSE(lockserver_->lockForWrite("x", 3));
    EXPECT_FALSE(lockserver_->lockForRead("x", 4));

    std::unordered_set<uint64_t> holders;
    lockserver_->releaseForRead("x", 1, holders);
    EXPECT_EQ(holders.size(), 0);
    EXPECT_FALSE(lockserver_->lockForWrite("x", 3));
    EXPECT_FALSE(lockserver_->lockForRead("x", 4));

    lockserver_->releaseForRead("x", 2, holders);
    EXPECT_EQ(holders.size(), 1);
    EXPECT_EQ(holders.count(3), 1);
    EXPECT_EQ(holders.count(4), 0);
    EXPECT_TRUE(lockserver_->lockForWrite("x", 3));
    EXPECT_FALSE(lockserver_->lockForRead("x", 4));

    holders.clear();
    lockserver_->releaseForWrite("x", 3, holders);
    EXPECT_EQ(holders.size(), 1);
    EXPECT_EQ(holders.count(3), 0);
    EXPECT_EQ(holders.count(4), 1);
    EXPECT_TRUE(lockserver_->lockForRead("x", 4));
}

TEST_F(LockServerTest, ReadWriteLockTest)
{
    EXPECT_TRUE(lockserver_->lockForRead("x", 1));
    EXPECT_TRUE(lockserver_->lockForWrite("x", 1));
    EXPECT_FALSE(lockserver_->lockForRead("x", 2));
    EXPECT_FALSE(lockserver_->lockForWrite("x", 3));

    std::unordered_set<uint64_t> holders;
    lockserver_->releaseForWrite("x", 1, holders);
    EXPECT_EQ(holders.size(), 1);
    EXPECT_EQ(holders.count(2), 1);
    EXPECT_EQ(holders.count(3), 0);

    holders.clear();
    lockserver_->releaseForRead("x", 2, holders);
    EXPECT_EQ(holders.size(), 0);

    holders.clear();
    lockserver_->releaseForRead("x", 1, holders);
    EXPECT_EQ(holders.size(), 1);
    EXPECT_EQ(holders.count(3), 1);
    EXPECT_TRUE(lockserver_->lockForWrite("x", 3));
}

TEST_F(LockServerTest, MultiLcokTest)
{
    EXPECT_TRUE(lockserver_->lockForRead("x", 1));
    EXPECT_TRUE(lockserver_->lockForRead("w", 1));

    EXPECT_TRUE(lockserver_->lockForWrite("y", 2));
    EXPECT_TRUE(lockserver_->lockForWrite("z", 2));

    EXPECT_FALSE(lockserver_->lockForWrite("x", 3));
    EXPECT_FALSE(lockserver_->lockForRead("y", 3));
    EXPECT_FALSE(lockserver_->lockForWrite("w", 3));

    EXPECT_FALSE(lockserver_->lockForRead("y", 4));
    EXPECT_FALSE(lockserver_->lockForRead("z", 4));

    std::unordered_set<uint64_t> holders;
    lockserver_->releaseForWrite("y", 2, holders);
    EXPECT_EQ(holders.size(), 2);
    EXPECT_EQ(holders.count(3), 1);
    EXPECT_EQ(holders.count(4), 1);
    holders.clear();
    lockserver_->releaseForWrite("z", 2, holders);
    EXPECT_EQ(holders.size(), 1);
    EXPECT_EQ(holders.count(4), 1);

    holders.clear();
    lockserver_->releaseForRead("x", 1, holders);
    EXPECT_EQ(holders.size(), 1);
    EXPECT_EQ(holders.count(3), 1);
    holders.clear();
    lockserver_->releaseForRead("w", 1, holders);
    EXPECT_EQ(holders.size(), 1);
    EXPECT_EQ(holders.count(3), 1);
}
