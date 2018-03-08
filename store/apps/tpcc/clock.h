#ifndef CLOCK_H__
#define CLOCK_H__

//~ #include <cstdint>
#include <stdint.h>

// Interface to the real time system clock.
class Clock {
public:
    virtual ~Clock() {}

    static const int DATETIME_SIZE = 14;

    // now must have at least DATETIME_SIZE+1 bytes.
    virtual void getDateTimestamp(char* now) = 0;

    // Returns the number of microseconds since the epoch.
    virtual int64_t getMicroseconds() = 0;
};

// Uses gettimeofday.
class SystemClock : public Clock {
public:
    virtual void getDateTimestamp(char* now);
    virtual int64_t getMicroseconds();
};

#endif
