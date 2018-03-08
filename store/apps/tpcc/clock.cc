#include "store/apps/tpcc/clock.h"

#include <sys/time.h>

#include <cstdio>
#include <ctime>

#include "store/apps/tpcc/assert.h"

// Fills output with the base-10 ASCII representation of value, using digits digits.
static char* makeInt(char* output, int value, int digits) {
    char* last = output + digits;
    char* next = last;
    for (int i = 0; i < digits; ++i) {
        int digit = value % 10;
        value = value / 10;
        next -= 1;
        *next = static_cast<char>('0' + digit);
    }
    assert(value == 0);
    return last;
}

void SystemClock::getDateTimestamp(char* now) {
    // Get the system time. Convert it to local time
    time_t seconds_since_epoch = time(NULL);
    assert(seconds_since_epoch != -1);

    struct tm local_calendar;
    struct tm* result = localtime_r(&seconds_since_epoch, &local_calendar);
    ASSERT(result == &local_calendar);

    // Format the time
    // strftime is slow: it ends up consulting timezone info
    // snprintf is also slow, since it needs to parse the input string. This is significantly
    // faster, saving ~10% of the run time.
    //~ int bytes = snprintf(now, DATETIME_SIZE+1, "%04d%02d%02d%02d%02d%02d",
            //~ local_calendar.tm_year+1900, local_calendar.tm_mon+1, local_calendar.tm_mday,
            //~ local_calendar.tm_hour, local_calendar.tm_min, local_calendar.tm_sec);
    //~ int bytes = strftime(now, DATETIME_SIZE+1, "%Y%m%d%H%M%S", &broken_down_local_time);
    char* next = makeInt(now, local_calendar.tm_year+1900, 4);
    next = makeInt(next, local_calendar.tm_mon+1, 2);
    next = makeInt(next, local_calendar.tm_mday, 2);
    next = makeInt(next, local_calendar.tm_hour, 2);
    next = makeInt(next, local_calendar.tm_min, 2);
    next = makeInt(next, local_calendar.tm_sec, 2);
    *next = '\0';
    assert(next == now + DATETIME_SIZE);
}

int64_t SystemClock::getMicroseconds() {
    struct timeval time;
    int error = gettimeofday(&time, NULL);
    ASSERT(error == 0);
    int64_t result = time.tv_sec * 1000000;
    result += time.tv_usec;
    return result;
}
