#include "store/apps/tpcc/randomgenerator.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>

#include "store/apps/tpcc/assert.h"

namespace tpcc {

NURandC NURandC::makeRandom(RandomGenerator* generator) {
    NURandC c;
    c.c_last_ = generator->number(0, 255);
    c.c_id_ = generator->number(0, 1023);
    c.ol_i_id_ = generator->number(0, 8191);
    return c;
}

// Returns true if the C-Run value is valid. See TPC-C 2.1.6.1 (page 20).
static bool validCRun(int cRun, int cLoad) {
    int cDelta = abs(cRun - cLoad);
    return 65 <= cDelta && cDelta <= 119 && cDelta != 96 && cDelta != 112;
}

NURandC NURandC::makeRandomForRun(RandomGenerator* generator, const NURandC& c_load) {
    NURandC c = makeRandom(generator);

    while (!validCRun(c.c_last_, c_load.c_last_)) {
        c.c_last_ = generator->number(0, 255);
    }
    ASSERT(validCRun(c.c_last_, c_load.c_last_));

    return c;
}

int RandomGenerator::numberExcluding(int lower, int upper, int excluding) {
    ASSERT(lower < upper);
    ASSERT(lower <= excluding && excluding <= upper);

    // Generate 1 less number than the range
    int num = number(lower, upper-1);

    // Adjust the numbers to remove excluding
    if (num >= excluding) {
        num += 1;
    }
    ASSERT(lower <= num && num <= upper && num != excluding);
    return num;
}

static void generateString(RandomGenerator* generator, char* s, int lower_length, int upper_length,
        char base_character, int num_characters) {
    int length = generator->number(lower_length, upper_length);
    for (int i = 0; i < length; ++i) {
        s[i] = static_cast<char>(base_character + generator->number(0, num_characters-1));
    }
    s[length] = '\0';
}

void RandomGenerator::astring(char* s, int lower_length, int upper_length) {
    generateString(this, s, lower_length, upper_length, 'a', 26);
}

void RandomGenerator::nstring(char* s, int lower_length, int upper_length) {
    generateString(this, s, lower_length, upper_length, '0', 10);
}

void RandomGenerator::lastName(char* c_last, int max_cid) {
    makeLastName(NURand(255, 0, std::min(999, max_cid-1)), c_last);
}

float RandomGenerator::fixedPoint(int digits, float lower, float upper) {
    int multiplier = 1;
    for (int i = 0; i < digits; ++i) {
        multiplier *= 10;
    }

    int int_lower = static_cast<int>(lower * static_cast<double>(multiplier) + 0.5);
    int int_upper = static_cast<int>(upper * static_cast<double>(multiplier) + 0.5);
    return (float) number(int_lower, int_upper) / (float) multiplier;
}

int RandomGenerator::NURand(int A, int x, int y) {
    int C = 0;
    switch(A) {
        case 255:
            C = c_values_.c_last_;
            break;
        case 1023:
            C = c_values_.c_id_;
            break;
        case 8191:
            C = c_values_.ol_i_id_;
            break;
        default:
            fprintf(stderr, "Error: NURand: A = %d not supported\n", A);
            exit(1);
    }
    return (((number(0, A) | number(x, y)) + C) % (y - x + 1)) + x;
}

int* RandomGenerator::makePermutation(int lower, int upper) {
    // initialize with consecutive values
    int* array = new int[upper - lower + 1];
    for (int i = 0; i <= upper - lower; ++i) {
        array[i] = lower + i;
    }

    for (int i = 0; i < upper - lower; ++i) {
        // choose a value to go into this position, including this position
        int index = number(i, upper - lower);
        int temp = array[i];
        array[i] = array[index];
        array[index] = temp;
    }

    return array;
}

// Defined by TPC-C 4.3.2.3.
void makeLastName(int num, char* name) {
    static const char* const SYLLABLES[] = {
        "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING", };
    static const int LENGTHS[] = { 3, 5, 4, 3, 4, 3, 4, 5, 5, 4, };

    ASSERT(0 <= num && num <= 999);
    int indicies[] = { num/100, (num/10)%10, num%10 };

    int offset = 0;
    for (int i = 0; i < sizeof(indicies)/sizeof(*indicies); ++i) {
        ASSERT(strlen(SYLLABLES[indicies[i]]) == LENGTHS[indicies[i]]);
        memcpy(name + offset, SYLLABLES[indicies[i]], LENGTHS[indicies[i]]);
        offset += LENGTHS[indicies[i]];
    }
    name[offset] = '\0';
}

RealRandomGenerator::RealRandomGenerator() {
#ifdef HAVE_RANDOM_R
    // Set the random state to zeros. glibc will attempt to access the old state if not NULL.
    memset(&state, 0, sizeof(state));
    int result = initstate_r(static_cast<unsigned int>(time(NULL)), state_array,
            sizeof(state_array), &state);
    ASSERT(result == 0);
#else
    seed(time(NULL));
#endif
}

int RealRandomGenerator::number(int lower, int upper) {
    int rand_int;
#ifdef HAVE_RANDOM_R
    int error = random_r(&state, &rand_int);
    ASSERT(error == 0);
#else
    rand_int = nrand48(state);
#endif
    ASSERT(0 <= rand_int && rand_int <= RAND_MAX);

    // Select a number in [0, range_size-1]
    int range_size = upper - lower + 1;
    rand_int %= range_size;
    ASSERT(0 <= rand_int && rand_int < range_size);

    // Shift the range to [lower, upper]
    rand_int += lower;
    ASSERT(lower <= rand_int && rand_int <= upper);
    return rand_int;
}

void RealRandomGenerator::seed(unsigned int seed) {
#ifdef HAVE_RANDOM_R
    int error = srandom_r(seed, &state);
    ASSERT(error == 0);
#else
    memcpy(state, &seed, std::min(sizeof(seed), sizeof(state)));
#endif
}

}  // namespace tpcc
