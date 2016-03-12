#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <time.h>

/**
 *
 * Compile command : gcc -std=gnu99 hamming_weight.c
 *
 * This is a toy program that tries to compare the performance
 * of two ways of population count (i.e. counting the number
 * of 1's in a stream of bytes), also known as Hamming Weight.
 *
 * The first method uses an instruction to calculate the number of 1s
 * for 32 bits. The second method uses some math tricks.
 *
 * @author Maithem
 *
 * Below is a test run on my local machine. Note that 3294967295
 * corresponds to a binary stream of size 13.17 gigabytes, which
 * means that method 2 was processing at a rate of 6.14 Gb/sec.
 * It is possible to gain more dramatic speed ups by using uint64_t
 * type for counting, this would easily double the throughput, but the
 * intention of this exercise was to compare the relative performance
 * of the different methods, not maximizing throughput.
 *  
 * (gdb) run 3294967295
 * Starting program: /home/m/interesting-experiments/a.out 3294967295
 * Method 1 (popcnt), time spent 2.298996 
 * Method 2 (Math magic), time spent 2.145960 
 * Method 3 (lookup Count), time spent 5.476081 
 * Number of 1's is : 1179829828
 * 
 */


// An automatically generated bit count array where
// BitCountTable[i] = popcount(i)
static unsigned char BitCountTable[] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 
    2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 
    2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 
    4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 
    3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 
    4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 
    3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 
    4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 
    3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 
    5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 
    4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
    };

void printArray(unsigned int *arr, unsigned int len) {
    for (int x = 0; x < len; x++) {
        printf("%u, ", arr[x]);
    }
    printf("\n");
}

bool checkInstruction() {
    if (__builtin_cpu_supports("popcnt") == 0) {
        return false;
    }
    return true;
}

unsigned int generateRandom32Bit() {
    unsigned int x = rand() & 0xff;
    x |= (rand() & 0xff) << 8;
    x |= (rand() & 0xff) << 16;
    x |= (rand() & 0xff) << 24;
    return x;
}

unsigned int *generateArray(unsigned int len) {
    unsigned int *arr = malloc(sizeof(int) * len);
    for (unsigned int x = 0; x < len; x++) {
        arr[x] = generateRandom32Bit();
    }
    return arr;
}

unsigned int getPop(unsigned int n) {
    unsigned int count;
    asm("popcnt %1,%0" : "=r"(count) : "rm"(n) : "cc");
    return count;
}

unsigned int fastCount(unsigned int *arr, unsigned int len) {
    unsigned int total = 0;
    for (unsigned int x = 0 ; x < len; x++) {
        total += getPop(arr[x]);
    }
    return total;
}

unsigned int lookupCount(unsigned int *arr, unsigned int len) {
    unsigned int total = 0;
    for (unsigned int x = 0; x < len; x++) {
        // extracting each byte from the 32-bit number
        total += BitCountTable[(arr[x] >> (8*0)) & 0xff];
        total += BitCountTable[(arr[x] >> (8*1)) & 0xff];
        total += BitCountTable[(arr[x] >> (8*2)) & 0xff];
        total += BitCountTable[(arr[x] >> (8*3)) & 0xff];
    }
    return total;
}

/**
 * This function was copied from stackoverflow
 * http://stackoverflow.com/questions/109023/how-to-count-the-number-of-set-bits-in-a-32-bit-integer
 */
unsigned int magicGetPop(unsigned int i) {
    i = i - ((i >> 1) & 0x55555555);
    i = (i & 0x33333333) + ((i >> 2) & 0x33333333);
    return (((i + (i >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24;
}

unsigned int magicCount(unsigned int *arr, unsigned int len) {
    unsigned int total = 0;
    for (unsigned int x = 0 ; x < len; x++) {
        total += magicGetPop(arr[x]);
    }
    return total;
}

int main(int argc, char *argv[]) {
    if (!checkInstruction()) {
        printf("Your cpu doesn't support the popcnt instruction \n");
        return -1;
    }
    
    if (argc < 2) {
        printf("Please specify the stream len!\n");
        return -1;
    }
    
    unsigned int len = strtol(argv[1], NULL, 0);
    
    srand(time(NULL));
    
    // Generate random stream
    unsigned int *stream = generateArray(len);
    
    // Start timing first method
    clock_t begin, end;
    double time_spent;
    
    begin = clock();
    unsigned int pop_count1 = fastCount(stream, len);
    end = clock();
    time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
    printf("Method 1 (popcnt), time spent %lf \n", time_spent);
    
    // Start timing second method
    begin = clock();
    unsigned int pop_count2 = magicCount(stream, len);
    end = clock();
    time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
    printf("Method 2 (Math magic), time spent %lf \n", time_spent);
    
    // Start timing third method
    begin = clock();
    unsigned int pop_count3 = lookupCount(stream, len);
    end = clock();
    time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
    printf("Method 3 (lookup Count), time spent %lf \n", time_spent);
    
    printf("Number of 1's is : %u\n", pop_count1);
    
    if (pop_count1 != pop_count2 ||
        pop_count1 != pop_count3) {
        printf("Something bad happened: methods returned"
               " different pop counts!\n");
        return -1;
    }
    
  return 0;
}
