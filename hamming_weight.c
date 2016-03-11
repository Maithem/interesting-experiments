#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <time.h>

/**
 *
 * Compile command : gcc -std=gnu99 pop.c
 *
 * This is a toy program that tries to compare the performance
 * of two ways of population counting (i.e. counting the number
 * of 1's in a stream of bytes), also known as Hamming Weight.
 *
 * The first method uses an instruction to calculate the number of 1s
 * for 32 bits. The second method uses some math tricks.
 *
 * @author Maithem
 * 
 */

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
    for (int x = 0; x < len; x++) {
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
    for (int x = 0 ; x < len; x++) {
        total += getPop(arr[x]);
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
    for (int x = 0 ; x < len; x++) {
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
    
    printf("Number of 1's is : %u\n", pop_count1);
    
    if (pop_count1 != pop_count2) {
        printf("Something bad happened: methods returned"
               " different pop counts!\n");
        return -1;
    }
    
  return 0;
}
