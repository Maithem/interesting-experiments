#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <inttypes.h>
#include <dirent.h>
#include <unistd.h>
#include <errno.h>

#include "shared.h"

/**
 * This program verifies the integrity of the dataset
 * generated by generate_data.c. Since generate_data.c
 * will generate integers from 0 to n - 1, then the sum
 * of all integers should equal to f(n) = (n -1) ((n -1) + 1) / 2.
 * Verify_data will sum up all the integers in the
 * dataset and check if it is equal to f(n). If f(n) equals
 * the total sum and the total number of integers counted is
 * equal to n, then the dataset must contains all integers
 * from 0 to n - 1.
 *
 * @author Maithem
 *
 **/

struct chunkStat {
    int64_t rangeLen;
    int64_t totalSum;
};

typedef struct chunkStat stat;
typedef unsigned __int128 int128_t;

void computeChunkStat(stat *st, FILE *chunkFile) {
    
    size_t read;
    int headerLen = 3;
    int64_t header[headerLen];
    
    // Read file header and set the stat's range length
    read = fread(header, sizeof(int64_t), headerLen, chunkFile);
    assert(read == headerLen);
    int64_t rangeLen = header[1] - header[0] + 1;
    st->rangeLen = rangeLen;
    
    // Start reading the integers and compute total sum
    int64_t *dataBuff = malloc(rangeLen * sizeof(int64_t));
    read = fread(dataBuff, sizeof(int64_t), rangeLen, chunkFile);
    assert(read == rangeLen);
    
    for (int64_t x = 0; x < rangeLen; x++) {
        st->totalSum += dataBuff[x];
    }
    
    free(dataBuff);
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: verify <dir>\n");
        return -1;
    }
    
    const char *chunksDir = argv[1];
    
    int128_t totalSum = 0;
    int128_t numOfInts = 0;

    FilesList *files = read_dir(chunksDir);
    
    if (files == NULL) {
        printf("Couldn't read directory %s\n", chunksDir);
        return -1;
    }
    
    for (int x = 0; x < files->len; x++) {
    
        // Open chunk file in read-only mode
        FILE *file = fopen(files->file_paths[x], "r");
        
        if (file == NULL) {
            printf("Can't open file %s\n", files->file_paths[x]);
            return -1;
        }
        
        stat chunkStat;
        chunkStat.rangeLen = 0;
        chunkStat.totalSum = 0;
        
        computeChunkStat(&chunkStat, file);
        
        // Aggregate range lengths and total sums
        numOfInts += chunkStat.rangeLen;
        totalSum += chunkStat.totalSum;

        //dont forget to close file
        fclose(file);
    }
    

    // Compute the sum of the range sequence and check if it
    // equals the aggregate chunks sums
    int128_t sequenceSum = 0;
    // since the sequence is 0 to n - 1, we need to
    // subtract 1 from the global range length. Otherwise
    // it would compute the total sequence sum for 1 to n
    int128_t n = numOfInts - 1;
    sequenceSum = (n * (n + 1)) / 2;
    
    assert(totalSum == sequenceSum);
    printf("Great! check-sum passed!\n");

    return 0;
}