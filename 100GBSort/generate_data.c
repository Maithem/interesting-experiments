#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <stdint.h>
#include <assert.h>
#include <time.h>

/**
 * This program generates a dataset made of
 * of files that contain 64-bit positive integers.
 * The user is able to specify the chunk size
 * (i.e. file) and the number of chunks.
 *
 * Each file will have the following layout:
 * 
 * | Range Start | Range End | Data Width | data[1]
 * | data[2] | ...| data[End - Start + 1] | EOF |
 *
 * Range Start and Range End indicate the file's location
 * within the whole dataset, where the difference between
 * the two values is equal to the number of elements in the
 * file.
 *
 * Data Width is the number of bits of each element of the data.
 *
 * data[i] points to a data element.
 *
 * Range Start, Range End and Data Width are all 64-bit positive
 * integers.
 *
 * 
 **/

#define BytesInMb 1000000
typedef unsigned int u_i;


void create_chunk(int64_t start, int64_t end, const char *dir) {
    char filePath[100];
    sprintf(filePath, "%s/%" PRId64 "-%" PRId64 ".bin", dir, start, end);
    FILE *filePtr = fopen(filePath, "w+");

    if (filePtr == NULL) {
        printf("Error opening file!\n");
        exit(1);
    }
    
    int headerLen = 3;
    int64_t header[headerLen];
    header[0] = start;
    header[1] = end;
    header[2] = sizeof(int64_t);
    
    size_t written = fwrite(header, sizeof(int64_t), headerLen, filePtr);
    assert(written == headerLen);
    // Write data
    
    fclose(filePtr);
}


int comp (const void * elem1, const void * elem2) 
{
    int f = *((int*)elem1);
    int s = *((int*)elem2);
    if (f > s) return  1;
    if (f < s) return -1;
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        printf("Usage: generate <chunk-size> <number-of-chunks> <chunks-path>\n"
               "       [chunk-size should be specified in Megabytes]\n");
        return -1;
    }
    
    u_i chunkSize = strtol(argv[1], NULL, 0);
    u_i numChunks = strtol(argv[2], NULL, 0);
    const char *chunksDir = argv[3];

        
    int64_t rangeWidth = (BytesInMb * chunkSize) / sizeof(int64_t);
    
    for (int x = 0; x < numChunks; x++) {
        int64_t start = x * rangeWidth;
        int64_t end = start + rangeWidth - 1;
        //printf("%"PRId64" %"PRId64"\n", start, end);
        create_chunk(start, end, chunksDir);
    } 
    
    return 0;
}