#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <stdint.h>
#include <assert.h>

/**
 * This program generates a dataset made of
 * of files that contain 64-bit positive integers.
 * The user is able to specify the chunk size
 * (i.e. file) and the number of chunks. For example,
 * if the chunk size is 1mb and the number of chunks is
 * 2, then this program will create two files (1mb each)
 * for each chunk. Where each file contains the integers
 * its range, sequentially.
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
 * @author Maithem
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
    int64_t dataLen = end - start + 1; 
    int64_t *dataBuff = malloc(dataLen * sizeof(int64_t));
    for (int64_t x = 0; x < dataLen; x++) {
        dataBuff[x] = start + x;
    }
    
    // Write data buffer to file
    written = fwrite(dataBuff, sizeof(int64_t), dataLen, filePtr);
    assert(written == dataLen);
    
    free(dataBuff);
    fclose(filePtr);
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
    // Note: there is no need to concurrently write the files
    // to disk, as no speed up will be achieved. On my machine it took
    // 10 minutes to generate a 10Gb dataset. The write throughput of
    // 156 mb/sec matches my disk's write thoughput benchmark.
    for (int x = 0; x < numChunks; x++) {
        int64_t start = x * rangeWidth;
        int64_t end = start + rangeWidth - 1;
        create_chunk(start, end, chunksDir);
    } 

    return 0;
}
