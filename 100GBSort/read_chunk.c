#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <inttypes.h>

/**
 * read_chunk.c is a debugging tool that will print
 * the contents of *.bin files generated by generate_data.c.
 *
 * @author Maithem
 *
 **/

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: read <file-path>\n");
        return -1;
    }
    
    const char *filePath = argv[1];
    FILE *file = fopen(filePath, "r");
    
    size_t headerLen = 3;
    int64_t header[headerLen];
    // Read chunk header
    size_t read = fread(header, sizeof(int64_t), headerLen, file);
    assert(read == headerLen);
    
    // print chunk contents
    printf("**Contents of chunk %s\n", filePath);
    printf("Start Range : %" PRId64 "\n", header[0]);
    printf("Start Range : %" PRId64 "\n", header[1]);
    printf("Width : %" PRId64 "\n", header[2]);
    
    int64_t chunkLen = header[1] - header[0] + 1;
    int64_t *dataBuff = malloc(sizeof(int64_t) * chunkLen);
    
    fread(dataBuff, sizeof(int64_t), chunkLen, file);
    
    for (int64_t x = 0; x < chunkLen; x++) {
        printf("[%" PRId64 "] %" PRId64 "\n", x, dataBuff[x]);
    }
    
    free(dataBuff);
    fclose(file);
    
}