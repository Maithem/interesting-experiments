#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <inttypes.h>
#include <dirent.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>

#include "shared.h"


/**
 * This program doesn't shuffle individual chunks. In other
 * words, there needs to be more than one chunk to shuffle.
 *
 * An extra shuffle can be added to each file, but not
 * necessary for now.
 *
 **/

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: shuffle <path-to-chunks>");
        return -1;
    }
    
    // Seed generator
    srand(time(NULL));
    
    const char *path = argv[1];
    FilesDirectory *files = read_dir(path);
    // Try to at least shuffle every set of two files
    // TODO(Maithem) this is very slow for a large dataset
    // might need to parallize it later
    //unsigned int passes = nCk(files->len, 2) * 2;
    unsigned int passes = 300;
    
    if (files->len == 1) {
        printf("Can't shuffle a single chunk\n");
        return -1;
    }
    
    for (unsigned int x = 0; x < passes; x++) {
        
        printf("* Shuffle pass %u out of %u...\n", x + 1, passes);
        
        // Select to random files, since we are generating
        // random numbers over a large distribution, a collision
        // is not probable.
        unsigned int first = rand() % files->len;
        unsigned int second = rand() % files->len;
        
        while(first == second) {
            second = rand() % files->len;
        }

        printf("* selected of %u  %u...\n", first, second);
        
        FILE *firstChunk = fopen(files->file_paths[first] ,"r+");
        FILE *secondChunk = fopen(files->file_paths[second] ,"r+");
        
        // Read the header information for both chunks
        
        size_t read;
        int headerLen = 3;
        int64_t header[headerLen];
        
        read = fread(header, sizeof(int64_t), headerLen, firstChunk);
        assert(read == headerLen);
        int64_t rangeLen1 = header[1] - header[0] + 1;
        
        read = fread(header, sizeof(int64_t), headerLen, secondChunk);
        assert(read == headerLen);
        int64_t rangeLen2 = header[1] - header[0] + 1;

        // Chunks should be of the same size, otherwise we need to
        // rewrite the header information
        assert(rangeLen1 == rangeLen2);
        
        // Create a larger array to merge both chunks
        int64_t *merged = malloc(sizeof(int64_t) * (rangeLen1 + rangeLen2));
        
        // Read both chunks into memory
        read = fread(merged, sizeof(int64_t), rangeLen1, firstChunk);
        assert(read == rangeLen1);
        read = fread(&merged[rangeLen1], sizeof(int64_t), rangeLen2, secondChunk);
        assert(read == rangeLen2);
        
        // Shuffle data
        shuffle(merged, rangeLen1 + rangeLen2);
        
        // Rewind file pointers to overwrite the data with the new shuffled data
        fseek(firstChunk, sizeof(int64_t) * headerLen, SEEK_SET);
        fseek(secondChunk, sizeof(int64_t) * headerLen, SEEK_SET);
        
        size_t written = fwrite(merged, sizeof(int64_t), rangeLen1, firstChunk);
        assert(written == rangeLen1);
        written = fwrite(&merged[rangeLen1], sizeof(int64_t), rangeLen2, secondChunk);
        assert(written == rangeLen2);
        
        // Close files and flush buffers
        fclose(firstChunk);
        fclose(secondChunk);
        free(merged);
    }
    
    return 0;
}