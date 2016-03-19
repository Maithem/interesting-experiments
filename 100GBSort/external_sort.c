#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "thpool.h"
#include "shared.h"

pthread_mutex_t lock;

void *sort_file(void *param) {
    const char *file_name = (char *) param;
    FILE *file = fopen(file_name ,"r+");
    
    size_t read;
    int headerLen = 3;
    int64_t header[headerLen];
    
    read = fread(header, sizeof(int64_t), headerLen, file);
    assert(read == headerLen);
    int64_t rangeLen = header[1] - header[0] + 1;
    
    // Sort each file individually in-memory and write back
    // the result to the same file
    int64_t *dataBuff = malloc(rangeLen * sizeof(int64_t));
    read = fread(dataBuff, sizeof(int64_t), rangeLen, file);
    assert(read == rangeLen);
    
    fseek(file, sizeof(int64_t) * headerLen, SEEK_SET);
    
    qsort(dataBuff, rangeLen, sizeof(int64_t), cmp);
    
    size_t written = fwrite(dataBuff, sizeof(int64_t), rangeLen, file);
    assert(written == rangeLen);
    fclose(file);
    free(dataBuff);
}

int main(int argc, char *argv[]) {
    
    const char *dir = argv[1];
    
    // Initialize stack and threadpool
    Stack *stack = malloc(sizeof(Stack));
    stack->head = NULL;
    stack->size = 0;

    threadpool thpool = thpool_init(16);
    
    FilesDirectory *files = read_dir(dir);
    
    for (int x = 0; x < files->len; x++) {
        char *file_name = files->file_paths[x];
        thpool_add_work(thpool, sort_file, file_name);
    }
    
    thpool_wait(thpool);
    printf("Done waiting....\n");

    return 0;
}