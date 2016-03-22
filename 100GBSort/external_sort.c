#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
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

void *merge(void *param) {
    MergeList *list = param;
    sleep(3);
    FilesList *temp = malloc(sizeof(FilesList));
    temp->file_paths = malloc(sizeof(char**));
    temp->file_paths[0] = NULL;
    temp->len = 1;
    push(list->stack, temp, &lock);
}

int main(int argc, char *argv[]) {
    const int threads = atoi(argv[1]);
    const char *dir = argv[2];

    // Initialize stack and threadpool
    Stack *sortedStack = malloc(sizeof(Stack));
    sortedStack->head = NULL;
    sortedStack->size = 0;

    threadpool thpool = thpool_init(threads);
    FilesList *files = read_dir(dir);
    
    for (int x = 0; x < files->len; x++) {
        char *file_name = files->file_paths[x];
        thpool_add_work(thpool, sort_file, file_name);
    }

    thpool_wait(thpool);
    printf("First pass done!\n");
    
    // Add each chunk to the sorted stack
    for (int x = 0; x < files->len; x++) {
        FilesList *temp = malloc(sizeof(FilesList));
        temp->file_paths = malloc(sizeof(char**));
        temp->file_paths[0] = files->file_paths[x];
        temp->len = 1;
        push(sortedStack, temp, &lock);
    }
    
    for(;;) {
        printf("size of stack %d\n", sortedStack->size);
        if (sortedStack->size == 1 &&
            thpool_count(thpool) == 0) break;
    
        for(;;) {
            if(sortedStack->size >= 2) {
                FilesList *a = pop(sortedStack, &lock);
                FilesList *b = pop(sortedStack, &lock);
                MergeList *mergeList = malloc(sizeof(MergeList));
                mergeList->a = a;
                mergeList->b = b;
                mergeList->stack = sortedStack;
                thpool_add_work(thpool, merge, mergeList);
            } else {
                break;
            }
        }
        // Sleep before we check the sorted stack again
        sleep(1);
    }

    return 0;
}