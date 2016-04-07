#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <inttypes.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <assert.h>

#include "shared.h"
#include "thpool.h"

pthread_mutex_t iolock;
pthread_mutex_t stacklock;

void timestamp() {
    time_t ltime;
    ltime=time(NULL);
    printf("%s",asctime( localtime(&ltime) ) );
}

typedef struct _alloc{
    void *ptr;
    int line;
} Alloc;

Alloc mem[1000];
unsigned int deleted = 0;
unsigned int allocPtr = 0;

void* my_malloc(size_t size, const char *file, int line, const char *func)
{

    void *p = malloc(size);
    mem[allocPtr].line = line;
    mem[allocPtr].ptr = p;
    allocPtr++;

    return p;
}

void* my_malloc2(size_t size,size_t size2, const char *file, int line, const char *func)
{

    void *p = aligned_alloc(size, size2);
    mem[allocPtr].line = line;
    mem[allocPtr].ptr = p;
    allocPtr++;

    return p;
}

void* my_free(void *ptr, const char *file, int line, const char *func)
{

    free(ptr);
    for (int x = 0; x < allocPtr; x++){
        if(ptr == mem[x].ptr){
            mem[x].ptr = NULL;
        }
    }
    deleted++;
}

#define malloc(X) my_malloc( X, __FILE__, __LINE__, __FUNCTION__)
#define free(X) my_free( X, __FILE__, __LINE__, __FUNCTION__)
#define aligned_alloc(X,Y) my_malloc2(X, Y,__FILE__, __LINE__, __FUNCTION__)

unsigned int writes = 0;

unsigned int align = 4;

typedef struct _req {
    unsigned int len;
    unsigned int a;
    unsigned int b;
    int64_t **buffers;
} Req;

void freeMergeList(MergeList *list) {
    for (int x = 0; x < list->len; x++) {
        for (int y = 0; y < list->lists[x]->len; y++) {
            free(list->lists[x]->file_paths[y]);
        }
        free(list->lists[x]);
    }
    free(list);
}

void *sort(void *param) {
    Req *r = param;
    for (int x = r->a; x <= r->b; x++) {
       qsort(r->buffers[x], r->len, sizeof(int64_t), cmp);
       printf("[%d]sorting\n", x);
    }
}

void readBuff(int64_t *buff, FILE *file) {
    pthread_mutex_lock(&iolock);
    
    size_t read;
    int headerLen = 3;
    int64_t header[headerLen];
    // Skip header information
    read = fread(header, sizeof(int64_t), headerLen, file);
    assert(read == headerLen);
    int64_t rangeLen = header[1] - header[0] + 1;

    read = fread(buff, sizeof(int64_t), rangeLen, file);
    assert(read == rangeLen);
    
    pthread_mutex_unlock(&iolock);
}

void write_file(char *file_path, int64_t *buff, unsigned int len) {
    pthread_mutex_lock(&iolock);
    
    FILE *chunk = fopen(file_path ,"w+");
    size_t read;
    int headerLen = 3;
    int64_t header[headerLen];

    header[1] = len - 1;
    header[0] = 0;
    header[2] = len;
    
    size_t written = fwrite(header, sizeof(int64_t), headerLen, chunk);
    assert(written == headerLen);
    writes++;
    written = fwrite(buff, sizeof(int64_t), len, chunk);
    assert(written == len);
    
    fclose(chunk);
    
    pthread_mutex_unlock(&iolock);
}

void *merge(void *param) {
    MergeList *list = param;

    // Allocate a file list for the merged result
    FilesList *temp = malloc(sizeof(FilesList));
    temp->len = 0;
    for (int x = 0; x < list->len; x++) {
        temp->len += list->lists[x]->len;
    }
    temp->file_paths = malloc(sizeof(char*) * temp->len);

    // Number of elements to merge
    int64_t mergeSize = temp->len * list->width;

    int *indicies = aligned_alloc(align, sizeof(int) * list->len);
    int *file_nums = aligned_alloc(align, sizeof(int) * list->len);
    int64_t **streamBuffs = aligned_alloc(align, sizeof(int64_t *) * list->len);

    // Initialize stream buffers
    // TODO(Maithem) implement double buffering
    for (int x = 0; x < list->len; x++) {
        indicies[x] = list->width;
        file_nums[x] = 0;
        streamBuffs[x] = aligned_alloc(align, sizeof(int64_t) * list->width);
    }

    int64_t *merge_buff = aligned_alloc(align, sizeof(int64_t) * list->width);

    unsigned int merge_ind = 0;
    unsigned int merge_chunk = 0;

    printf("merging %" PRId64 "\n", mergeSize);
    
    while(mergeSize--) {
        // Load stream buffers
        for (int x = 0; x < list->len; x++) {
            if (indicies[x] == -1) {
                // Reached end of buffer on stream, ignore
                continue;
            }
            else if(indicies[x] >= list->width && file_nums[x] < list->lists[x]->len) {
                FILE *file = fopen(list->lists[x]->file_paths[file_nums[x]], "r");
                // read file into buffer
                readBuff(streamBuffs[x], file);
                fclose(file);
                int ret = remove(list->lists[x]->file_paths[file_nums[x]]);
                if (ret != 0) {
                    printf("couldnt delete file %s\n", list->lists[x]->file_paths[file_nums[x]]);
                }
                // Rewind stream buffer index and increament the file index
                indicies[x] = 0;
                file_nums[x]++;
            } else if (indicies[x] >= list->width && file_nums[x] >= list->lists[x]->len) {
                indicies[x] = -1;
            }
        }

        // Select min via linear scan, as long as k is relatively small, then we
        // don't need a faster method (i.e. min-heap)
        int64_t min_val = -1;
        int64_t min_index = -1;

        for (int x = 0; x < list->len; x++) {
            if (indicies[x] != -1) {
                if (min_index == -1) {
                    // Initialize min, will only be hit once, so it won't be a
                    // problem for the cpu's branch predictor
                    min_val = streamBuffs[x][indicies[x]];
                    min_index = x;
                }
                else if(streamBuffs[x][indicies[x]] < min_val) {
                    min_val = streamBuffs[x][indicies[x]];
                    min_index = x;
                }
            }
        }

        // Move index on min stream and merge buffer
        indicies[min_index]++;
        merge_buff[merge_ind] = min_val;
        merge_ind++;

        if (merge_ind == list->width) {
            char *filePath = malloc(sizeof(char) * 120);
            sprintf(filePath, "%s/s-%u-%u.bin", list->dir, list->seq, merge_chunk);

            // write buffer to file
            write_file(filePath, merge_buff, list->width);

            temp->file_paths[merge_chunk] = filePath;
            merge_chunk++;

            // Set a new buffer to be used, wait for buffers
            // to free up, if no buffers are available
            merge_ind = 0;
        }
    }

    for (int x = 0; x < list->len; x++) {
        free(streamBuffs[x]);
    }
    free(streamBuffs);
    free(indicies);
    free(file_nums);

    freeMergeList(list);
    //printf("Merge seq %u total %" PRId64 "\n", list->seq, total);
    push(list->stack, temp, &stacklock);
}

int main(int argc, char *argv[]) {
    const int threads = atoi(argv[1]);
    char *dir = argv[2];
    unsigned int k = 32;
    
    unsigned int streamLen = 1250000;
    unsigned int numBuffers = 200;
    unsigned int headerLen = 3;
    unsigned int firstPassThreadNum = 8;
    
    threadpool thpool = thpool_init(firstPassThreadNum);

    FilesList *files = read_dir(dir);
    
    int64_t *buffers[numBuffers];
    FILE *filefds[numBuffers];
    
    for (int x = 0; x < numBuffers; x++){
        buffers[x] = aligned_alloc(4, sizeof(int64_t) * streamLen);
    }
    
    unsigned int buff_ind = 0;
    
    for (int x = 0; x < files->len; x++) {
        filefds[buff_ind] = fopen(files->file_paths[x] ,"r+");
        readBuff(buffers[buff_ind], filefds[buff_ind]);
        buff_ind++;
        // All buffers are filled, or all data has been read.
        // sort and persist to files.
        if (buff_ind == numBuffers || x == files->len - 1) {
            unsigned int numOfChunks = buff_ind/firstPassThreadNum;
            pthread_t t[firstPassThreadNum];
            Req r[firstPassThreadNum];
            // TODO(Maithem) This will only work for chunks of multiple
            // of firstPassThreadNum, make it arbitrary.
            for (int i = 0; i < firstPassThreadNum; i++) {
                r[i].len = streamLen;
                r[i].a = i * numOfChunks;
                
                // Since the number of chunks is not always
                // divisable by the number of workers, the last
                // worker will have slightly more work to do
                if (i == firstPassThreadNum - 1){
                    r[i].b = buff_ind - 1;
                } else {
                    r[i].b = r[i].a + numOfChunks - 1;
                }

                r[i].buffers = buffers;
                thpool_add_work(thpool, sort, &r[i]);
            }

            thpool_wait(thpool);

            // Write sorted buffers back to files
            for(int y = 0; y < buff_ind; y++){
                fseek(filefds[y], sizeof(int64_t) * headerLen, SEEK_SET);
                size_t written = fwrite(buffers[y], sizeof(int64_t), streamLen, filefds[y]);
                assert(written == streamLen);
                fclose(filefds[y]);
            }
            // Reset index
            buff_ind = 0;
        }
    }    

    // Free buffers
    for (int x = 0; x < numBuffers; x++){
        free(buffers[x]);
    }
    
    thpool_destroy(thpool);
    //---------------Code that happens after the first pass---------------//

    // Initialize sorting stack and threadpool
    Stack *sortedStack = malloc(sizeof(Stack));
    sortedStack->head = NULL;
    sortedStack->size = 0;

    thpool = thpool_init(threads);
    
    // Add each sorted file to the sorting stack to be merged
    // into one file list
    for (int x = 0; x < files->len; x++) {
        FilesList *temp = malloc(sizeof(FilesList));
        temp->file_paths = malloc(sizeof(char**));
        temp->file_paths[0] = files->file_paths[x];
        temp->len = 1;
        push(sortedStack, temp, &stacklock);
    }
    
    
    // Start merging all individually sorted files into one
    // sorted file list
    unsigned int seq = 0;

    for(;;) {
        if (sortedStack->size == 1 &&
            thpool_count(thpool) == 0) break;

        // Drain stack
        for(;;) {
            if(sortedStack->size >= k) {
                MergeList *mergeList = malloc(sizeof(MergeList));
                mergeList->lists = malloc(sizeof(FilesList) * k);
                mergeList->len = k;
                for (int x = 0; x < k; x++){
                    mergeList->lists[x] = pop(sortedStack, &stacklock);
                }
                mergeList->seq = ++seq;
                mergeList->width = streamLen;
                mergeList->stack = sortedStack;
                mergeList->dir = dir;
                mergeList->k = k;
                thpool_add_work(thpool, merge, mergeList);
            } else if (sortedStack->size >= 2) {
                MergeList *mergeList = malloc(sizeof(MergeList));
                mergeList->lists = malloc(sizeof(FilesList) * 2);
                mergeList->len = 2;
                mergeList->lists[0] = pop(sortedStack, &stacklock);
                mergeList->lists[1] = pop(sortedStack, &stacklock);
                mergeList->seq = ++seq;
                mergeList->width = streamLen;
                mergeList->stack = sortedStack;
                mergeList->dir = dir;
                mergeList->k = k;
                thpool_add_work(thpool, merge, mergeList);
            } else {
                break;
            }
        }
        // Sleep before we check the sorted stack again
        sleep(1);
    }

    printf("writes   %u \n ", writes);
    
    printf("==========leadked at =========\n");
    unsigned int counter = 0;
    for (int x = 0; x < allocPtr; x++){
        if(mem[x].ptr != NULL) {counter++; printf("line %d\n", mem[x].line);}
    }
    printf("leaked %d\n", counter - deleted);
   return 0;
}
