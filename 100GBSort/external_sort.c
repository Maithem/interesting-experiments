#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <assert.h>
#include <stdint.h>
#include <inttypes.h>
#include<pthread.h>

#include "thpool.h"
#include "shared.h"

#include <time.h>
void timestamp()
{
    time_t ltime; /* calendar time */
    ltime=time(NULL); /* get current cal time */
    printf("%s",asctime( localtime(&ltime) ) );
}
int64_t iter = 0;
int sleepTime = 10000;
pthread_mutex_t lock;
pthread_mutex_t writeBuffsLock;
int64_t width;

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

void write_file(char *file_path, int64_t *buff, unsigned int len) {
    FILE *chunk = fopen(file_path ,"w+");
    size_t read;
    int headerLen = 3;
    int64_t header[headerLen];

    header[1] = len - 1;
    header[0] = 0;
    header[2] = len;
    
    size_t written = fwrite(header, sizeof(int64_t), headerLen, chunk);
    assert(written == headerLen);
    written = fwrite(buff, sizeof(int64_t), len, chunk);
    assert(written == len);
    
    fclose(chunk);
}


void freeMergeList(MergeList *list) {
    for (int x = 0; x < list->len; x++) {
        for (int y = 0; y < list->lists[x]->len; y++) {
            free(list->lists[x]->file_paths[y]);
        }
        free(list->lists[x]);
    }
    free(list);
}

void *write_worker(void *params) {
    WriteReq *req = params;
    int64_t *buff = req->buff;
    write_file(req->name, buff, req->len);
    sem_wait(req->sem);
    free(req);
    // Return buffer to be reused by merge
    push(req->freeStack, buff, &lock);
}

int64_t *get_write_buff(Stack *writeBuffs){
    // Warning waiting till the stack size is greater than zero
    // then poping an element is not a thread-safe. The right way
    // of doing this is pop would be a blocking call with a timeout.
    while(writeBuffs->size == 0) {usleep(sleepTime); printf("waiting\n");}
    return pop(writeBuffs, &writeBuffsLock);
}

void *merge(void *param) {
    MergeList *list = param;
    
    Stack *writeBuffs = list->buffStack;
    threadpool thpool = list->thpool;
    sem_t *sem = malloc(sizeof(sem_t));
    sem_init(sem, 0, 0);

    FilesList *temp = malloc(sizeof(FilesList));
    temp->len = 0;
    for (int x = 0; x < list->len; x++) {
        temp->len += list->lists[x]->len;
    }
    
    temp->file_paths = malloc(sizeof(char*) * temp->len);

    int64_t mergeSize = temp->len * width;
    
    int *indicies = malloc(sizeof(int) * list->len);
    int *file_nums = malloc(sizeof(int) * list->len);
    int64_t **streamBuffs = malloc(sizeof(int64_t *) * list->len);

    // Initialize stream buffers
    // TODO(Maithem) implement double buffering
    for (int x = 0; x < list->len; x++) {
        indicies[x] = width;
        file_nums[x] = 0;
        streamBuffs[x] = malloc(sizeof(int64_t) * width);
    }

    int64_t *merge_buff = get_write_buff(writeBuffs);

    int64_t merge_ind = 0;
    printf("%u-merge\n", list->k);
    unsigned int merge_chunk = 0;

    while(mergeSize--) {
        // Load stream buffers
        for (int x = 0; x < list->len; x++) {
            if (indicies[x] == -1) {
                // Reached end of buffer on stream, ignore
                continue;
            }
            else if(indicies[x] >= width && file_nums[x] < list->lists[x]->len) {
                FILE *file = fopen(list->lists[x]->file_paths[file_nums[x]], "r");
                // Skip header
                fseek(file, sizeof(int64_t)*3, SEEK_SET);
                // Load the data into it's stream buffer and delete the file
                size_t read = fread(streamBuffs[x], sizeof(int64_t), width, file);
                assert(read == width);
                fclose(file);
                int ret = remove(list->lists[x]->file_paths[file_nums[x]]);
                if (ret != 0) {
                    printf("couldnt delete file %s\n", list->lists[x]->file_paths[file_nums[x]]);
                }
                // Rewind stream buffer index and increament the file index
                indicies[x] = 0;
                file_nums[x]++;
            } else if (indicies[x] >= width && file_nums[x] >= list->lists[x]->len) {
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

        if (merge_ind == width) {
            char *filePath = malloc(sizeof(char) * 120);
            sprintf(filePath, "%s/s-%u-%u.bin", list->dir, list->seq, merge_chunk);

            WriteReq *req = malloc(sizeof(WriteReq));
            req->name = filePath;
            req->buff = merge_buff;
            req->len = width;
            req->freeStack = writeBuffs;
            req->sem = sem;

            sem_post(sem);
            thpool_add_work(thpool, write_worker, req);

            temp->file_paths[merge_chunk] = filePath;
            merge_chunk++;

            // Set a new buffer to be used, wait for buffers
            // to free up, if no buffers are available
            merge_ind = 0;
            merge_buff = get_write_buff(writeBuffs);
        }
    }

    push(writeBuffs, merge_buff, &writeBuffsLock);
    
    // Wait until all write buffers from this merge step to be
    // flushed
    while(1) {
        int vlap;
        sem_getvalue(sem, &vlap);
        //printf("%d\n", vlap);
        if (vlap == 0) break;
        
        usleep(sleepTime);
    }

    for (int x = 0; x < list->len; x++) {
        free(streamBuffs[x]);
    }
    free(streamBuffs);
    free(indicies);
    free(file_nums);
    free(sem);
    freeMergeList(list);
    //printf("Merge seq %u total %" PRId64 "\n", list->seq, total);
    push(list->stack, temp, &lock);
}

typedef struct _req {
    unsigned int len;
    unsigned int a;
    unsigned int b;
    int64_t **buffers;
} Req;

void *sort(void *param) {
    Req *r = param;
    for (int x = r->a; x < r->b; x++) {
       qsort(r->buffers[x], r->len, sizeof(int64_t), cmp);
       printf("sorting\n");
    }
}

int main(int argc, char *argv[]) {
    const int threads = atoi(argv[1]);
    char *dir = argv[2];
    unsigned int k = 10;
    unsigned int initial = 100;
    int headerLen = 3;

    // Initialize stack and threadpool
    Stack *sortedStack = malloc(sizeof(Stack));
    sortedStack->head = NULL;
    sortedStack->size = 0;

    threadpool thpool = thpool_init(threads);
    FilesList *files = read_dir(dir);
    
    size_t read;
    int64_t header[headerLen];
    FILE *file = fopen(files->file_paths[0], "r");
    read = fread(header, sizeof(int64_t), headerLen, file);
    assert(read == headerLen);
    fclose(file);
    
    width = (header[1] - header[0] + 1);
    
    printf("width %" PRId64"\n", width);
    
    timestamp();
    
    // allocate first-pass buffers
    int64_t *buffs[initial];
    FILE *filesFds[initial];
    unsigned int buffs_ind = 0;
    for (int x = 0; x < initial; x++){
        buffs[x] = malloc(sizeof(int64_t) * width);
    }

    // Start reading as much data as we can fit into the buffers
    // then sort them in-memory, then write all the buffers to
    // files
    printf("h4asd - "); timestamp();
    printf("h1 - "); timestamp();
    for (int x = 0; x < files->len; x++) {
        char *file_name = files->file_paths[x];
        FILE *file = fopen(file_name ,"r+");
        filesFds[buffs_ind] = file;
        //Skip header information
        fseek(file, sizeof(int64_t) * headerLen, SEEK_SET);
        size_t read = fread(buffs[buffs_ind], sizeof(int64_t), width, file);
        assert(read == width);
        
        buffs_ind++;
        
        if (buffs_ind == initial || x == files->len) {
            for (int ig = 0; ig < buffs_ind; ig++) {
                qsort(buffs[ig], width, sizeof(int64_t), cmp);
            }

            for(int y = 0; y < buffs_ind; y++){
                fseek(filesFds[y], sizeof(int64_t) * headerLen, SEEK_SET);
                size_t written = fwrite(buffs[y], sizeof(int64_t), width, filesFds[y]);
                assert(written == width);
                fclose(filesFds[y]);
            }
            printf("h4 - "); timestamp();
            // Reset the buffers index to be re-used
            buffs_ind = 0;
        }
    }
    printf("h4asd - "); timestamp();
    for (int x = 0; x < initial; x++){
        free(buffs[x]);
    }

    printf("First pass done!\n");
    
    // Setup the write buffer
    threadpool writeThpool = thpool_init(1);
    Stack *writeBuffs = malloc(sizeof(Stack));
    writeBuffs->head = NULL;
    writeBuffs->size = 0;
    
    unsigned int numBuffs = k * 10 * threads;
    
    for(int x = 0; x < numBuffs; x++) {
        int64_t *buff = malloc(sizeof(int64_t) * width);
        push(writeBuffs, buff, &writeBuffsLock);
    }

    // Add each chunk to the sorted stack
    for (int x = 0; x < files->len; x++) {
        FilesList *temp = malloc(sizeof(FilesList));
        temp->file_paths = malloc(sizeof(char**));
        temp->file_paths[0] = files->file_paths[x];
        temp->len = 1;
        push(sortedStack, temp, &lock);
    }

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
                    mergeList->lists[x] = pop(sortedStack, &lock);
                }
                mergeList->seq = ++seq;
                mergeList->stack = sortedStack;
                mergeList->thpool = writeThpool;
                mergeList->buffStack = writeBuffs;
                mergeList->dir = dir;
                mergeList->k = k;
                thpool_add_work(thpool, merge, mergeList);
            } else if (sortedStack->size >= 2) {
                MergeList *mergeList = malloc(sizeof(MergeList));
                mergeList->lists = malloc(sizeof(FilesList) * 2);
                mergeList->len = 2;
                mergeList->lists[0] = pop(sortedStack, &lock);
                mergeList->lists[1] = pop(sortedStack, &lock);
                mergeList->seq = ++seq;
                mergeList->stack = sortedStack;
                mergeList->thpool = writeThpool;
                mergeList->buffStack = writeBuffs;
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
    
    // Free write buffers
    for (int x = 0; x < numBuffs; x++) {
        free(pop(writeBuffs, &writeBuffsLock));
    }
    free(writeBuffs);

    return 0;
}