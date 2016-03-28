#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <assert.h>
#include <stdint.h>
#include <inttypes.h>

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
pthread_mutex_t lock;
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

void *merge(void *param) {
    MergeList *list = param;

    FilesList *temp = malloc(sizeof(FilesList));
    temp->len = 0;
    for (int x = 0; x < list->len; x++) {
        temp->len += list->lists[x]->len;
    }

    //printf("Merging %u files \n", temp->len);
    
    temp->file_paths = malloc(sizeof(char*) * temp->len);
    
    //printf("Merging streams:\n");
    //for(int x =0; x < list->a->len; x++){
    //    printf(" %s ", list->a->file_paths[x]);
    //}
    //printf("\n");
    //for(int x =0; x < list->b->len; x++){
    //    printf(" %s ", list->b->file_paths[x]);
    //}
    //printf("\n");

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
    
    int64_t *merge_buff = malloc(sizeof(int64_t) * width * list->k);
    if (merge_buff == NULL) {
        printf("Can't malloc bro\n");
    }
    int64_t merge_ind = 0;
    printf("%u-merge\n", list->k);
    unsigned int merge_chunk = 0;
    timestamp();
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
                    iter++;
                }
                else if(streamBuffs[x][indicies[x]] < min_val) {
                    min_val = streamBuffs[x][indicies[x]];
                    min_index = x;
                    iter++;
                }
            }
        }
        
        // Move index on min stream and merge buffer
        indicies[min_index]++;
        merge_buff[merge_ind] = min_val;
        merge_ind++;

        if (merge_ind == (width * list->k) || mergeSize == 0) {
            unsigned k_widths = 0;
            if (mergeSize == 0) {
                k_widths = (merge_ind + 1) / width;
            } else {
                k_widths = list->k;
            }
            // Write k-files at a time, because if the merge buffer
            // is larger than the write size, contention can happen
            for (int x = 0 ; x < k_widths; x++) {
                char *filePath = malloc(sizeof(char) * 120);
                sprintf(filePath, "%s/s-%u-%u.bin", list->dir, list->seq, merge_chunk);
                write_file(filePath, &merge_buff[x * width], width);
                temp->file_paths[merge_chunk] = filePath;
                merge_chunk++;
            }
            merge_ind = 0;
        }
    }
timestamp();
    for (int x = 0; x < list->len; x++) {
        free(streamBuffs[x]);
    }
    free(streamBuffs);
    free(indicies);
    free(file_nums);
    free(merge_buff);
    freeMergeList(list);
    printf("Iter %" PRId64 "\n", iter);
    //printf("Merge seq %u total %" PRId64 "\n", list->seq, total);
    push(list->stack, temp, &lock);
}

int main(int argc, char *argv[]) {
    const int threads = atoi(argv[1]);
    char *dir = argv[2];
    unsigned int k = 10;

    // Initialize stack and threadpool
    Stack *sortedStack = malloc(sizeof(Stack));
    sortedStack->head = NULL;
    sortedStack->size = 0;

    threadpool thpool = thpool_init(threads);
    FilesList *files = read_dir(dir);
    
    size_t read;
    int headerLen = 3;
    int64_t header[headerLen];
    FILE *file = fopen(files->file_paths[0], "r");
    read = fread(header, sizeof(int64_t), headerLen, file);
    assert(read == headerLen);
    fclose(file);
    
    width = (header[1] - header[0] + 1);
    
    printf("width %" PRId64"\n", width);
    
    timestamp();
    for (int x = 0; x < files->len; x++) {
        char *file_name = files->file_paths[x];
        sort_file(file_name);
    }
    timestamp();

    printf("First pass done!\n");

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
        //printf("size of stack %d\n", sortedStack->size);
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

    return 0;
}