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


int64_t getTotal(FilesList *a) {
    int64_t range = 0;
    size_t read;
    int headerLen = 3;
    int64_t header[headerLen];
    FILE *file = fopen(a->file_paths[0], "r");
    read = fread(header, sizeof(int64_t), headerLen, file);
    assert(read == headerLen);
    fclose(file);
    
    return (header[1] - header[0] + 1) * a->len;
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
    for(int x = 0; x < list->a->len; x++) {
        free(list->a->file_paths[x]);
    }    
    for(int x = 0; x < list->b->len; x++) {
        free(list->b->file_paths[x]);
    }

    free(list->a);
    free(list->b);
    free(list);
}

void *merge(void *param) {
    MergeList *list = param;
    
    FilesList *temp = malloc(sizeof(FilesList));
    temp->len = list->a->len + list->b->len;
    temp->file_paths = malloc(sizeof(char*) * temp->len);
    
    printf("Merging streams:\n");
    for(int x =0; x < list->a->len; x++){
        printf(" %s ", list->a->file_paths[x]);
    }
    printf("\n");
    for(int x =0; x < list->b->len; x++){
        printf(" %s ", list->b->file_paths[x]);
    }
    printf("\n");
    
    int64_t stream1 = width * (list->a->len);
    int64_t stream2 = width * (list->b->len);
    int64_t total = stream1 + stream2;
    
    int64_t ind1 = 0;
    int64_t ind2 = 0;
    unsigned int file_num1 = 0;
    unsigned int file_num2 = 0;

    ind1 = width;
    ind2 = width;
    
    int64_t *streamBuff1=NULL;
    int64_t *streamBuff2=NULL;
    
    int64_t *merge_buff = malloc(sizeof(int64_t) * width);
    int64_t merge_ind = 0;
    
    unsigned int merge_chunk = 0;
    
    while(total--) {
        if (ind1 == -1) {
          // Do nothing  
        } else if (ind1 >= width && file_num1 < list->a->len) {
            if(streamBuff1 != NULL) free(streamBuff1);
            streamBuff1 = malloc(sizeof(int64_t) * width);
            FILE *file = fopen(list->a->file_paths[file_num1], "r");

            size_t headerLen = 3;
            int64_t header[headerLen];
            // Read chunk header
            size_t read = fread(header, sizeof(int64_t), headerLen, file);
            assert(read == headerLen);
            read = fread(streamBuff1, sizeof(int64_t), width, file);
            assert(read == width);
            fclose(file);
            int ret = remove(list->a->file_paths[file_num1]);
            if (ret != 0) {
                printf("couldnt delete file %s\n", list->a->file_paths[file_num1]);
            }
            ind1 = 0;
            file_num1++;
        } else if(ind1 >= width && file_num1 >= list->a->len){
            ind1 = -1;
            free(streamBuff1);
            streamBuff1 = NULL;
        }

        if (ind2 == -1) {
          // Do nothing  
        } else if (ind2 >= width && file_num2 < list->b->len) {
            if(streamBuff2 != NULL) free(streamBuff2);
            streamBuff2 = malloc(sizeof(int64_t) * width);
            FILE *file = fopen(list->b->file_paths[file_num2], "r");
            size_t headerLen = 3;
            int64_t header[headerLen];
            // Read chunk header
            size_t read = fread(header, sizeof(int64_t), headerLen, file);
            assert(read == headerLen);
            read = fread(streamBuff2, sizeof(int64_t), width, file);
            assert(read == width);
            fclose(file);
            int ret = remove(list->b->file_paths[file_num2]);
            if (ret != 0) {
                printf("couldnt delete file %s\n", list->b->file_paths[file_num2]);
            }
            
            ind2 = 0;
            file_num2++;
        } else if(ind2 >= width && file_num2 >= list->b->len){
            ind2 = -1;
            free(streamBuff2);
            streamBuff2 = NULL;
        }
        
        if (ind1 == -1 && ind2 == -1) {
        } else if(ind1 == -1) {
            merge_buff[merge_ind] = streamBuff2[ind2];
            merge_ind++;
            ind2++;
        } else if(ind2 == -1) {
            merge_buff[merge_ind] = streamBuff1[ind1];
            merge_ind++;
            ind1++;
        } else {
            if (streamBuff1[ind1] < streamBuff2[ind2]) {
                merge_buff[merge_ind] = streamBuff1[ind1];
                ind1++;
            } else {
                merge_buff[merge_ind] = streamBuff2[ind2];
                ind2++;
            }
            merge_ind++;
        }
        
        if(merge_ind == width) {
            // Merged a chunk, need to flush buffer
            // and write file
            char *filePath = malloc(sizeof(char) * 120);
            sprintf(filePath, "%s/s-%u-%u.bin", list->dir, list->seq, merge_chunk);
            write_file(filePath, merge_buff, width);
            
            temp->file_paths[merge_chunk] = filePath;
            merge_chunk++;
            merge_ind = 0;
        }
    }
    
    if (streamBuff1 != NULL) free(streamBuff1);
    if (streamBuff2 != NULL) free(streamBuff2);
    
    free(merge_buff);

    freeMergeList(list);
    
    printf("Merge seq %u total %" PRId64 "\n", list->seq, total);
    push(list->stack, temp, &lock);
}

int main(int argc, char *argv[]) {
    const int threads = atoi(argv[1]);
    char *dir = argv[2];

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
        printf("size of stack %d\n", sortedStack->size);
        if (sortedStack->size == 1 &&
            thpool_count(thpool) == 0) break;
    
        // Drain stack
        for(;;) {
            if(sortedStack->size >= 2) {
                FilesList *a = pop(sortedStack, &lock);
                FilesList *b = pop(sortedStack, &lock);
                MergeList *mergeList = malloc(sizeof(MergeList));
                mergeList->seq = ++seq;
                mergeList->a = a;
                mergeList->b = b;
                mergeList->stack = sortedStack;
                mergeList->dir = dir;
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