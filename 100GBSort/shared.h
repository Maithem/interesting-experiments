#ifndef SHARED_H
#define SHARED_H

#include <pthread.h>
#include <semaphore.h>

#include "thpool.h"

typedef struct FilesList {
    char **file_paths;
    unsigned int len;
} FilesList;

typedef struct _WriteBuffer {
    void *buff;
    unsigned int len;
    char *file_path;
} WriteBuffer;

typedef struct _linkedList {
    void *list;
    struct _linkedList *next;
} LinkedList;

typedef struct _stack {
    unsigned int size;
    LinkedList *head;
} Stack;

typedef struct _WriteReq{
    char *name;
    int64_t *buff;
    unsigned int len;
    Stack *freeStack;
    sem_t *sem;
} WriteReq;

typedef struct mergeList {
    unsigned int seq;
    FilesList **lists;
    unsigned int len;
    unsigned int width;
    Stack *stack;
    Stack *buffStack;
    threadpool thpool;
    char *dir;
    unsigned int k;
} MergeList;

typedef struct _fileBuffs {
    unsigned a;
    unsigned b;
    int64_t **buff;
} FileBuffs;

int cmp(const void *a, const void *b);

void push(Stack *stack, void *range, pthread_mutex_t *lock);

void *pop(Stack *stack, pthread_mutex_t *lock);

FilesList *read_dir(const char *dir_path);

unsigned nCk( unsigned n, unsigned k );

void shuffle(int64_t *a, int64_t n);

#endif