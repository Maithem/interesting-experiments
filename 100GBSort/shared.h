#ifndef SHARED_H
#define SHARED_H

#include <pthread.h>

typedef struct FilesList {
    char **file_paths;
    unsigned int len;
} FilesList;

typedef struct _linkedList {
    FilesList *list;
    struct _linkedList *next;
} LinkedList;

typedef struct _stack {
    unsigned int size;
    LinkedList *head;
} Stack;

typedef struct mergeList {
    unsigned int seq;
    FilesList *a;
    FilesList *b;
    Stack *stack;
    char *dir;
} MergeList;


int cmp(const void *a, const void *b);

void push(Stack *stack, FilesList *range, pthread_mutex_t *lock);

FilesList *pop(Stack *stack, pthread_mutex_t *lock);

FilesList *read_dir(const char *dir_path);

unsigned nCk( unsigned n, unsigned k );

void shuffle(int64_t *a, int64_t n);

#endif