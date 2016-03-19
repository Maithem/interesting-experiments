#ifndef SHARED_H
#define SHARED_H

#include <pthread.h>

typedef struct FilesDirectory {
    char **file_paths;
    unsigned int len;
} FilesDirectory;

typedef struct _range {
    
} Range;

typedef struct _linkedList {
    Range *elem;
    struct _linkedList *next;
} LinkedList;

typedef struct _stack {
    unsigned int size;
    LinkedList *head;
} Stack;


int cmp(const void *a, const void *b);

void push(Stack *stack, Range *range, pthread_mutex_t *lock);

Range *pop(Stack *stack, pthread_mutex_t *lock);

FilesDirectory *read_dir(const char *dir_path);

unsigned nCk( unsigned n, unsigned k );

void shuffle(int64_t *a, int64_t n);

#endif