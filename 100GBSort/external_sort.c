#include <stdlib.h>
#include <stdio.h>

#include "thpool.h"
#include "shared.h"

pthread_mutex_t lock;

int main(int argc, char *argv[]) {
    
    // Initialize stack and threadpool
    Stack *stack = malloc(sizeof(Stack));
    stack->head = NULL;
    stack->size = 0;

    threadpool thpool = thpool_init(8);
    
    return 0;
}