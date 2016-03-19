#include <stdlib.h>
#include <stdio.h>
#include "shared.h"

pthread_mutex_t lock;

int main(int argc, char *argv[]) {
    
    // Initialize stack
    Stack *stack = malloc(sizeof(Stack));
    stack->head = NULL;
    stack->size = 0;

    // free stack and linked list?
    
    return 0;
}