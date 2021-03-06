#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>

#include "shared.h"


/**
 *  This function reads a directory and return a
 *  Directory structure (i.e. a list of all files inside
 *  the directory).
 **/

FilesList *read_dir(const char *dir_path) {
    
    FilesList *temp = malloc(sizeof(FilesList));
    
    DIR *fd = opendir(dir_path);
    
    if (fd == NULL) {
        printf("Couldn't open directory %s\n", dir_path);
        return NULL;
    }
    
    struct dirent* dirFile;
    FILE *file;
    unsigned int len = 0;
    
    while ((dirFile = readdir(fd)) != NULL) {
        if (dirFile->d_type == DT_REG) {
            len++;
        }
    }
    
    temp->len = len;
    temp->file_paths = malloc(sizeof(char *) * len);
    // Close and open the directory to get another
    // iterator
    closedir(fd);
    fd = opendir(dir_path);

    while (dirFile = readdir(fd)) {
        if (dirFile->d_type == DT_REG) {
            char *curr_file_path = malloc(sizeof(char) * 100);
            sprintf(curr_file_path, "%s/%s", dir_path, dirFile->d_name);
            temp->file_paths[--len] = curr_file_path;
        }
    }
    closedir(fd);
    return temp;
}


int cmp(const void *a, const void *b) {
  const int64_t da = *((const int64_t *) a);
  const int64_t db = *((const int64_t *) b);
  return (da < db) ? -1 : (da == db) ? 0 : 1;
}

void push(Stack *stack, void *range, pthread_mutex_t *lock) {
    pthread_mutex_lock(lock);
    
    LinkedList *head = stack->head;
    LinkedList *newNode = malloc(sizeof(LinkedList));
    newNode->list = range;
    newNode->next = NULL;

    if (head == NULL) {
        stack->head = newNode;
    } else {
        newNode->next = head;
        stack->head = newNode;
    }
    stack->size++;
    
    pthread_mutex_unlock(lock);
}


void *pop(Stack *stack, pthread_mutex_t *lock) {
    pthread_mutex_lock(lock);
    LinkedList *head = stack->head;
    if (head == NULL) {
        pthread_mutex_unlock(lock);
        return NULL;
    } else {
        stack->head = stack->head->next;
        stack->size--;
        FilesList *temp = head->list;
        free(head);
        pthread_mutex_unlock(lock);
        pthread_mutex_unlock(lock);
        return temp;
    }
}


/*
 * Compute n choose k
 **/
unsigned nCk( unsigned n, unsigned k ) {
    if (k > n) return 0;
    if (k * 2 > n) k = n-k;
    if (k == 0) return 1;

    int result = n;
    for( int i = 2; i <= k; ++i ) {
        result *= (n-i+1);
        result /= i;
    }
    return result;
}

/*
 * Randomly shuffle an array.
 */
void shuffle(int64_t *a, int64_t n) {
    int64_t i;
    int64_t r;
    int64_t temp;
    
    for(i = n - 1; i > 0; i--) {
        r = rand() % i;
        temp = a[r];
        a[r] = a[i];
        a[i] = temp;
    }
}