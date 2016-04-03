#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <inttypes.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <assert.h>

void timestamp() {
    time_t ltime;
    ltime=time(NULL);
    printf("%s",asctime( localtime(&ltime) ) );
}

int cmp(const void *a, const void *b) {
  const int64_t da = *((const int64_t *) a);
  const int64_t db = *((const int64_t *) b);
  return (da < db) ? -1 : (da == db) ? 0 : 1;
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

int main() {

    unsigned int streamLen = 1250000;
    unsigned int numBuffers = 100;
    int64_t *buffers[numBuffers];
    
    for (int x =0; x < numBuffers; x++){
        buffers[x] = malloc(sizeof(int64_t) *  streamLen);
    }
    
    // Initialize buffers
    for (int x = 0; x < streamLen; x++) {
        buffers[0][x] = streamLen - x;
    }
    
    for (int x = 1; x < numBuffers; x++) {
        memcpy(buffers[x], buffers[0], sizeof(int64_t) * streamLen);
    }

    // Method1: Start sorting arrays
    timestamp();
    for (int x = 0; x < numBuffers; x++) {
       qsort(buffers[x], streamLen, sizeof(int64_t), cmp);
       printf("sortinga\n");
    }
    timestamp();
    
    
    // Method2: divide work over 4 threads
    Req r1;
    Req r2;
    Req r3;
    Req r4;
    
    r1.len = streamLen;
    r1.a = 0;
    r1.b = 24;
    r1.buffers = buffers;
    
    r2.len = streamLen;
    r2.a = 25;
    r2.b = 49;
    r2.buffers = buffers;
    
    r3.len = streamLen;
    r3.a = 50;
    r3.b = 74;
    r3.buffers = buffers;
    
    r4.len = streamLen;
    r4.a = 75;
    r4.b = 99;
    r4.buffers = buffers;
    
    pthread_t t1;
    pthread_t t2;
    pthread_t t3;
    pthread_t t4;
    
    timestamp();
    pthread_create(&t1, NULL, sort, &r1);
    pthread_create(&t2, NULL, sort, &r2);
    pthread_create(&t3, NULL, sort, &r3);
    pthread_create(&t4, NULL, sort, &r4);
    
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    pthread_join(t3, NULL);
    pthread_join(t4, NULL);
    timestamp();

   return 0;
}
