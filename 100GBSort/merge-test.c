#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <unistd.h>
#include <time.h>
#include <assert.h>

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

void timestamp()
{
    time_t ltime; /* calendar time */
    ltime=time(NULL); /* get current cal time */
    printf("%s",asctime( localtime(&ltime) ) );
}

int main() {

    // 3gb stream
    unsigned int streamLen = 375000000;
    int64_t *s1 = malloc(sizeof(int64_t) *  streamLen);
    if (s1 == NULL) {printf("Couldn't allocate stream1");}
    int64_t *s2 = malloc(sizeof(int64_t) *  streamLen);
    if (s2 == NULL) {printf("Couldn't allocate stream2");}
    int64_t *ms = malloc(sizeof(int64_t) *  streamLen * 2);
    if (ms == NULL) {printf("Couldn't allocate merge-stream");}
    // Zero merge array and split numbers between the two streams
    for (unsigned int x = 0; x < streamLen * 2; x++) {
        ms[x] = 0;
    }
    
    shuffle(ms, streamLen * 2);
//    shuffle(ms, streamLen * 2);
    
    for (unsigned int x = 0; x < streamLen; x++) {
        s1[x] = ms[x];
        s1[x] = ms[x + streamLen];
    }
    
   unsigned int ind1 = 0;
   unsigned int ind2 = 0;
   timestamp();
   for(unsigned int x = 0; x < streamLen*2; x++) {
      if(ind1 >= streamLen) {
        ms[x] = s2[ind2++];
      } else if(ind2 >= streamLen) {
        ms[x] = s1[ind1++];
      } else {
        if (s1[ind1] < s2[ind2]) {
           ms[x] = s1[ind1++];
        } else {
           ms[x] = s2[ind2++];
        }
      }
      
      if (x % 156250 == 0) {
        //FILE *f = fopen("merge.bin", "w+");
        //size_t written = fwrite(ms, sizeof(int64_t), 156250, f);
        //assert(written == 156250);
       //fclose(f);
      }
   }
   timestamp();
   sleep(15);
   return 0;
}
