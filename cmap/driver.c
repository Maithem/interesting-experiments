#include <stdio.h>

#include "uthash.h"


struct stream {
    unsigned char uuid[16];   
    unsigned long long s_tail;
    UT_hash_handle hh;
};

int main(int argc, char *argv[]) {
    struct stream *streams = NULL;
    
    struct stream *s = malloc(sizeof(struct stream));
    s->uuid[0] = 1;
    s->s_tail = 5;
    
    HASH_ADD_KEYPTR(hh, streams, s->uuid, 16, s);
    
    unsigned char uuid_key[16];
    uuid_key[0] = 0;
    HASH_FIND(hh, streams, uuid_key, 16, s);
}
