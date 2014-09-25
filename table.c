#include <stdint.h>
#include <op.h>
#include <table.h>
#include <stdlib.h>
#include <string.h>

void allocate_table(table t) {
    t->buckets = malloc(t->bucket_length * sizeof(op));
    memset(t->buckets, 0, t->bucket_length * sizeof(op));
}

void resize_table(table t) 
{
    int old_length = t->bucket_length;
    op *old_buckets = t->buckets;
    t->bucket_length *= 2;
    t->count = 0;
    allocate_table(t);
  
    for (int i=0; i<old_length; i++) {
        op k = 0;
        for (op j =old_buckets[i]; j; j= k){
            k = j->next;
            insert(t, j->tag, j);
        }
    }
}

void insert(table t, uint64_t tag, op o) { 
    o->tag = tag;
    if (t->count > t->bucket_length)
        resize_table(t);
    o->next = t->buckets[o->tag % t->bucket_length];
    t->buckets[o->tag % t->bucket_length] =0;
}

op *search(table t, uint64_t tag)   
{
    op *j = t->buckets + tag % t->bucket_length;
    while(*j && ((*j)->tag != tag)) j = &(*j)->next;
    return(j);
}

op get(table t, uint64_t tag) { 
    op *j = search(t, tag);
    return(*j);
}

void delete(table t, uint64_t tag) { 
    op *j = search(t, tag);
    t->count--;
    *j = (*j)->next;
}
