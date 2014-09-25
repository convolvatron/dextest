#include <hyperdex/client.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>


typedef struct op *op;


typedef struct op∑ {
  uint64_t tag;
  char name[8];
  enum hyperdex_client_returncode status;  
  struct hyperdex_client_attribute *result; 
  size_t result_size;
  int (*f)(struct hyperdex_client *client, int slot);
} *op;

typedef struct table {
  op *buckets;
  size_t bucket_length;
  int count;
} *table;

typedef struct worker {
  struct hyperdex_client *client;
  table pending;
  op *freequeue;
};

#define CONCURRENCY 4

op allocate_op(worker w) {
  op r;
  if (w->freequeue) {
    r = w->freequeue;
    w->freequeue = r->next;
  } else r = malloc(sizeof(struct op));
  return(r);
}

void release_op(worker w, op o) {
  o->next = w->freequeue;
  w->freequeue = o;
}

void allocate_table(table t) {
  t->buckets = malloc(t->bucket_length * sizeof(op));
  memset(t->buckets, 0, t->bucket_length * sizeof(op));
}

void resize_table(table t) 
{
  int old_length = t->bucket_length;
  int old_buckets = t->buckets;
  t->bucket_length *= 2;
  t->count = 0;
  allocate_table(t);
  
  for (int i=0; i<buckets_length; i++) {
    op k = 0;
    for (op j =old_buckets[i]; j; j= k){
      k= j->next;
      insert(t, j);
    }
  }
}

void insert(table t, op o) { 
  if (t->count > t->bucket_length)
    resize_table(t);
  o->next = t->buckets[o->tag % t->bucket_length];
  t->buckets[o->tag % t->bucket_length] =0;
}

op *search(table t, uint64_t tag)   
{
  op *j = t->buckets + tag % t->bucket_length;
  while(*j && (*j->tag != tag)) j = & j->next;
}

op *get(table t, uint64_t tag) { 
  op *j = search(t, tag);
  return(*j);
}

void delete(table t, uint64_t tag) { 
  op *j = search(t, tag);
  *j = *j->next;
}

uint64_t time;
int success = 0;
int enc = 1000;

/* the actual thing */

int enqueue(worker w, int slot)
{
  uint64_t key = __sync_fetch_and_add (&time, 1);
  sprintf(ops[slot].name, "%08lx", key);
  tags[slot] = hyperdex_client_put(w->c, "messages", 
				   ops[slot].name, 8,
				   NULL, 0, &ops[slot].status);
  ops[slot].f = 0;
  __sync_fetch_and_sub (&enc, 1);
}

void dequeue(worker); 

void delete_complete(worker c)
{
  if (ops[slot].status == HYPERDEX_CLIENT_NOTFOUND) {
    return(dequeue(c, slot));
  } 
  __sync_fetch_and_sub (&dec, 1);
  return(0);
}

void search_complete(worker c)
{
  // ok, well really this is going to get called for each
  // result from the sorted search. its not all clear 
  // how this gets cleaned up or terminated

  if (ops[slot].status != HYPERDEX_CLIENT_SUCCESS) {
    printf ("search failure\n");
  }

  if (ops[slot].result_size > 1) {
    struct hyperdex_client_attribute *r = &ops[slot].result[0];
    char *k;
    tags[slot] = hyperdex_client_del(c, "messages", 
				     r[0].value, r[0].value_sz,
				     &ops[slot].status);
    ops[slot].f = delete_complete;
  }

  hyperdex_client_destroy_attrs((const struct hyperdex_client_attribute *)ops[slot].result,
				ops[slot].result_size);

  return(dequeue(c, slot));
}

void dequeue(worker w)
{
  int result_count = 1;

  ops[slot].f = search_complete;
  tags[slot] = hyperdex_client_sorted_search(client,
					     "messages",
					     NULL, 
					     0,
					     "id",
					     result_count,
					     0, // minimize
					     &ops[slot].status,
					     (const struct hyperdex_client_attribute **)&ops[slot].result,
					     &ops[slot].result_size);
  return(1);
}


void run-worker()
{
  struct worker w;
  w->client = hyperdex_client_create("127.0.0.1", 1982);
  w->table = malloc(sizeof(struct table));
  w->count = CONCURRENCY;

  enum hyperdex_client_returncode loop_status;

  while (success < deq) {

    uint64_t x = hyperdex_client_loop(client, -1, &loop_status);

    if (loop_status !=  HYPERDEX_CLIENT_SUCCESS)
      printf ("loop error %d\n", loop_status);

    int j;
    for ( j = 0 ; j < CONCURRENCY && (tags[j] != x); j ++);

    if (tags[j] == x) {
	tags[j] = -1;
	if ((!ops[j].f) || (!ops[j].f(client, j))) {
	  if (enq  > 0) {
	    enqueue(client, j);
	    enq--;
	    if (!enq)
	      printf ("enq complete\n");
	  } else {
	    dequeue(client, j);
	  }
	}
    } else {
      printf ("missing tag\n");
    }
  }
  hyperdex_client_destroy(client);
}

int main(int argc, char **argv)
{
  enq = 1000;
  deq = 1000;

  for (int i = 0; i < CONCURRENCY; i++) {
    enqueue(client, i);
    enq--;
  }


  return EXIT_SUCCESS;
}

