#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <op.h>
#include <table.h>

struct worker {
    struct hyperdex_client *client;
    table pending;
    op freequeue;
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
    delete(w->pending, o->tag);
    o->next = w->freequeue;
    w->freequeue = o;
}


uint64_t time;
int enq = 100;
int deq = 100;

void enqueue(worker w)
{
    op o = allocate_op(w);
    uint64_t key = __sync_fetch_and_add (&time, 1);
    sprintf(o->name, "%08lx", key);
    __sync_fetch_and_sub (&enq, 1);
    o->f = release_op;
    uint64_t tag = hyperdex_client_put(w->client, "messages", 
                                       o->name, 8,
                                       NULL, 0, &o->status);
    insert(w->pending, tag, o);
}

void dequeue(worker); 

void delete_complete(worker w, op o)
{
    if (o->status == HYPERDEX_CLIENT_NOTFOUND) {
        dequeue(w);
    } else {
        printf ("del complete %d\n", deq);
        __sync_fetch_and_sub (&deq, 1);
    }
    release_op(w, o);
}

void search_complete(worker w, op o)
{
    // ok, well really this is going to get called for each
    // result from the sorted search. its not all clear 
    // how this gets cleaned up or terminated

    if (o->status == HYPERDEX_CLIENT_SEARCHDONE) {
        release_op(w, o);
        return;
    }

    if (o->status != HYPERDEX_CLIENT_SUCCESS) {
        printf ("search failure\n");
        // apparently non-success is the end of the pipe
        release_op(w, o);
        return;
    }

    if (o->result_size > 1) {
        struct hyperdex_client_attribute *r = &o->result[0];
        char *k;
        op od = allocate_op(w);
        uint64_t tag = hyperdex_client_del(w->client, "messages", 
                                           r[0].value, r[0].value_sz,
                                           &od->status);
        insert(w->pending, tag, od);
        od->f = delete_complete;
        hyperdex_client_destroy_attrs((const struct hyperdex_client_attribute *)o->result,
                                      o->result_size);
    }
}

void dequeue(worker w)
{
    int result_count = 1;
    op o = allocate_op(w);
    o->f = search_complete;

    uint64_t tag = hyperdex_client_sorted_search(w->client,
                                                 "messages",
                                                 NULL, 
                                                 0,
                                                 "id",
                                                 result_count,
                                                 0, // minimize
                                                 &o->status,
                                                 (const struct hyperdex_client_attribute **)&o->result,
                                                 &o->result_size);
    insert(w->pending, tag, o);
}


void run_worker()
{
    struct worker w;
    w.client = hyperdex_client_create("127.0.0.1", 1982);
    w.pending = malloc(sizeof(struct table));
    w.pending->bucket_length = CONCURRENCY;
    w.freequeue = 0;
    allocate_table(w.pending);

    enum hyperdex_client_returncode loop_status;

    while (enq || (deq > 0) || w.pending->count) {
        if (w.pending->count < CONCURRENCY) {
            if (enq) {
                enqueue(&w);
            } else 
                if (deq > 0) dequeue(&w);
        }

        uint64_t x = hyperdex_client_loop(w.client, 100, &loop_status);

        if (loop_status != HYPERDEX_CLIENT_NONEPENDING) {        
            if (loop_status !=  HYPERDEX_CLIENT_SUCCESS) {
                printf ("loop error %d\n", loop_status);
            } else {
                op o = get(w.pending, x);
                if (o) {
                    o->f(&w, o);
                } else {
                    printf ("missing tag\n");
                }
            }
        }
    }
    hyperdex_client_destroy(w.client);
}

int main(int argc, char **argv)
{
    run_worker();
    return 0;
}

