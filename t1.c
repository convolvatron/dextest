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
uint64_t enq = 10000;
// this guy can go negative
int64_t deq = 10000;
uint64_t search_r = 0;
uint64_t search_w = 0;

uint64_t max_issued; 

#define QUEUE_DEPTH 128

uint64_t queue[QUEUE_DEPTH];

#define QUEUE_EMPTY (0xffffffffffffffffull)

// single writer
void add_search(uint64_t x)
{
    queue[search_w++] = x;
}

// but multiple readers
uint64_t remove_search()
{
    uint64_t r, w, tag;

    while(r = search_r, w = search_w, tag = queue[r & (QUEUE_DEPTH - 1)], r!= w)
        if (__sync_bool_compare_and_swap(&search_r, r, r+1))
            return(tag);

    return(QUEUE_EMPTY);
}


void enqueue(worker w)
{
    op o = allocate_op(w);
    uint64_t key = __sync_fetch_and_add (&time, 1);
    sprintf(o->name, "%08lx", key);
    __sync_fetch_and_sub (&enq, 1);

    printf ("enqueue\n");

    if (enq == 0) 
        printf ("enqueue last submitted\n");

    o->f = release_op;
    uint64_t tag = hyperdex_client_put(w->client, "messages", 
                                       o->name, 8,
                                       NULL, 0, &o->status);
    insert(w->pending, tag, o);
}

void dequeue(worker); 

void delete_complete(worker w, op o)
{
    if (o->status == HYPERDEX_CLIENT_SUCCESS) {
        printf ("del complete %ld\n", deq);
        __sync_fetch_and_sub (&deq, 1);
    }
    release_op(w, o);
}

void start_delete(worker w, uint64_t t)
{
    op od = allocate_op(w);
    uint64_t size = sprintf(od->name, "%08lx", t);

    printf ("start delete: %d %d\n", deq, t);
    
    uint64_t tag = hyperdex_client_del(w->client, "messages", 
                                       od->name, size,
                                       &od->status);
    od->f = delete_complete;

    insert(w->pending, tag, od);
}


void start_search(worker w);

void search_complete(worker w, op o)
{
    if (o->status == HYPERDEX_CLIENT_SEARCHDONE) {
        release_op(w, o);
        // we're shutting down
        if (deq > 0) start_search(w);
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
        uint64_t t;
        sscanf(r->value, "%08lx", &t);
        add_search(t);
        hyperdex_client_destroy_attrs((const struct hyperdex_client_attribute *)o->result,
                                      o->result_size);
    }
}

void start_search(worker w)
{
    op o = allocate_op(w);
    o->f = search_complete;
    int len = QUEUE_DEPTH - (search_w - search_r);
    printf ("start search %ld %ld %ld\n", search_r, search_w,  len);
    uint64_t tag = hyperdex_client_sorted_search(w->client,
                                                 "messages",
                                                 NULL, 
                                                 0,
                                                 "id",
                                                 len,
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

    // should just be one guy running
    start_search(&w);

    while (enq || (deq > 0) || w.pending->count) {
        if (w.pending->count < CONCURRENCY) {
            if (enq) enqueue(&w);
            if (deq > 0) {
                uint64_t t = remove_search();
                if (t != QUEUE_EMPTY) 
                    start_delete(&w, t);
            }
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

