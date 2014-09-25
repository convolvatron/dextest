#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <op.h>
#include <table.h>
#include <sys/time.h>

struct worker {
    struct hyperdex_client *client;
    table pending;
    op freequeue;
};

#define CONCURRENCY 12

static int val(char x)
{
    if ((x >= '0') && (x <= '9')) return(x - '0');
    if ((x >= 'a') && (x <= 'f')) return(x - 'a' + 10);
    if ((x >= 'A') && (x <= 'F')) return(x - 'A' + 10);
    return(-1);
}


static __inline__ unsigned long long tick()
{
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

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
uint64_t enq = 1000;
// this guy can go negative
int64_t deq = 1000;
uint64_t search_r = 0;
uint64_t search_w = 0;
uint64_t collisions = 0;
uint64_t boundary = 0;
uint64_t in_search = 0;

uint64_t max_issued; 
uint64_t epoch; 
uint64_t tick_base; 

#define QUEUE_DEPTH 128

uint64_t queue[QUEUE_DEPTH];

#define QUEUE_EMPTY (0xffffffffffffffffull)

// single writer who ever overruns
void add_search(uint64_t x)
{
    if (x > boundary){
        queue[search_w & (QUEUE_DEPTH-1) ] = x;
        search_w++;
    }
}

// but multiple readers
uint64_t remove_search()
{
    uint64_t r, w, tag;

    while(r = search_r, w = search_w, tag = queue[r & (QUEUE_DEPTH - 1)], r!= w){
        if (__sync_bool_compare_and_swap(&search_r, r, r+1)) {
            if (tag > boundary){
                boundary = tag;
                return(tag);
            }
        }
    }

    return(QUEUE_EMPTY);
}


void enqueue(worker w)
{
    op o = allocate_op(w);
    struct timeval x;
    
    uint64_t key = tick(); // - tick_base + (epoch<<32);

    int klen = sprintf(o->name, "%016lx", key);
    __sync_fetch_and_sub (&enq, 1);

    if (enq == 0) 
        printf ("enqueue last submitted\n");

    o->f = release_op;
    uint64_t tag = hyperdex_client_put(w->client, "messages", 
                                       o->name, klen,
                                       NULL, 0, &o->status);
    insert(w->pending, tag, o);
}

void delete_complete(worker w, op o)
{
    if (o->status == HYPERDEX_CLIENT_SUCCESS) {
        __sync_fetch_and_sub (&deq, 1);
    }

    if (o->status == HYPERDEX_CLIENT_NOTFOUND) {
        __sync_fetch_and_add (&collisions, 1);
    }

    release_op(w, o);
}

void start_delete(worker w, uint64_t t)
{
    op od = allocate_op(w);
    uint64_t size = sprintf(od->name, "%016lx", t);

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
        __sync_fetch_and_sub (&in_search, 1);
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
        uint64_t t = 0;
        for (int i = 0 ; i < 16; i++)
            t = t*16 + val(r->value[i]);

        add_search(t);
        hyperdex_client_destroy_attrs((const struct hyperdex_client_attribute *)o->result,
                                      o->result_size);
    }
}

void start_search(worker w)
{
    op o = allocate_op(w);
    o->f = search_complete;
    int len = QUEUE_DEPTH - (search_w - search_r) - 1;
    if (len > (QUEUE_DEPTH /4)) {
        __sync_fetch_and_add (&in_search, 1);
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
}


void run_worker()
{
    struct worker w;
    int loop_count = 0;
    w.client = hyperdex_client_create("127.0.0.1", 1982);
    w.pending = allocate_table(CONCURRENCY);
    w.freequeue = 0;

    enum hyperdex_client_returncode loop_status;

    // should just be one guy running
    start_search(&w);

    while (enq || (deq > 0) || w.pending->count) {
        if (!(loop_count++)  % 10){
            
        }

        if (w.pending->count < CONCURRENCY) {
            if (enq) enqueue(&w);
            if (deq > 0) {
                uint64_t t = remove_search();
                if (t != QUEUE_EMPTY) 
                    start_delete(&w, t);
            }
            if (!in_search)
                start_search(&w);
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
    struct timeval start, end;
    gettimeofday(&start, 0);
    //    epoch = x.tv_sec - 1411671753;
    //    tick_base = tick();

    run_worker();
    printf ("collisions: %ld\n", collisions);
    gettimeofday(&end, 0);
    printf ("time: %d\n", (unsigned int)(end.tv_sec - start.tv_sec));
    return 0;
}

