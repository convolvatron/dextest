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
void check_status(worker w, op o)
{
    if (o->status != HYPERDEX_CLIENT_SUCCESS) {
        printf ("bad status: %d\n", o->status);
    }
    release_op(w, o);
}

uint64_t time;
uint64_t enq = 0;
// this guy can go negative
int64_t deq = 0;
uint64_t search_r = 0;
uint64_t search_w = 0;
uint64_t collisions = 0;
uint64_t in_search = 0;
uint64_t reported = 0;
uint64_t max_issued; 
uint64_t epoch; 
uint64_t searches = 0;
uint64_t search_results = 0;
uint64_t tick_base; 
uint64_t last_search_reported; 
uint64_t concurrent;

#define QUEUE_DEPTH 1024

uint64_t queue[QUEUE_DEPTH];

#define QUEUE_EMPTY (0xffffffffffffffffull)

// single writer who ever overruns
void add_search(uint64_t x)
{
    if ((search_w - search_r) > QUEUE_DEPTH) 
        printf ("overrun!\n");
    queue[search_w & (QUEUE_DEPTH-1) ] = x;
    search_w++;
}

int encode_time(char *dest, uint64_t time)
{
    return(sprintf(dest, "%016lx", time));
}

static int val(char x)
{
    if ((x >= '0') && (x <= '9')) return(x - '0');
    if ((x >= 'a') && (x <= 'f')) return(x - 'a' + 10);
    if ((x >= 'A') && (x <= 'F')) return(x - 'A' + 10);
    return(-1);
}


uint64_t decode_time(char *source)
{
    uint64_t t = 0;
    for (int i = 0 ; i < 16; i++)
        t = t*16 + val(source[i]);
    return(t);
}


int encode_time_bin(char *dest, uint64_t time)
{
    for (int i = 7; i>=0; i--){
        dest[i] = time&0xff;
        time>>=8;
    }
    return(8);
}

uint64_t decode_time_bin(unsigned char *source)
{
    uint64_t dest = 0;

    for (int i=0;i<8;i++) 
        dest = (dest<<8) + source[i];

    return(dest);
}


// but multiple readers
uint64_t remove_search()
{
    uint64_t r, w, tag;

    while(r = search_r, w = search_w, tag = queue[r & (QUEUE_DEPTH - 1)], r!= w){
        if (__sync_bool_compare_and_swap(&search_r, r, r+1)) {
            return(tag);
        }
    }

    return(QUEUE_EMPTY);
}


void enqueue(worker w)
{
    op o = allocate_op(w);
    struct timeval x;
    
    uint64_t key = tick(); // - tick_base + (epoch<<32);

    int klen = encode_time_bin(o->name, key);
    __sync_fetch_and_sub (&enq, 1);

    if (enq == 0) 
        printf ("enqueue last submitted\n");

    o->f = check_status;
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
    uint64_t size =  encode_time_bin(od->name, t);
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
        uint64_t t=decode_time_bin((unsigned char *)r->value);
        last_search_reported = t;
        search_results ++;
        add_search(t);
        hyperdex_client_destroy_attrs((const struct hyperdex_client_attribute *)o->result,
                                      o->result_size);
    }
}

void start_search(worker w)
{
    static int loop_count = 0;
    op o = allocate_op(w);
    o->f = search_complete;
    int len = QUEUE_DEPTH - (search_w - search_r) - 1;
    if (len > (QUEUE_DEPTH /2)) {
        searches++;
        __sync_fetch_and_add (&in_search, 1);
        o->check.attr = "id";
        o->check.value_sz = encode_time_bin(o->name, last_search_reported);
        o->check.value = o->name;
        o->check.datatype = HYPERDATATYPE_STRING;
        o->check.predicate = HYPERPREDICATE_GREATER_THAN;
        uint64_t tag = hyperdex_client_sorted_search(w->client,
                                                     "messages",
                                                     &o->check, 
                                                     1,
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

    while (enq || (deq > 0) || w.pending->count) {
        if (!(loop_count++ % 5)){
            struct timeval now;
            gettimeofday(&now, 0);        
            if (reported < now.tv_sec) 
                if (__sync_bool_compare_and_swap(&reported, reported, now.tv_sec)) {
                    printf ("enc: %ld deq: %ld searches: %ld queue :%d search results %ld\n", 
                            enq, deq, searches, (int)(search_w - search_r), search_results);
                }
        }


        if (w.pending->count < CONCURRENCY) {
            if (enq) enqueue(&w);
            if (((enq == 0) || concurrent) && (deq > 0)) {
                uint64_t t = remove_search();
                if (t != QUEUE_EMPTY) 
                    start_delete(&w, t);
            }
            if ((!in_search) && (deq > 0))
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

struct arg {
    char *name;
    int isbool;
    uint64_t *val;
}args[] = {
    {"-e", 0, &enq},
    {"-d", 0, &deq},
    {"-c", 1, &concurrent}
};


int main(int argc, char **argv)
{
    struct timeval start, end;
    gettimeofday(&start, 0);

    for (int i = 1; i <argc ;i++) {
        int found = 0;
        for (int j = 0; j < sizeof(args)/sizeof(struct arg); j++) {
            struct arg *a = args + j;
            int alen = strlen(a->name);

            if (!strncmp(argv[i], a->name, alen)){
                if (a->isbool)  {
                    *a->val = 1;
                } else {
                    if (strlen(argv[i]) > alen) {
                        *a->val = atoi(argv[i] + alen); 
                    } else {
                        if (i == argc) {
                            printf ("missing argument\n");
                            exit(1);
                        }
                        *a->val = atoi(argv[++i]);
                    }
                }
                found = 1;
            }
        }
        if (!found) {
            printf ("no such arg %s\n", argv[i]);
            exit(1);
        }
    }


    reported = start.tv_sec;
    run_worker();
    printf ("collisions: %ld\n", collisions);
    gettimeofday(&end, 0);
    printf ("time: %d\n", (unsigned int)(end.tv_sec - start.tv_sec));
    return 0;
}

