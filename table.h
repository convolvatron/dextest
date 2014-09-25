typedef struct op *op;

typedef struct table {
    op *buckets;
    uint64_t bucket_length;
    int count;
} *table;

void insert(table t, uint64_t tag, op o);
op get(table t, uint64_t tag);
void delete(table t, uint64_t tag);
// fix
void allocate_table(table t);
