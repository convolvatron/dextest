#include <hyperdex/client.h>

// just because table is hardwired on range

typedef struct worker *worker;

typedef struct op *op;


struct op {
    uint64_t tag;
    char name[8];
    enum hyperdex_client_returncode status;  
    struct hyperdex_client_attribute *result; 
    struct hyperdex_client_attribute_check check;
    size_t result_size;
    void (*f)(worker w, op o);
    op next;
};
