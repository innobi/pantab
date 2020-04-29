#ifndef PANTAB
#define PANTAB

#define PY_SSIZE_T_CLEAN
#include "tableauhyperapi.h"
#include <Python.h>
#include <inttypes.h>

#define MICROSECONDS_PER_DAY                                                   \
    (INT64_C(24) * INT64_C(60) * INT64_C(60) * INT64_C(1000000))

typedef enum {
    INT16_ = 1,
    INT32_,
    INT64_,
    INT16NA = 6,
    INT32NA,
    INT64NA,
    FLOAT32_ = 11,
    FLOAT64_,
    BOOLEAN = 50,
    BOOLEANNA,
    DATETIME64_NS = 100,
    DATETIME64_NS_UTC,
    DATE,
    TIMEDELTA64_NS = 200,
    OBJECT = 220,
    STRING,
    UNKNOWN = 255
} DTYPE;

static const struct {
    DTYPE dtype;
    const char *str;
} dtype_map[] = {{INT16_, "int16"},
                 {INT32_, "int32"},
                 {INT64_, "int64"},
                 {INT16NA, "Int16"},
                 {INT32NA, "Int32"},
                 {INT64NA, "Int64"},
                 {FLOAT32_, "float32"},
                 {FLOAT64_, "float64"},
                 {BOOLEAN, "bool"},
                 {BOOLEAN, "boolean"},
                 {DATETIME64_NS, "datetime64[ns]"},
                 {DATETIME64_NS_UTC, "datetime64[ns, UTC]"},
                 {DATE, "date"}, // TODO: this isn't actually a dtype
                 {TIMEDELTA64_NS, "timedelta64[ns]"},
                 {STRING, "string"},
                 {OBJECT, "object"}};

// creates an enumeration from a tuple of strings,
// so ("int16", "int32") -> [INT16_, INT32_]
// caller is responsible for freeing memory
// returns NULL on failure
DTYPE *makeEnumeratedDtypes(PyTupleObject *obj);

#endif
