#include "pantab.h"

static DTYPE stringToDtype(const char *str) {
    for (Py_ssize_t i = 0;
         i < (Py_ssize_t)(sizeof(dtype_map) / sizeof(dtype_map[0])); i++) {
        if (strcmp(str, dtype_map[i].str) == 0) {
            return dtype_map[i].dtype;
        }
    }

    return UNKNOWN;
}

// Caller is responsible for returned object
DTYPE *makeEnumeratedDtypes(PyTupleObject *obj) {
    Py_ssize_t len = PyTuple_GET_SIZE(obj);
    DTYPE *result = malloc(len * sizeof(DTYPE));

    for (Py_ssize_t i = 0; i < len; i++) {
        PyObject *dtypeObj = PyTuple_GET_ITEM(obj, i);
        const char *dtypeStr = PyUnicode_AsUTF8(dtypeObj);
        DTYPE dtype = stringToDtype(dtypeStr);

        if (dtype == UNKNOWN) {
            free(result);
            PyObject *errMsg =
                PyUnicode_FromFormat("Unknown dtype: \"%s\"\n", dtypeStr);
            PyErr_SetObject(PyExc_TypeError, errMsg);
            Py_DECREF(errMsg);
            return NULL;
        }

        result[i] = dtype;
    }

    return result;
}
