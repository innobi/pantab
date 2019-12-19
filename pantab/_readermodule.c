#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <datetime.h>

#include "dtypes.h"
#include "tableauhyperapi.h"

// the pointer to size is only used if receiving a character array
static PyObject *read_value(const uint8_t *value, DTYPE dtype,
                            const size_t *size) {
    switch (dtype) {
    case INT16_:
    case INT16NA:
    case INT32_:
    case INT32NA:
    case INT64_:
    case INT64NA:
        return PyLong_FromLongLong(*value);

    case BOOLEAN:
        return PyBool_FromLong(*value);

    case FLOAT32_:
    case FLOAT64_:
        return PyFloat_FromDouble(*((double *)value));

    case OBJECT:
        return PyUnicode_FromStringAndSize(value, *size);

    case DATETIME64_NS:
    case DATETIME64_NS_UTC: {
        // TODO: these don't belong here
        uint64_t val = *((uint64_t *)value);

        // This is a macro in the writer module but overflows here...
        static const uint64_t ms_per_day = 24ULL * 60ULL * 60ULL * 1000000ULL;
        uint64_t encoded_date = val / ms_per_day;
        uint64_t encoded_time = val % ms_per_day;
        hyper_date_components_t date = hyper_decode_date(encoded_date);
        hyper_time_components_t time = hyper_decode_time(encoded_time);

        return PyDateTime_FromDateAndTime(date.year, date.month, date.day,
                                          time.hour, time.minute, time.second,
                                          time.microsecond);
    }
    }
}

static PyObject *read_hyper_query(PyObject *dummy, PyObject *args) {
    int ok;
    PyObject *row, *dtypes;
    hyper_connection_t *connection;
    hyper_rowset_t *rowset;
    hyper_rowset_chunk_t *chunk;
    const char *query;
    hyper_error_t *hyper_err;
    size_t num_cols, num_rows;
    const uint8_t *const *values;
    const size_t *sizes;
    const int8_t *null_flags;

    // TODO: support platforms where uintptr_t may not equal unsigned long long
    ok = PyArg_ParseTuple(args, "KsO!", &connection, &query, &PyTuple_Type,
                          &dtypes);
    if (!ok)
        return NULL;

    hyper_err = hyper_execute_query(connection, query, &rowset);
    if (hyper_err) {
        return NULL;
    }

    DTYPE *enumeratedDtypes = makeEnumeratedDtypes(dtypes);
    if (enumeratedDtypes == NULL)
        return NULL;

    PyObject *result = PyList_New(0);
    if (result == NULL) {
        return NULL;
    }

    while (1) {

        hyper_err = hyper_rowset_get_next_chunk(rowset, &chunk);
        if (hyper_err) {
            // TODO: clean up anything appended to list
            return NULL;
        }

        if (chunk == NULL) {
            break; // No more to parse
        }

        hyper_err = hyper_rowset_chunk_field_values(
            chunk, &num_cols, &num_rows, &values, &sizes, &null_flags);

        if (hyper_err) {
            // TODO: clean up anything appended to list
            return NULL;
        }

        for (size_t i = 0; i < num_rows; i++) { // TODO: why is i++ required?
            row = PyTuple_New(num_cols);
            if (row == NULL) {
                // TODO: clean up everything
                Py_DECREF(result);
                return NULL;
            }

            for (size_t j = 0; j < num_cols; j++) {
                PyObject *val;
                if (*null_flags == 1) {
                    val = Py_None;
                    Py_INCREF(val);
                } else {
                    DTYPE dtype = enumeratedDtypes[j];
                    val = read_value(*values, dtype, sizes);
                }

                values++, sizes++, null_flags++;

                if (val == NULL) {
                    // TODO: clean up everything
                    Py_DECREF(result);
                    Py_DECREF(row);
                    return NULL;
                }

                PyTuple_SET_ITEM(row, j, val);
            }

            int ret = PyList_Append(result, row);
            if (ret != 0) {
                // Clean up any previously inserted elements
                for (Py_ssize_t i = 0; i < PyList_GET_SIZE(result) - 1; i++) {
                    PyObject *tup = PyList_GET_ITEM(result, i);
                    for (Py_ssize_t j = 0; j < PyTuple_GET_SIZE(tup); j++) {
                        Py_DECREF(PyTuple_GET_ITEM(tup, j));
                    }

                    Py_DECREF(tup);
                }

                // Clean up current elements
                for (Py_ssize_t j = 0; j < PyTuple_GET_SIZE(row); j++) {
                    Py_DECREF(PyTuple_GET_ITEM(row, j));
                }
                Py_DECREF(row);
                Py_DECREF(result);

                return NULL;
            }
        }

        hyper_destroy_rowset_chunk(chunk);
    }

    hyper_close_rowset(rowset);

    if (PyErr_Occurred())
        return NULL;

    return result;
}

static PyMethodDef ReaderMethods[] = {
    {"read_hyper_query", read_hyper_query, METH_VARARGS,
     "Reads a hyper query from a given connection."},
    {NULL, NULL, 0, NULL}};

static struct PyModuleDef readermodule = {PyModuleDef_HEAD_INIT,
                                          "libreader", // Name of module
                                          NULL,        // Documentation
                                          -1, // keep state in global variables
                                          ReaderMethods};

PyMODINIT_FUNC PyInit_libreader(void) {
    PyDateTime_IMPORT;
    return PyModule_Create(&readermodule);
}
