#include "pantab.h"

static PyObject *cls_timedelta = NULL;

// the pointer to size is only used if receiving a character array
static PyObject *read_value(const uint8_t *value, DTYPE dtype,
                            const size_t *size) {
    if (PyErr_CheckSignals()) {
        return NULL;
    }

    switch (dtype) {
    case INT16_:
    case INT16NA:
        return PyLong_FromLong(*((int16_t *)value));
    case INT32_:
    case INT32NA:
        return PyLong_FromLong(*((int32_t *)value));
    case INT64_:
    case INT64NA:
        return PyLong_FromLongLong(*((int64_t *)value));

    case BOOLEAN:
        return PyBool_FromLong(*value);

    case FLOAT32_:
    case FLOAT64_:
        return PyFloat_FromDouble(*((double *)value));

    case OBJECT:
        // TODO: are there any platforms where we cant cast char* and
        // unsigned char* ???
        return PyUnicode_FromStringAndSize((const char *)value, *size);

    case DATETIME64_NS:
    case DATETIME64_NS_UTC: {
        uint64_t val = *((uint64_t *)value);

        uint64_t encoded_date = val / MICROSECONDS_PER_DAY;
        uint64_t encoded_time = val % MICROSECONDS_PER_DAY;
        hyper_date_components_t date = hyper_decode_date(encoded_date);
        hyper_time_components_t time = hyper_decode_time(encoded_time);

        // Special case NULL value as it isn't contained in null_flags
        // Note that the sentinel to compare to varies by platform, so
        // have to fully parse and compare components for now
        if ((date.year == 1) && (date.month == 1) && (date.day == 1) &&
            (time.hour == 0) && (time.minute == 0) && (time.microsecond == 0)) {
            Py_RETURN_NONE;
        }

        return PyDateTime_FromDateAndTime(date.year, date.month, date.day,
                                          time.hour, time.minute, time.second,
                                          time.microsecond);
    }

    case TIMEDELTA64_NS: {
        // Unfortunately PyDelta_FromDSU and the pandas Timedelta class
        // are not compatible in signature, particularly when it comes
        // to handling negative days. As such, we construct the pandas
        // object instead of using the CPython API

        if (cls_timedelta == NULL) {
            PyObject *mod_pandas = PyImport_ImportModule("pandas");
            if (mod_pandas == NULL) {
                return NULL;
            }

            cls_timedelta = PyObject_GetAttrString(mod_pandas, "Timedelta");
            Py_DECREF(mod_pandas);
            if (cls_timedelta == NULL) {
                return NULL;
            }
        }

        py_interval interval = *((py_interval *)value);
        if (interval.months != 0) {
            PyObject *errMsg = PyUnicode_FromFormat(
                "Cannot read Intervals with month components.");
            PyErr_SetObject(PyExc_ValueError, errMsg);
            Py_DECREF(errMsg);
            return NULL;
        }

        PyObject *kwargs = PyDict_New();
        if (kwargs == NULL)
            return NULL;

        PyDict_SetItemString(kwargs, "days",
                             PyLong_FromLongLong(interval.days));
        PyDict_SetItemString(kwargs, "microseconds",
                             PyLong_FromLongLong(interval.microseconds));
        PyObject *dummy = PyTuple_New(0); // need this for PyObject_Call

        PyObject *td = PyObject_Call(cls_timedelta, dummy, kwargs);
        Py_DECREF(dummy);
        Py_DECREF(kwargs);

        return td;
    }

    default: {
        PyObject *errMsg = PyUnicode_FromFormat("Invalid dtype: \"%s\"");
        PyErr_SetObject(PyExc_ValueError, errMsg);
        Py_DECREF(errMsg);
        return NULL;
    }
    }
}

static PyObject *read_hyper_query(PyObject *dummy, PyObject *args) {
    int ok;
    PyObject *row = NULL;
    PyTupleObject *dtypes;
    hyper_connection_t *connection;
    hyper_rowset_t *rowset = NULL;
    hyper_rowset_chunk_t *chunk = NULL;
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
            goto ERROR_CLEANUP;
        }

        if (chunk == NULL) {
            break; // No more to parse
        }

        hyper_err = hyper_rowset_chunk_field_values(
            chunk, &num_cols, &num_rows, &values, &sizes, &null_flags);

        if (hyper_err) {
            goto ERROR_CLEANUP;
        }

        for (size_t i = 0; i < num_rows; i++) {
            row = PyTuple_New(num_cols);
            if (row == NULL) {
                goto ERROR_CLEANUP;
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
                    for (Py_ssize_t i = 0; i < PyList_GET_SIZE(result); i++) {
                        PyObject *tup = PyList_GET_ITEM(result, i);
                        for (Py_ssize_t j = 0; j < PyTuple_GET_SIZE(tup); j++) {
                            Py_DECREF(PyTuple_GET_ITEM(tup, j));
                        }

                        Py_DECREF(tup);
                    }

                    // Stop at j - 2 columns because side effect will have
                    // incremented j at start of loop, and current value cannot
                    // be decrefed
                    for (size_t j2 = 0; j2 < j - 2; j2++) {
                        Py_DECREF(PyTuple_GET_ITEM(row, j2));
                    }

                    goto ERROR_CLEANUP;
                }

                PyTuple_SET_ITEM(row, j, val);
            }

            int ret = PyList_Append(result, row);
            if (ret != 0) {
                // Clean up any previously inserted elements
                for (Py_ssize_t i = 0; i < PyList_GET_SIZE(result); i++) {
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
                goto ERROR_CLEANUP;
            }
        }

        hyper_destroy_rowset_chunk(chunk);
    }

    hyper_close_rowset(rowset);
    Py_XDECREF(cls_timedelta);

    return result;

ERROR_CLEANUP:
    Py_XDECREF(row);
    Py_XDECREF(result);
    Py_XDECREF(cls_timedelta);
    if (chunk != NULL)
        hyper_destroy_rowset_chunk(chunk);
    if (rowset != NULL)
        hyper_close_rowset(rowset);

    return NULL;
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
