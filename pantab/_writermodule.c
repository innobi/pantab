#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <inttypes.h>

#include "tableauhyperapi.h"

int isNull(PyObject *data) {
    if ((data == Py_None) ||
        (PyFloat_Check(data) && isnan(PyFloat_AS_DOUBLE(data)))) {
        return 1;
    } else {
        return 0;
    }
}

// TODO: Make error handling consistent. Right now errors occur if
// 1. The return value is non-NULL OR
// 2. PyErr is set within this function
hyper_error_t *write_data_for_dtype(PyObject *data, PyObject *dtype,
                                    hyper_inserter_buffer_t *insertBuffer) {
    const char *dtypeStr = PyUnicode_AsUTF8(dtype);
    hyper_error_t *result;

    // Non-Nullable types
    if (strcmp(dtypeStr, "int16") == 0) {
        int16_t val = (int16_t)PyLong_AsLong(data);
        result = hyper_inserter_buffer_add_int16(insertBuffer, val);
    } else if (strcmp(dtypeStr, "int32") == 0) {
        int32_t val = (int32_t)PyLong_AsLong(data);
        result = hyper_inserter_buffer_add_int32(insertBuffer, val);
    } else if (strcmp(dtypeStr, "int64") == 0) {
        int64_t val = (int64_t)PyLong_AsLongLong(data);
        result = hyper_inserter_buffer_add_int64(insertBuffer, val);
    } else if (strcmp(dtypeStr, "bool") == 0) {
        if (PyObject_IsTrue(data)) {
            result = hyper_inserter_buffer_add_bool(insertBuffer, 1);
        } else {
            result = hyper_inserter_buffer_add_bool(insertBuffer, 0);
        }
    }
    // Nullable types
    else if (strcmp(dtypeStr, "Int16") == 0) {
        if (isNull(data)) {
            result = hyper_inserter_buffer_add_null(insertBuffer);
        } else {
            int16_t val = (int16_t)PyLong_AsLong(data);
            result = hyper_inserter_buffer_add_int16(insertBuffer, val);
        }
    } else if (strcmp(dtypeStr, "Int32") == 0) {
        if (isNull(data)) {
            result = hyper_inserter_buffer_add_null(insertBuffer);
        } else {
            int32_t val = (int32_t)PyLong_AsLong(data);
            result = hyper_inserter_buffer_add_int32(insertBuffer, val);
        }
    } else if (strcmp(dtypeStr, "Int64") == 0) {
        if (isNull(data)) {
            result = hyper_inserter_buffer_add_null(insertBuffer);
        } else {
            int64_t val = (int64_t)PyLong_AsLongLong(data);
            result = hyper_inserter_buffer_add_int64(insertBuffer, val);
        }
    } else if (strcmp(dtypeStr, "float32") == 0) {
        if (isNull(data)) {
            result = hyper_inserter_buffer_add_null(insertBuffer);
        } else {
            double val = PyFloat_AsDouble(data);
            result = hyper_inserter_buffer_add_double(insertBuffer, val);
        }
    } else if (strcmp(dtypeStr, "float64") == 0) {
        if (isNull(data)) {
            result = hyper_inserter_buffer_add_null(insertBuffer);
        } else {
            double val = PyFloat_AsDouble(data);
            result = hyper_inserter_buffer_add_double(insertBuffer, val);
        }
    } else {
        PyObject *errMsg = PyUnicode_FromFormat("Invalid dtype: \"%s\"");
        PyErr_SetObject(PyExc_ValueError, errMsg);
        Py_DECREF(errMsg);
        return NULL;
    }

    return result;
}

// This function gets performance by sacrificing bounds checking
// Particulary no checking happens that the length of each iterable
// in data matches the length of the callables supplied at every step
// in the process,though note that this is critical!
// If this doesn't hold true behavior is undefined
static PyObject *write_to_hyper(PyObject *dummy, PyObject *args) {
    int ok;
    PyObject *data, *iterator, *row, *val, *dtypes, *dtype;
    Py_ssize_t row_counter, ncols;
    hyper_inserter_buffer_t *insertBuffer;
    hyper_error_t *result;

    // TOOD: Find better way to accept buffer pointer than putting in long
    ok = PyArg_ParseTuple(args, "OlnO!", &data, &insertBuffer, &ncols,
                          &PyTuple_Type, &dtypes);
    if (!ok)
        return NULL;

    if (!PyIter_Check(data)) {
        PyErr_SetString(PyExc_TypeError, "First argument must be iterable");
        return NULL;
    }

    iterator = PyObject_GetIter(data);
    if (iterator == NULL)
        return NULL;

    row_counter = 0;
    while ((row = PyIter_Next(iterator))) {
        // Undefined behavior if the number of tuple elements does't match
        // callable list length
        for (Py_ssize_t i = 0; i < ncols; i++) {
            val = PyTuple_GET_ITEM(row, i);
            dtype = PyTuple_GET_ITEM(dtypes, i);
            result = write_data_for_dtype(val, dtype, insertBuffer);

            if ((result != NULL) || (PyErr_Occurred())) {
                // TODO: clean up error handling mechanisms
                return NULL;
            }
        }
        Py_DECREF(row);
        row_counter += 1;
    }

    Py_DECREF(iterator);

    if (PyErr_Occurred())
        return NULL;

    return Py_None;
}

static PyMethodDef WriterMethods[] = {{"write_to_hyper", write_to_hyper,
                                       METH_VARARGS,
                                       "Writes a numpy array to a hyper file."},
                                      {NULL, NULL, 0, NULL}};

static struct PyModuleDef writermodule = {PyModuleDef_HEAD_INIT,
                                          "libwriter", // Name of module
                                          NULL,        // Documentation
                                          -1, // keep state in global variables
                                          WriterMethods};

PyMODINIT_FUNC PyInit_libwriter(void) { return PyModule_Create(&writermodule); }
