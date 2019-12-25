#include "pantab.h"

// TODO: Make error handling consistent. Right now errors occur if
// 1. The return value is non-NULL OR
// 2. PyErr is set within this function
static hyper_error_t *writeNonNullData(PyObject *data, DTYPE dtype,
                                       hyper_inserter_buffer_t *insertBuffer,
                                       Py_ssize_t row, Py_ssize_t col) {
    hyper_error_t *result;
    // Check again for non-null data
    switch (dtype) {
    case INT16_:
    case INT16NA: {
        int16_t val = (int16_t)PyLong_AsLong(data);
        result = hyper_inserter_buffer_add_int16(insertBuffer, val);
        break;
    }
    case INT32_:
    case INT32NA: {
        int32_t val = (int32_t)PyLong_AsLong(data);
        result = hyper_inserter_buffer_add_int32(insertBuffer, val);
        break;
    }
    case INT64_:
    case INT64NA: {
        int64_t val = (int64_t)PyLong_AsLongLong(data);
        result = hyper_inserter_buffer_add_int64(insertBuffer, val);
        break;
    }
    case FLOAT32_:
    case FLOAT64_: {
        double val = PyFloat_AsDouble(data);
        result = hyper_inserter_buffer_add_double(insertBuffer, val);
        break;
    }
    case BOOLEAN: {
        if (PyObject_IsTrue(data)) {
            result = hyper_inserter_buffer_add_bool(insertBuffer, 1);
        } else {
            result = hyper_inserter_buffer_add_bool(insertBuffer, 0);
        }
        break;
    }
    case DATETIME64_NS:
    case DATETIME64_NS_UTC: {
        hyper_date_components_t date_components = {
            .year = PyDateTime_GET_YEAR(data),
            .month = PyDateTime_GET_MONTH(data),
            .day = PyDateTime_GET_DAY(data)};

        hyper_time_components_t time_components = {
            .hour = PyDateTime_DATE_GET_HOUR(data),
            .minute = PyDateTime_DATE_GET_MINUTE(data),
            .second = PyDateTime_DATE_GET_SECOND(data),
            .microsecond = PyDateTime_DATE_GET_MICROSECOND(data)};

        hyper_date_t date = hyper_encode_date(date_components);
        hyper_time_t time = hyper_encode_time(time_components);

        int64_t val = time + (int64_t)date * MICROSECONDS_PER_DAY;

        result = hyper_inserter_buffer_add_int64(insertBuffer, val);
        break;
    }
    case TIMEDELTA64_NS: {
        // TODO: Add error message for failed attribute access
        PyObject *us = PyObject_GetAttrString(data, "microseconds");
        if (us == NULL) {
            return NULL;
        }
        PyObject *days = PyObject_GetAttrString(data, "days");
        if (days == NULL) {
            Py_DECREF(us);
            return NULL;
        }

        PyObject *months = PyObject_GetAttrString(data, "months");
        if (months == NULL) {
            Py_DECREF(us);
            Py_DECREF(days);
            return NULL;
        }

        py_interval interval = {.microseconds = PyLong_AsLongLong(us),
                                .days = PyLong_AsLong(days),
                                .months = PyLong_AsLong(months)};

        // TODO: it appears there is some buffer packing being done, though
        // not sure this actually works in Tableau
        result = hyper_inserter_buffer_add_raw(insertBuffer,
                                               (const unsigned char *)&interval,
                                               sizeof(py_interval));
        Py_DECREF(us);
        Py_DECREF(days);
        Py_DECREF(months);
        break;
    }
    case OBJECT: {
        // N.B. all other dtypes in pandas are well defined, but object is
        // really anything For purposes of Tableau these need to be strings,
        // so error out if not In the future should enforce StringDtype from
        // pandas once released (1.0.0)
        if (!PyUnicode_Check(data)) {
            PyObject *errMsg = PyUnicode_FromFormat(
                "Invalid value \"%R\" found (row %zd column %zd)", data, row,
                col);
            PyErr_SetObject(PyExc_TypeError, errMsg);
            Py_DECREF(errMsg);
            return NULL;
        }
        Py_ssize_t len;
        // TODO: CPython uses a const char* buffer but Hyper accepts
        // const unsigned char* - is this always safe?
        const unsigned char *buf =
            (const unsigned char *)PyUnicode_AsUTF8AndSize(data, &len);
        result = hyper_inserter_buffer_add_binary(insertBuffer, buf, len);
        break;
    }
    default: {
        PyObject *errMsg = PyUnicode_FromFormat("Invalid dtype: \"%s\"");
        PyErr_SetObject(PyExc_ValueError, errMsg);
        Py_DECREF(errMsg);
        return NULL;
    }
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
    PyObject *data, *iterator, *row, *val, *dtypes, *null_mask;
    Py_ssize_t row_counter, ncols;
    hyper_inserter_buffer_t *insertBuffer;
    hyper_error_t *result;
    Py_buffer buf;

    // TOOD: Find better way to accept buffer pointer than putting in long
    ok = PyArg_ParseTuple(args, "OOKnO!", &data, &null_mask, &insertBuffer,
                          &ncols, &PyTuple_Type, &dtypes);
    if (!ok)
        return NULL;

    if (!PyIter_Check(data)) {
        PyErr_SetString(PyExc_TypeError, "First argument must be iterable");
        return NULL;
    }

    if (!PyObject_CheckBuffer(null_mask)) {
        PyErr_SetString(PyExc_TypeError,
                        "Second argument must support buffer protocol");
        return NULL;
    }

    iterator = PyObject_GetIter(data);
    if (iterator == NULL)
        return NULL;

    if (PyObject_GetBuffer(null_mask, &buf, PyBUF_CONTIG_RO | PyBUF_FORMAT) <
        0) {
        Py_DECREF(iterator);
        return NULL;
    }

    if (buf.ndim != 2) {
        Py_DECREF(iterator);
        PyBuffer_Release(&buf);
        PyErr_SetString(PyExc_ValueError, "null_mask must be 2D");
        return NULL;
    }

    if (strncmp(buf.format, "?", 1) != 0) {
        Py_DECREF(iterator);
        PyBuffer_Release(&buf);
        PyErr_SetString(PyExc_ValueError, "null_mask must be boolean");
        return NULL;
    }

    DTYPE *enumerated_dtypes = makeEnumeratedDtypes((PyTupleObject *)dtypes);
    row_counter = 0;
    Py_ssize_t item_counter =
        0; // Needed as pointer arith doesn't work for void * buf
    while ((row = PyIter_Next(iterator))) {
        // TODO: Add validation that the total length of all elements
        //  matches the length of the null buffer, otherwise wrong data
        //  is returned
        for (Py_ssize_t i = 0; i < ncols; i++) {
            if (((uint8_t *)buf.buf)[item_counter++] == 1) {
                result = hyper_inserter_buffer_add_null(insertBuffer);
            } else {
                val = PyTuple_GET_ITEM(row, i);
                result = writeNonNullData(val, enumerated_dtypes[i],
                                          insertBuffer, row_counter, i);
            }

            if ((result != NULL) || (PyErr_Occurred())) {
                free(enumerated_dtypes);
                Py_DECREF(row);
                Py_DECREF(iterator);
                PyBuffer_Release(&buf);
                return NULL;
            }
        }
        Py_DECREF(row);
        row_counter += 1;
    }

    free(enumerated_dtypes);
    Py_DECREF(iterator);
    PyBuffer_Release(&buf);

    if (PyErr_Occurred())
        return NULL;

    Py_RETURN_NONE;
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

PyMODINIT_FUNC PyInit_libwriter(void) {
    PyDateTime_IMPORT;
    return PyModule_Create(&writermodule);
}
