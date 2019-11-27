#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <datetime.h>
#include <inttypes.h>

#include "tableauhyperapi.h"

#define MICROSECONDS_PER_DAY 24 * 60 * 60 * 1000000

// datetime.timedelta has no C-API so need to convert manually
typedef struct {
    int64_t microseconds;
    int32_t days;
    int32_t months;
} py_interval;

static int isNull(PyObject *data) {
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
static hyper_error_t *write_data_for_dtype(PyObject *data, PyObject *dtype,
                                    hyper_inserter_buffer_t *insertBuffer,
                                    Py_ssize_t row, Py_ssize_t col) {
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
    } else if (strcmp(dtypeStr, "datetime64[ns]") == 0) {
        if (isNull(data)) {
            result = hyper_inserter_buffer_add_null(insertBuffer);
        } else {
            hyper_date_components_t date_components = {
                .year = PyDateTime_GET_YEAR(data),
                .month = PyDateTime_GET_MONTH(data),
                .day = PyDateTime_GET_DAY(data)};

            hyper_time_components_t time_components = {
                .hour = (int8_t)PyDateTime_DATE_GET_HOUR(data),
                .minute = (int8_t)PyDateTime_DATE_GET_MINUTE(data),
                .second = (int8_t)PyDateTime_DATE_GET_SECOND(data),
                .microsecond = (int32_t)PyDateTime_DATE_GET_MICROSECOND(data)};

            hyper_date_t date = hyper_encode_date(date_components);
            hyper_time_t time = hyper_encode_time(time_components);

            int64_t val = time + (int64_t)date * MICROSECONDS_PER_DAY;
            result = hyper_inserter_buffer_add_int64(insertBuffer, val);
        }
    } else if (strcmp(dtypeStr, "datetime64[ns, UTC]") == 0) {
        if (isNull(data)) {
            result = hyper_inserter_buffer_add_null(insertBuffer);
        } else {
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
        }
    } else if (strcmp(dtypeStr, "timedelta64[ns]") == 0) {
        if (isNull(data)) {
            result = hyper_inserter_buffer_add_null(insertBuffer);
        } else {
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
            result = hyper_inserter_buffer_add_raw(
                insertBuffer, (const unsigned char *)&interval,
                sizeof(py_interval));
            Py_DECREF(us);
            Py_DECREF(days);
            Py_DECREF(months);
        }
    } else if (strcmp(dtypeStr, "object") == 0) {
        if (isNull(data)) {
            result = hyper_inserter_buffer_add_null(insertBuffer);
        } else {
            // N.B. all other dtypes in pandas are well defined, but object is
            // really anything For purposes of Tableau these need to be strings,
            // so error out if not In the future should enforce StringDtype from
            // pandas once released (1.0.0)
            if (!PyUnicode_Check(data)) {
                PyObject *errMsg = PyUnicode_FromFormat(
                    "Invalid value \"%R\" found (row %zd column %zd)", data,
                    row, col);
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
            result =
                write_data_for_dtype(val, dtype, insertBuffer, row_counter, i);

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

PyMODINIT_FUNC PyInit_libwriter(void) {
    PyDateTime_IMPORT;
    return PyModule_Create(&writermodule);
}
