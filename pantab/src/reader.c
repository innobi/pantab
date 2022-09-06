#include "cffi.h"
#include "type.h"
#include <datetime.h>

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
  case BOOLEANNA:
    return PyBool_FromLong(*value);

  case FLOAT32_:
  case FLOAT64_:
  case FLOAT32NA:
  case FLOAT64NA:
    return PyFloat_FromDouble(*((double *)value));

  case STRING:
  case OBJECT:
    return PyUnicode_FromStringAndSize((const char *)value, *size);

  case DATE: {
    hyper_date_components_t date = hyper_decode_date(*((hyper_date_t *)value));
    return PyDate_FromDate(date.year, date.month, date.day);
  }

  case DATETIME64_NS:
  case DATETIME64_NS_UTC: {
    hyper_time_t val = *((hyper_time_t *)value);

    hyper_date_t encoded_date =
        (hyper_date_t)(val / (hyper_time_t)MICROSECONDS_PER_DAY);
    hyper_time_t encoded_time = val % (hyper_time_t)MICROSECONDS_PER_DAY;
    hyper_date_components_t date = hyper_decode_date(encoded_date);
    hyper_time_components_t time = hyper_decode_time(encoded_time);

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
      PyObject *errMsg =
          PyUnicode_FromFormat("Cannot read Intervals with month components.");
      PyErr_SetObject(PyExc_ValueError, errMsg);
      Py_DECREF(errMsg);
      return NULL;
    }

    PyObject *kwargs = PyDict_New();
    if (kwargs == NULL)
      return NULL;

    PyDict_SetItemString(kwargs, "days", PyLong_FromLongLong(interval.days));
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

PyObject *read_hyper_query(PyObject *Py_UNUSED(dummy), PyObject *args) {
  int ok;
  PyObject *row = NULL, *resultObj;
  PyTupleObject *dtypes;
  hyper_rowset_t *rowset;
  hyper_rowset_chunk_t *chunk;
  hyper_error_t *hyper_err;
  size_t num_cols, num_rows;
  const uint8_t *const *values;
  const size_t *sizes;

  PyDateTime_IMPORT;

  ok = PyArg_ParseTuple(args, "OO!", &resultObj, &PyTuple_Type, &dtypes);
  if (!ok)
    return NULL;

  // TODO: check that we get an instance of CDataObject; else will segfault
  rowset = (hyper_rowset_t *)((CDataObject *)resultObj)->c_data;

  // TODO: we need to free these somewhere as these currently leak...
  DTYPE *enumeratedDtypes = makeEnumeratedDtypes(dtypes);
  if (enumeratedDtypes == NULL)
    return NULL;

  PyObject *result = PyList_New(0);
  if (result == NULL) {
    return NULL;
  }

  // Iterate over each result chunk
  while (1) {

    hyper_err = hyper_rowset_get_next_chunk(rowset, &chunk);
    if (hyper_err) {
      goto ERROR_CLEANUP;
    }

    if (chunk == NULL) {
      break; // No more to parse
    }

    hyper_rowset_chunk_field_values(chunk, &num_cols, &num_rows, &values,
                                    &sizes);

    // For each row inside the chunk...
    for (size_t i = 0; i < num_rows; i++) {
      row = PyTuple_New(num_cols);
      if (row == NULL) {
        goto ERROR_CLEANUP;
      }

      // For each column inside the row...
      for (size_t j = 0; j < num_cols; j++) {
        PyObject *val;
        if (*values == NULL) {
          val = Py_None;
          Py_INCREF(val);
        } else {
          DTYPE dtype = enumeratedDtypes[j];
          val = read_value(*values, dtype, sizes);
        }

        values++, sizes++;

        if (val == NULL) {
          goto ERROR_CLEANUP;
        }

        PyTuple_SET_ITEM(row, j, val);
      }

      int ret = PyList_Append(result, row);
      if (ret != 0) {
        goto ERROR_CLEANUP;
      }
    }
    hyper_destroy_rowset_chunk(chunk);
  }

  Py_XDECREF(cls_timedelta);

  return result;

ERROR_CLEANUP:
  Py_XDECREF(row);
  Py_XDECREF(result);
  Py_XDECREF(cls_timedelta);
  if (chunk != NULL)
    hyper_destroy_rowset_chunk(chunk);

  return NULL;
}
