#include "cffi.h"
#include "type.h"
#include <datetime.h>
#define NO_IMPORT_ARRAY
#define PY_ARRAY_UNIQUE_SYMBOL PANTAB_ARRAY_API
#include "numpy_datetime.h"
#include <numpy/arrayobject.h>
#include <numpy/arrayscalars.h>

/*
Creates an array of NpyIter structs in the same order as the arrays supplied.

Caller is responsible for freeing memory. Returns NULL on error
*/
static NpyIter **initiateIters(PyObject *arrList) {
  NpyIter **npyIters =
      PyObject_Malloc(sizeof(NpyIter *) * PyObject_Length(arrList));
  if (npyIters == NULL) {
    PyErr_NoMemory();
    return NULL;
  }

  for (Py_ssize_t i = 0; i < PyObject_Length(arrList); i++) {
    PyArrayObject *arr = (PyArrayObject *)PyList_GET_ITEM(arrList, i);

    // Check contents of each numpy array
    NpyIter *iter = NpyIter_New(arr, NPY_ITER_READONLY | NPY_ITER_REFS_OK,
                                NPY_KEEPORDER, NPY_NO_CASTING, NULL);

    // TODO: do we need to check NpyIter_IterationNeedsAPI(iter) anywhere?
    // Applicable because of NPY_ITER_REFS_OK flags
    if (iter == NULL) {
      if (i > 0) {
        while (--i) {
          NpyIter_Deallocate(npyIters[i]);
        }
      }

      PyErr_NoMemory();
      return NULL;
    }

    npyIters[i] = iter;
  }

  return npyIters;
}

/* Initiate iters outside of any loop for performance.
   Caller is responsible for releasing memory.

   Returns NULL on error
*/
static NpyIter_IterNextFunc **initiateIterNextFuncs(NpyIter **npyIters,
                                                    Py_ssize_t len) {
  NpyIter_IterNextFunc **npyIterNextFuncs =
      PyObject_Malloc(sizeof(NpyIter_IterNextFunc *) * len);
  if (npyIterNextFuncs == NULL) {
    PyErr_NoMemory();
    return NULL;
  }

  for (Py_ssize_t i = 0; i < len; i++) {
    NpyIter_IterNextFunc *func = NpyIter_GetIterNext(npyIters[i], NULL);
    if (func == NULL) {
      return NULL;
    }

    npyIterNextFuncs[i] = func;
  }

  return npyIterNextFuncs;
}

static char ***initiateDataPtrs(NpyIter **npyIters, Py_ssize_t len) {
  char ***dataptrs = PyObject_Malloc(sizeof(char **) * len);
  if (dataptrs == NULL) {
    PyErr_NoMemory();
    return NULL;
  }

  for (Py_ssize_t i = 0; i < len; i++) {
    char **dataptr = NpyIter_GetDataPtrArray(npyIters[i]);
    if (dataptr == NULL) {
      return NULL;
    }

    dataptrs[i] = dataptr;
  }

  return dataptrs;
}

/*
Free an array of numpy array iterators.

TODO: dynamically calculate how many to free rather than require length as arg
*/
static void freeIters(NpyIter **iters, Py_ssize_t length) {
  for (Py_ssize_t i = 0; i < length; i++) {
    NpyIter_Deallocate(iters[i]);
  }
}

static hyper_error_t *writeNonNullData(char **dataptr, DTYPE dtype,
                                       hyper_inserter_buffer_t *insertBuffer,
                                       Py_ssize_t row, Py_ssize_t col) {
  hyper_error_t *result;
  switch (dtype) {
  case INT16_: {
    int16_t **ptr = (int16_t **)dataptr;
    int16_t val = **ptr;
    result = hyper_inserter_buffer_add_int16(insertBuffer, val);
    break;
  }
  case INT16NA: {
    PyObject ***ptr = (PyObject ***)dataptr;
    // The fact that NA datatypes are stored as objects is a bit
    // unfortunate for sizing, as the CPython API only exposes
    // Long / LongLong data types
    PyObject *obj = **ptr;
    long val = PyLong_AsLong(obj);
    result = hyper_inserter_buffer_add_int16(insertBuffer, (int16_t)val);
    break;
  }
  case INT32_: {
    int32_t **ptr = (int32_t **)dataptr;
    int32_t val = **ptr;
    result = hyper_inserter_buffer_add_int32(insertBuffer, val);
    break;
  }
  case INT32NA: {
    PyObject ***ptr = (PyObject ***)dataptr;
    PyObject *obj = **ptr;
    long val = PyLong_AsLong(obj);
    result = hyper_inserter_buffer_add_int32(insertBuffer, val);
    break;
  }
  case INT64_: {
    int64_t **ptr = (int64_t **)dataptr;
    int64_t val = **ptr;
    result = hyper_inserter_buffer_add_int64(insertBuffer, val);
    break;
  }
  case INT64NA: {
    PyObject ***ptr = (PyObject ***)dataptr;
    PyObject *obj = **ptr;
    long long val = PyLong_AsLongLong(obj);
    result = hyper_inserter_buffer_add_int64(insertBuffer, val);
    break;
  }
  case FLOAT32_: {
    float_t **ptr = (float_t **)dataptr;
    float_t val = **ptr;
    result = hyper_inserter_buffer_add_double(insertBuffer, val);
    break;
  }
  case FLOAT64_: {
    double_t **ptr = (double_t **)dataptr;
    double_t val = **ptr;
    result = hyper_inserter_buffer_add_double(insertBuffer, val);
    break;
  }
  case FLOAT32NA:
  case FLOAT64NA: {
    PyObject ***ptr = (PyObject ***)dataptr;
    PyObject *obj = **ptr;
    double_t val = PyFloat_AsDouble(obj);
    result = hyper_inserter_buffer_add_double(insertBuffer, val);
    break;
  }
  case BOOLEAN: {
    npy_bool **ptr = (npy_bool **)dataptr;
    npy_bool val = **ptr;
    result = hyper_inserter_buffer_add_bool(insertBuffer, val);
    break;
  }
  case BOOLEANNA: {
    PyObject ***ptr = (PyObject ***)dataptr;
    PyObject *obj = **ptr;
    int val = obj == Py_True;
    result = hyper_inserter_buffer_add_bool(insertBuffer, val);
    break;
  }
  case DATETIME64_NS:
  case DATETIME64_NS_UTC: {
    npy_datetime **ptr = (npy_datetime **)dataptr;
    npy_datetime val = **ptr;

    npy_datetimestruct dts;

    // TODO: here we are using dummy metadata, but ideally
    // should get from array in case pandas ever allows for
    // different precision datetimes
    PyArray_DatetimeMetaData meta = {.base = NPY_FR_ns, .num = 1};
    int ret = convert_datetime_to_datetimestruct(&meta, val, &dts);
    if (ret != 0) {
      PyObject *errMsg =
          PyUnicode_FromFormat("Failed to convert numpy datetime");
      PyErr_SetObject(PyExc_RuntimeError, errMsg);
      Py_DECREF(errMsg);
      return NULL;
    }

    hyper_date_components_t date_components = {
        .year = dts.year, .month = dts.month, .day = dts.day};

    hyper_time_components_t time_components = {.hour = dts.hour,
                                               .minute = dts.min,
                                               .second = dts.sec,
                                               .microsecond = dts.us};

    hyper_date_t date = hyper_encode_date(date_components);
    hyper_time_t time = hyper_encode_time(time_components);

    // TODO: Tableau uses typedefs for unsigned 32 / 64 integers for
    // date and time respectively, but stores as int64; here we cast
    // explicitly but should probably bounds check for overflow as well
    int64_t ms = (int64_t)time + (int64_t)date * MICROSECONDS_PER_DAY;

    result = hyper_inserter_buffer_add_int64(insertBuffer, ms);
    break;
  }
  case TIMEDELTA64_NS: {
    PyObject ***ptr = (PyObject ***)dataptr;
    PyObject *data = **ptr;

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
        insertBuffer, (const unsigned char *)&interval, sizeof(py_interval));
    Py_DECREF(us);
    Py_DECREF(days);
    Py_DECREF(months);
    break;
  }
  case STRING:
  case OBJECT: {
    PyObject ***ptr = (PyObject ***)dataptr;
    PyObject *obj = **ptr;
    if (dtype == OBJECT) {
      // N.B. all other dtypes in pandas are well defined, but object is
      // really anything For purposes of Tableau these need to be strings,
      // so error out if not In the future should enforce StringDtype from
      // pandas once released (1.0.0)
      if (!PyUnicode_Check(obj)) {
        PyObject *errMsg = PyUnicode_FromFormat(
            "Invalid value \"%R\" found (row %zd column %zd)", obj, row, col);
        PyErr_SetObject(PyExc_TypeError, errMsg);
        Py_DECREF(errMsg);
        return NULL;
      }
    }
    Py_ssize_t len;
    // TODO: CPython uses a const char* buffer but Hyper accepts
    // const unsigned char* - is this always safe?
    const unsigned char *buf =
        (const unsigned char *)PyUnicode_AsUTF8AndSize(obj, &len);
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

// TODO: Make error handling consistent. Right now errors occur if
// 1. The return value is non-NULL OR
// 2. PyErr is set within this function
static hyper_error_t *
writeNonNullDataLegacy(PyObject *data, DTYPE dtype,
                       hyper_inserter_buffer_t *insertBuffer, Py_ssize_t row,
                       Py_ssize_t col) {
  hyper_error_t *result;
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
  case FLOAT64_:
  case FLOAT32NA:
  case FLOAT64NA: {
    double val = PyFloat_AsDouble(data);
    result = hyper_inserter_buffer_add_double(insertBuffer, val);
    break;
  }
  case BOOLEAN:
  case BOOLEANNA: {
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

    // TODO: Tableau uses typedefs for unsigned 32 / 64 integers for
    // date and time respectively, but stores as int64; here we cast
    // explicitly but should probably bounds check for overflow as well
    int64_t val = (int64_t)time + (int64_t)date * MICROSECONDS_PER_DAY;

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
    result = hyper_inserter_buffer_add_raw(
        insertBuffer, (const unsigned char *)&interval, sizeof(py_interval));
    Py_DECREF(us);
    Py_DECREF(days);
    Py_DECREF(months);
    break;
  }
  case STRING:
  case OBJECT: {
    if (dtype == OBJECT) {
      // N.B. all other dtypes in pandas are well defined, but object is
      // really anything For purposes of Tableau these need to be strings,
      // so error out if not In the future should enforce StringDtype from
      // pandas once released (1.0.0)
      if (!PyUnicode_Check(data)) {
        PyObject *errMsg = PyUnicode_FromFormat(
            "Invalid value \"%R\" found (row %zd column %zd)", data, row, col);
        PyErr_SetObject(PyExc_TypeError, errMsg);
        Py_DECREF(errMsg);
        return NULL;
      }
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
PyObject *write_to_hyper_legacy(PyObject *Py_UNUSED(dummy), PyObject *args) {
  int ok;
  PyObject *data, *iterator, *row, *val, *dtypes, *null_mask, *insertBufferObj;
  Py_ssize_t row_counter, ncols;
  hyper_inserter_buffer_t *insertBuffer;
  hyper_error_t *result;
  Py_buffer buf;

  PyDateTime_IMPORT;

  // TOOD: Find better way to accept buffer pointer than putting in long
  ok = PyArg_ParseTuple(args, "OOOnO!", &data, &null_mask, &insertBufferObj,
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

  // TODO: check that we get an instance of CDataObject; else will segfault
  insertBuffer =
      (hyper_inserter_buffer_t *)((CDataObject *)insertBufferObj)->c_data;

  iterator = PyObject_GetIter(data);
  if (iterator == NULL)
    return NULL;

  if (PyObject_GetBuffer(null_mask, &buf, PyBUF_CONTIG_RO | PyBUF_FORMAT) < 0) {
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
        result = writeNonNullDataLegacy(val, enumerated_dtypes[i], insertBuffer,
                                        row_counter, i);
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

PyObject *write_to_hyper(PyObject *Py_UNUSED(dummy), PyObject *args) {
  int ok, success = 1;
  PyObject *df, *dtypes, *null_mask, *insertBufferObj;
  hyper_inserter_buffer_t *insertBuffer;
  hyper_error_t *result;
  Py_buffer buf;

  // TOOD: Find better way to accept buffer pointer than putting in long
  ok = PyArg_ParseTuple(args, "OOOO!", &df, &null_mask, &insertBufferObj,
                        &PyTuple_Type, &dtypes);
  if (!ok)
    return NULL;

  if (!PyObject_CheckBuffer(null_mask)) {
    PyErr_SetString(PyExc_TypeError,
                    "Second argument must support buffer protocol");
    return NULL;
  }

  // TODO: check that we get an instance of CDataObject; else will segfault
  insertBuffer =
      (hyper_inserter_buffer_t *)((CDataObject *)insertBufferObj)->c_data;

  if (PyObject_GetBuffer(null_mask, &buf, PyBUF_CONTIG_RO | PyBUF_FORMAT) < 0) {
    return NULL;
  }

  if (buf.ndim != 2) {
    PyBuffer_Release(&buf);
    PyErr_SetString(PyExc_ValueError, "null_mask must be 2D");
    return NULL;
  }

  if (strncmp(buf.format, "?", 1) != 0) {
    PyBuffer_Release(&buf);
    PyErr_SetString(PyExc_ValueError, "null_mask must be boolean");
    return NULL;
  }

  DTYPE *enumerated_dtypes = makeEnumeratedDtypes((PyTupleObject *)dtypes);

  PyObject *mgr = PyObject_GetAttrString(df, "_mgr");
  if (mgr == NULL) {
    PyBuffer_Release(&buf);
    free(enumerated_dtypes);
    return NULL;
  }

  PyObject *arrList = PyObject_GetAttrString(mgr, "column_arrays");
  Py_DECREF(mgr);
  if (arrList == NULL) {
    PyBuffer_Release(&buf);
    free(enumerated_dtypes);
    return NULL;
  }

  Py_ssize_t rowcount = PyObject_Length(df);
  Py_ssize_t colcount = PyObject_Length(arrList);
  Py_ssize_t bufPos;
  NpyIter **npyIters = initiateIters(arrList);
  Py_DECREF(arrList);

  if (npyIters == NULL) {
    PyBuffer_Release(&buf);
    free(enumerated_dtypes);
    return NULL;
  }
  NpyIter_IterNextFunc **npyIterNextFuncs =
      initiateIterNextFuncs(npyIters, colcount);
  if (npyIterNextFuncs == NULL) {
    success = 0;
    goto CLEANUP;
  }

  char ***dataptrs = initiateDataPtrs(npyIters, colcount);
  if (dataptrs == NULL) {
    success = 0;
    goto CLEANUP;
  }

  NpyIter *iter;
  NpyIter_IterNextFunc *iternext;
  char **dataptr;

  for (Py_ssize_t rowIndex = 0; rowIndex < rowcount; rowIndex++) {
    for (Py_ssize_t colIndex = 0; colIndex < colcount; colIndex++) {
      bufPos = (rowIndex * colcount) + colIndex;
      iter = npyIters[colIndex];
      iternext = npyIterNextFuncs[colIndex];
      dataptr = dataptrs[colIndex];
      if (((uint8_t *)buf.buf)[bufPos] == 1) {
        result = hyper_inserter_buffer_add_null(insertBuffer);
      } else {
        result = writeNonNullData(dataptr, enumerated_dtypes[colIndex],
                                  insertBuffer, rowIndex, colIndex);
      }
      iternext(iter);

      if ((result != NULL) || (PyErr_Occurred())) {
        success = 0;
        goto CLEANUP;
      }
    }
  }

CLEANUP:
  freeIters(npyIters, colcount);
  free(enumerated_dtypes);
  PyBuffer_Release(&buf);

  if (success)
    Py_RETURN_NONE;
  else
    return NULL;
}
