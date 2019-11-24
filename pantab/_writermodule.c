#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "tableauhyperapi.h"


// Returns reference the appropriate insert function, NULL on error
void * function_for_type(PyObject *obj) {
  const char *dtype = PyUnicode_AsUTF8(obj);
  
  if (strcmp(dtype, "int16") == 0)
    return &hyper_inserter_buffer_add_int16;
  else if (strcmp(dtype, "int32") == 0)  
    return &hyper_inserter_buffer_add_int32;
  else if (strcmp(dtype, "int64") == 0)
    return &hyper_inserter_buffer_add_int64;
  if (strcmp(dtype, "Int16") == 0)
    return &hyper_inserter_buffer_add_int16;
  else if (strcmp(dtype, "Int32") == 0)  
    return &hyper_inserter_buffer_add_int32;
  else if (strcmp(dtype, "Int64") == 0)
    return &hyper_inserter_buffer_add_int64;
  else if (strcmp(dtype, "float32") == 0)  
    return &hyper_inserter_buffer_add_double;
  else if (strcmp(dtype, "float64") == 0)  
    return &hyper_inserter_buffer_add_double;
  else if (strcmp(dtype, "bool") == 0)    
    return &hyper_inserter_buffer_add_bool;
  
  PyObject *errMsg = PyUnicode_FromFormat("Invalid dtype: \"%s\"");
  PyErr_SetObject(PyExc_ValueError, errMsg);
  Py_DECREF(errMsg);

  return NULL;
}

// This function gets performance by sacrificing bounds checking
// Particulary no checking happens that the length of each iterable
// in data matches the length of the callables supplied at every step
// in the process,though note that this is critical!
// If this doesn't hold true behavior is undefined
static PyObject *write_to_hyper(PyObject *dummy, PyObject *args) {
    int ok;
    PyObject *data, *iterator, *row, *val, *dtypes;
    Py_ssize_t row_counter, ncols;
    hyper_inserter_buffer_t *insertBuffer;
    hyper_error_t *result, *fnPtr;
    void *funcList;  // array matching ncols length holding appropriate function

    // TOOD: Find better way to accept buffer pointer than putting in long
    ok = PyArg_ParseTuple(args, "OlnO!", &data, &insertBuffer, &ncols, &PyTuple_Type, &dtypes);
    if (!ok)
        return NULL;

    if (!PyIter_Check(data)) {
        PyErr_SetString(PyExc_TypeError, "First argument must be iterable");
        return NULL;
    }

    iterator = PyObject_GetIter(data);
    if (iterator == NULL)
        return NULL;

    funcList = malloc(sizeof(void *) * ncols);
    for (Py_ssize_t i = 0; i < ncols; i++) {
      fnPtr = function_for_type(PyTuple_GET_ITEM(dtypes, i));
      if (fnPtr == NULL) {
	free(funcList);
	return NULL;
      }
      funcList[i] = fnPtr;
    }
    
    row_counter = 0;
    while ((row = PyIter_Next(iterator))) {
        // Undefined behavior if the number of tuple elements does't match
        // callable list length
        for (Py_ssize_t i = 0; i < ncols; i++) {
            val = PyTuple_GET_ITEM(row, i);
            if ((val == Py_None) ||
                (PyFloat_Check(val) && isnan(PyFloat_AS_DOUBLE(val)))) {
	      result = hyper_inserter_buffer_add_null(insertBuffer);
	    }
            else {
	      result = hyper_inserter_buffer_add_int64(insertBuffer, 1);	      
	    }

	    if (result != NULL) {
	      PyObject *errMsg = PyUnicode_FromFormat(
		 "Invalid value \"%S\" found (row %zd column %zd)", val,
		 row_counter, i);
	      PyErr_SetObject(PyExc_TypeError, errMsg);
	      Py_DECREF(errMsg);
	      return NULL;
	    }
        }
        Py_DECREF(row);
        row_counter += 1;
    }

    free(funcList);
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
