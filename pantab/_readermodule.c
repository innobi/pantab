#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "tableauhyperapi.h"

static PyObject *read_hyper_query(PyObject *dummy, PyObject *args) {
  int ok;
  PyObject *row;
  hyper_connection_t *connection;
  hyper_rowset_t *rowset;
  hyper_rowset_chunk_t *chunk;
  const char *query;
  hyper_error_t *hyper_err;
  size_t num_cols, num_rows;
  const uint8_t * const * values;
  const size_t *sizes;
  const int8_t *null_flags;

  // TODO: support platforms where uintptr_t may not equal unsigned long long
  ok = PyArg_ParseTuple(args, "Ks", &connection, &query);
  if (!ok)
    return NULL;

  hyper_err = hyper_execute_query(connection, query, &rowset);
  if (hyper_err) {
    return NULL;
  }

  hyper_err = hyper_rowset_get_next_chunk(rowset, &chunk);
  if (hyper_err) {
    return NULL;
  }

  
  hyper_err = hyper_rowset_chunk_field_values(chunk, &num_cols, &num_rows, 
					      &values, &sizes, &null_flags);
  if (hyper_err) {
    return NULL;
  }
  

  PyObject *result = PyList_New(0);
  if (result == NULL) {
    return NULL;
  }
  
  for (size_t i = 0; i < num_rows; i++) {
    row = PyTuple_New(num_cols);
    if (row == NULL) {
      Py_DECREF(result);
      return NULL;
    }
    
    for (size_t j = 0; j < num_cols; j++) {
      PyObject *val = PyLong_FromLongLong(*(*values++));

      if (val == NULL) {
	Py_DECREF(result);
	Py_DECREF(row);
	return NULL;
      }
      
      PyTuple_SET_ITEM(row, j, val);
    }
    
    int ret = PyList_Append(result, row);
    if (ret != 0) {
      // Clean up any previously inserted elements
      for (Py_ssize_t i=0; i < PyList_GET_SIZE(result) - 1; i++) {
	PyObject *tup = PyList_GET_ITEM(result, i);
	for (Py_ssize_t j=0; j < PyTuple_GET_SIZE(tup); j++) {
	  Py_DECREF(PyTuple_GET_ITEM(tup, j));
	}
	
	Py_DECREF(tup);
      }

      // Clean up current elements
      for (Py_ssize_t j=0; j < PyTuple_GET_SIZE(row); j++) {
	Py_DECREF(PyTuple_GET_ITEM(row, j));
      }
      Py_DECREF(row);
      Py_DECREF(result);

      return NULL;
    }
  }

  if (PyErr_Occurred())
    return NULL;

  return result;
}

static PyMethodDef ReaderMethods[] = {{"read_hyper_query", read_hyper_query,
                                       METH_VARARGS,
                                       "Reads a hyper query from a given connection."},
                                      {NULL, NULL, 0, NULL}};

static struct PyModuleDef readermodule = {PyModuleDef_HEAD_INIT,
                                          "libreader", // Name of module
                                          NULL,        // Documentation
                                          -1, // keep state in global variables
                                          ReaderMethods};

PyMODINIT_FUNC PyInit_libreader(void) {
    return PyModule_Create(&readermodule);
}
