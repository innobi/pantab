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
  hyper_error_t *result;
  size_t num_cols, num_rows;
  const uint8_t * const * values;
  const size_t *sizes;
  const int8_t *null_flags;

  printf("parsing args\n");
  // TODO: support platforms where uintptr_t may not equal unsigned long long
  ok = PyArg_ParseTuple(args, "Ks", &connection, &query);
  printf("at least got here\n");
  if (!ok)
    return NULL;

  row = PyTuple_New(2);
  
  if (row == NULL)
    return NULL;

  printf("execute query\n");
  result = hyper_execute_query(connection, query, &rowset);
  if (result) {
    Py_DECREF(row);
    return NULL;
  }

  printf("next_chunk\n");
  result = hyper_rowset_get_next_chunk(rowset, &chunk);
  if (result) {
    Py_DECREF(row);
    return NULL;
  }

  
  result = hyper_rowset_chunk_field_values(chunk, &num_cols, &num_rows, 
					   &values, &sizes, &null_flags);
  if (result) {
    Py_DECREF(row);
    return NULL;
  }
  
  printf("number of rows: %lld\nnumber of columns: %lld\n", num_rows, num_cols);

  for (int i = 0; i < num_rows; i++) {
    for (int j = 0; j < num_cols; j++) {
      printf("row %d col %d value is: %lld\n", i, j, values[i][j]);
    }
  }

  Py_DECREF(row);
  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
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
