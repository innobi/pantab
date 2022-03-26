#define PY_SSIZE_T_CLEAN
#include <Python.h>
#define PY_ARRAY_UNIQUE_SYMBOL PANTAB_ARRAY_API
#include <numpy/arrayobject.h>

#include "cffi.h"
#include "reader.h"
#include "tableauhyperapi.h"
#include "writer.h"

// Function pointers, initialized by `load_hapi_functions` function
#define C(RET, NAME, ARGS) RET(*NAME) ARGS = NULL;
HYPERAPI_FUNCTIONS(C)
#undef C

static PyObject *load_hapi_functions(PyObject *Py_UNUSED(dummy),
                                     PyObject *args) {
  bool ok;
#define C(RET, NAME, ARGS) PyObject *NAME##_arg;
  HYPERAPI_FUNCTIONS(C)
#undef C
  const char *formatStr =
#define C(RET, NAME, ARGS) "O"
      HYPERAPI_FUNCTIONS(C)
#undef C
      ;

  ok = PyArg_ParseTuple(args, formatStr
#define C(RET, NAME, ARGS) , &NAME##_arg
                                  HYPERAPI_FUNCTIONS(C)
#undef C
  );
  if (!ok)
    return NULL;

    // TODO: check that we get an instance of CDataObject; else will
    // segfault
#define C(RET, NAME, ARGS)                                                     \
  NAME = (RET(*) ARGS)(((CDataObject *)NAME##_arg)->c_data);
  HYPERAPI_FUNCTIONS(C)
#undef C

  Py_RETURN_NONE;
}

static PyMethodDef methods[] = {
    {"load_hapi_functions", load_hapi_functions, METH_VARARGS,
     "Initializes the HyperAPI functions used by pantab."},
    {"write_to_hyper_legacy", write_to_hyper_legacy, METH_VARARGS,
     "Legacy method to Write a numpy array to a hyper file."},
    {"write_to_hyper", write_to_hyper, METH_VARARGS,
     "Writes a dataframe array to a hyper file."},
    {"read_hyper_query", read_hyper_query, METH_VARARGS,
     "Reads a hyper query from a given connection."},
    {NULL, NULL, 0, NULL}};

static struct PyModuleDef pantabmodule = {.m_base = PyModuleDef_HEAD_INIT,
                                          .m_name = "libpantab",
                                          .m_methods = methods};

PyMODINIT_FUNC PyInit_libpantab(void) {
  import_array();
  return PyModule_Create(&pantabmodule);
}
