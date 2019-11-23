#define PY_SSIZE_T_CLEAN
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

#include <Python.h>
#include <numpy/arrayobject.h>


static PyObject *write_to_hyper(PyObject *dummy, PyObject *args) {
  int ok;
  PyArrayObject *array;
  PyObject *funcList;
  NpyIter* iter;
  NpyIter_IterNextFunc *iternext;
  int nonzero_count = 0;
  
  ok = PyArg_ParseTuple(args, "O!O!", &PyArray_Type, &array, &PyList_Type, &funcList);

  if (!ok)
    return NULL;

  // Sanity check our objects
  if (!(PyArray_NDIM(array) == 2)) {
    PyErr_SetString(PyExc_ValueError, "Must supply a 2D array");
    return NULL;
  }

  if (PyArray_SHAPE(array)[1] != PyList_Size(funcList)) {
    PyErr_SetString(PyExc_ValueError, "Number of columns in supplied data must match value list");
    return NULL;
  }

  // Do nothing for unsized arrays
  if (PyArray_SIZE(array) == 0) {
    return Py_None;
  }

  iter = NpyIter_New(array, NPY_ITER_READONLY, NPY_KEEPORDER, NPY_NO_CASTING, NULL);
  if (iter == NULL)
    return NULL;

  iternext = NpyIter_GetIterNext(iter, NULL);
  if (iternext == NULL) {
    NpyIter_Deallocate(iter);
    return NULL;
  }

  do {
    nonzero_count += 1;
    /* Increment the iterator to the next inner loop */
  } while(iternext(iter));

   NpyIter_Deallocate(iter);

   printf("I have %d elements\n", nonzero_count);
  
  return Py_None;
}

static PyMethodDef WriterMethods[] = {
				      {"write_to_hyper", write_to_hyper, METH_VARARGS,
				       "Writes a numpy array to a hyper file."},
				      {NULL, NULL, 0, NULL}
};

  
static struct PyModuleDef writermodule = {
					  PyModuleDef_HEAD_INIT,
					  "libwriter", // Name of module
					  NULL, // Documentation
					  -1, // keep state in global variables
					  WriterMethods
};


PyMODINIT_FUNC
PyInit_libwriter(void) {
  import_array();
  return PyModule_Create(&writermodule);
}
