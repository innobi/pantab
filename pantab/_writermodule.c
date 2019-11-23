#define PY_SSIZE_T_CLEAN
#include <Python.h>


// This function gets performance by sacrificing bounds checking
// Particulary no checking happens that the length of each iterable
// in data matches the length of the callables supplied at every step
// in the process,though note that this is critical!
static PyObject *write_to_hyper(PyObject *dummy, PyObject *args) {
  int ok;
  PyObject *data, *funcTuple, *iterator, *row, *insertFunc, *val, *result;
  const Py_ssize_t columnLen = PyTuple_Size(funcTuple);
  
  ok = PyArg_ParseTuple(args, "OO!", &data, &PyTuple_Type, &funcTuple);

  if (!ok)
    return NULL;

  if (!PyIter_Check(data)) {
    PyErr_SetString(PyExc_ValueError, "First argument must be iterable");
    return NULL;
  }

  for (Py_ssize_t i = 0; i < columnLen; i++) {
    if (!PyCallable_Check(PyTuple_GET_ITEM(funcTuple, i))) {
      PyErr_SetString(PyExc_ValueError, "Supplied argument must contain all callables");
      return NULL;
    }
  }

  iterator = PyObject_GetIter(data);
  if (iterator == NULL)
    return NULL;

  while ((row = PyIter_Next(iterator))) {
    // Undefined behaviof if the number of tuple elements doens't match callable list length
    for (Py_ssize_t i = 0; i < columnLen; i++) {
      val = PyTuple_GET_ITEM(row, i);
      insertFunc = PyTuple_GET_ITEM(funcTuple, i);

      result = PyObject_CallFunction(insertFunc, "O", val);

      if (result == NULL) {
	Py_DECREF(row);
	Py_DECREF(iterator);
	return NULL;
      }
      Py_DECREF(result);
    }
    Py_DECREF(row);
  }

  Py_DECREF(iterator);

  if (PyErr_Occurred())
    return NULL;
  
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
  return PyModule_Create(&writermodule);
}
