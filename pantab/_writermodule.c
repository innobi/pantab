#define PY_SSIZE_T_CLEAN
#include <Python.h>


// This function gets performance by sacrificing bounds checking
// Do not use unless you really understand it...
static PyObject *write_to_hyper(PyObject *dummy, PyObject *args) {
  int ok;
  PyObject *data, *funcTuple, *iterator, *row, *insertFunc, *val, *arglist, *result;
  
  ok = PyArg_ParseTuple(args, "OO!", &data, &PyTuple_Type, &funcTuple);

  if (!ok)
    return NULL;

  if (!PyIter_Check(data)) {
    PyErr_SetString(PyExc_ValueError, "First argument must be iterable");
    return NULL;
  }

  for (Py_ssize_t i = 0; i < PyTuple_Size(funcTuple); i++) {
    if (!PyCallable_Check(PyTuple_GET_ITEM(funcTuple, i))) {
      PyErr_SetString(PyExc_ValueError, "Supplied argument must contain all callables");
      return NULL;
    }
  }

  iterator = PyObject_GetIter(data);
  if (iterator == NULL)
    return NULL;

  while ((row = PyIter_Next(iterator))) {
    for (Py_ssize_t i = 0; i < PyTuple_Size(row); i++) {
      val = PyTuple_GET_ITEM(row, i);
      insertFunc = PyTuple_GET_ITEM(funcTuple, i);

      arglist = Py_BuildValue("(O)", val);
      result = PyObject_CallObject(insertFunc, arglist);
      Py_DECREF(arglist);

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
