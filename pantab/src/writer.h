#ifndef PANTAB_WRITER_H
#define PANTAB_WRITER_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

PyObject *write_to_hyper(PyObject *Py_UNUSED(dummy), PyObject *args);
PyObject *write_to_hyper_new(PyObject *Py_UNUSED(dummy), PyObject *args);

#endif
