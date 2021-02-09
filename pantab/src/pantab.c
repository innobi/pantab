#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "reader.h"
#include "writer.h"

static PyMethodDef methods[] = {
    {"write_to_hyper", write_to_hyper, METH_VARARGS,
     "Writes a numpy array to a hyper file."},
    {"read_hyper_query", read_hyper_query, METH_VARARGS,
     "Reads a hyper query from a given connection."},
    {NULL, NULL, 0, NULL}};

static struct PyModuleDef pantabmodule = {.m_base = PyModuleDef_HEAD_INIT,
                                          .m_name = "libpantab",
                                          .m_methods = methods};

PyMODINIT_FUNC PyInit_libpantab(void) {
    return PyModule_Create(&pantabmodule);
}
