/* This header file is copied directly from cffi to allow interaction
with cffi C-level objects without including the entire library.

cffi is licensed under the MIT license, with originaly copyright included
below:

Except when otherwise stated (look for LICENSE files in directories or
information at the beginning of each file) all software and
documentation is licensed as follows:

    The MIT License

    Permission is hereby granted, free of charge, to any person
    obtaining a copy of this software and associated documentation
    files (the "Software"), to deal in the Software without
    restriction, including without limitation the rights to use,
    copy, modify, merge, publish, distribute, sublicense, and/or
    sell copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included
    in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
    OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
    THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
    DEALINGS IN THE SOFTWARE.
*/

#ifndef PANTAB_CFFI_H
#define PANTAB_CFFI_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

typedef struct _ctypedescr {
    PyObject_VAR_HEAD

        struct _ctypedescr *ct_itemdescr; /* ptrs and arrays: the item type */
    PyObject *ct_stuff;                   /* structs: dict of the fields
                                             arrays: ctypedescr of the ptr type
                                             function: tuple(abi, ctres, ctargs..)
                                             enum: pair {"name":x},{x:"name"}
                                             ptrs: lazily, ctypedescr of array */
    void *ct_extra;                       /* structs: first field (not a ref!)
                                             function types: cif_description
                                             primitives: prebuilt "cif" object */

    PyObject *ct_weakreflist; /* weakref support */

    PyObject *ct_unique_key; /* key in unique_cache (a string, but not
                                human-readable) */

    Py_ssize_t ct_size;   /* size of instances, or -1 if unknown */
    Py_ssize_t ct_length; /* length of arrays, or -1 if unknown;
                             or alignment of primitive and struct types;
                             always -1 for pointers */
    int ct_flags;         /* CT_xxx flags */

    int ct_name_position; /* index in ct_name of where to put a var name */
    char ct_name[1];      /* string, e.g. "int *" for pointers to ints */
} CTypeDescrObject;

typedef struct {
    PyObject_HEAD CTypeDescrObject *c_type;
    char *c_data;
    PyObject *c_weakreflist;
} CDataObject;

#endif
