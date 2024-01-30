/*
 * This file is derived from NumPy 1.20. See NUMPY_LICENSE.txt
 */

#ifndef _NPY_PRIVATE__DATETIME_H_
#define _NPY_PRIVATE__DATETIME_H_
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/ndarraytypes.h>

/*
 * Converts a datetime based on the given metadata into a datetimestruct
 */
int convert_datetime_to_datetimestruct(PyArray_DatetimeMetaData *meta,
                                       npy_datetime dt,
                                       npy_datetimestruct *out);

/*
 * Converts a datetime from a datetimestruct to a datetime based
 * on some metadata. The date is assumed to be valid.
 *
 * TODO: If meta->num is really big, there could be overflow
 *
 * Returns 0 on success, -1 on failure.
 */
int convert_datetimestruct_to_datetime(PyArray_DatetimeMetaData *meta,
                                       const npy_datetimestruct *dts,
                                       npy_datetime *out);

#endif
