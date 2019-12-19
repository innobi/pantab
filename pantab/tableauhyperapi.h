/* This file is a modified port of the lib_h.py file provided by Tableau.

The original copyright notice is included below for reference.

# -----------------------------------------------------------------------------
#
# This file is the copyrighted property of Tableau Software and is protected
# by registered patents and other applicable U.S. and international laws and
# regulations.
#
# Unlicensed use of the contents of this file is prohibited. Please refer to
# the NOTICES.txt file for further details.
#
# -----------------------------------------------------------------------------
*/

#ifndef PANTAB_HYPER_API
#define PANTAB_HYPER_API

#include <stdbool.h>

typedef uint32_t hyper_date_t;
typedef struct
{
int32_t year;
int16_t month;
int16_t day;
} hyper_date_components_t;
typedef uint64_t hyper_time_t;
typedef struct
{
int8_t hour;
int8_t minute;
int8_t second;
int32_t microsecond;
} hyper_time_components_t;
hyper_date_components_t hyper_decode_date(hyper_date_t date);
hyper_date_t hyper_encode_date(hyper_date_components_t components);
hyper_time_components_t hyper_decode_time(hyper_time_t time);
hyper_time_t hyper_encode_time(hyper_time_components_t components);
typedef struct hyper_error_t hyper_error_t;
typedef struct hyper_inserter_buffer_t hyper_inserter_buffer_t;
hyper_error_t* hyper_inserter_buffer_add_null(hyper_inserter_buffer_t* buffer);
hyper_error_t* hyper_inserter_buffer_add_bool(hyper_inserter_buffer_t* buffer, bool value);
hyper_error_t* hyper_inserter_buffer_add_int16(hyper_inserter_buffer_t* buffer, int16_t value);
hyper_error_t* hyper_inserter_buffer_add_int32(hyper_inserter_buffer_t* buffer, int32_t value);
hyper_error_t* hyper_inserter_buffer_add_int64(hyper_inserter_buffer_t* buffer, int64_t value);
hyper_error_t* hyper_inserter_buffer_add_double(hyper_inserter_buffer_t* buffer, double value);
hyper_error_t* hyper_inserter_buffer_add_binary(hyper_inserter_buffer_t* buffer, const uint8_t* value, size_t size);
hyper_error_t* hyper_inserter_buffer_add_raw(hyper_inserter_buffer_t* buffer, const uint8_t* value, size_t size);

typedef struct hyper_connection_t hyper_connection_t;
typedef struct hyper_rowset_t hyper_rowset_t;
typedef struct hyper_rowset_chunk_t hyper_rowset_chunk_t;
hyper_error_t* hyper_execute_query(hyper_connection_t* connection, const char* query, hyper_rowset_t** rowset);
hyper_error_t* hyper_rowset_get_next_chunk(hyper_rowset_t* rowset, hyper_rowset_chunk_t** rowset_chunk);
int64_t hyper_read_int64(const uint8_t* source);
#endif
