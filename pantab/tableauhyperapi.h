#ifndef PANTAB_HYPER_API
#define PANTAB_HYPER_API

#include <stdbool.h>

typedef struct hyper_error_t hyper_error_t;
typedef struct hyper_inserter_buffer_t hyper_inserter_buffer_t;
hyper_error_t* hyper_inserter_buffer_add_null(hyper_inserter_buffer_t* buffer);
hyper_error_t* hyper_inserter_buffer_add_bool(hyper_inserter_buffer_t* buffer, bool value);
hyper_error_t* hyper_inserter_buffer_add_int16(hyper_inserter_buffer_t* buffer, int16_t value);
hyper_error_t* hyper_inserter_buffer_add_int32(hyper_inserter_buffer_t* buffer, int32_t value);
hyper_error_t* hyper_inserter_buffer_add_int64(hyper_inserter_buffer_t* buffer, int64_t value);
hyper_error_t* hyper_inserter_buffer_add_double(hyper_inserter_buffer_t* buffer, double value);
hyper_error_t* hyper_inserter_buffer_add_binary(hyper_inserter_buffer_t* buffer, const uint8_t* value, size_t size);
#endif
