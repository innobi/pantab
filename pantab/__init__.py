__version__ = "2.0.0"

import libpantab  # type: ignore

from ._reader import frame_from_hyper, frame_from_hyper_query, frames_from_hyper
from ._tester import test
from ._writer import frame_to_hyper, frames_to_hyper

__all__ = [
    "__version__",
    "frame_from_hyper",
    "frame_from_hyper_query",
    "frames_from_hyper",
    "frame_to_hyper",
    "frames_to_hyper",
    "test",
]

# We link against HyperAPI in a fun way: In Python, we extract the function
# pointers directly from the Python HyperAPI. We pass those function pointers
# over to the C module which will then use those pointers to directly interact
# with HyperAPI. Furthermore, we check the function signatures to guard
# against API-breaking changes in HyperAPI.
#
# Directly using HyperAPI's C functions always was and still is discouraged and
# unsupported by Tableu. In particular, Tableau will not be able to provide
# official support for this hack.
#
# Because this is highly brittle, we try to make the error message as
# actionable as possible and guide users in the right direction.

api_incompatibility_msg = """
pantab is incompatible with the version of Tableau Hyper API installed on your
system. Please upgrade both `tableauhyperapi` and `pantab` to the latest version.
If doing so does not fix this issue, please file an issue at
https://github.com/innobi/pantab/issues mentioning the exact pantab and HyperAPI
versions which triggered this error.
"""

try:
    from tableauhyperapi.impl.dll import ffi, lib
except ImportError as e:
    raise NotImplementedError(api_incompatibility_msg) from e


def _check_compatibility(check, message):
    if not check:
        raise NotImplementedError(message + "\n" + api_incompatibility_msg)


def _get_hapi_function(name, sig):
    _check_compatibility(hasattr(lib, name), f"function '{name}' missing")
    f = getattr(lib, name)
    func_type = ffi.typeof(f)
    _check_compatibility(
        func_type.kind == "function",
        f"expected '{name}' to be a function, got {func_type.kind}",
    )
    _check_compatibility(
        func_type.cname == sig,
        f"expected '{name}' to have the signature '{sig}', got '{func_type.cname}'",
    )
    return f


libpantab.load_hapi_functions(
    _get_hapi_function("hyper_decode_date", "hyper_date_components_t(*)(uint32_t)"),
    _get_hapi_function("hyper_encode_date", "uint32_t(*)(hyper_date_components_t)"),
    _get_hapi_function("hyper_decode_time", "hyper_time_components_t(*)(uint64_t)"),
    _get_hapi_function("hyper_encode_time", "uint64_t(*)(hyper_time_components_t)"),
    _get_hapi_function(
        "hyper_inserter_buffer_add_null",
        "struct hyper_error_t *(*)(struct hyper_inserter_buffer_t *)",
    ),
    _get_hapi_function(
        "hyper_inserter_buffer_add_bool",
        "struct hyper_error_t *(*)(struct hyper_inserter_buffer_t *, _Bool)",
    ),
    _get_hapi_function(
        "hyper_inserter_buffer_add_int16",
        "struct hyper_error_t *(*)(struct hyper_inserter_buffer_t *, int16_t)",
    ),
    _get_hapi_function(
        "hyper_inserter_buffer_add_int32",
        "struct hyper_error_t *(*)(struct hyper_inserter_buffer_t *, int32_t)",
    ),
    _get_hapi_function(
        "hyper_inserter_buffer_add_int64",
        "struct hyper_error_t *(*)(struct hyper_inserter_buffer_t *, int64_t)",
    ),
    _get_hapi_function(
        "hyper_inserter_buffer_add_double",
        "struct hyper_error_t *(*)(struct hyper_inserter_buffer_t *, double)",
    ),
    _get_hapi_function(
        "hyper_inserter_buffer_add_binary",
        "struct hyper_error_t *(*)(struct hyper_inserter_buffer_t *, uint8_t *, size_t)",
    ),
    _get_hapi_function(
        "hyper_inserter_buffer_add_raw",
        "struct hyper_error_t *(*)(struct hyper_inserter_buffer_t *, uint8_t *, size_t)",
    ),
    _get_hapi_function(
        "hyper_rowset_get_next_chunk",
        "struct hyper_error_t *(*)(struct hyper_rowset_t *, struct hyper_rowset_chunk_t * *)",
    ),
    _get_hapi_function(
        "hyper_destroy_rowset_chunk", "void(*)(struct hyper_rowset_chunk_t *)"
    ),
    _get_hapi_function(
        "hyper_rowset_chunk_field_values",
        "struct hyper_error_t *(*)(struct hyper_rowset_chunk_t *, size_t *, size_t *, uint8_t * * *, size_t * *, int8_t * *)",
    ),
)
