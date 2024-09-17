#pragma once

#include <nanobind/nanobind.h>
#include <nanobind/stl/map.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/tuple.h>
#include <nanobind/stl/unordered_map.h>

namespace nb = nanobind;

using SchemaAndTableName = std::tuple<std::string, std::string>;

void write_to_hyper(
    const std::map<SchemaAndTableName, nanobind::capsule> &dict_of_capsules,
    const std::string &path, const std::string &table_mode,
    const nb::iterable not_null_columns, const nb::iterable json_columns,
    const nb::iterable geo_columns,
    std::unordered_map<std::string, std::string> &&process_params);
