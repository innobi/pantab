#pragma once

#include <nanobind/nanobind.h>
#include <nanobind/stl/map.h>
#include <nanobind/stl/set.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/tuple.h>

using SchemaAndTableName = std::tuple<std::string, std::string>;

void write_to_hyper(
    const std::map<SchemaAndTableName, nanobind::capsule> &dict_of_capsules,
    const std::string &path, const std::string &table_mode,
    const std::set<std::string> &not_null_columns,
    const std::set<std::string> &json_columns,
    const std::set<std::string> &geo_columns);
