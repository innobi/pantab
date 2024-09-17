#pragma once

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/unordered_map.h>

auto read_from_hyper_query(
    const std::string &path, const std::string &query,
    std::unordered_map<std::string, std::string> &&process_params)
    -> nanobind::capsule;
