#pragma once

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

auto read_from_hyper_query(const std::string &path, const std::string &query)
    -> nanobind::capsule;
