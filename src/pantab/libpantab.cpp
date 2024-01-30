#include <nanobind/nanobind.h>

#include "datetime.h"
#include "reader.hpp"
#include "writer.hpp"

namespace nb = nanobind;

NB_MODULE(libpantab, m) { // NOLINT
  m.def("write_to_hyper", &write_to_hyper, nb::arg("dict_of_capsules"),
        nb::arg("path"), nb::arg("table_mode"), nb::arg("not_null_columns"),
        nb::arg("json_columns"), nb::arg("geo_columns"))
      .def("read_from_hyper_query", &read_from_hyper_query, nb::arg("path"),
           nb::arg("query"));
  PyDateTime_IMPORT;
}
