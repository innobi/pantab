#include <unordered_map>

#include <hyperapi/hyperapi.hpp>
#include <nanobind/nanobind.h>
#include <nanobind/stl/vector.h>

#include "reader.hpp"
#include "writer.hpp"

namespace nb = nanobind;

NB_MODULE(libpantab, m) { // NOLINT
  m.def(
       "get_table_names",
       [](const std::string &path) {
         std::unordered_map<std::string, std::string> params{
             {"log_config", ""}};
         const hyperapi::HyperProcess hyper{
             hyperapi::Telemetry::DoNotSendUsageDataToTableau, "",
             std::move(params)};
         hyperapi::Connection connection(hyper.getEndpoint(), path);

         std::vector<std::string> result;
         for (const auto &schema_name :
              connection.getCatalog().getSchemaNames()) {
           for (const auto &table_name :
                connection.getCatalog().getTableNames(schema_name)) {
             const auto schema_prefix = table_name.getSchemaName();
             if (schema_prefix) {
               result.emplace_back(schema_prefix->getName().getUnescaped() +
                                   "." + table_name.getName().getUnescaped());
             } else {
               result.emplace_back(table_name.getName().getUnescaped());
             }
           }
         }

         return result;
       },
       nb::arg("path"))
      .def("write_to_hyper", &write_to_hyper, nb::arg("dict_of_capsules"),
           nb::arg("path"), nb::arg("table_mode"), nb::arg("not_null_columns"),
           nb::arg("json_columns"), nb::arg("geo_columns"),
           nb::arg("process_params"))
      .def("read_from_hyper_query", &read_from_hyper_query, nb::arg("path"),
           nb::arg("query"), nb::arg("process_params"));
}
