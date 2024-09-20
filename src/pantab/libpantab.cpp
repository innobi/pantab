#include <unordered_map>

#include <hyperapi/hyperapi.hpp>
#include <nanobind/nanobind.h>
#include <nanobind/stl/vector.h>

#include "reader.hpp"
#include "writer.hpp"

namespace nb = nanobind;

NB_MODULE(libpantab, m) { // NOLINT
  m.def("escape_sql_identifier",
        [](const nb::str &str) {
          const auto required_size =
              hyper_quote_sql_identifier(nullptr, 0, str.c_str(), nb::len(str));
          std::string result(required_size, 'x');
          hyper_quote_sql_identifier(result.data(), required_size, str.c_str(),
                                     nb::len(str));
          return result;
        })
      .def(
          "get_table_names",
          [](const std::string &path) {
            std::unordered_map<std::string, std::string> params{
                {"log_config", ""}};
            const hyperapi::HyperProcess hyper{
                hyperapi::Telemetry::DoNotSendUsageDataToTableau, "",
                std::move(params)};
            hyperapi::Connection connection(hyper.getEndpoint(), path);

            nb::list result;
            for (const auto &schema_name :
                 connection.getCatalog().getSchemaNames()) {
              for (const auto &table_name :
                   connection.getCatalog().getTableNames(schema_name)) {
                const auto schema_prefix = table_name.getSchemaName();
                if (schema_prefix) {
                  const auto tup =
                      nb::make_tuple(schema_prefix->getName().getUnescaped(),
                                     table_name.getName().getUnescaped());
                  result.append(tup);
                } else {
                  result.append(
                      nb::str(table_name.getName().getUnescaped().c_str()));
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
