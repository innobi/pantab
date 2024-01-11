#include <chrono>
#include <cstddef>
#include <hyperapi/SqlType.hpp>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <hyperapi/hyperapi.hpp>
#include <hyperapi/impl/Inserter.impl.hpp>
#include <nanoarrow/nanoarrow.hpp>
#include <nanobind/nanobind.h>
#include <nanobind/stl/chrono.h>
#include <nanobind/stl/map.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/tuple.h>
#include <nanobind/stl/vector.h>

#include "nanoarrow/nanoarrow.h"
#include "nanoarrow/nanoarrow_types.h"
#include "numpy_datetime.h"

namespace nb = nanobind;

using Dtype = std::tuple<int, int, std::string, std::string>;

enum TimeUnit { SECOND, MILLI, MICRO, NANO };

static hyperapi::SqlType hyperTypeFromArrowSchema(struct ArrowSchema *schema,
                                                  ArrowError *error) {
  struct ArrowSchemaView schema_view;
  if (ArrowSchemaViewInit(&schema_view, schema, error) != 0) {
    throw std::runtime_error("Issue converting to hyper type: " +
                             std::string(error->message));
  }

  switch (schema_view.type) {
  case NANOARROW_TYPE_INT16:
    return hyperapi::SqlType::smallInt();
  case NANOARROW_TYPE_INT32:
    return hyperapi::SqlType::integer();
  case NANOARROW_TYPE_INT64:
    return hyperapi::SqlType::bigInt();
  case NANOARROW_TYPE_FLOAT:
  case NANOARROW_TYPE_DOUBLE:
    return hyperapi::SqlType::doublePrecision();
  case NANOARROW_TYPE_BOOL:
    return hyperapi::SqlType::boolean();
  case NANOARROW_TYPE_STRING:
  case NANOARROW_TYPE_LARGE_STRING:
    return hyperapi::SqlType::text();
  case NANOARROW_TYPE_TIMESTAMP:
    if (std::strcmp("", schema_view.timezone)) {
      return hyperapi::SqlType::timestampTZ();
    } else {
      return hyperapi::SqlType::timestamp();
    }
  default:
    throw std::invalid_argument("Unsupported Arrow type: " +
                                std::to_string(schema_view.type));
  }
}

class InsertHelper {
public:
  InsertHelper(std::shared_ptr<hyperapi::Inserter> inserter,
               struct ArrowArrayView *array_view)
      : inserter_(inserter), array_view_(array_view) {}

  virtual ~InsertHelper() {}

  virtual void insertValueAtIndex(size_t) {}

protected:
  std::shared_ptr<hyperapi::Inserter> inserter_;
  struct ArrowArrayView *array_view_;
};

template <typename T> class PrimitiveInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    if (ArrowArrayViewIsNull(array_view_, idx)) {
      inserter_->add(std::optional<T>{std::nullopt});
    }
    constexpr size_t elem_size = sizeof(T);
    T result;
    memcpy(&result,
           array_view_->buffer_views[1].data.as_uint8 + (idx * elem_size),
           elem_size);
    inserter_->add(result);
  }
};

template <typename OffsetT> class Utf8InsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    if (ArrowArrayViewIsNull(array_view_, idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<std:::string_view>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
    }

    const auto offsets_buffer = array_view_->buffer_views[1].data.as_uint8;
    OffsetT current_offset, next_offset;
    constexpr size_t offset_size = sizeof(OffsetT);
    memcpy(&current_offset, offsets_buffer + (idx * offset_size), offset_size);
    memcpy(&next_offset, offsets_buffer + ((idx + 1) * offset_size),
           offset_size);
    const OffsetT size_bytes = next_offset - current_offset;
    if (size_bytes < 0) {
      throw std::invalid_argument("invalid offset sizes");
    }
    const auto usize_bytes = static_cast<const size_t>(size_bytes);
    hyperapi::string_view result{array_view_->buffer_views[2].data.as_char +
                                     current_offset,
                                 usize_bytes};
    inserter_->add(result);
  }
};

template <enum TimeUnit TU, bool TZAware>
class TimestampInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    constexpr size_t elem_size = sizeof(int64_t);
    if (ArrowArrayViewIsNull(array_view_, idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<timestamp_t>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
    }
    int64_t value;

    memcpy(&value,
           array_view_->buffer_views[1].data.as_uint8 + (idx * elem_size),
           elem_size);

    // using timestamp_t =
    //    typename std::conditional<TZAware, hyperapi::OffsetTimestamp,
    //                              hyperapi::Timestamp>::type;

    // TODO: need overflow checks here
    npy_datetimestruct dts;
    PyArray_DatetimeMetaData meta;
    if constexpr (TU == TimeUnit::SECOND) {
      meta = {NPY_FR_s, 1};
    } else if constexpr (TU == TimeUnit::MILLI) {
      meta = {NPY_FR_ms, 1};
    } else if constexpr (TU == TimeUnit::MICRO) {
      meta = {NPY_FR_us, 1};
    } else if constexpr (TU == TimeUnit::NANO) {
      // we assume pandas is ns here but should check format
      meta = {NPY_FR_ns, 1};
    }

    int ret = convert_datetime_to_datetimestruct(&meta, value, &dts);
    if (ret != 0) {
      throw std::invalid_argument("could not convert datetime value ");
    }
    hyperapi::Date dt{static_cast<int32_t>(dts.year),
                      static_cast<int16_t>(dts.month),
                      static_cast<int16_t>(dts.day)};
    hyperapi::Time time{static_cast<int8_t>(dts.hour),
                        static_cast<int8_t>(dts.min),
                        static_cast<int8_t>(dts.sec), dts.us};

    if constexpr (TZAware) {
      hyperapi::OffsetTimestamp ts{dt, time, std::chrono::minutes{0}};
      inserter_->add<hyperapi::OffsetTimestamp>(ts);

    } else {
      hyperapi::Timestamp ts{dt, time};
      inserter_->add<hyperapi::Timestamp>(ts);
    }
  }
};

static std::unique_ptr<InsertHelper> makeInsertHelper(
    std::shared_ptr<hyperapi::Inserter> inserter, struct ArrowSchema *schema,
    nanoarrow::UniqueArrayView &&array_view, struct ArrowError *error) {
  // TODO: we should provide the full dtype here not just format string, so
  // boolean fields can determine whether they are bit or byte masks

  // right now we pass false as the template paramter to the
  // PrimitiveInsertHelper as that is all pandas generates; other libraries may
  // need the true variant
  struct ArrowSchemaView schema_view;
  if (ArrowSchemaViewInit(&schema_view, schema, error) != 0) {
    throw std::runtime_error("Issue generating insert helper: " +
                             std::string(error->message));
  }

  switch (schema_view.type) {
  case NANOARROW_TYPE_INT16:
    return std::unique_ptr<InsertHelper>(
        new PrimitiveInsertHelper<int16_t>(inserter, array_view.get()));
  case NANOARROW_TYPE_INT32:
    return std::unique_ptr<InsertHelper>(
        new PrimitiveInsertHelper<int32_t>(inserter, array_view.get()));
  case NANOARROW_TYPE_INT64:
    return std::unique_ptr<InsertHelper>(
        new PrimitiveInsertHelper<int64_t>(inserter, array_view.get()));
  case NANOARROW_TYPE_FLOAT:
    return std::unique_ptr<InsertHelper>(
        new PrimitiveInsertHelper<float>(inserter, array_view.get()));
  case NANOARROW_TYPE_DOUBLE:
    return std::unique_ptr<InsertHelper>(
        new PrimitiveInsertHelper<double>(inserter, array_view.get()));
  case NANOARROW_TYPE_BOOL:
    return std::unique_ptr<InsertHelper>(
        new PrimitiveInsertHelper<bool>(inserter, array_view.get()));
  case NANOARROW_TYPE_STRING:
  case NANOARROW_TYPE_LARGE_STRING:
    return std::unique_ptr<InsertHelper>(
        new Utf8InsertHelper<int64_t>(inserter, array_view.get()));
  case NANOARROW_TYPE_TIMESTAMP:
    switch (schema_view.time_unit) {
    case NANOARROW_TIME_UNIT_SECOND:
      if (std::strcmp("", schema_view.timezone)) {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::SECOND, true>(
                inserter, array_view.get()));
      } else {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::SECOND, false>(
                inserter, array_view.get()));
      }
    case NANOARROW_TIME_UNIT_MILLI:
      if (std::strcmp("", schema_view.timezone)) {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::MILLI, true>(inserter,
                                                             array_view.get()));
      } else {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::MILLI, false>(
                inserter, array_view.get()));
      }
    case NANOARROW_TIME_UNIT_MICRO:
      if (std::strcmp("", schema_view.timezone)) {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::MICRO, true>(inserter,
                                                             array_view.get()));
      } else {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::MICRO, false>(
                inserter, array_view.get()));
      }
    case NANOARROW_TIME_UNIT_NANO:
      if (std::strcmp("", schema_view.timezone)) {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::NANO, true>(inserter,
                                                            array_view.get()));
      } else {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::NANO, false>(inserter,
                                                             array_view.get()));
      }
    }
    throw std::runtime_error(
        "This code block should not be hit - contact a developer");
  default:
    throw std::invalid_argument("Unsupported Arrow type: " +
                                std::to_string(schema_view.type));
  }
}

using SchemaAndTableName = std::tuple<std::string, std::string>;

void write_to_hyper(
    const std::map<SchemaAndTableName, nb::object> &dict_of_exportable,
    const std::string &path, const std::string &table_mode) {
  hyperapi::HyperProcess hyper{
      hyperapi::Telemetry::DoNotSendUsageDataToTableau};

  // TODO: we don't have separate table / database create modes in the API
  // but probably should; for now we infer this from table mode
  const auto createMode = table_mode == "w"
                              ? hyperapi::CreateMode::CreateAndReplace
                              : hyperapi::CreateMode::CreateIfNotExists;

  hyperapi::Connection connection{hyper.getEndpoint(), path, createMode};
  const hyperapi::Catalog &catalog = connection.getCatalog();

  for (auto const &[schema_and_table, exportable] : dict_of_exportable) {
    const auto hyper_schema = std::get<0>(schema_and_table);
    const auto hyper_table = std::get<1>(schema_and_table);
    auto arrow_c_stream = nb::getattr(exportable, "__arrow_c_stream__")();

    PyObject *obj = arrow_c_stream.ptr();
    if (!PyCapsule_CheckExact(obj)) {
      throw std::invalid_argument("Object does not provide capsule");
    }
    auto stream = static_cast<struct ArrowArrayStream *>(
        PyCapsule_GetPointer(obj, "arrow_array_stream"));
    struct ArrowSchema schema;
    if (stream->get_schema(stream, &schema) != 0) {
      std::string error_msg{stream->get_last_error(stream)};
      throw std::runtime_error("Could not read from arrow schema:" + error_msg);
    }

    struct ArrowError error;
    auto names_vec = std::vector<std::string>{};
    std::vector<hyperapi::TableDefinition::Column> hyper_columns;

    for (int64_t i = 0; i < schema.n_children; i++) {
      const auto hypertype =
          hyperTypeFromArrowSchema(schema.children[i], &error);
      const auto name = std::string{schema.children[i]->name};
      names_vec.push_back(name);

      // Almost all arrow types are nullable
      hyper_columns.push_back(hyperapi::TableDefinition::Column{
          name, hypertype, hyperapi::Nullability::Nullable});
    }

    hyperapi::TableName table_name{hyper_schema, hyper_table};
    hyperapi::TableDefinition tableDef{table_name, hyper_columns};
    catalog.createSchemaIfNotExists(*table_name.getSchemaName());
    if (table_mode == "w") {
      catalog.createTable(tableDef);
    } else if (table_mode == "a") {
      catalog.createTableIfNotExists(tableDef);
    }
    auto inserter = std::make_shared<hyperapi::Inserter>(connection, tableDef);

    struct ArrowArray chunk;
    int errcode;
    while ((errcode = stream->get_next(stream, &chunk) == 0) &&
           chunk.release != NULL) {
      const int nrows = chunk.length;
      if (nrows < 0) {
        throw std::runtime_error("Unexpected array length < 0");
      }

      std::vector<std::unique_ptr<InsertHelper>> insert_helpers;
      for (int64_t i = 0; i < schema.n_children; i++) {
        struct ArrowArrayView c_array_view;
        struct ArrowSchema *child_schema = schema.children[i];
        if (ArrowArrayViewInitFromSchema(&c_array_view, child_schema, &error) !=
            0) {
          throw std::runtime_error("Could not construct array view: " +
                                   std::string{error.message});
        }

        if (ArrowArrayViewSetArray(&c_array_view, chunk.children[i], &error) !=
            0) {
          throw std::runtime_error("Could not set array view: " +
                                   std::string{error.message});
        }

        nanoarrow::UniqueArrayView array_view{&c_array_view};
        auto insert_helper = makeInsertHelper(inserter, child_schema,
                                              std::move(array_view), &error);

        insert_helpers.push_back(std::move(insert_helper));
      }

      for (int64_t row_idx = 0; row_idx < nrows; row_idx++) {
        for (const auto &insert_helper : insert_helpers) {
          insert_helper->insertValueAtIndex(row_idx);
        }
        inserter->endRow();
      }
    }

    inserter->execute();
  }
}

class ReadHelper {
public:
  ReadHelper() {}
  virtual ~ReadHelper() {}
  virtual nb::object Read(const hyperapi::Value &) { return nb::none(); }
};

class IntegralReadHelper : public ReadHelper {
  nb::object Read(const hyperapi::Value &value) {
    if (value.isNull()) {
      return nb::none();
    }
    return nb::int_(value.get<int64_t>());
  }
};

class FloatReadHelper : public ReadHelper {
  nb::object Read(const hyperapi::Value &value) {
    if (value.isNull()) {
      return nb::none();
    }
    return nb::float_(value.get<double>());
  }
};

class BooleanReadHelper : public ReadHelper {
  nb::object Read(const hyperapi::Value &value) {
    // TODO: bool support added in nanobind >= 1..9.0
    // return nb::bool_(value.get<bool>());
    if (value.isNull()) {
      return nb::none();
    }
    return nb::int_(value.get<bool>());
  }
};

class StringReadHelper : public ReadHelper {
  nb::object Read(const hyperapi::Value &value) {
    if (value.isNull()) {
      return nb::none();
    }
    return nb::str(value.get<std::string>().c_str());
  }
};

class DateReadHelper : public ReadHelper {
  nb::object Read(const hyperapi::Value &value) {
    if (value.isNull()) {
      return nb::none();
    }

    const auto hyper_date = value.get<hyperapi::Date>();
    const auto year = hyper_date.getYear();
    const auto month = hyper_date.getMonth();
    const auto day = hyper_date.getDay();

    PyObject *result = PyDate_FromDate(year, month, day);
    if (result == nullptr) {
      throw std::invalid_argument("could not parse date");
    }
    return nb::object(result, nb::detail::steal_t{});
  }
};

template <bool TZAware> class DatetimeReadHelper : public ReadHelper {
  nb::object Read(const hyperapi::Value &value) {
    if (value.isNull()) {
      return nb::none();
    }

    using timestamp_t =
        typename std::conditional<TZAware, hyperapi::OffsetTimestamp,
                                  hyperapi::Timestamp>::type;
    const auto hyper_ts = value.get<timestamp_t>();
    const auto hyper_date = hyper_ts.getDate();
    const auto hyper_time = hyper_ts.getTime();
    const auto year = hyper_date.getYear();
    const auto month = hyper_date.getMonth();
    const auto day = hyper_date.getDay();
    const auto hour = hyper_time.getHour();
    const auto min = hyper_time.getMinute();
    const auto sec = hyper_time.getSecond();
    const auto usec = hyper_time.getMicrosecond();

    PyObject *result =
        PyDateTime_FromDateAndTime(year, month, day, hour, min, sec, usec);
    if (result == nullptr) {
      throw std::invalid_argument("could not parse timestamp");
    }
    return nb::object(result, nb::detail::steal_t{});
  }
};

static std::unique_ptr<ReadHelper> makeReadHelper(hyperapi::SqlType sqltype) {
  if ((sqltype == hyperapi::SqlType::smallInt()) ||
      (sqltype == hyperapi::SqlType::integer()) ||
      (sqltype == hyperapi::SqlType::bigInt())) {
    return std::unique_ptr<ReadHelper>(new IntegralReadHelper());
  } else if (sqltype == hyperapi::SqlType::doublePrecision()) {
    return std::unique_ptr<ReadHelper>(new FloatReadHelper());
  } else if ((sqltype == hyperapi::SqlType::text())) {
    return std::unique_ptr<ReadHelper>(new StringReadHelper());
  } else if (sqltype == hyperapi::SqlType::boolean()) {
    return std::unique_ptr<ReadHelper>(new BooleanReadHelper());
  } else if (sqltype == hyperapi::SqlType::date()) {
    return std::unique_ptr<ReadHelper>(new DateReadHelper());
  } else if (sqltype == hyperapi::SqlType::timestamp()) {
    return std::unique_ptr<ReadHelper>(new DatetimeReadHelper<false>());
  } else if (sqltype == hyperapi::SqlType::timestampTZ()) {
    return std::unique_ptr<ReadHelper>(new DatetimeReadHelper<true>());
  }

  throw nb::type_error(("cannot read sql type: " + sqltype.toString()).c_str());
}

static std::string pandasDtypeFromHyper(const hyperapi::SqlType &sqltype,
                                        bool nullable) {
  if (sqltype == hyperapi::SqlType::smallInt()) {
    return nullable ? "Int16" : "int16";
  } else if (sqltype == hyperapi::SqlType::integer()) {
    return nullable ? "Int32" : "int32";
  } else if (sqltype == hyperapi::SqlType::bigInt()) {
    return nullable ? "Int64" : "int64";
  } else if (sqltype == hyperapi::SqlType::doublePrecision()) {
    return "float64";
  } else if (sqltype == hyperapi::SqlType::text()) {
    return "string";
  } else if (sqltype == hyperapi::SqlType::boolean()) {
    return nullable ? "boolean" : "bool";
  } else if (sqltype == hyperapi::SqlType::timestamp()) {
    return "datetime64[ns]";
  } else if (sqltype == hyperapi::SqlType::timestampTZ()) {
    return "datetime64[ns, UTC]";
  } else if (sqltype == hyperapi::SqlType::date()) {
    return "datetime64[ns]";
  }

  throw nb::type_error(
      ("unimplemented pandas dtype for type: " + sqltype.toString()).c_str());
}

using ColumnNames = std::vector<std::string>;
using ResultBody = std::vector<std::vector<nb::object>>;
// In a future version of pantab it would be nice to not require pandas dtypes
// However, the current reader just creates PyObjects and loses that information
// when passing back to the Python runtime; hence the explicit passing
using PandasDtypes = std::vector<std::string>;
///
/// read_from_hyper_query is slightly different than read_from_hyper_table
/// because the former detects a schema from the hyper Result object
/// which does not hold nullability information
///
std::tuple<ResultBody, ColumnNames, PandasDtypes>
read_from_hyper_query(const std::string &path, const std::string &query) {
  std::vector<std::vector<nb::object>> result;
  hyperapi::HyperProcess hyper{
      hyperapi::Telemetry::DoNotSendUsageDataToTableau};
  hyperapi::Connection connection(hyper.getEndpoint(), path);

  std::vector<std::string> columnNames;
  std::vector<std::string> pandasDtypes;
  std::vector<std::unique_ptr<ReadHelper>> read_helpers;

  hyperapi::Result hyperResult = connection.executeQuery(query);
  const auto resultSchema = hyperResult.getSchema();
  for (const auto &column : resultSchema.getColumns()) {
    read_helpers.push_back(makeReadHelper(column.getType()));
    auto name = column.getName().getUnescaped();
    columnNames.push_back(name);

    // the query result set does not tell us if columns are nullable or not
    auto const sqltype = column.getType();
    pandasDtypes.push_back(
        pandasDtypeFromHyper(sqltype, hyperapi::Nullability::Nullable));
  }
  for (const hyperapi::Row &row : hyperResult) {
    std::vector<nb::object> rowdata;
    size_t column_idx = 0;
    for (const hyperapi::Value &value : row) {
      const auto &read_helper = read_helpers[column_idx];
      rowdata.push_back(read_helper->Read(value));
      column_idx++;
    }
    result.push_back(rowdata);
  }

  return std::make_tuple(result, columnNames, pandasDtypes);
}

std::tuple<ResultBody, ColumnNames, PandasDtypes>
read_from_hyper_table(const std::string &path, const std::string &schema,
                      const std::string &table) {
  std::vector<std::vector<nb::object>> result;
  hyperapi::HyperProcess hyper{
      hyperapi::Telemetry::DoNotSendUsageDataToTableau};
  hyperapi::Connection connection(hyper.getEndpoint(), path);
  hyperapi::TableName extractTable{schema, table};
  const hyperapi::Catalog &catalog = connection.getCatalog();
  const hyperapi::TableDefinition tableDef =
      catalog.getTableDefinition(extractTable);

  std::vector<std::string> columnNames;
  std::vector<std::string> pandasDtypes;
  std::vector<std::unique_ptr<ReadHelper>> read_helpers;

  for (auto &column : tableDef.getColumns()) {
    read_helpers.push_back(makeReadHelper(column.getType()));
    auto name = column.getName().getUnescaped();
    columnNames.push_back(name);

    auto const sqltype = column.getType();
    auto const nullability = column.getNullability();
    pandasDtypes.push_back(pandasDtypeFromHyper(sqltype, nullability));
  }

  hyperapi::Result hyperResult =
      connection.executeQuery("SELECT * FROM " + extractTable.toString());
  for (const hyperapi::Row &row : hyperResult) {
    std::vector<nb::object> rowdata;
    size_t column_idx = 0;
    for (const hyperapi::Value &value : row) {
      const auto &read_helper = read_helpers[column_idx];
      rowdata.push_back(read_helper->Read(value));
      column_idx++;
    }
    result.push_back(rowdata);
  }

  return std::make_tuple(result, columnNames, pandasDtypes);
}

NB_MODULE(pantab, m) {
  m.def("write_to_hyper", &write_to_hyper, nb::arg("dict_of_exportable"),
        nb::arg("path"), nb::arg("table_mode"))
      .def("read_from_hyper_query", &read_from_hyper_query, nb::arg("path"),
           nb::arg("query"))
      .def("read_from_hyper_table", &read_from_hyper_table, nb::arg("path"),
           nb::arg("schema"), nb::arg("table"));
  PyDateTime_IMPORT;
}
