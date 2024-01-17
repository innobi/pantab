#include <chrono>
#include <cstddef>
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

#include "numpy_datetime.h"

namespace nb = nanobind;

using Dtype = std::tuple<int, int, std::string, std::string>;

enum TimeUnit { SECOND, MILLI, MICRO, NANO };

static auto hyperTypeFromArrowSchema(struct ArrowSchema *schema,
                                     ArrowError *error) -> hyperapi::SqlType {
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
  case NANOARROW_TYPE_DATE32:
    return hyperapi::SqlType::date();
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
               const struct ArrowArray *chunk, const struct ArrowSchema *schema,
               struct ArrowError *error, int64_t column_position)
      : inserter_(std::move(inserter)), chunk_(chunk), schema_(schema),
        error_(error), column_position_(column_position) {}

  virtual ~InsertHelper() = default;

  void Init() {
    struct ArrowSchema *child_schema = schema_->children[column_position_];

    if (ArrowArrayViewInitFromSchema(&array_view_, child_schema, error_) != 0) {
      throw std::runtime_error("Could not construct insert helper: " +
                               std::string{error_->message});
    }

    if (ArrowArrayViewSetArray(&array_view_, chunk_->children[column_position_],
                               error_) != 0) {
      throw std::runtime_error("Could not set array view: " +
                               std::string{error_->message});
    }
  }

  virtual void insertValueAtIndex(size_t) {}

protected:
  std::shared_ptr<hyperapi::Inserter> inserter_;
  const struct ArrowArray *chunk_;
  const struct ArrowSchema *schema_;
  struct ArrowError *error_;
  const int64_t column_position_;
  struct ArrowArrayView array_view_;
};

template <typename T> class IntegralInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    if (ArrowArrayViewIsNull(&array_view_, idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<T>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    const int64_t value = ArrowArrayViewGetIntUnsafe(&array_view_, idx);
    hyperapi::internal::ValueInserter{*inserter_}.addValue(
        static_cast<T>(value));
  }
};

template <typename T> class FloatingInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    if (ArrowArrayViewIsNull(&array_view_, idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<T>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    const double value = ArrowArrayViewGetDoubleUnsafe(&array_view_, idx);
    hyperapi::internal::ValueInserter{*inserter_}.addValue(
        static_cast<T>(value));
  }
};

template <typename OffsetT> class Utf8InsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    if (ArrowArrayViewIsNull(&array_view_, idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<std:::string_view>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    struct ArrowBufferView buffer_view =
        ArrowArrayViewGetBytesUnsafe(&array_view_, idx);
    auto result = std::string{buffer_view.data.as_char,
                              static_cast<size_t>(buffer_view.size_bytes)};
    hyperapi::internal::ValueInserter{*inserter_}.addValue(result);
  }
};

class Date32InsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    constexpr size_t elem_size = sizeof(int32_t);
    int32_t value;
    if (ArrowArrayViewIsNull(&array_view_, idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<timestamp_t>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    memcpy(&value,
           array_view_.buffer_views[1].data.as_uint8 + (idx * elem_size),
           elem_size);

    const std::chrono::duration<int32_t, std::ratio<86400>> dur{value};
    const std::chrono::time_point<
        std::chrono::system_clock,
        std::chrono::duration<int32_t, std::ratio<86400>>>
        tp{dur};
    const auto tt = std::chrono::system_clock::to_time_t(tp);

    const struct tm utc_tm = *std::gmtime(&tt);
    hyperapi::Date dt{1900 + utc_tm.tm_year,
                      static_cast<int16_t>(1 + utc_tm.tm_mon),
                      static_cast<int16_t>(1 + utc_tm.tm_yday)};

    hyperapi::internal::ValueInserter{*inserter_}.addValue(dt);
  }
};

template <enum TimeUnit TU, bool TZAware>
class TimestampInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    constexpr size_t elem_size = sizeof(int64_t);
    if (ArrowArrayViewIsNull(&array_view_, idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<timestamp_t>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }
    int64_t value;

    memcpy(&value,
           array_view_.buffer_views[1].data.as_uint8 + (idx * elem_size),
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
      hyperapi::internal::ValueInserter{*inserter_}.addValue(
          static_cast<hyperapi::OffsetTimestamp>(ts));

    } else {
      hyperapi::Timestamp ts{dt, time};
      hyperapi::internal::ValueInserter{*inserter_}.addValue(
          static_cast<hyperapi::Timestamp>(ts));
    }
  }
};

static auto makeInsertHelper(std::shared_ptr<hyperapi::Inserter> inserter,
                             struct ArrowArray *chunk,
                             struct ArrowSchema *schema,
                             struct ArrowError *error, int64_t column_position)
    -> std::unique_ptr<InsertHelper> {
  // TODO: we should provide the full dtype here not just format string, so
  // boolean fields can determine whether they are bit or byte masks

  // right now we pass false as the template paramter to the
  // PrimitiveInsertHelper as that is all pandas generates; other libraries may
  // need the true variant
  struct ArrowSchemaView schema_view;
  if (ArrowSchemaViewInit(&schema_view, schema->children[column_position],
                          error) != 0) {
    throw std::runtime_error("Issue generating insert helper: " +
                             std::string(error->message));
  }

  switch (schema_view.type) {
  case NANOARROW_TYPE_INT16:
    return std::unique_ptr<InsertHelper>(new IntegralInsertHelper<int16_t>(
        inserter, chunk, schema, error, column_position));
  case NANOARROW_TYPE_INT32:
    return std::unique_ptr<InsertHelper>(new IntegralInsertHelper<int32_t>(
        inserter, chunk, schema, error, column_position));
  case NANOARROW_TYPE_INT64:
    return std::unique_ptr<InsertHelper>(new IntegralInsertHelper<int64_t>(
        inserter, chunk, schema, error, column_position));
  case NANOARROW_TYPE_FLOAT:
    return std::unique_ptr<InsertHelper>(new FloatingInsertHelper<float>(
        inserter, chunk, schema, error, column_position));
  case NANOARROW_TYPE_DOUBLE:
    return std::unique_ptr<InsertHelper>(new FloatingInsertHelper<double>(
        inserter, chunk, schema, error, column_position));
  case NANOARROW_TYPE_BOOL:
    return std::unique_ptr<InsertHelper>(new IntegralInsertHelper<bool>(
        inserter, chunk, schema, error, column_position));
  case NANOARROW_TYPE_STRING:
  case NANOARROW_TYPE_LARGE_STRING:
    return std::unique_ptr<InsertHelper>(new Utf8InsertHelper<int64_t>(
        inserter, chunk, schema, error, column_position));
  case NANOARROW_TYPE_DATE32:
    return std::unique_ptr<InsertHelper>(new Date32InsertHelper(
        inserter, chunk, schema, error, column_position));
  case NANOARROW_TYPE_TIMESTAMP:
    switch (schema_view.time_unit) {
    case NANOARROW_TIME_UNIT_SECOND:
      if (std::strcmp("", schema_view.timezone)) {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::SECOND, true>(
                inserter, chunk, schema, error, column_position));
      } else {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::SECOND, false>(
                inserter, chunk, schema, error, column_position));
      }
    case NANOARROW_TIME_UNIT_MILLI:
      if (std::strcmp("", schema_view.timezone)) {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::MILLI, true>(
                inserter, chunk, schema, error, column_position));
      } else {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::MILLI, false>(
                inserter, chunk, schema, error, column_position));
      }
    case NANOARROW_TIME_UNIT_MICRO:
      if (std::strcmp("", schema_view.timezone)) {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::MICRO, true>(
                inserter, chunk, schema, error, column_position));
      } else {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::MICRO, false>(
                inserter, chunk, schema, error, column_position));
      }
    case NANOARROW_TIME_UNIT_NANO:
      if (std::strcmp("", schema_view.timezone)) {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::NANO, true>(
                inserter, chunk, schema, error, column_position));
      } else {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::NANO, false>(
                inserter, chunk, schema, error, column_position));
      }
    }
    throw std::runtime_error(
        "This code block should not be hit - contact a developer");
  default:
    throw std::invalid_argument("makeInsertHelper: Unsupported Arrow type: " +
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
    auto c_stream = static_cast<struct ArrowArrayStream *>(
        PyCapsule_GetPointer(obj, "arrow_array_stream"));
    auto stream = nanoarrow::UniqueArrayStream{c_stream};

    struct ArrowSchema schema;
    if (stream->get_schema(stream.get(), &schema) != 0) {
      std::string error_msg{stream->get_last_error(stream.get())};
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
      hyper_columns.emplace_back(hyperapi::TableDefinition::Column{
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
    while ((errcode = stream->get_next(stream.get(), &chunk) == 0) &&
           chunk.release != nullptr) {
      const int nrows = chunk.length;
      if (nrows < 0) {
        throw std::runtime_error("Unexpected array length < 0");
      }

      std::vector<std::unique_ptr<InsertHelper>> insert_helpers;
      for (int64_t i = 0; i < schema.n_children; i++) {
        // the lifetime of the inserthelper cannot exceed that of chunk or
        // schema this is implicit; we should make this explicit
        auto insert_helper =
            makeInsertHelper(inserter, &chunk, &schema, &error, i);

        insert_helper->Init();
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
  ReadHelper() = default;
  virtual ~ReadHelper() = default;
  virtual auto Read(const hyperapi::Value &) -> nb::object = 0;
};

class IntegralReadHelper : public ReadHelper {
  auto Read(const hyperapi::Value &value) -> nb::object override {
    if (value.isNull()) {
      return nb::none();
    }
    return nb::int_(value.get<int64_t>());
  }
};

class FloatReadHelper : public ReadHelper {
  auto Read(const hyperapi::Value &value) -> nb::object override {
    if (value.isNull()) {
      return nb::none();
    }
    return nb::float_(value.get<double>());
  }
};

class BooleanReadHelper : public ReadHelper {
  auto Read(const hyperapi::Value &value) -> nb::object override {
    // TODO: bool support added in nanobind >= 1..9.0
    // return nb::bool_(value.get<bool>());
    if (value.isNull()) {
      return nb::none();
    }
    return nb::int_(value.get<bool>());
  }
};

class StringReadHelper : public ReadHelper {
  auto Read(const hyperapi::Value &value) -> nb::object override {
    if (value.isNull()) {
      return nb::none();
    }
    return nb::str(value.get<std::string>().c_str());
  }
};

class DateReadHelper : public ReadHelper {
  auto Read(const hyperapi::Value &value) -> nb::object override {
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
  auto Read(const hyperapi::Value &value) -> nb::object override {
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

static auto makeReadHelper(hyperapi::SqlType sqltype)
    -> std::unique_ptr<ReadHelper> {
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

static auto pandasDtypeFromHyper(const hyperapi::SqlType &sqltype)
    -> std::string {
  if (sqltype == hyperapi::SqlType::smallInt()) {
    return "int16[pyarrow]";
  } else if (sqltype == hyperapi::SqlType::integer()) {
    return "int32[pyarrow]";
  } else if (sqltype == hyperapi::SqlType::bigInt()) {
    return "int64[pyarrow]";
  } else if (sqltype == hyperapi::SqlType::doublePrecision()) {
    return "double[pyarrow]";
  } else if (sqltype == hyperapi::SqlType::text()) {
    return "string[pyarrow]";
  } else if (sqltype == hyperapi::SqlType::boolean()) {
    return "boolean[pyarrow]";
  } else if (sqltype == hyperapi::SqlType::timestamp()) {
    return "timestamp[us][pyarrow]";
  } else if (sqltype == hyperapi::SqlType::timestampTZ()) {
    return "timestamp[us, UTC][pyarrow]";
  } else if (sqltype == hyperapi::SqlType::date()) {
    return "date32[pyarrow]";
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
auto read_from_hyper_query(const std::string &path, const std::string &query)
    -> std::tuple<ResultBody, ColumnNames, PandasDtypes> {
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
    pandasDtypes.push_back(pandasDtypeFromHyper(sqltype));
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

auto read_from_hyper_table(const std::string &path, const std::string &schema,
                           const std::string &table)
    -> std::tuple<ResultBody, ColumnNames, PandasDtypes> {
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
    pandasDtypes.push_back(pandasDtypeFromHyper(sqltype));
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

NB_MODULE(pantab, m) { // NOLINT
  m.def("write_to_hyper", &write_to_hyper, nb::arg("dict_of_exportable"),
        nb::arg("path"), nb::arg("table_mode"))
      .def("read_from_hyper_query", &read_from_hyper_query, nb::arg("path"),
           nb::arg("query"))
      .def("read_from_hyper_table", &read_from_hyper_table, nb::arg("path"),
           nb::arg("schema"), nb::arg("table"));
  PyDateTime_IMPORT;
}
