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
  ReadHelper(struct ArrowArray *array) : array_(array) {}
  virtual ~ReadHelper() = default;
  virtual auto Read(const hyperapi::Value &) -> void = 0;

protected:
  struct ArrowArray *array_;
};

class IntegralReadHelper : public ReadHelper {
  using ReadHelper::ReadHelper;

  auto Read(const hyperapi::Value &value) -> void override {
    if (value.isNull()) {
      if (ArrowArrayAppendNull(array_, 1)) {
        throw std::runtime_error("ArrowAppendNull failed");
      }
      return;
    }
    if (ArrowArrayAppendInt(array_, value.get<int64_t>())) {
      throw std::runtime_error("ArrowAppendInt failed");
    };
  }
};

class FloatReadHelper : public ReadHelper {
  using ReadHelper::ReadHelper;

  auto Read(const hyperapi::Value &value) -> void override {
    if (value.isNull()) {
      if (ArrowArrayAppendNull(array_, 1)) {
        throw std::runtime_error("ArrowAppendNull failed");
      }
      return;
    }
    if (ArrowArrayAppendDouble(array_, value.get<double>())) {
      throw std::runtime_error("ArrowAppendDouble failed");
    };
  }
};

class BooleanReadHelper : public ReadHelper {
  using ReadHelper::ReadHelper;

  auto Read(const hyperapi::Value &value) -> void override {
    if (value.isNull()) {
      if (ArrowArrayAppendNull(array_, 1)) {
        throw std::runtime_error("ArrowAppendNull failed");
      }
      return;
    }
    if (ArrowArrayAppendInt(array_, value.get<bool>())) {
      throw std::runtime_error("ArrowAppendBool failed");
    };
  }
};

class StringReadHelper : public ReadHelper {
  using ReadHelper::ReadHelper;

  auto Read(const hyperapi::Value &value) -> void override {
    if (value.isNull()) {
      if (ArrowArrayAppendNull(array_, 1)) {
        throw std::runtime_error("ArrowAppendNull failed");
      }
      return;
    }

    const auto strval = value.get<std::string>();
    const ArrowStringView string_view{strval.c_str(),
                                      static_cast<int64_t>(strval.size())};

    if (ArrowArrayAppendString(array_, string_view)) {
      throw std::runtime_error("ArrowAppendString failed");
    };
  }
};

class DateReadHelper : public ReadHelper {
  using ReadHelper::ReadHelper;

  auto Read(const hyperapi::Value &value) -> void override {
    if (value.isNull()) {
      if (ArrowArrayAppendNull(array_, 1)) {
        throw std::runtime_error("ArrowAppendNull failed");
      }
      return;
    }

    // TODO: need some bounds /overflow checking
    // tableau uses uint32 but we have int32
    constexpr int32_t tableau_to_unix_days = 2440588;
    const auto hyper_date = value.get<hyperapi::Date>();
    const auto raw_value = static_cast<int32_t>(hyper_date.getRaw());
    const auto arrow_value = raw_value - tableau_to_unix_days;

    struct ArrowBuffer *data_buffer = ArrowArrayBuffer(array_, 1);
    if (ArrowBufferAppendInt32(data_buffer, arrow_value)) {
      throw std::runtime_error("Failed to append date32 value");
    }
    array_->length++;
  }
};

template <bool TZAware> class DatetimeReadHelper : public ReadHelper {
  using ReadHelper::ReadHelper;

  auto Read(const hyperapi::Value &value) -> void override {
    if (value.isNull()) {
      if (ArrowArrayAppendNull(array_, 1)) {
        throw std::runtime_error("ArrowAppendNull failed");
      }
      return;
    }

    using timestamp_t =
        typename std::conditional<TZAware, hyperapi::OffsetTimestamp,
                                  hyperapi::Timestamp>::type;
    const auto hyper_ts = value.get<timestamp_t>();

    // TODO: need some bounds /overflow checking
    // tableau uses uint64 but we have int64
    constexpr int64_t tableau_to_unix_usec =
        2440588LL * 24 * 60 * 60 * 1000 * 1000;
    const auto raw_usec = static_cast<int64_t>(hyper_ts.getRaw());
    const auto arrow_value = raw_usec - tableau_to_unix_usec;

    struct ArrowBuffer *data_buffer = ArrowArrayBuffer(array_, 1);
    if (ArrowBufferAppendInt64(data_buffer, arrow_value)) {
      throw std::runtime_error("Failed to append timestamp64 value");
    }
    array_->length++;
  }
};

static auto makeReadHelper(const ArrowSchemaView *schema_view,
                           struct ArrowArray *array)
    -> std::unique_ptr<ReadHelper> {
  switch (schema_view->type) {
  case NANOARROW_TYPE_INT16:
  case NANOARROW_TYPE_INT32:
  case NANOARROW_TYPE_INT64:
    return std::unique_ptr<ReadHelper>(new IntegralReadHelper(array));
  case NANOARROW_TYPE_DOUBLE:
    return std::unique_ptr<ReadHelper>(new FloatReadHelper(array));
  case NANOARROW_TYPE_LARGE_STRING:
    return std::unique_ptr<ReadHelper>(new StringReadHelper(array));
  case NANOARROW_TYPE_BOOL:
    return std::unique_ptr<ReadHelper>(new BooleanReadHelper(array));
  case NANOARROW_TYPE_DATE32:
    return std::unique_ptr<ReadHelper>(new DateReadHelper(array));
  case NANOARROW_TYPE_TIMESTAMP:
    if (strcmp("", schema_view->timezone)) {
      return std::unique_ptr<ReadHelper>(new DatetimeReadHelper<true>(array));
    } else {
      return std::unique_ptr<ReadHelper>(new DatetimeReadHelper<false>(array));
    }
  default:
    throw nb::type_error("unknownn arrow type provided");
  }
}

static auto
arrowTypeFromHyper(const hyperapi::SqlType &sqltype) -> enum ArrowType {
  if (sqltype == hyperapi::SqlType::smallInt()){return NANOARROW_TYPE_INT16;}
else if (sqltype == hyperapi::SqlType::integer()) {
  return NANOARROW_TYPE_INT32;
}
else if (sqltype == hyperapi::SqlType::bigInt()) {
  return NANOARROW_TYPE_INT64;
}
else if (sqltype == hyperapi::SqlType::doublePrecision()) {
  return NANOARROW_TYPE_DOUBLE;
}
else if (sqltype == hyperapi::SqlType::text()) {
  return NANOARROW_TYPE_LARGE_STRING;
}
else if (sqltype == hyperapi::SqlType::boolean()) {
  return NANOARROW_TYPE_BOOL;
}
else if (sqltype == hyperapi::SqlType::timestamp()) {
  return NANOARROW_TYPE_TIMESTAMP;
}
else if (sqltype == hyperapi::SqlType::timestampTZ()) {
  return NANOARROW_TYPE_TIMESTAMP; // todo: how to encode tz info?
}
else if (sqltype == hyperapi::SqlType::date()) {
  return NANOARROW_TYPE_DATE32;
}

throw nb::type_error(
    ("unimplemented pandas dtype for type: " + sqltype.toString()).c_str());
}

static auto releaseArrowStream(void *ptr) noexcept -> void {
  auto stream = static_cast<ArrowArrayStream *>(ptr);
  ArrowArrayStreamRelease(stream);
}

///
/// read_from_hyper_query is slightly different than read_from_hyper_table
/// because the former detects a schema from the hyper Result object
/// which does not hold nullability information
///
auto read_from_hyper_query(const std::string &path, const std::string &query)
    -> nb::capsule {
  hyperapi::HyperProcess hyper{
      hyperapi::Telemetry::DoNotSendUsageDataToTableau};
  hyperapi::Connection connection(hyper.getEndpoint(), path);

  hyperapi::Result hyperResult = connection.executeQuery(query);
  const auto resultSchema = hyperResult.getSchema();

  auto schema = std::unique_ptr<struct ArrowSchema>{new (struct ArrowSchema)};

  ArrowSchemaInit(schema.get());
  if (ArrowSchemaSetTypeStruct(schema.get(), resultSchema.getColumnCount())) {
    throw std::runtime_error("ArrowSchemaSetTypeStruct failed");
  }

  const auto column_count = resultSchema.getColumnCount();
  for (size_t i = 0; i < column_count; i++) {
    const auto column = resultSchema.getColumn(i);
    auto name = column.getName().getUnescaped();
    if (ArrowSchemaSetName(schema->children[i], name.c_str())) {
      throw std::runtime_error("ArrowSchemaSetName failed");
    }

    auto const sqltype = column.getType();
    if (sqltype.getTag() == hyperapi::TypeTag::TimestampTZ) {
      if (ArrowSchemaSetTypeDateTime(schema->children[i],
                                     NANOARROW_TYPE_TIMESTAMP,
                                     NANOARROW_TIME_UNIT_MICRO, "UTC")) {
        throw std::runtime_error("ArrowSchemaSetDateTime failed");
      }
    } else if (sqltype.getTag() == hyperapi::TypeTag::Timestamp) {
      if (ArrowSchemaSetTypeDateTime(schema->children[i],
                                     NANOARROW_TYPE_TIMESTAMP,
                                     NANOARROW_TIME_UNIT_MICRO, nullptr)) {
        throw std::runtime_error("ArrowSchemaSetDateTime failed");
      }
    } else {
      const enum ArrowType arrow_type = arrowTypeFromHyper(sqltype);
      if (ArrowSchemaSetType(schema->children[i], arrow_type)) {
        throw std::runtime_error("ArrowSchemaSetType failed");
      }
    }
  }

  auto array = std::unique_ptr<struct ArrowArray>{new (struct ArrowArray)};
  if (ArrowArrayInitFromSchema(array.get(), schema.get(), nullptr)) {
    throw std::runtime_error("ArrowSchemaInitFromSchema failed");
  }
  std::vector<std::unique_ptr<ReadHelper>> read_helpers{column_count};
  for (size_t i = 0; i < column_count; i++) {
    struct ArrowSchemaView schema_view;
    if (ArrowSchemaViewInit(&schema_view, schema->children[i], nullptr)) {
      throw std::runtime_error("ArrowSchemaViewInit failed");
    }

    auto read_helper = makeReadHelper(&schema_view, array->children[i]);
    read_helpers[i] = std::move(read_helper);
  }

  if (ArrowArrayStartAppending(array.get())) {
    throw std::runtime_error("ArrowArrayStartAppending failed");
  }
  for (const hyperapi::Row &row : hyperResult) {
    size_t column_idx = 0;
    for (const hyperapi::Value &value : row) {
      const auto &read_helper = read_helpers[column_idx];
      read_helper->Read(value);
      column_idx++;
    }
    if (ArrowArrayFinishElement(array.get())) {
      throw std::runtime_error("ArrowArrayFinishElement failed");
    }
  }
  if (ArrowArrayFinishBuildingDefault(array.get(), nullptr)) {
    throw std::runtime_error("ArrowArrayFinishBuildingDefault failed");
  }

  auto stream =
      (struct ArrowArrayStream *)malloc(sizeof(struct ArrowArrayStream));
  if (ArrowBasicArrayStreamInit(stream, schema.get(), 1)) {
    free(stream);
    throw std::runtime_error("ArrowBasicArrayStreamInit failed");
  }
  ArrowBasicArrayStreamSetArray(stream, 0, array.get());

  nb::capsule result{stream, "arrow_array_stream", &releaseArrowStream};
  return result;
}

auto get_sample_capsule() -> nb::capsule {
  auto schema = std::unique_ptr<struct ArrowSchema>{new (struct ArrowSchema)};

  ArrowSchemaInit(schema.get());
  if (ArrowSchemaSetTypeStruct(schema.get(), 1)) {
    throw std::runtime_error("ArrowSchemaSetTypeStruct failed");
  }
  if (ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_INT32)) {
    throw std::runtime_error("ArrowSchemaSetType failed");
  }

  if (ArrowSchemaSetName(schema->children[0], "int_column")) {
    throw std::runtime_error("ArrowSchemaSetName failed");
  }

  auto array = std::unique_ptr<struct ArrowArray>{new (struct ArrowArray)};
  if (ArrowArrayInitFromSchema(array.get(), schema.get(), nullptr)) {
    throw std::runtime_error("ArrowSchemaInitFromSchema failed");
  }
  if (ArrowArrayStartAppending(array.get())) {
    throw std::runtime_error("ArrowArrayStartAppending failed");
  }
  for (auto i : {1, 2, 4, 8}) {
    if (ArrowArrayAppendInt(array->children[0], i)) {
      throw std::runtime_error("ArrowArrayAppendInt failed");
    }
    if (ArrowArrayFinishElement(array.get())) {
      throw std::runtime_error("ArrowArrayFinishElement failed");
    }
  }
  if (ArrowArrayFinishBuildingDefault(array.get(), nullptr)) {
    throw std::runtime_error("ArrowArrayFinishBuildingDefault failed");
  }

  auto stream =
      (struct ArrowArrayStream *)malloc(sizeof(struct ArrowArrayStream));
  if (ArrowBasicArrayStreamInit(stream, schema.get(), 1)) {
    free(stream);
    throw std::runtime_error("ArrowBasicArrayStreamInit failed");
  }
  ArrowBasicArrayStreamSetArray(stream, 0, array.get());

  nb::capsule result{stream, "arrow_array_stream", &releaseArrowStream};
  return result;
}

NB_MODULE(pantab, m) { // NOLINT
  m.def("write_to_hyper", &write_to_hyper, nb::arg("dict_of_exportable"),
        nb::arg("path"), nb::arg("table_mode"))
      .def("read_from_hyper_query", &read_from_hyper_query, nb::arg("path"),
           nb::arg("query"))
      .def("get_sample_capsule", &get_sample_capsule);
  PyDateTime_IMPORT;
}
