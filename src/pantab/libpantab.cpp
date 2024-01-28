#include <chrono>
#include <string>
#include <tuple>
#include <vector>

#include <hyperapi/hyperapi.hpp>
#include <hyperapi/impl/Inserter.impl.hpp>
#include <nanoarrow/nanoarrow.hpp>
#include <nanobind/nanobind.h>
#include <nanobind/stl/map.h>
#include <nanobind/stl/set.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/tuple.h>

#include "datetime.h"
#include "nanoarrow/array_inline.h"
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
  case NANOARROW_TYPE_UINT32:
    return hyperapi::SqlType::oid();
  case NANOARROW_TYPE_FLOAT:
  case NANOARROW_TYPE_DOUBLE:
    return hyperapi::SqlType::doublePrecision();
  case NANOARROW_TYPE_BOOL:
    return hyperapi::SqlType::boolean();
  case NANOARROW_TYPE_BINARY:
  case NANOARROW_TYPE_LARGE_BINARY:
    return hyperapi::SqlType::bytes();
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
  case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
    return hyperapi::SqlType::interval();
  case NANOARROW_TYPE_TIME64:
    return hyperapi::SqlType::time();
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
        error_(error), column_position_(column_position) {
    const struct ArrowSchema *child_schema =
        schema_->children[column_position_];

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

  virtual ~InsertHelper() = default;
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

class UInt32InsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    if (ArrowArrayViewIsNull(&array_view_, idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<T>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    const uint64_t value = ArrowArrayViewGetUIntUnsafe(&array_view_, idx);
    hyperapi::internal::ValueInserter{*inserter_}.addValue(
        static_cast<uint32_t>(value));
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

class BinaryInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    if (ArrowArrayViewIsNull(&array_view_, idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<std:::string_view>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    const struct ArrowBufferView buffer_view =
        ArrowArrayViewGetBytesUnsafe(&array_view_, idx);
    const hyperapi::ByteSpan result{
        buffer_view.data.as_uint8, static_cast<size_t>(buffer_view.size_bytes)};
    hyperapi::internal::ValueInserter{*inserter_}.addValue(result);
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

    const struct ArrowBufferView buffer_view =
        ArrowArrayViewGetBytesUnsafe(&array_view_, idx);
#if defined(_WIN32) && defined(_MSC_VER)
    const auto result = std::string{
        buffer_view.data.as_char, static_cast<size_t>(buffer_view.size_bytes)};
#else
    const auto result = std::string_view{
        buffer_view.data.as_char, static_cast<size_t>(buffer_view.size_bytes)};
#endif
    hyperapi::internal::ValueInserter{*inserter_}.addValue(result);
  }
};

class Date32InsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    constexpr size_t elem_size = sizeof(int32_t);
    if (ArrowArrayViewIsNull(&array_view_, idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<timestamp_t>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    int32_t value;
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
    const hyperapi::Date dt{1900 + utc_tm.tm_year,
                            static_cast<int16_t>(1 + utc_tm.tm_mon),
                            static_cast<int16_t>(1 + utc_tm.tm_yday)};

    hyperapi::internal::ValueInserter{*inserter_}.addValue(dt);
  }
};

template <enum ArrowTimeUnit TU> class TimeInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    if (ArrowArrayViewIsNull(&array_view_, idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<T>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    int64_t value = ArrowArrayViewGetIntUnsafe(&array_view_, idx);
    // TODO: check for overflow in these branches
    if constexpr (TU == NANOARROW_TIME_UNIT_SECOND) {
      value *= 1'000'000;
    } else if constexpr (TU == NANOARROW_TIME_UNIT_MILLI) {
      value *= 1000;
    } else if constexpr (TU == NANOARROW_TIME_UNIT_NANO) {
      value /= 1000;
    }
    hyperapi::internal::ValueInserter{*inserter_}.addValue(value);
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
    const hyperapi::Date dt{static_cast<int32_t>(dts.year),
                            static_cast<int16_t>(dts.month),
                            static_cast<int16_t>(dts.day)};
    const hyperapi::Time time{static_cast<int8_t>(dts.hour),
                              static_cast<int8_t>(dts.min),
                              static_cast<int8_t>(dts.sec), dts.us};

    if constexpr (TZAware) {
      const hyperapi::OffsetTimestamp ts{dt, time, std::chrono::minutes{0}};
      hyperapi::internal::ValueInserter{*inserter_}.addValue(
          static_cast<hyperapi::OffsetTimestamp>(ts));

    } else {
      const hyperapi::Timestamp ts{dt, time};
      hyperapi::internal::ValueInserter{*inserter_}.addValue(
          static_cast<hyperapi::Timestamp>(ts));
    }
  }
};

class IntervalInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    if (ArrowArrayViewIsNull(&array_view_, idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<timestamp_t>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    struct ArrowInterval arrow_interval;
    ArrowIntervalInit(&arrow_interval, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
    ArrowArrayViewGetIntervalUnsafe(&array_view_, idx, &arrow_interval);
    const auto usec = static_cast<int32_t>(arrow_interval.ns / 1000);

    // Hyper has no template specialization to insert an interval; instead we
    // must use their internal representation
    hyperapi::Interval interval(0, arrow_interval.months, arrow_interval.days,
                                0, 0, 0, usec);
    // hyperapi::Interval interval{0, arrow_interval.months,
    // arrow_interval.days, 0, 0, 0, usec};
    inserter_->add(interval);
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
  case NANOARROW_TYPE_UINT32:
    return std::unique_ptr<InsertHelper>(new UInt32InsertHelper(
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
  case NANOARROW_TYPE_BINARY:
  case NANOARROW_TYPE_LARGE_BINARY:
    return std::unique_ptr<InsertHelper>(new BinaryInsertHelper(
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
  case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
    return std::unique_ptr<InsertHelper>(new IntervalInsertHelper(
        inserter, chunk, schema, error, column_position));
  case NANOARROW_TYPE_TIME64:
    switch (schema_view.time_unit) {
    // must be a smarter way to do this!
    case NANOARROW_TIME_UNIT_SECOND: // untested
      return std::unique_ptr<InsertHelper>(
          new TimeInsertHelper<NANOARROW_TIME_UNIT_SECOND>(
              inserter, chunk, schema, error, column_position));
    case NANOARROW_TIME_UNIT_MILLI: // untested
      return std::unique_ptr<InsertHelper>(
          new TimeInsertHelper<NANOARROW_TIME_UNIT_MILLI>(
              inserter, chunk, schema, error, column_position));
    case NANOARROW_TIME_UNIT_MICRO:
      return std::unique_ptr<InsertHelper>(
          new TimeInsertHelper<NANOARROW_TIME_UNIT_MICRO>(
              inserter, chunk, schema, error, column_position));
    case NANOARROW_TIME_UNIT_NANO:
      return std::unique_ptr<InsertHelper>(
          new TimeInsertHelper<NANOARROW_TIME_UNIT_NANO>(
              inserter, chunk, schema, error, column_position));
    }
    throw std::runtime_error(
        "This code block should not be hit - contact a developer");
  default:
    throw std::invalid_argument("makeInsertHelper: Unsupported Arrow type: " +
                                std::to_string(schema_view.type));
  }
}

static bool isCompatibleHyperType(const hyperapi::SqlType &new_type,
                                  const hyperapi::SqlType &old_type) {
  if (new_type == old_type) {
    return true;
  }

  // we don't ever write varchar, but a user may want to append to a database
  // which has an existing varchar column and I *think* that is OK
  if ((new_type == hyperapi::SqlType::text()) &&
      (old_type.getTag() == hyperapi::TypeTag::Varchar)) {
    return true;
  }

  return false;
}

///
/// If a table already exists, ensure the structure is the same as what we
/// append
///
static void assertColumnsEqual(
    const std::vector<hyperapi::TableDefinition::Column> &new_columns,
    const std::vector<hyperapi::TableDefinition::Column> &old_columns) {
  const size_t new_size = new_columns.size();
  const size_t old_size = old_columns.size();
  if (new_size != old_size) {
    throw std::invalid_argument(
        "Number of columns in new table definition does not match existing");
  }

  for (size_t i = 0; i < new_size; i++) {
    const auto new_col = new_columns[i];
    const auto old_col = old_columns[i];
    const auto new_name = new_col.getName();
    const auto old_name = old_col.getName();
    if (new_name != old_name) {
      throw std::invalid_argument(
          "Column name mismatch at index " + std::to_string(i) +
          "; new: " + new_name.toString() + " old: " + old_name.toString());
    }

    const auto new_type = new_col.getType();
    const auto old_type = old_col.getType();
    if (!isCompatibleHyperType(new_type, old_type)) {
      throw std::invalid_argument(
          "Column type mismatch at index " + std::to_string(i) +
          "; new: " + new_type.toString() + " old: " + old_type.toString());
    }
  }
};

using SchemaAndTableName = std::tuple<std::string, std::string>;

void write_to_hyper(
    const std::map<SchemaAndTableName, nb::capsule> &dict_of_capsules,
    const std::string &path, const std::string &table_mode,
    const std::set<std::string> &json_columns,
    const std::set<std::string> &geo_columns) {
  const std::unordered_map<std::string, std::string> params = {
      {"log_config", ""}};
  const hyperapi::HyperProcess hyper{
      hyperapi::Telemetry::DoNotSendUsageDataToTableau, "", std::move(params)};

  // TODO: we don't have separate table / database create modes in the API
  // but probably should; for now we infer this from table mode
  const auto createMode = table_mode == "w"
                              ? hyperapi::CreateMode::CreateAndReplace
                              : hyperapi::CreateMode::CreateIfNotExists;

  hyperapi::Connection connection{hyper.getEndpoint(), path, createMode};
  const hyperapi::Catalog &catalog = connection.getCatalog();

  for (auto const &[schema_and_table, capsule] : dict_of_capsules) {
    const auto hyper_schema = std::get<0>(schema_and_table);
    const auto hyper_table = std::get<1>(schema_and_table);

    const auto c_stream = static_cast<struct ArrowArrayStream *>(
        PyCapsule_GetPointer(capsule.ptr(), "arrow_array_stream"));
    auto stream = nanoarrow::UniqueArrayStream{c_stream};

    struct ArrowSchema schema;
    if (stream->get_schema(stream.get(), &schema) != 0) {
      std::string error_msg{stream->get_last_error(stream.get())};
      throw std::runtime_error("Could not read from arrow schema:" + error_msg);
    }

    struct ArrowError error;
    std::vector<hyperapi::TableDefinition::Column> hyper_columns;
    std::vector<hyperapi::Inserter::ColumnMapping> column_mappings;
    // subtley different from hyper_columns with geo
    std::vector<hyperapi::TableDefinition::Column> inserter_defs;
    for (int64_t i = 0; i < schema.n_children; i++) {
      const auto col_name = std::string{schema.children[i]->name};
      if (json_columns.find(col_name) != json_columns.end()) {
        const auto hypertype = hyperapi::SqlType::json();
        const hyperapi::TableDefinition::Column column{
            col_name, hypertype, hyperapi::Nullability::Nullable};

        hyper_columns.emplace_back(column);
        inserter_defs.emplace_back(std::move(column));
        const hyperapi::Inserter::ColumnMapping mapping{col_name};
        column_mappings.emplace_back(mapping);
      } else if (geo_columns.find(col_name) != geo_columns.end()) {
        // if binary just write as is; for text we provide conversion
        const auto detected_type =
            hyperTypeFromArrowSchema(schema.children[i], &error);
        if (detected_type == hyperapi::SqlType::text()) {
          const auto hypertype = hyperapi::SqlType::geography();
          const hyperapi::TableDefinition::Column column{
              col_name, hypertype, hyperapi::Nullability::Nullable};

          hyper_columns.emplace_back(std::move(column));
          const auto insertertype = hyperapi::SqlType::text();
          const auto as_text_name = col_name + "_as_text";
          const hyperapi::TableDefinition::Column inserter_column{
              as_text_name, insertertype, hyperapi::Nullability::Nullable};
          inserter_defs.emplace_back(std::move(inserter_column));

          const auto escaped = hyperapi::escapeName(as_text_name);
          const hyperapi::Inserter::ColumnMapping mapping{
              col_name, "CAST(" + escaped + " AS GEOGRAPHY)"};
          column_mappings.emplace_back(mapping);
        } else if (detected_type == hyperapi::SqlType::bytes()) {
          const auto hypertype = hyperapi::SqlType::geography();
          const hyperapi::TableDefinition::Column column{
              col_name, hypertype, hyperapi::Nullability::Nullable};

          hyper_columns.emplace_back(column);
          inserter_defs.emplace_back(std::move(column));
          const hyperapi::Inserter::ColumnMapping mapping{col_name};
          column_mappings.emplace_back(mapping);
        } else {
          throw std::runtime_error(
              "Unexpected code path hit - contact a developer");
        }
      } else {
        const auto hypertype =
            hyperTypeFromArrowSchema(schema.children[i], &error);
        const hyperapi::TableDefinition::Column column{
            col_name, hypertype, hyperapi::Nullability::Nullable};

        hyper_columns.emplace_back(column);
        inserter_defs.emplace_back(std::move(column));
        const hyperapi::Inserter::ColumnMapping mapping{col_name};
        column_mappings.emplace_back(mapping);
      }
    }

    const hyperapi::TableName table_name{hyper_schema, hyper_table};
    const hyperapi::TableDefinition tableDef{table_name, hyper_columns};
    catalog.createSchemaIfNotExists(*table_name.getSchemaName());

    if ((table_mode == "a") && (catalog.hasTable(table_name))) {
      const auto existing_def = catalog.getTableDefinition(table_name);
      assertColumnsEqual(hyper_columns, std::move(existing_def.getColumns()));
    } else {
      catalog.createTable(tableDef);
    }
    const auto inserter = std::make_shared<hyperapi::Inserter>(
        connection, tableDef, column_mappings, inserter_defs);

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

template <typename T> class IntegralReadHelper : public ReadHelper {
  using ReadHelper::ReadHelper;

  auto Read(const hyperapi::Value &value) -> void override {
    if (value.isNull()) {
      if (ArrowArrayAppendNull(array_, 1)) {
        throw std::runtime_error("ArrowAppendNull failed");
      }
      return;
    }
    if (ArrowArrayAppendInt(array_, value.get<T>())) {
      throw std::runtime_error("ArrowAppendInt failed");
    };
  }
};

class OidReadHelper : public ReadHelper {
  using ReadHelper::ReadHelper;

  auto Read(const hyperapi::Value &value) -> void override {
    if (value.isNull()) {
      if (ArrowArrayAppendNull(array_, 1)) {
        throw std::runtime_error("ArrowAppendNull failed");
      }
      return;
    }
    if (ArrowArrayAppendUInt(array_, value.get<uint32_t>())) {
      throw std::runtime_error("ArrowAppendUInt failed");
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

class BytesReadHelper : public ReadHelper {
  using ReadHelper::ReadHelper;

  auto Read(const hyperapi::Value &value) -> void override {
    if (value.isNull()) {
      if (ArrowArrayAppendNull(array_, 1)) {
        throw std::runtime_error("ArrowAppendNull failed");
      }
      return;
    }

    // TODO: we can use the non-owning hyperapi::ByteSpan template type but
    // there is a bug in that header file that needs an upstream fix first
    const auto bytes = value.get<std::vector<uint8_t>>();
    const ArrowBufferView arrow_buffer_view{bytes.data(),
                                            static_cast<int64_t>(bytes.size())};

    if (ArrowArrayAppendBytes(array_, arrow_buffer_view)) {
      throw std::runtime_error("ArrowAppendString failed");
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

#if defined(_WIN32) && defined(_MSC_VER)
    const auto strval = value.get<std::string>();
    const ArrowStringView arrow_string_view{
        strval.c_str(), static_cast<int64_t>(strval.size())};
#else
    const auto strval = value.get<std::string_view>();
    const ArrowStringView arrow_string_view{
        strval.data(), static_cast<int64_t>(strval.size())};
#endif

    if (ArrowArrayAppendString(array_, arrow_string_view)) {
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

    struct ArrowBitmap *validity_bitmap = ArrowArrayValidityBitmap(array_);
    if (ArrowBitmapAppend(validity_bitmap, true, 1)) {
      throw std::runtime_error("Could not append validity buffer for date32");
    };
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

    struct ArrowBitmap *validity_bitmap = ArrowArrayValidityBitmap(array_);
    if (ArrowBitmapAppend(validity_bitmap, true, 1)) {
      throw std::runtime_error(
          "Could not append validity buffer for timestamp");
    };
    array_->length++;
  }
};

class TimeReadHelper : public ReadHelper {
  using ReadHelper::ReadHelper;

  auto Read(const hyperapi::Value &value) -> void override {
    if (value.isNull()) {
      if (ArrowArrayAppendNull(array_, 1)) {
        throw std::runtime_error("ArrowAppendNull failed");
      }
      return;
    }

    const auto time = value.get<hyperapi::Time>();
    const auto raw_value = time.getRaw();
    if (ArrowArrayAppendInt(array_, raw_value)) {
      throw std::runtime_error("ArrowAppendInt failed");
    }
  }
};

class IntervalReadHelper : public ReadHelper {
  using ReadHelper::ReadHelper;

  auto Read(const hyperapi::Value &value) -> void override {
    if (value.isNull()) {
      if (ArrowArrayAppendNull(array_, 1)) {
        throw std::runtime_error("ArrowAppendNull failed");
      }
      return;
    }

    struct ArrowInterval arrow_interval;
    ArrowIntervalInit(&arrow_interval, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
    const auto interval_value = value.get<hyperapi::Interval>();
    arrow_interval.months =
        interval_value.getYears() * 12 + interval_value.getMonths();
    arrow_interval.days = interval_value.getDays();
    arrow_interval.ns = interval_value.getHours() * 3'600'000'000'000LL +
                        interval_value.getMinutes() * 60'000'000'000LL +
                        interval_value.getSeconds() * 1'000'000'000LL +
                        interval_value.getMicroseconds() * 1'000LL;

    if (ArrowArrayAppendInterval(array_, &arrow_interval)) {
      throw std::runtime_error("Failed to append interval value");
    }
  }
};

static auto makeReadHelper(const ArrowSchemaView *schema_view,
                           struct ArrowArray *array)
    -> std::unique_ptr<ReadHelper> {
  switch (schema_view->type) {
  case NANOARROW_TYPE_INT16:
    return std::unique_ptr<ReadHelper>(new IntegralReadHelper<int16_t>(array));
  case NANOARROW_TYPE_INT32:
    return std::unique_ptr<ReadHelper>(new IntegralReadHelper<int32_t>(array));
  case NANOARROW_TYPE_INT64:
    return std::unique_ptr<ReadHelper>(new IntegralReadHelper<int64_t>(array));
  case NANOARROW_TYPE_UINT32:
    return std::unique_ptr<ReadHelper>(new OidReadHelper(array));
  case NANOARROW_TYPE_DOUBLE:
    return std::unique_ptr<ReadHelper>(new FloatReadHelper(array));
  case NANOARROW_TYPE_LARGE_BINARY:
    return std::unique_ptr<ReadHelper>(new BytesReadHelper(array));
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
  case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
    return std::unique_ptr<ReadHelper>(new IntervalReadHelper(array));
  case NANOARROW_TYPE_TIME64:
    return std::unique_ptr<ReadHelper>(new TimeReadHelper(array));
  default:
    throw nb::type_error("unknownn arrow type provided");
  }
}

static auto arrowTypeFromHyper(const hyperapi::SqlType &sqltype)
    -> enum ArrowType {
      switch (sqltype.getTag()){
        case hyperapi::TypeTag::SmallInt : return NANOARROW_TYPE_INT16;
        case hyperapi::TypeTag::Int : return NANOARROW_TYPE_INT32;
        case hyperapi::TypeTag::BigInt : return NANOARROW_TYPE_INT64;
        case hyperapi::TypeTag::Oid : return NANOARROW_TYPE_UINT32;
        case hyperapi::TypeTag::Double : return NANOARROW_TYPE_DOUBLE;
        case hyperapi::TypeTag::Geography : case hyperapi::TypeTag::
        Bytes : return NANOARROW_TYPE_LARGE_BINARY;
        case hyperapi::TypeTag::Varchar : case hyperapi::TypeTag::
        Char : case hyperapi::TypeTag::Text : case hyperapi::TypeTag::
        Json : return NANOARROW_TYPE_LARGE_STRING;
        case hyperapi::TypeTag::Bool : return NANOARROW_TYPE_BOOL;
        case hyperapi::TypeTag::Date : return NANOARROW_TYPE_DATE32;
        case hyperapi::TypeTag::Timestamp : case hyperapi::TypeTag::
        TimestampTZ : return NANOARROW_TYPE_TIMESTAMP;
        case hyperapi::TypeTag::
        Interval : return NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO;
        case hyperapi::TypeTag::Time : return NANOARROW_TYPE_TIME64;
        default : throw nb::type_error(
            ("Reader not implemented for type: " + sqltype.toString()).c_str());
      }
}

static auto releaseArrowStream(void *ptr) noexcept -> void {
  auto stream = static_cast<ArrowArrayStream *>(ptr);
  if (stream->release != nullptr) {
    ArrowArrayStreamRelease(stream);
  }
}

///
/// read_from_hyper_query is slightly different than read_from_hyper_table
/// because the former detects a schema from the hyper Result object
/// which does not hold nullability information
///
auto read_from_hyper_query(const std::string &path, const std::string &query)
    -> nb::capsule {
  const std::unordered_map<std::string, std::string> params = {
      {"log_config", ""}};
  const hyperapi::HyperProcess hyper{
      hyperapi::Telemetry::DoNotSendUsageDataToTableau, "", std::move(params)};
  hyperapi::Connection connection(hyper.getEndpoint(), path);

  auto hyperResult = connection.executeQuery(query);
  const auto resultSchema = hyperResult.getSchema();

  auto schema = std::unique_ptr<struct ArrowSchema>{new (struct ArrowSchema)};

  ArrowSchemaInit(schema.get());
  if (ArrowSchemaSetTypeStruct(schema.get(), resultSchema.getColumnCount())) {
    throw std::runtime_error("ArrowSchemaSetTypeStruct failed");
  }

  const auto column_count = resultSchema.getColumnCount();
  for (size_t i = 0; i < column_count; i++) {
    const auto column = resultSchema.getColumn(i);
    const auto name = column.getName().getUnescaped();
    if (ArrowSchemaSetName(schema->children[i], name.c_str())) {
      throw std::runtime_error("ArrowSchemaSetName failed");
    }

    const auto sqltype = column.getType();
    switch (sqltype.getTag()) {
    case hyperapi::TypeTag::TimestampTZ:
      if (ArrowSchemaSetTypeDateTime(schema->children[i],
                                     NANOARROW_TYPE_TIMESTAMP,
                                     NANOARROW_TIME_UNIT_MICRO, "UTC")) {
        throw std::runtime_error(
            "ArrowSchemaSetDateTime failed for TimestampTZ type");
      }
      break;
    case hyperapi::TypeTag::Timestamp:
      if (ArrowSchemaSetTypeDateTime(schema->children[i],
                                     NANOARROW_TYPE_TIMESTAMP,
                                     NANOARROW_TIME_UNIT_MICRO, nullptr)) {
        throw std::runtime_error(
            "ArrowSchemaSetDateTime failed for Timestamp type");
      }
      break;
    case hyperapi::TypeTag::Time:
      if (ArrowSchemaSetTypeDateTime(schema->children[i], NANOARROW_TYPE_TIME64,
                                     NANOARROW_TIME_UNIT_MICRO, nullptr)) {
        throw std::runtime_error("ArrowSchemaSetDateTime failed for Time type");
      }
      break;
    default:
      const enum ArrowType arrow_type = arrowTypeFromHyper(sqltype);
      if (ArrowSchemaSetType(schema->children[i], arrow_type)) {
        throw std::runtime_error("ArrowSchemaSetType failed");
      }
    }
  }

  const auto array =
      std::unique_ptr<struct ArrowArray>{new (struct ArrowArray)};
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
  for (const auto &row : hyperResult) {
    size_t column_idx = 0;
    for (const auto &value : row) {
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

NB_MODULE(libpantab, m) { // NOLINT
  m.def("write_to_hyper", &write_to_hyper, nb::arg("dict_of_capsules"),
        nb::arg("path"), nb::arg("table_mode"), nb::arg("json_columns"),
        nb::arg("geo_columns"))
      .def("read_from_hyper_query", &read_from_hyper_query, nb::arg("path"),
           nb::arg("query"));
  PyDateTime_IMPORT;
}
