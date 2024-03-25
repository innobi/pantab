#include "writer.hpp"

#include <chrono>
#include <set>

#include <hyperapi/hyperapi.hpp>
#include <nanoarrow/nanoarrow.hpp>

#include "numpy_datetime.h"

static auto GetHyperTypeFromArrowSchema(struct ArrowSchema *schema,
                                        ArrowError *error)
    -> hyperapi::SqlType {
  struct ArrowSchemaView schema_view;
  if (ArrowSchemaViewInit(&schema_view, schema, error) != 0) {
    throw std::runtime_error("Issue converting to hyper type: " +
                             std::string(error->message));
  }

  switch (schema_view.type) {
  case NANOARROW_TYPE_INT8:
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

    if (ArrowArrayViewInitFromSchema(array_view_.get(), child_schema, error_) !=
        0) {
      throw std::runtime_error("Could not construct insert helper: " +
                               std::string{error_->message});
    }

    if (ArrowArrayViewSetArray(array_view_.get(),
                               chunk_->children[column_position_],
                               error_) != 0) {
      throw std::runtime_error("Could not set array view: " +
                               std::string{error_->message});
    }
  }

  virtual ~InsertHelper() = default;
  virtual void InsertValueAtIndex(size_t) {}

protected:
  std::shared_ptr<hyperapi::Inserter> inserter_;
  const struct ArrowArray *chunk_;
  const struct ArrowSchema *schema_;
  struct ArrowError *error_;
  const int64_t column_position_;
  nanoarrow::UniqueArrayView array_view_;
};

template <typename T> class IntegralInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(size_t idx) override {
    if (ArrowArrayViewIsNull(array_view_.get(), idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<T>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    const int64_t value = ArrowArrayViewGetIntUnsafe(array_view_.get(), idx);
    hyperapi::internal::ValueInserter{*inserter_}.addValue(
        static_cast<T>(value));
  }
};

class UInt32InsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(size_t idx) override {
    if (ArrowArrayViewIsNull(array_view_.get(), idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<T>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    const uint64_t value = ArrowArrayViewGetUIntUnsafe(array_view_.get(), idx);
    hyperapi::internal::ValueInserter{*inserter_}.addValue(
        static_cast<uint32_t>(value));
  }
};

template <typename T> class FloatingInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(size_t idx) override {
    if (ArrowArrayViewIsNull(array_view_.get(), idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<T>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    const double value = ArrowArrayViewGetDoubleUnsafe(array_view_.get(), idx);
    hyperapi::internal::ValueInserter{*inserter_}.addValue(
        static_cast<T>(value));
  }
};

class BinaryInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(size_t idx) override {
    if (ArrowArrayViewIsNull(array_view_.get(), idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<std:::string_view>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    const struct ArrowBufferView buffer_view =
        ArrowArrayViewGetBytesUnsafe(array_view_.get(), idx);
    const hyperapi::ByteSpan result{
        buffer_view.data.as_uint8, static_cast<size_t>(buffer_view.size_bytes)};
    hyperapi::internal::ValueInserter{*inserter_}.addValue(result);
  }
};

template <typename OffsetT> class Utf8InsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(size_t idx) override {
    if (ArrowArrayViewIsNull(array_view_.get(), idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<std:::string_view>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    const struct ArrowBufferView buffer_view =
        ArrowArrayViewGetBytesUnsafe(array_view_.get(), idx);
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

  void InsertValueAtIndex(size_t idx) override {
    constexpr size_t elem_size = sizeof(int32_t);
    if (ArrowArrayViewIsNull(array_view_.get(), idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<timestamp_t>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    int32_t value;
    memcpy(&value,
           array_view_->buffer_views[1].data.as_uint8 + (idx * elem_size),
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
                            static_cast<int16_t>(utc_tm.tm_mday)};

    hyperapi::internal::ValueInserter{*inserter_}.addValue(dt);
  }
};

template <enum ArrowTimeUnit TU> class TimeInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(size_t idx) override {
    if (ArrowArrayViewIsNull(array_view_.get(), idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<T>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    int64_t value = ArrowArrayViewGetIntUnsafe(array_view_.get(), idx);
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

template <enum ArrowTimeUnit TU, bool TZAware>
class TimestampInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(size_t idx) override {
    constexpr size_t elem_size = sizeof(int64_t);
    if (ArrowArrayViewIsNull(array_view_.get(), idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<timestamp_t>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
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
    if constexpr (TU == NANOARROW_TIME_UNIT_SECOND) {
      meta = {NPY_FR_s, 1};
    } else if constexpr (TU == NANOARROW_TIME_UNIT_MILLI) {
      meta = {NPY_FR_ms, 1};
    } else if constexpr (TU == NANOARROW_TIME_UNIT_MICRO) {
      meta = {NPY_FR_us, 1};
    } else if constexpr (TU == NANOARROW_TIME_UNIT_NANO) {
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

  void InsertValueAtIndex(size_t idx) override {
    if (ArrowArrayViewIsNull(array_view_.get(), idx)) {
      // MSVC on cibuildwheel doesn't like this templated optional
      // inserter_->add(std::optional<timestamp_t>{std::nullopt});
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    struct ArrowInterval arrow_interval;
    ArrowIntervalInit(&arrow_interval, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
    ArrowArrayViewGetIntervalUnsafe(array_view_.get(), idx, &arrow_interval);
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

static auto MakeInsertHelper(std::shared_ptr<hyperapi::Inserter> inserter,
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
  case NANOARROW_TYPE_INT8:
  case NANOARROW_TYPE_INT16:
    return std::make_unique<IntegralInsertHelper<int16_t>>(
        inserter, chunk, schema, error, column_position);
  case NANOARROW_TYPE_INT32:
    return std::make_unique<IntegralInsertHelper<int32_t>>(
        inserter, chunk, schema, error, column_position);
  case NANOARROW_TYPE_INT64:
    return std::make_unique<IntegralInsertHelper<int64_t>>(
        inserter, chunk, schema, error, column_position);
  case NANOARROW_TYPE_UINT32:
    return std::make_unique<UInt32InsertHelper>(inserter, chunk, schema, error,
                                                column_position);
  case NANOARROW_TYPE_FLOAT:
    return std::make_unique<FloatingInsertHelper<float>>(
        inserter, chunk, schema, error, column_position);
  case NANOARROW_TYPE_DOUBLE:
    return std::make_unique<FloatingInsertHelper<double>>(
        inserter, chunk, schema, error, column_position);
  case NANOARROW_TYPE_BOOL:
    return std::make_unique<IntegralInsertHelper<bool>>(inserter, chunk, schema,
                                                        error, column_position);
  case NANOARROW_TYPE_BINARY:
  case NANOARROW_TYPE_LARGE_BINARY:
    return std::make_unique<BinaryInsertHelper>(inserter, chunk, schema, error,
                                                column_position);
  case NANOARROW_TYPE_STRING:
  case NANOARROW_TYPE_LARGE_STRING:
    return std::make_unique<Utf8InsertHelper<int64_t>>(inserter, chunk, schema,
                                                       error, column_position);
  case NANOARROW_TYPE_DATE32:
    return std::make_unique<Date32InsertHelper>(inserter, chunk, schema, error,
                                                column_position);
  case NANOARROW_TYPE_TIMESTAMP:
    switch (schema_view.time_unit) {
    case NANOARROW_TIME_UNIT_SECOND:
      if (std::strcmp("", schema_view.timezone)) {
        return std::make_unique<
            TimestampInsertHelper<NANOARROW_TIME_UNIT_SECOND, true>>(
            inserter, chunk, schema, error, column_position);
      } else {
        return std::make_unique<
            TimestampInsertHelper<NANOARROW_TIME_UNIT_SECOND, false>>(
            inserter, chunk, schema, error, column_position);
      }
    case NANOARROW_TIME_UNIT_MILLI:
      if (std::strcmp("", schema_view.timezone)) {
        return std::make_unique<
            TimestampInsertHelper<NANOARROW_TIME_UNIT_MILLI, true>>(
            inserter, chunk, schema, error, column_position);
      } else {
        return std::make_unique<
            TimestampInsertHelper<NANOARROW_TIME_UNIT_MILLI, false>>(
            inserter, chunk, schema, error, column_position);
      }
    case NANOARROW_TIME_UNIT_MICRO:
      if (std::strcmp("", schema_view.timezone)) {
        return std::make_unique<
            TimestampInsertHelper<NANOARROW_TIME_UNIT_MICRO, true>>(
            inserter, chunk, schema, error, column_position);
      } else {
        return std::make_unique<
            TimestampInsertHelper<NANOARROW_TIME_UNIT_MICRO, false>>(
            inserter, chunk, schema, error, column_position);
      }
    case NANOARROW_TIME_UNIT_NANO:
      if (std::strcmp("", schema_view.timezone)) {
        return std::make_unique<
            TimestampInsertHelper<NANOARROW_TIME_UNIT_NANO, true>>(
            inserter, chunk, schema, error, column_position);
      } else {
        return std::make_unique<
            TimestampInsertHelper<NANOARROW_TIME_UNIT_NANO, false>>(
            inserter, chunk, schema, error, column_position);
      }
    }
    throw std::runtime_error(
        "This code block should not be hit - contact a developer");
  case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
    return std::make_unique<IntervalInsertHelper>(inserter, chunk, schema,
                                                  error, column_position);
  case NANOARROW_TYPE_TIME64:
    switch (schema_view.time_unit) {
    // must be a smarter way to do this!
    case NANOARROW_TIME_UNIT_SECOND: // untested
      return std::make_unique<TimeInsertHelper<NANOARROW_TIME_UNIT_SECOND>>(
          inserter, chunk, schema, error, column_position);
    case NANOARROW_TIME_UNIT_MILLI: // untested
      return std::make_unique<TimeInsertHelper<NANOARROW_TIME_UNIT_MILLI>>(
          inserter, chunk, schema, error, column_position);
    case NANOARROW_TIME_UNIT_MICRO:
      return std::make_unique<TimeInsertHelper<NANOARROW_TIME_UNIT_MICRO>>(
          inserter, chunk, schema, error, column_position);
    case NANOARROW_TIME_UNIT_NANO:
      return std::make_unique<TimeInsertHelper<NANOARROW_TIME_UNIT_NANO>>(
          inserter, chunk, schema, error, column_position);
    }
    throw std::runtime_error(
        "This code block should not be hit - contact a developer");
  default:
    throw std::invalid_argument("MakeInsertHelper: Unsupported Arrow type: " +
                                std::to_string(schema_view.type));
  }
}

static bool IsCompatibleHyperType(const hyperapi::SqlType &new_type,
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
static void AssertColumnsEqual(
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
    if (!IsCompatibleHyperType(new_type, old_type)) {
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
    nb::iterable not_null_columns, nb::iterable json_columns,
    nb::iterable geo_columns) {

  std::set<std::string> not_null_set;
  for (auto col : not_null_columns) {
    const auto colstr = nb::cast<std::string>(col);
    not_null_set.insert(colstr);
  }

  std::set<std::string> json_set;
  for (auto col : json_columns) {
    const auto colstr = nb::cast<std::string>(col);
    json_set.insert(colstr);
  }

  std::set<std::string> geo_set;
  for (auto col : geo_columns) {
    const auto colstr = nb::cast<std::string>(col);
    geo_set.insert(colstr);
  }

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
    if (c_stream == nullptr) {
      throw std::invalid_argument("Invalid PyCapsule provided!");
    }
    auto stream = nanoarrow::UniqueArrayStream{c_stream};

    nanoarrow::UniqueSchema schema{};
    if (stream->get_schema(stream.get(), schema.get()) != 0) {
      std::string error_msg{stream->get_last_error(stream.get())};
      throw std::runtime_error("Could not read from arrow schema:" + error_msg);
    }

    struct ArrowError error;
    std::vector<hyperapi::TableDefinition::Column> hyper_columns;
    std::vector<hyperapi::Inserter::ColumnMapping> column_mappings;
    // subtley different from hyper_columns with geo
    std::vector<hyperapi::TableDefinition::Column> inserter_defs;
    for (int64_t i = 0; i < schema->n_children; i++) {
      const auto col_name = std::string{schema->children[i]->name};
      const auto nullability = not_null_set.find(col_name) != not_null_set.end()
                                   ? hyperapi::Nullability::NotNullable
                                   : hyperapi::Nullability::Nullable;

      if (json_set.find(col_name) != json_set.end()) {
        const auto hypertype = hyperapi::SqlType::json();
        const hyperapi::TableDefinition::Column column{col_name, hypertype,
                                                       nullability};

        hyper_columns.emplace_back(column);
        inserter_defs.emplace_back(std::move(column));
        const hyperapi::Inserter::ColumnMapping mapping{col_name};
        column_mappings.emplace_back(mapping);
      } else if (geo_set.find(col_name) != geo_set.end()) {
        // if binary just write as is; for text we provide conversion
        const auto detected_type =
            GetHyperTypeFromArrowSchema(schema->children[i], &error);
        if (detected_type == hyperapi::SqlType::text()) {
          const auto hypertype = hyperapi::SqlType::geography();
          const hyperapi::TableDefinition::Column column{col_name, hypertype,
                                                         nullability};

          hyper_columns.emplace_back(std::move(column));
          const auto insertertype = hyperapi::SqlType::text();
          const auto as_text_name = col_name + "_as_text";
          const hyperapi::TableDefinition::Column inserter_column{
              as_text_name, insertertype, nullability};
          inserter_defs.emplace_back(std::move(inserter_column));

          const auto escaped = hyperapi::escapeName(as_text_name);
          const hyperapi::Inserter::ColumnMapping mapping{
              col_name, "CAST(" + escaped + " AS GEOGRAPHY)"};
          column_mappings.emplace_back(mapping);
        } else if (detected_type == hyperapi::SqlType::bytes()) {
          const auto hypertype = hyperapi::SqlType::geography();
          const hyperapi::TableDefinition::Column column{col_name, hypertype,
                                                         nullability};

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
            GetHyperTypeFromArrowSchema(schema->children[i], &error);
        const hyperapi::TableDefinition::Column column{col_name, hypertype,
                                                       nullability};

        hyper_columns.emplace_back(column);
        inserter_defs.emplace_back(std::move(column));
        const hyperapi::Inserter::ColumnMapping mapping{col_name};
        column_mappings.emplace_back(mapping);
      }
    }

    const hyperapi::TableName table_name{hyper_schema, hyper_table};
    const hyperapi::TableDefinition table_def{table_name, hyper_columns};
    catalog.createSchemaIfNotExists(*table_name.getSchemaName());

    if ((table_mode == "a") && (catalog.hasTable(table_name))) {
      const auto existing_def = catalog.getTableDefinition(table_name);
      AssertColumnsEqual(hyper_columns, std::move(existing_def.getColumns()));
    } else {
      catalog.createTable(table_def);
    }
    const auto inserter = std::make_shared<hyperapi::Inserter>(
        connection, table_def, column_mappings, inserter_defs);

    struct ArrowArray c_chunk;
    int errcode;
    while ((errcode = stream->get_next(stream.get(), &c_chunk) == 0) &&
           c_chunk.release != nullptr) {
      nanoarrow::UniqueArray chunk{&c_chunk};
      const int nrows = chunk->length;
      if (nrows < 0) {
        throw std::runtime_error("Unexpected array length < 0");
      }

      std::vector<std::unique_ptr<InsertHelper>> insert_helpers;
      for (int64_t i = 0; i < schema->n_children; i++) {
        // the lifetime of the inserthelper cannot exceed that of chunk or
        // schema this is implicit; we should make this explicit
        auto insert_helper =
            MakeInsertHelper(inserter, chunk.get(), schema.get(), &error, i);

        insert_helpers.push_back(std::move(insert_helper));
      }

      for (int64_t row_idx = 0; row_idx < nrows; row_idx++) {
        for (const auto &insert_helper : insert_helpers) {
          insert_helper->InsertValueAtIndex(row_idx);
        }
        inserter->endRow();
      }
    }

    inserter->execute();
  }
}
