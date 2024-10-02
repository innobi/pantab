#include "writer.hpp"
#include "numeric_gen.hpp"

#include <hyperapi/hyperapi.hpp>
#include <nanoarrow/nanoarrow.hpp>

#include <chrono>
#include <set>
#include <span>
#include <utility>
#include <variant>

static auto GetHyperTypeFromArrowSchema(struct ArrowSchema *schema,
                                        ArrowError *error)
    -> hyperapi::SqlType {
  struct ArrowSchemaView schema_view {};
  if (ArrowSchemaViewInit(&schema_view, schema, error) != 0) {
    throw std::runtime_error("Issue converting to hyper type: " +
                             std::string(&error->message[0]));
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
    return hyperapi::SqlType::real();
  case NANOARROW_TYPE_DOUBLE:
    return hyperapi::SqlType::doublePrecision();
  case NANOARROW_TYPE_BOOL:
    return hyperapi::SqlType::boolean();
  case NANOARROW_TYPE_BINARY:
  case NANOARROW_TYPE_LARGE_BINARY:
  case NANOARROW_TYPE_BINARY_VIEW:
    return hyperapi::SqlType::bytes();
  case NANOARROW_TYPE_STRING:
  case NANOARROW_TYPE_LARGE_STRING:
  case NANOARROW_TYPE_STRING_VIEW:
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
  case NANOARROW_TYPE_DECIMAL128: {
    // TODO: here we have hardcoded the precision and scale
    // because the Tableau SqlType constructor requires it...
    // but it doesn't appear like these are actually used?
    // We still always get the values from the SchemaView at runtime
    constexpr int16_t precision = 38;
    constexpr int16_t scale = 0;
    return hyperapi::SqlType::numeric(precision, scale);
  }
  case NANOARROW_TYPE_DICTIONARY: {
    struct ArrowSchemaView value_view {};
    struct ArrowError error {};
    NANOARROW_THROW_NOT_OK(
        ArrowSchemaViewInit(&value_view, schema->dictionary, &error));

    // only support dictionary-encoded string values for now
    switch (value_view.type) {
    case NANOARROW_TYPE_STRING:
    case NANOARROW_TYPE_LARGE_STRING:
    case NANOARROW_TYPE_STRING_VIEW:
      return hyperapi::SqlType::text();
    default:
      throw std::invalid_argument(
          std::string(
              "Can only encode dictionaries with binary value types, got:") +
          ArrowTypeString(value_view.type));
    }
  }
  default:
    throw std::invalid_argument(std::string("Unsupported Arrow type: ") +
                                ArrowTypeString(schema_view.type));
  }
}

class InsertHelper {
public:
  InsertHelper(hyperapi::Inserter &inserter, const struct ArrowArray *chunk,
               const struct ArrowSchema *schema, struct ArrowError *error,
               int64_t column_position)
      : inserter_(inserter), chunk_(chunk), error_(error),
        column_position_(column_position) {

    if (ArrowArrayViewInitFromSchema(array_view_.get(), schema, error_) != 0) {
      throw std::runtime_error("Could not construct insert helper: " +
                               std::string{&error_->message[0]});
    }

    std::span children{chunk_->children,
                       static_cast<size_t>(chunk_->n_children)};
    if (ArrowArrayViewSetArray(array_view_.get(), children[column_position_],
                               error_) != 0) {
      throw std::runtime_error("Could not set array view: " +
                               std::string{&error_->message[0]});
    }
  }

  InsertHelper(const InsertHelper &) = delete;
  InsertHelper &operator=(const InsertHelper &) = delete;
  InsertHelper(InsertHelper &&) = delete;
  InsertHelper &operator=(InsertHelper &&) = delete;

  virtual ~InsertHelper() = default;
  virtual void InsertValueAtIndex(int64_t) {}

protected:
  auto CheckNull(int64_t idx) const {
    return ArrowArrayViewIsNull(array_view_.get(), idx);
  }

  template <typename T> auto InsertNull() {
    inserter_.add(hyperapi::optional<T>{});
  }

  auto GetArrayView() const { return array_view_.get(); }

  template <typename T> auto InsertValue(T &&value) {
    inserter_.add(std::forward<T>(value));
  }

private:
  hyperapi::Inserter &inserter_;
  const struct ArrowArray *chunk_;
  struct ArrowError *error_;
  const int64_t column_position_;
  nanoarrow::UniqueArrayView array_view_;
};

template <typename T> class IntegralInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(int64_t idx) override {
    if (CheckNull(idx)) {
      InsertNull<T>();
      return;
    }

    const int64_t value = ArrowArrayViewGetIntUnsafe(GetArrayView(), idx);
    InsertValue(static_cast<T>(value));
  }
};

class UInt32InsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(int64_t idx) override {
    if (CheckNull(idx)) {
      InsertNull<uint32_t>();
      return;
    }

    const uint64_t value = ArrowArrayViewGetUIntUnsafe(GetArrayView(), idx);
    InsertValue(static_cast<uint32_t>(value));
  }
};

template <typename T> class FloatingInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(int64_t idx) override {
    if (CheckNull(idx)) {
      InsertNull<T>();
      return;
    }

    const double value = ArrowArrayViewGetDoubleUnsafe(GetArrayView(), idx);
    InsertValue(static_cast<T>(value));
  }
};

class BinaryInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(int64_t idx) override {
    if (CheckNull(idx)) {
      InsertNull<hyperapi::ByteSpan>();
      return;
    }

    const struct ArrowBufferView buffer_view =
        ArrowArrayViewGetBytesUnsafe(GetArrayView(), idx);
    const hyperapi::ByteSpan result{
        buffer_view.data.as_uint8, static_cast<size_t>(buffer_view.size_bytes)};
    InsertValue(std::move(result));
  }
};

template <typename OffsetT> class Utf8InsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(int64_t idx) override {
    if (CheckNull(idx)) {
      InsertNull<hyperapi::string_view>();
      return;
    }

    const struct ArrowBufferView buffer_view =
        ArrowArrayViewGetBytesUnsafe(GetArrayView(), idx);
    const hyperapi::string_view result{
        buffer_view.data.as_char, static_cast<size_t>(buffer_view.size_bytes)};
    InsertValue(std::move(result));
  }
};

template <bool IsString> class BinaryViewInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(int64_t idx) override {
    if (CheckNull(idx)) {
      if constexpr (IsString) {
        InsertNull<hyperapi::string_view>();
      } else {
        InsertNull<hyperapi::ByteSpan>();
      }
      return;
    }

    const auto buf_view = GetArrayView()->buffer_views[1];
    const std::span view_data{buf_view.data.as_binary_view,
                              static_cast<size_t>(buf_view.size_bytes)};

    const std::span buffers{
        GetArrayView()->array->buffers,
        static_cast<size_t>(GetArrayView()->array->n_buffers)};

    const union ArrowBinaryView bv = view_data[idx];
    struct ArrowBufferView bin_data = {{NULL}, bv.inlined.size};
    if (bv.inlined.size <= NANOARROW_BINARY_VIEW_INLINE_SIZE) {
      bin_data.data.as_uint8 = &bv.inlined.data[0];
    } else {
      const int32_t buf_index =
          bv.ref.buffer_index + NANOARROW_BINARY_VIEW_FIXED_BUFFERS;
      bin_data.data.data = buffers[buf_index];
      const std::span bin_span{bin_data.data.as_uint8,
                               static_cast<size_t>(bin_data.size_bytes)};
      bin_data.data.as_uint8 = &bin_span[bv.ref.offset];
    }

    if constexpr (IsString) {
#if defined(_WIN32) && defined(_MSC_VER)
      const std::string result(bin_data.data.as_char,
                               static_cast<size_t>(bin_data.size_bytes));
#else
      const std::string_view result{bin_data.data.as_char,
                                    static_cast<size_t>(bin_data.size_bytes)};
#endif
      InsertValue(std::move(result));
    } else {
      const hyperapi::ByteSpan result{bin_data.data.as_uint8,
                                      static_cast<size_t>(bin_data.size_bytes)};
      InsertValue(std::move(result));
    }
  }
};

class Date32InsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(int64_t idx) override {
    constexpr size_t elem_size = sizeof(int32_t);
    if (CheckNull(idx)) {
      InsertNull<hyperapi::Date>();
      return;
    }

    int32_t value{};

    const auto buf_view = GetArrayView()->buffer_views[1];
    std::span view_data{buf_view.data.as_uint8,
                        static_cast<size_t>(buf_view.size_bytes)};
    memcpy(&value, &view_data[idx * elem_size], elem_size);

    const std::chrono::duration<int32_t, std::ratio<86400>> dur{value};
    const std::chrono::time_point<
        std::chrono::system_clock,
        std::chrono::duration<int32_t, std::ratio<86400>>>
        tp{dur};
    const auto tt = std::chrono::system_clock::to_time_t(tp);

    const struct tm utc_tm = *std::gmtime(&tt);
    using YearT = decltype(std::declval<hyperapi::Date>().getYear());
    static constexpr auto epoch = static_cast<YearT>(1900);
    hyperapi::Date dt{epoch + utc_tm.tm_year,
                      static_cast<int16_t>(1 + utc_tm.tm_mon),
                      static_cast<int16_t>(utc_tm.tm_mday)};

    InsertValue(std::move(dt));
  }
};

static constexpr int64_t MicrosecondsPerSecond = 1'000'000;
static constexpr int64_t MicrosecondsPerMillisecond = 1'000;
static constexpr int64_t NanosecondsPerMicrosecond = 1'000;

template <enum ArrowTimeUnit TU> class TimeInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(int64_t idx) override {
    if (CheckNull(idx)) {
      InsertNull<hyperapi::Time>();
      return;
    }

    int64_t value = ArrowArrayViewGetIntUnsafe(GetArrayView(), idx);
    // TODO: check for overflow in these branches
    if constexpr (TU == NANOARROW_TIME_UNIT_SECOND) {
      value *= MicrosecondsPerSecond;
    } else if constexpr (TU == NANOARROW_TIME_UNIT_MILLI) {
      value *= MicrosecondsPerMillisecond;
    } else if constexpr (TU == NANOARROW_TIME_UNIT_NANO) {
      value /= NanosecondsPerMicrosecond;
    }
    InsertValue(hyperapi::Time{static_cast<hyper_time_t>(value), {}});
  }
};

template <enum ArrowTimeUnit TU, bool TZAware>
class TimestampInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  using timestamp_t =
      typename std::conditional<TZAware, hyperapi::OffsetTimestamp,
                                hyperapi::Timestamp>::type;

  void InsertValueAtIndex(int64_t idx) override {
    constexpr size_t elem_size = sizeof(int64_t);
    if (CheckNull(idx)) {
      InsertNull<timestamp_t>();
      return;
    }

    int64_t value{};
    const auto buf_view = GetArrayView()->buffer_views[1];
    std::span view_data{buf_view.data.as_uint8,
                        static_cast<size_t>(buf_view.size_bytes)};
    memcpy(&value, &view_data[idx * elem_size], elem_size);

    // TODO: need overflow checks here
    if constexpr (TU == NANOARROW_TIME_UNIT_SECOND) {
      value *= MicrosecondsPerSecond;
    } else if constexpr (TU == NANOARROW_TIME_UNIT_MILLI) {
      value *= MicrosecondsPerMillisecond;
    } else if constexpr (TU == NANOARROW_TIME_UNIT_NANO) {
      value /= NanosecondsPerMicrosecond;
    }

    constexpr int64_t USEC_TABLEAU_TO_UNIX_EPOCH = 210866803200000000LL;
    hyper_timestamp_t raw_timestamp =
        static_cast<hyper_timestamp_t>(value + USEC_TABLEAU_TO_UNIX_EPOCH);

    const timestamp_t ts{raw_timestamp, {}};
    InsertValue(std::move(static_cast<timestamp_t>(ts)));
  }
};

class IntervalInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(int64_t idx) override {
    if (CheckNull(idx)) {
      InsertNull<hyperapi::Interval>();
      return;
    }

    struct ArrowInterval arrow_interval {};
    ArrowIntervalInit(&arrow_interval, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
    ArrowArrayViewGetIntervalUnsafe(GetArrayView(), idx, &arrow_interval);
    const auto usec = static_cast<int32_t>(arrow_interval.ns / 1000);

    hyperapi::Interval interval(0, arrow_interval.months, arrow_interval.days,
                                0, 0, 0, usec);
    InsertValue(std::move(interval));
  }
};

class DecimalInsertHelper : public InsertHelper {
public:
  DecimalInsertHelper(hyperapi::Inserter &inserter,
                      const struct ArrowArray *chunk,
                      const struct ArrowSchema *schema,
                      struct ArrowError *error, int64_t column_position,
                      int32_t precision, int32_t scale)
      : InsertHelper(inserter, chunk, schema, error, column_position),
        precision_(precision), scale_(scale) {}

  void InsertValueAtIndex(int64_t idx) override {
    constexpr auto PrecisionLimit = 39; // of-by-one error in solution?
    if (precision_ >= PrecisionLimit) {
      throw nb::value_error("Numeric precision may not exceed 38!");
    }
    if (scale_ >= PrecisionLimit) {
      throw nb::value_error("Numeric scale may not exceed 38!");
    }

    if (CheckNull(idx)) {
      std::visit(
          [&](auto P, auto S) {
            if constexpr (S() <= P()) {
              InsertNull<hyperapi::Numeric<P(), S()>>();
              return;
            } else {
              throw "unreachable";
            }
          },
          to_integral_variant<PrecisionLimit>(precision_),
          to_integral_variant<PrecisionLimit>(scale_));
      return;
    }

    constexpr int32_t bitwidth = 128;
    struct ArrowDecimal decimal {};
    ArrowDecimalInit(&decimal, bitwidth, precision_, scale_);
    ArrowArrayViewGetDecimalUnsafe(GetArrayView(), idx, &decimal);

    struct ArrowBuffer buffer {};
    ArrowBufferInit(&buffer);
    if (ArrowDecimalAppendDigitsToBuffer(&decimal, &buffer)) {
      throw std::runtime_error("could not create buffer from decmial value");
    }

    const std::span bufspan{buffer.data,
                            static_cast<size_t>(buffer.size_bytes)};
    std::string str{bufspan.begin(), bufspan.end()};

    // The Hyper API wants the string to include the decimal place, which
    // nanoarrow does not provide.
    if (scale_ > 0) {
      // nanoarrow strips leading zeros
      const auto insert_pos = static_cast<int64_t>(str.size()) - scale_;
      if (insert_pos < 0) {
        std::string newstr{};
        newstr.reserve(str.size() - insert_pos + 1);
        newstr.append(1, '.');
        newstr.append(-insert_pos, '0');
        newstr.append(str);
        str = std::move(newstr);
      } else {
        str = str.insert(str.size() - scale_, 1, '.');
      }
    }

    std::visit(
        [&](auto P, auto S) {
          if constexpr (S() <= P()) {
            const auto value = hyperapi::Numeric<P(), S()>{str};
            InsertValue(std::move(value));
            return;
          } else {
            throw "unreachable";
          }
        },
        to_integral_variant<PrecisionLimit>(precision_),
        to_integral_variant<PrecisionLimit>(scale_));

    ArrowBufferReset(&buffer);
  }

private:
  int32_t precision_;
  int32_t scale_;
};

class DictionaryInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void InsertValueAtIndex(int64_t idx) override {
    if (CheckNull(idx)) {
      InsertNull<hyperapi::Time>();
      return;
    }

    const auto key = ArrowArrayViewGetIntUnsafe(GetArrayView(), idx);
    const auto value =
        ArrowArrayViewGetStringUnsafe(GetArrayView()->dictionary, key);

    const hyperapi::string_view result{value.data,
                                       static_cast<size_t>(value.size_bytes)};
    InsertValue(std::move(result));
  }
};

static auto MakeInsertHelper(hyperapi::Inserter &inserter,
                             struct ArrowArray *chunk,
                             const struct ArrowSchema *schema,
                             struct ArrowError *error, int64_t column_position)
    -> std::unique_ptr<InsertHelper> {
  struct ArrowSchemaView schema_view {};
  std::span children{schema->children, static_cast<size_t>(schema->n_children)};
  const struct ArrowSchema *child_schema = children[column_position];
  if (ArrowSchemaViewInit(&schema_view, child_schema, error) != 0) {
    throw std::runtime_error("Issue generating insert helper: " +
                             std::string(&error->message[0]));
  }

  switch (schema_view.type) {
  case NANOARROW_TYPE_INT8:
  case NANOARROW_TYPE_INT16:
    return std::make_unique<IntegralInsertHelper<int16_t>>(
        inserter, chunk, child_schema, error, column_position);
  case NANOARROW_TYPE_INT32:
    return std::make_unique<IntegralInsertHelper<int32_t>>(
        inserter, chunk, child_schema, error, column_position);
  case NANOARROW_TYPE_INT64:
    return std::make_unique<IntegralInsertHelper<int64_t>>(
        inserter, chunk, child_schema, error, column_position);
  case NANOARROW_TYPE_UINT32:
    return std::make_unique<UInt32InsertHelper>(inserter, chunk, child_schema,
                                                error, column_position);
  case NANOARROW_TYPE_FLOAT:
    return std::make_unique<FloatingInsertHelper<float>>(
        inserter, chunk, child_schema, error, column_position);
  case NANOARROW_TYPE_DOUBLE:
    return std::make_unique<FloatingInsertHelper<double>>(
        inserter, chunk, child_schema, error, column_position);
  case NANOARROW_TYPE_BOOL:
    return std::make_unique<IntegralInsertHelper<bool>>(
        inserter, chunk, child_schema, error, column_position);
  case NANOARROW_TYPE_BINARY:
  case NANOARROW_TYPE_LARGE_BINARY:
    return std::make_unique<BinaryInsertHelper>(inserter, chunk, child_schema,
                                                error, column_position);
  case NANOARROW_TYPE_STRING:
  case NANOARROW_TYPE_LARGE_STRING:
    return std::make_unique<Utf8InsertHelper<int64_t>>(
        inserter, chunk, child_schema, error, column_position);
  case NANOARROW_TYPE_DATE32:
    return std::make_unique<Date32InsertHelper>(inserter, chunk, child_schema,
                                                error, column_position);
  case NANOARROW_TYPE_TIMESTAMP:
    switch (schema_view.time_unit) {
    case NANOARROW_TIME_UNIT_SECOND:
      if (std::strcmp("", schema_view.timezone)) {
        return std::make_unique<
            TimestampInsertHelper<NANOARROW_TIME_UNIT_SECOND, true>>(
            inserter, chunk, child_schema, error, column_position);
      } else {
        return std::make_unique<
            TimestampInsertHelper<NANOARROW_TIME_UNIT_SECOND, false>>(
            inserter, chunk, child_schema, error, column_position);
      }
    case NANOARROW_TIME_UNIT_MILLI:
      if (std::strcmp("", schema_view.timezone)) {
        return std::make_unique<
            TimestampInsertHelper<NANOARROW_TIME_UNIT_MILLI, true>>(
            inserter, chunk, child_schema, error, column_position);
      } else {
        return std::make_unique<
            TimestampInsertHelper<NANOARROW_TIME_UNIT_MILLI, false>>(
            inserter, chunk, child_schema, error, column_position);
      }
    case NANOARROW_TIME_UNIT_MICRO:
      if (std::strcmp("", schema_view.timezone)) {
        return std::make_unique<
            TimestampInsertHelper<NANOARROW_TIME_UNIT_MICRO, true>>(
            inserter, chunk, child_schema, error, column_position);
      } else {
        return std::make_unique<
            TimestampInsertHelper<NANOARROW_TIME_UNIT_MICRO, false>>(
            inserter, chunk, child_schema, error, column_position);
      }
    case NANOARROW_TIME_UNIT_NANO:
      if (std::strcmp("", schema_view.timezone)) {
        return std::make_unique<
            TimestampInsertHelper<NANOARROW_TIME_UNIT_NANO, true>>(
            inserter, chunk, child_schema, error, column_position);
      } else {
        return std::make_unique<
            TimestampInsertHelper<NANOARROW_TIME_UNIT_NANO, false>>(
            inserter, chunk, child_schema, error, column_position);
      }
    }
    throw std::runtime_error(
        "This code block should not be hit - contact a developer");
  case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
    return std::make_unique<IntervalInsertHelper>(inserter, chunk, child_schema,
                                                  error, column_position);
  case NANOARROW_TYPE_TIME64:
    switch (schema_view.time_unit) {
    // must be a smarter way to do this!
    case NANOARROW_TIME_UNIT_SECOND: // untested
      return std::make_unique<TimeInsertHelper<NANOARROW_TIME_UNIT_SECOND>>(
          inserter, chunk, child_schema, error, column_position);
    case NANOARROW_TIME_UNIT_MILLI: // untested
      return std::make_unique<TimeInsertHelper<NANOARROW_TIME_UNIT_MILLI>>(
          inserter, chunk, child_schema, error, column_position);
    case NANOARROW_TIME_UNIT_MICRO:
      return std::make_unique<TimeInsertHelper<NANOARROW_TIME_UNIT_MICRO>>(
          inserter, chunk, child_schema, error, column_position);
    case NANOARROW_TIME_UNIT_NANO:
      return std::make_unique<TimeInsertHelper<NANOARROW_TIME_UNIT_NANO>>(
          inserter, chunk, child_schema, error, column_position);
    }
    throw std::runtime_error(
        "This code block should not be hit - contact a developer");
  case NANOARROW_TYPE_DECIMAL128: {
    const auto precision = schema_view.decimal_precision;
    const auto scale = schema_view.decimal_scale;
    return std::make_unique<DecimalInsertHelper>(inserter, chunk, child_schema,
                                                 error, column_position,
                                                 precision, scale);
  }
  case NANOARROW_TYPE_BINARY_VIEW:
    return std::make_unique<BinaryViewInsertHelper<false>>(
        inserter, chunk, child_schema, error, column_position);
  case NANOARROW_TYPE_STRING_VIEW:
    return std::make_unique<BinaryViewInsertHelper<true>>(
        inserter, chunk, child_schema, error, column_position);
  case NANOARROW_TYPE_DICTIONARY: {
    struct ArrowSchemaView value_view {};
    NANOARROW_THROW_NOT_OK(
        ArrowSchemaViewInit(&value_view, child_schema->dictionary, error));

    // only support dictionary-encoded string values for now
    switch (value_view.type) {
    case NANOARROW_TYPE_STRING:
    case NANOARROW_TYPE_LARGE_STRING:
    case NANOARROW_TYPE_STRING_VIEW:
      return std::make_unique<DictionaryInsertHelper>(
          inserter, chunk, child_schema, error, column_position);
    default:
      throw std::invalid_argument(
          std::string("MakeInsertHelper: Can only encode dictionaries with "
                      "binary value types, got:") +
          ArrowTypeString(value_view.type));
    }
  }
  default:
    throw std::invalid_argument(
        std::string("MakeInsertHelper: Unsupported Arrow type: ") +
        ArrowTypeString(schema_view.type));
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

void write_to_hyper(
    const nb::object &dict_of_capsules, const std::string &path,
    const std::string &table_mode, const nb::iterable not_null_columns,
    const nb::iterable json_columns, const nb::iterable geo_columns,
    std::unordered_map<std::string, std::string> &&process_params) {

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

  if (!process_params.count("log_config"))
    process_params["log_config"] = "";
  if (!process_params.count("default_database_version"))
    process_params["default_database_version"] = "2";

  const hyperapi::HyperProcess hyper{
      hyperapi::Telemetry::DoNotSendUsageDataToTableau, "",
      std::move(process_params)};

  // TODO: we don't have separate table / database create modes in the API
  // but probably should; for now we infer this from table mode
  const auto createMode = table_mode == "w"
                              ? hyperapi::CreateMode::CreateAndReplace
                              : hyperapi::CreateMode::CreateIfNotExists;

  hyperapi::Connection connection{hyper.getEndpoint(), path, createMode};
  const hyperapi::Catalog &catalog = connection.getCatalog();

  for (auto const &[name, capsule] :
       nb::cast<nb::dict>(dict_of_capsules, false)) {
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

    struct ArrowError error {};
    std::vector<hyperapi::TableDefinition::Column> hyper_columns;
    std::vector<hyperapi::Inserter::ColumnMapping> column_mappings;
    // subtley different from hyper_columns with geo
    std::vector<hyperapi::TableDefinition::Column> inserter_defs;

    const std::span children{schema->children,
                             static_cast<size_t>(schema->n_children)};
    for (int64_t i = 0; i < schema->n_children; i++) {
      const std::string col_name{children[i]->name};
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
            GetHyperTypeFromArrowSchema(children[i], &error);
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
        struct ArrowSchemaView schema_view {};
        if (ArrowSchemaViewInit(&schema_view, children[i], &error)) {
          throw std::runtime_error(
              "Could not init schema view from child schema " +
              std::to_string(i) + ": " + std::string(&error.message[0]));
        }

        if (schema_view.type == NANOARROW_TYPE_DECIMAL128) {
          const auto precision = schema_view.decimal_precision;
          const auto scale = schema_view.decimal_scale;
          const auto hypertype = hyperapi::SqlType::numeric(precision, scale);
          const hyperapi::TableDefinition::Column column{col_name, hypertype,
                                                         nullability};

          hyper_columns.emplace_back(column);
          inserter_defs.emplace_back(std::move(column));
          const hyperapi::Inserter::ColumnMapping mapping{col_name};
          column_mappings.emplace_back(mapping);
        } else {
          const auto hypertype =
              GetHyperTypeFromArrowSchema(children[i], &error);
          const hyperapi::TableDefinition::Column column{col_name, hypertype,
                                                         nullability};

          hyper_columns.emplace_back(column);
          inserter_defs.emplace_back(std::move(column));
          const hyperapi::Inserter::ColumnMapping mapping{col_name};
          column_mappings.emplace_back(mapping);
        }
      }
    }

    std::tuple<std::string, std::string> schema_and_table;
    std::string t_name;
    const auto is_tup = nb::try_cast(name, schema_and_table, false);
    const auto is_str = nb::try_cast(name, t_name, false);
    if (!(is_tup || is_str)) {
      throw nb::type_error("Expected string or tuple key");
    }
    const auto table_name =
        is_tup ? hyperapi::TableName(std::get<0>(schema_and_table),
                                     std::get<1>(schema_and_table))
               : hyperapi::TableName(t_name);
    const hyperapi::TableDefinition table_def{table_name, hyper_columns};

    const auto schema_name =
        table_name.getSchemaName() ? *table_name.getSchemaName() : "public";
    catalog.createSchemaIfNotExists(schema_name);

    if ((table_mode == "a") && (catalog.hasTable(table_name))) {
      const auto existing_def = catalog.getTableDefinition(table_name);
      AssertColumnsEqual(hyper_columns, std::move(existing_def.getColumns()));
    } else {
      catalog.createTable(table_def);
    }
    auto inserter = hyperapi::Inserter(connection, table_def, column_mappings,
                                       inserter_defs);

    struct ArrowArray c_chunk {};
    int errcode{};
    while ((errcode = stream->get_next(stream.get(), &c_chunk) == 0) &&
           c_chunk.release != nullptr) {
      nanoarrow::UniqueArray chunk{&c_chunk};
      const auto nrows = chunk->length;
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
        inserter.endRow();
      }
    }

    inserter.execute();
  }
}
