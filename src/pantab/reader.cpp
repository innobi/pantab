#include "reader.hpp"
#include "numeric_gen.hpp"

#include <variant>
#include <vector>

#include <hyperapi/hyperapi.hpp>
#include <nanoarrow/nanoarrow.hpp>

namespace nb = nanobind;

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

template <typename T> class FloatReadHelper : public ReadHelper {
  using ReadHelper::ReadHelper;

  auto Read(const hyperapi::Value &value) -> void override {
    if (value.isNull()) {
      if (ArrowArrayAppendNull(array_, 1)) {
        throw std::runtime_error("ArrowAppendNull failed");
      }
      return;
    }
    if (ArrowArrayAppendDouble(array_, value.get<T>())) {
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
    const ArrowBufferView arrow_buffer_view{{bytes.data()},
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

class DecimalReadHelper : public ReadHelper {
public:
  explicit DecimalReadHelper(struct ArrowArray *array, int32_t precision,
                             int32_t scale)
      : ReadHelper(array), precision_(precision), scale_(scale) {}

  auto Read(const hyperapi::Value &value) -> void override {
    if (value.isNull()) {
      if (ArrowArrayAppendNull(array_, 1)) {
        throw std::runtime_error("ArrowAppendNull failed");
      }
      return;
    }

    constexpr int32_t bitwidth = 128;
    struct ArrowDecimal decimal;
    ArrowDecimalInit(&decimal, bitwidth, precision_, scale_);

    constexpr auto PrecisionLimit = 39; // of-by-one error in solution?
    if (precision_ >= PrecisionLimit) {
      throw nb::value_error("Numeric precision may not exceed 38!");
    }
    if (scale_ >= PrecisionLimit) {
      throw nb::value_error("Numeric scale may not exceed 38!");
    }

    const auto decimal_string = std::visit(
        [&value](auto P, auto S) -> std::string {
          if constexpr (S() <= P()) {
            const auto decimal_value = value.get<hyperapi::Numeric<P(), S()>>();
            auto value_string = decimal_value.toString();
            std::erase(value_string, '.');
            return value_string;
          }
          throw "unreachable";
        },
        to_integral_variant<PrecisionLimit>(precision_),
        to_integral_variant<PrecisionLimit>(scale_));

    const struct ArrowStringView sv {
      decimal_string.data(), static_cast<int64_t>(decimal_string.size())
    };

    if (ArrowDecimalSetDigits(&decimal, sv)) {
      throw std::runtime_error(
          "Unable to convert tableau numeric to arrow decimal");
    }

    if (ArrowArrayAppendDecimal(array_, &decimal)) {
      throw std::runtime_error("Failed to append decimal value");
    }
  }

private:
  int32_t precision_;
  int32_t scale_;
};

static auto MakeReadHelper(const ArrowSchemaView *schema_view,
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
  case NANOARROW_TYPE_FLOAT:
    return std::unique_ptr<ReadHelper>(new FloatReadHelper<float>(array));
  case NANOARROW_TYPE_DOUBLE:
    return std::unique_ptr<ReadHelper>(new FloatReadHelper<double>(array));
  case NANOARROW_TYPE_LARGE_BINARY:
    return std::unique_ptr<ReadHelper>(new BytesReadHelper(array));
  case NANOARROW_TYPE_LARGE_STRING:
    return std::unique_ptr<ReadHelper>(new StringReadHelper(array));
  case NANOARROW_TYPE_BOOL:
    return std::unique_ptr<ReadHelper>(new BooleanReadHelper(array));
  case NANOARROW_TYPE_DATE32:
    return std::unique_ptr<ReadHelper>(new DateReadHelper(array));
  case NANOARROW_TYPE_TIMESTAMP: {
    if (strcmp("", schema_view->timezone)) {
      return std::unique_ptr<ReadHelper>(new DatetimeReadHelper<true>(array));
    } else {
      return std::unique_ptr<ReadHelper>(new DatetimeReadHelper<false>(array));
    }
  }
  case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
    return std::unique_ptr<ReadHelper>(new IntervalReadHelper(array));
  case NANOARROW_TYPE_TIME64:
    return std::unique_ptr<ReadHelper>(new TimeReadHelper(array));
  case NANOARROW_TYPE_DECIMAL128: {
    const auto precision = schema_view->decimal_precision;
    const auto scale = schema_view->decimal_scale;
    return std::unique_ptr<ReadHelper>(
        new DecimalReadHelper(array, precision, scale));
  }
  default:
    throw nb::type_error("unknownn arrow type provided");
  }
}

static auto GetArrowTypeFromHyper(const hyperapi::SqlType &sqltype)
    -> enum ArrowType {
      switch (sqltype.getTag()){
        case hyperapi::TypeTag::SmallInt : return NANOARROW_TYPE_INT16;
        case hyperapi::TypeTag::Int : return NANOARROW_TYPE_INT32;
        case hyperapi::TypeTag::BigInt : return NANOARROW_TYPE_INT64;
        case hyperapi::TypeTag::Oid : return NANOARROW_TYPE_UINT32;
        case hyperapi::TypeTag::Float : return NANOARROW_TYPE_FLOAT;
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

static auto SetSchemaTypeFromHyperType(struct ArrowSchema *schema,
                                       const hyperapi::SqlType &sqltype)
    -> void {
  switch (sqltype.getTag()) {
  case hyperapi::TypeTag::TimestampTZ:
    if (ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIMESTAMP,
                                   NANOARROW_TIME_UNIT_MICRO, "UTC")) {
      throw std::runtime_error(
          "ArrowSchemaSetDateTime failed for TimestampTZ type");
    }
    break;
  case hyperapi::TypeTag::Timestamp:
    if (ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIMESTAMP,
                                   NANOARROW_TIME_UNIT_MICRO, nullptr)) {
      throw std::runtime_error(
          "ArrowSchemaSetDateTime failed for Timestamp type");
    }
    break;
  case hyperapi::TypeTag::Time:
    if (ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIME64,
                                   NANOARROW_TIME_UNIT_MICRO, nullptr)) {
      throw std::runtime_error("ArrowSchemaSetDateTime failed for Time type");
    }
    break;
  case hyperapi::TypeTag::Numeric: {
    const auto precision = sqltype.getPrecision();
    const auto scale = sqltype.getScale();
    if (ArrowSchemaSetTypeDecimal(schema, NANOARROW_TYPE_DECIMAL128, precision,
                                  scale)) {
      throw std::runtime_error("ArrowSchemaSetTypeDecimal failed");
    }
    break;
  }
  default:
    const enum ArrowType arrow_type = GetArrowTypeFromHyper(sqltype);
    if (ArrowSchemaSetType(schema, arrow_type)) {
      throw std::runtime_error("ArrowSchemaSetType failed");
    }
  }
}

static auto ReleaseArrowStream(void *ptr) noexcept -> void {
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
auto read_from_hyper_query(
    const std::string &path, const std::string &query,
    std::unordered_map<std::string, std::string> &&process_params)
    -> nb::capsule {

  if (!process_params.count("log_config"))
    process_params["log_config"] = "";
  if (!process_params.count("default_database_version"))
    process_params["default_database_version"] = "2";

  const hyperapi::HyperProcess hyper{
      hyperapi::Telemetry::DoNotSendUsageDataToTableau, "",
      std::move(process_params)};
  hyperapi::Connection connection(hyper.getEndpoint(), path);

  auto hyperResult = connection.executeQuery(query);
  const auto resultSchema = hyperResult.getSchema();

  auto schema = std::make_unique<struct ArrowSchema>();
  ArrowSchemaInit(schema.get());
  if (ArrowSchemaSetTypeStruct(schema.get(), resultSchema.getColumnCount())) {
    throw std::runtime_error("ArrowSchemaSetTypeStruct failed");
  }

  const auto column_count = resultSchema.getColumnCount();
  std::unordered_map<std::string, size_t> name_counter;
  for (size_t i = 0; i < column_count; i++) {
    const auto column = resultSchema.getColumn(i);
    auto name = column.getName().getUnescaped();
    const auto &[elem, did_insert] = name_counter.emplace(name, 0);
    if (!did_insert) {
      name = name + "_" + std::to_string(elem->second);
    }
    elem->second += 1;

    if (ArrowSchemaSetName(schema->children[i], name.c_str())) {
      throw std::runtime_error("ArrowSchemaSetName failed");
    }

    SetSchemaTypeFromHyperType(schema->children[i], column.getType());
  }

  nanoarrow::UniqueArray array{};
  if (ArrowArrayInitFromSchema(array.get(), schema.get(), nullptr)) {
    throw std::runtime_error("ArrowSchemaInitFromSchema failed");
  }
  std::vector<std::unique_ptr<ReadHelper>> read_helpers{column_count};
  for (size_t i = 0; i < column_count; i++) {
    struct ArrowSchemaView schema_view;
    if (ArrowSchemaViewInit(&schema_view, schema->children[i], nullptr)) {
      throw std::runtime_error("ArrowSchemaViewInit failed");
    }

    auto read_helper = MakeReadHelper(&schema_view, array->children[i]);
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

  nb::capsule result{stream, "arrow_array_stream", &ReleaseArrowStream};
  return result;
}
