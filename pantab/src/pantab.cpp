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

static hyperapi::SqlType
hyperTypeFromArrowFormat(std::string const &format_string) {
  if (format_string == "s" || format_string == "S") {
    return hyperapi::SqlType::smallInt();
  } else if (format_string == "i" || format_string == "I") {
    return hyperapi::SqlType::integer();
  } else if (format_string == "l" || format_string == "L") {
    return hyperapi::SqlType::bigInt();
  } else if (format_string == "f" || format_string == "g") {
    return hyperapi::SqlType::doublePrecision();
  } else if (format_string == "b") {
    return hyperapi::SqlType::boolean();
  } else if (format_string == "u" || format_string == "U") {
    return hyperapi::SqlType::text();
  } else if (std::string_view(format_string).substr(0, 2) == "ts") {
    if (format_string.size() > 4) {
      return hyperapi::SqlType::timestampTZ();
    } else {
      return hyperapi::SqlType::timestamp();
    }
  }

  throw std::invalid_argument("unknown format string: " + format_string);
}

class InsertHelper {
public:
  InsertHelper(uintptr_t dataptr, int64_t nbytes, uintptr_t validity_ptr,
               uintptr_t offsets_ptr,
               std::shared_ptr<hyperapi::Inserter> inserter)
      : nbytes_(nbytes), inserter_(inserter) {
    buffer_ = reinterpret_cast<uint8_t *>(dataptr);
    validity_buffer_ = reinterpret_cast<uint8_t *>(validity_ptr);
    offsets_buffer_ = reinterpret_cast<uint8_t *>(offsets_ptr);
  }

  virtual ~InsertHelper() {}

  virtual void insertValueAtIndex(size_t) {}

protected:
  uint8_t *buffer_;
  int64_t nbytes_;
  std::shared_ptr<hyperapi::Inserter> inserter_;
  uint8_t *validity_buffer_;
  uint8_t *offsets_buffer_;
};

template <typename T, bool is_nullable>
class PrimitiveInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    // TODO: this assumes a byte-mask which pandas provides
    // others may provide a bitmask which would segfault
    if constexpr (is_nullable) {
      if (!validity_buffer_[idx]) {
        inserter_->add(T{});
        return;
      }
    }
    constexpr size_t elem_size = sizeof(T);
    T result;
    memcpy(&result, buffer_ + (idx * elem_size), elem_size);
    inserter_->add(result);
  }
};

template <typename T> class FloatingInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    constexpr size_t elem_size = sizeof(T);
    T result;
    memcpy(&result, buffer_ + (idx * elem_size), elem_size);
    inserter_->add(result);
  }
};

template <typename OffsetT> class Utf8InsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    if (!validity_buffer_[idx]) {
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      return;
    }

    OffsetT current_offset, next_offset;
    constexpr size_t offset_size = sizeof(OffsetT);
    memcpy(&current_offset, offsets_buffer_ + (idx * offset_size), offset_size);
    memcpy(&next_offset, offsets_buffer_ + ((idx + 1) * offset_size),
           offset_size);
    const OffsetT size_bytes = next_offset - current_offset;
    if (size_bytes < 0) {
      throw std::invalid_argument("invalid offset sizes");
    }
    const auto usize_bytes = static_cast<const size_t>(size_bytes);
    hyperapi::string_view result{
        reinterpret_cast<const char *>(buffer_) + current_offset, usize_bytes};
    inserter_->add(result);
  }
};

template <enum TimeUnit TU, bool TZAware>
class TimestampInsertHelper : public InsertHelper {
public:
  using InsertHelper::InsertHelper;

  void insertValueAtIndex(size_t idx) override {
    constexpr size_t elem_size = sizeof(int64_t);
    int64_t value;
    memcpy(&value, buffer_ + (idx * elem_size), elem_size);

    // using timestamp_t =
    //    typename std::conditional<TZAware, hyperapi::OffsetTimestamp,
    //                              hyperapi::Timestamp>::type;

    // this is pandas-specific logic; ideally get this from the
    // dataframe exchange protocol
    if (value == INT64_MIN) {
      hyperapi::internal::ValueInserter{*inserter_}.addNull();
      // inserter_->add(std::optional<timestamp_t>{std::nullopt});
      return;
    }

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

static std::unique_ptr<InsertHelper>
makeInsertHelper(uintptr_t dataptr, int64_t nbytes, uintptr_t validity_ptr,
                 uintptr_t offsets_ptr,
                 std::shared_ptr<hyperapi::Inserter> inserter,
                 std::string const &format_string) {
  // TODO: we should provide the full dtype here not just format string, so
  // boolean fields can determine whether they are bit or byte masks

  // right now we pass false as the template paramter to the
  // PrimitiveInsertHelper as that is all pandas generates; other libraries may
  // need the true variant
  if (format_string == "s") {
    return std::unique_ptr<InsertHelper>(
        new PrimitiveInsertHelper<int16_t, false>(dataptr, nbytes, validity_ptr,
                                                  offsets_ptr, inserter));
  } else if (format_string == "i") {
    return std::unique_ptr<InsertHelper>(
        new PrimitiveInsertHelper<int32_t, false>(dataptr, nbytes, validity_ptr,
                                                  offsets_ptr, inserter));
  } else if (format_string == "l") {
    return std::unique_ptr<InsertHelper>(
        new PrimitiveInsertHelper<int64_t, false>(dataptr, nbytes, validity_ptr,
                                                  offsets_ptr, inserter));
  } else if (format_string == "f") {
    return std::unique_ptr<InsertHelper>(new FloatingInsertHelper<float>(
        dataptr, nbytes, validity_ptr, offsets_ptr, inserter));
  } else if (format_string == "g") {
    return std::unique_ptr<InsertHelper>(new FloatingInsertHelper<double>(
        dataptr, nbytes, validity_ptr, offsets_ptr, inserter));
  } else if (format_string == "b") {
    return std::unique_ptr<InsertHelper>(new PrimitiveInsertHelper<bool, false>(
        dataptr, nbytes, validity_ptr, offsets_ptr, inserter));
  } else if (format_string == "u") {
    // TODO: bug in pandas? Offsets are still provided as 64 bit values even
    // with a "u" format
    return std::unique_ptr<InsertHelper>(new Utf8InsertHelper<int64_t>(
        dataptr, nbytes, validity_ptr, offsets_ptr, inserter));
  } else if (format_string == "U") {
    return std::unique_ptr<InsertHelper>(new Utf8InsertHelper<int64_t>(
        dataptr, nbytes, validity_ptr, offsets_ptr, inserter));
  } else if (std::string_view(format_string).substr(0, 2) == "ts") {
    const bool has_tz = format_string.size() > 4;
    if (has_tz && format_string.size() != 7 &&
        std::string_view(format_string).substr(4, 3) != "UTC") {
      throw std::invalid_argument("Only UTC timestamps are implemented");
    }

    const char unit = format_string.c_str()[2];
    switch (unit) {
    case 's':
      if (has_tz) {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::SECOND, true>(
                dataptr, nbytes, validity_ptr, offsets_ptr, inserter));
      } else {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::SECOND, false>(
                dataptr, nbytes, validity_ptr, offsets_ptr, inserter));
      }
    case 'm':
      if (has_tz) {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::MILLI, true>(
                dataptr, nbytes, validity_ptr, offsets_ptr, inserter));
      } else {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::MILLI, false>(
                dataptr, nbytes, validity_ptr, offsets_ptr, inserter));
      }
    case 'u':
      if (has_tz) {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::MICRO, true>(
                dataptr, nbytes, validity_ptr, offsets_ptr, inserter));
      } else {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::MICRO, false>(
                dataptr, nbytes, validity_ptr, offsets_ptr, inserter));
      }
    case 'n':
      if (has_tz) {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::NANO, true>(
                dataptr, nbytes, validity_ptr, offsets_ptr, inserter));
      } else {
        return std::unique_ptr<InsertHelper>(
            new TimestampInsertHelper<TimeUnit::NANO, false>(
                dataptr, nbytes, validity_ptr, offsets_ptr, inserter));
      }
    default:
      throw std::invalid_argument("unknown timestamp unit for format string: " +
                                  format_string);
    }
  }

  throw std::invalid_argument("unknown format string: " + format_string);
}

using SchemaAndTableName = std::tuple<std::string, std::string>;

void write_to_hyper(
    const std::map<SchemaAndTableName, nb::object> &dict_of_frames,
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

  for (auto const &[schema_and_table, df] : dict_of_frames) {
    const auto schema = std::get<0>(schema_and_table);
    const auto table = std::get<1>(schema_and_table);
    const auto df_protocol = nb::getattr(df, "__dataframe__")();
    const auto chunks = nb::getattr(df_protocol, "get_chunks")();

    size_t chunk_idx = 0;
    for (auto chunk : chunks) {
      auto names = nb::getattr(chunk, "column_names")();
      auto names_vec = nb::cast<std::vector<std::string>>(names);
      auto columns = nb::getattr(chunk, "get_columns")();

      std::map<std::string, Dtype> column_dtypes;
      std::vector<hyperapi::TableDefinition::Column> hyper_columns;

      // first pass is to get metadata for table definition
      // second pass builds inserter
      size_t column_idx = 0;
      for (auto column : columns) {
        const auto name = names_vec[column_idx];
        const auto dtype_obj = nb::getattr(column, "dtype");
        const auto dtype = nb::cast<Dtype>(dtype_obj);

        const auto format_string = std::get<2>(dtype);
        const auto hypertype = hyperTypeFromArrowFormat(format_string);

        const auto describe_null_obj = nb::getattr(column, "describe_null");
        const auto describe_null =
            nb::cast<std::tuple<int, nb::object>>(describe_null_obj);

        // the dataframe interchange api specifies a whole lot of different
        // ways to describe null. we pretty much assume either non-nullable
        // or the use of a bytemask for pandas
        const auto nullable_kind = std::get<0>(describe_null);
        const auto hyper_nullability = nullable_kind
                                           ? hyperapi::Nullability::Nullable
                                           : hyperapi::Nullability::NotNullable;
        hyper_columns.push_back(hyperapi::TableDefinition::Column{
            name, hypertype, hyper_nullability});
        column_idx++;
      }

      hyperapi::TableName table_name{schema, table};
      hyperapi::TableDefinition tableDef{table_name, hyper_columns};
      if (chunk_idx == 0) {
        catalog.createSchemaIfNotExists(*table_name.getSchemaName());
        if (table_mode == "w") {
          catalog.createTable(tableDef);
        } else if (table_mode == "a") {
          catalog.createTableIfNotExists(tableDef);
        }
      }
      auto inserter =
          std::make_shared<hyperapi::Inserter>(connection, tableDef);

      // some buffer objects in pandas copy and own a buffer; keeping them in a
      // vector here is a hack to prevent them from being Py_DECREF'ed
      // their data may be used
      std::vector<nb::object> buffers;
      std::vector<std::unique_ptr<InsertHelper>> insert_helpers;
      column_idx = 0;
      for (auto column : columns) {
        const auto dtype_obj = nb::getattr(column, "dtype");
        const auto dtype = nb::cast<Dtype>(dtype_obj);

        auto buffers_obj = nb::getattr(column, "get_buffers")();
        buffers.push_back(buffers_obj);
        auto buffers_map =
            nb::cast<std::map<std::string, nb::object>>(buffers_obj);

        const auto data_obj = buffers_map["data"];
        const auto data = nb::cast<std::tuple<nb::object, Dtype>>(data_obj);
        const auto data_buffer_obj = std::get<0>(data);
        const auto nbytes =
            nb::cast<int64_t>(nb::getattr(data_buffer_obj, "bufsize"));
        const auto dataptr =
            nb::cast<uintptr_t>(nb::getattr(data_buffer_obj, "ptr"));

        const auto validity_obj = buffers_map["validity"];
        const auto validity =
            nb::cast<std::optional<std::tuple<nb::object, Dtype>>>(
                validity_obj);

        uintptr_t validityptr = 0x0;
        if (validity) {
          auto valid_buffer_obj = std::get<0>(*validity);
          // TODO: this assumes a byte mask, which isn't always true
          validityptr =
              nb::cast<uintptr_t>(nb::getattr(valid_buffer_obj, "ptr"));
        }

        auto offsets_obj = buffers_map["offsets"];
        auto offsets =
            nb::cast<std::optional<std::tuple<nb::object, Dtype>>>(offsets_obj);
        uintptr_t offsetsptr = 0x0;
        if (offsets) {
          auto offsets_buffer_obj = std::get<0>(*offsets);
          // TODO: this assumes a byte mask, which isn't always true
          offsetsptr =
              nb::cast<uintptr_t>(nb::getattr(offsets_buffer_obj, "ptr"));
        }

        auto format_string = std::get<2>(dtype);
        auto insert_helper = makeInsertHelper(
            dataptr, nbytes, validityptr, offsetsptr, inserter, format_string);

        insert_helpers.push_back(std::move(insert_helper));
        column_idx++;
      }

      // TODO: num_rows is technically optional in the dataframe API - how
      // should we handle producers that may not populate?
      auto num_rows_obj = nb::getattr(chunk, "num_rows")();
      auto num_rows = nb::cast<size_t>(num_rows_obj);
      for (size_t i = 0; i < num_rows; i++) {
        for (auto &insert_helper : insert_helpers) {
          insert_helper->insertValueAtIndex(i);
        }
        inserter->endRow();
      }

      inserter->execute();
    }
    chunk_idx++;
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
  m.def("write_to_hyper", &write_to_hyper, nb::arg("dict_of_frames"),
        nb::arg("path"), nb::arg("table_mode"))
      .def("read_from_hyper_query", &read_from_hyper_query, nb::arg("path"),
           nb::arg("query"))
      .def("read_from_hyper_table", &read_from_hyper_table, nb::arg("path"),
           nb::arg("schema"), nb::arg("table"));
  PyDateTime_IMPORT;
}
