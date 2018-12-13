/*
 * Copyright 2018 Kenta Murata <mrkn@mrkn.jp>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "mysql2-arrow.h"
#include <mysql2/mysql_enc_to_ruby.h>

#include <ruby/thread.h>

#include <arrow/api.h>
#include <arrow/util/decimal.h>

#include <arrow-glib/record-batch.h>
#include <rbgobject.h>

#include <cstdlib>
#include <iostream>

namespace ruby {

class error {
 public:
  error(VALUE exc_klass, const char* message) {
    exc_ = rb_exc_new_cstr(exc_klass, message);
  }

  error(VALUE exc_klass, const std::string& message)
      : error(exc_klass, message.c_str()) {}

  VALUE exception_object() const { return exc_; }

 private:
  VALUE exc_;
};

}  // namespace ruby

static rb_encoding *binaryEncoding;

static ID intern_utc, intern_local, intern_merge;
static VALUE sym_symbolize_keys, sym_as, sym_array, sym_cast_booleans,
             sym_cache_rows, sym_cast, sym_database_timezone, sym_application_timezone, sym_local,
             sym_utc;

/* this is copied from mysql2/result.c */
/* this may be called manually or during GC */
static void rb_mysql_result_free_result(mysql2_result_wrapper * wrapper) {
  if (!wrapper) return;

  if (wrapper->resultFreed != 1) {
    if (wrapper->stmt_wrapper) {
      if (!wrapper->stmt_wrapper->closed) {
        mysql_stmt_free_result(wrapper->stmt_wrapper->stmt);

        /* MySQL BUG? If the statement handle was previously used, and so
         * mysql_stmt_bind_result was called, and if that result set and bind buffers were freed,
         * MySQL still thinks the result set buffer is available and will prefetch the
         * first result in mysql_stmt_execute. This will corrupt or crash the program.
         * By setting bind_result_done back to 0, we make MySQL think that a result set
         * has never been bound to this statement handle before to prevent the prefetch.
         */
        wrapper->stmt_wrapper->stmt->bind_result_done = 0;
      }

      if (wrapper->statement != Qnil) {
        decr_mysql2_stmt(wrapper->stmt_wrapper);
      }

      if (wrapper->result_buffers) {
        unsigned int i;
        for (i = 0; i < wrapper->numberOfFields; i++) {
          if (wrapper->result_buffers[i].buffer) {
            xfree(wrapper->result_buffers[i].buffer);
          }
        }
        xfree(wrapper->result_buffers);
        xfree(wrapper->is_null);
        xfree(wrapper->error);
        xfree(wrapper->length);
      }
      /* Clue that the next statement execute will need to allocate a new result buffer. */
      wrapper->result_buffers = NULL;
    }
    /* FIXME: this may call flush_use_result, which can hit the socket */
    /* For prepared statements, wrapper->result is the result metadata */
    mysql_free_result(wrapper->result);
    wrapper->resultFreed = 1;
  }
}

static void *nogvl_fetch_row(void *ptr) {
  MYSQL_RES *result = reinterpret_cast<MYSQL_RES*>(ptr);
  return mysql_fetch_row(result);
}

static void *nogvl_stmt_fetch(void *ptr) {
  MYSQL_STMT *stmt = reinterpret_cast<MYSQL_STMT*>(ptr);
  uintptr_t r = mysql_stmt_fetch(stmt);

  return (void *)r;
}

namespace internal {

struct Timezone {
  enum type {
    unknown,
    local,
    utc
  };
};

class ResultWrapper {
 public:
  ResultWrapper(mysql2_result_wrapper* wrapper)
      : result_(wrapper->result),
        num_fields_(mysql_num_fields(result_)),
        fields_(mysql_fetch_fields(result_)),
        default_internal_enc_(rb_default_internal_encoding()),
        conn_enc(rb_to_encoding(wrapper->encoding)) {}

  bool symbolizeKeys;
  bool asArray;
  bool castBool;
  bool cast;
  Timezone::type dbTimezone;
  Timezone::type appTimezone;

  unsigned int num_fields() const { return num_fields_; }

  const MYSQL_FIELD& field(unsigned int i) const { return fields_[i]; }

  std::string field_name(unsigned int i) const { return std::string{field(i).name}; }

  unsigned int field_flags(unsigned int i) const { return field(i).flags; }

  std::shared_ptr<arrow::Schema> schema() {
    if (schema_ == nullptr) { makeArrowSchema(); }
    return schema_;
  }

  bool fetch_row(std::unique_ptr<arrow::RecordBatchBuilder>& rbb) {
    MYSQL_ROW row = (MYSQL_ROW)rb_thread_call_without_gvl(
        (void* (*)(void*))nogvl_fetch_row, result_, RUBY_UBF_IO, 0);
    if (row == nullptr) {
      return false;
    }

    unsigned long* field_lengths = mysql_fetch_lengths(result_);

    for (unsigned int i = 0; i < num_fields(); ++i) {
      const enum enum_field_types field_type = field(i).type;
      const unsigned int flags = field(i).flags;
      const bool is_unsigned = 0 != (flags & UNSIGNED_FLAG);

      if (!cast) {
        if (field_type == MYSQL_TYPE_NULL) {
          rbb->GetFieldAs<arrow::NullBuilder>(i)->AppendNull();
        } else {
        as_binary:
          VALUE val = rb_str_new(row[i], field_lengths[i]);
          val = mysql2_set_field_string_encoding(val, field(i));
          rbb->GetFieldAs<arrow::BinaryBuilder>(i)->Append(RSTRING_PTR(val), RSTRING_LEN(val));
        }
        continue;
      }

      switch (field_type) {
        case MYSQL_TYPE_NULL:
          rbb->GetFieldAs<arrow::NullBuilder>(i)->AppendNull();
          continue;

        case MYSQL_TYPE_BIT:
          if (castBool && field_lengths[i] == 1) {
            rbb->GetFieldAs<arrow::BooleanBuilder>(i)->Append(*row[i] == 1);
          } else {
            rbb->GetFieldAs<arrow::BinaryBuilder>(i)->Append(row[i], field_lengths[i]);
          }
          continue;

        case MYSQL_TYPE_TINY:
          if (castBool && field_lengths[i] == 1) {
            rbb->GetFieldAs<arrow::BooleanBuilder>(i)->Append(*row[i] == 1);
          } else if (is_unsigned) {
            uint8_t val = static_cast<uint8_t>(std::strtoul(row[i], nullptr, 10));
            rbb->GetFieldAs<arrow::UInt8Builder>(i)->Append(val);
          } else {
            int8_t val = static_cast<int8_t>(std::strtol(row[i], nullptr, 10));
            rbb->GetFieldAs<arrow::Int8Builder>(i)->Append(val);
          }
          continue;

        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_YEAR:
          if (is_unsigned) {
            uint16_t val = static_cast<uint16_t>(std::strtoul(row[i], nullptr, 10));
            rbb->GetFieldAs<arrow::UInt16Builder>(i)->Append(val);
          } else {
            int16_t val = static_cast<int16_t>(std::strtol(row[i], nullptr, 10));
            rbb->GetFieldAs<arrow::Int16Builder>(i)->Append(val);
          }
          continue;

        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_INT24:
          if (is_unsigned) {
            uint32_t val = static_cast<uint32_t>(std::strtoul(row[i], nullptr, 10));
            rbb->GetFieldAs<arrow::UInt32Builder>(i)->Append(val);
          } else {
            int32_t val = static_cast<int32_t>(std::strtol(row[i], nullptr, 10));
            rbb->GetFieldAs<arrow::Int32Builder>(i)->Append(val);
          }
          continue;

        case MYSQL_TYPE_LONGLONG:
          if (is_unsigned) {
            uint64_t val = static_cast<uint64_t>(std::strtoull(row[i], nullptr, 10));
            rbb->GetFieldAs<arrow::UInt64Builder>(i)->Append(val);
          } else {
            int64_t val = static_cast<int64_t>(std::strtoll(row[i], nullptr, 10));
            rbb->GetFieldAs<arrow::Int64Builder>(i)->Append(val);
          }
          continue;

        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL:
          rbb->GetFieldAs<arrow::Decimal128Builder>(i)->Append(arrow::Decimal128(row[i]));
          continue;

        case MYSQL_TYPE_FLOAT:
          rbb->GetFieldAs<arrow::FloatBuilder>(i)->Append(
              std::strtof(row[i], nullptr));
          continue;

        case MYSQL_TYPE_DOUBLE:
          rbb->GetFieldAs<arrow::DoubleBuilder>(i)->Append(
              std::strtod(row[i], nullptr));
          continue;

        case MYSQL_TYPE_TIME:
          /* TODO */
          goto as_binary;

        case MYSQL_TYPE_TIMESTAMP:
          /* TODO */
          goto as_binary;

        case MYSQL_TYPE_DATETIME:
          /* TODO */
          goto as_binary;

        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
          /* TODO */
          goto as_binary;

        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_STRING:
          /* TODO: encoding */
          rbb->GetFieldAs<arrow::StringBuilder>(i)->Append(row[i], field_lengths[i]);
          continue;

        // TODO: support following types
        case MYSQL_TYPE_SET:
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_GEOMETRY:
          /* TODO */
          continue;

        default:
          continue;
      }
    }

    return true;
  }

  bool fetch_row_stmt(std::unique_ptr<arrow::RecordBatchBuilder>& rbb) {
    throw ruby::error(rb_eNotImpError, "Prepared statement is not supported");
    return false;
  }

 private:
  VALUE mysql2_set_field_string_encoding(VALUE val, const MYSQL_FIELD& field) {
    /* if binary flag is set, respect its wishes */
    if (field.flags & BINARY_FLAG && field.charsetnr == 63) {
      rb_enc_associate(val, binaryEncoding);
    } else if (!field.charsetnr) {
      /* MySQL 4.x may not provide an encoding, binary will get the bytes through */
      rb_enc_associate(val, binaryEncoding);
    } else {
      /* lookup the encoding configured on this field */
      const char *enc_name;
      int enc_index;

      enc_name = (field.charsetnr-1 < CHARSETNR_SIZE)
        ? mysql2_mysql_enc_to_rb[field.charsetnr-1]
        : nullptr;
      if (enc_name != nullptr) {
        /* use the field encoding we were able to match */
        enc_index = rb_enc_find_index(enc_name);
        rb_enc_set_index(val, enc_index);
      } else {
        /* otherwise fall-back to the connection's encoding */
        rb_enc_associate(val, conn_enc);
      }
      if (default_internal_enc_) {
        val = rb_str_export_to_enc(val, default_internal_enc_);
      }
    }
    return val;
  }

  void makeArrowSchema() {
    std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
    arrow_fields.reserve(num_fields());
    for (unsigned int i = 0; i < num_fields(); ++i) {
      bool nullable = 0 == (field_flags(i) & NOT_NULL_FLAG);
      arrow_fields.emplace_back(std::make_shared<arrow::Field>(field_name(i), mysql_field_to_arrow_type(i), nullable));
    }
    schema_ = std::make_shared<arrow::Schema>(std::move(arrow_fields));
  }

  std::shared_ptr<arrow::DataType>
  mysql_field_to_arrow_type(unsigned int i) const {
    const enum enum_field_types field_type = field(i).type;
    const unsigned int flags = field(i).flags;
    const bool is_unsigned = 0 != (flags & UNSIGNED_FLAG);

    if (!cast) {
      if (field_type == MYSQL_TYPE_NULL) {
        return arrow::null();
      }

      return arrow::utf8();
    }

    switch (field_type) {
      case MYSQL_TYPE_TINY:     /* TINYINT:   1 byte  */
        if (castBool && field(i).length == 1) {
          return arrow::boolean();
        } else {
          return is_unsigned ? arrow::uint8() : arrow::int8();
        }

      case MYSQL_TYPE_SHORT:    /* SMALLINT:  2 bytes */
        return is_unsigned ? arrow::uint16() : arrow::int16();

      case MYSQL_TYPE_INT24:    /* MEDIUMINT: 3 bytes */
      case MYSQL_TYPE_LONG:     /* INTEGER:   4 bytes */
        return is_unsigned ? arrow::uint32() : arrow::int32();

      case MYSQL_TYPE_LONGLONG: /* BIGINT:    8 bytes */
        return is_unsigned ? arrow::uint64() : arrow::int64();

      case MYSQL_TYPE_DECIMAL:  /* DECIMAL or NUMERIC */
      case MYSQL_TYPE_NEWDECIMAL: /* high precision DECIMAL or NUMERIC */
        return std::make_shared<arrow::Decimal128Type>(field(i).length, field(i).decimals);

      case MYSQL_TYPE_FLOAT:    /* FLOAT: 4 bytes */
        return arrow::float32();

      case MYSQL_TYPE_DOUBLE:   /* DOUBLE or REAL: 8 bytes */
        return arrow::float64();

      case MYSQL_TYPE_BIT:  /* 1 to 64 bits */
        if (castBool && field(i).length == 1) {
          return arrow::boolean();
        }
        return arrow::binary();

      case MYSQL_TYPE_TIMESTAMP:
        return std::make_shared<arrow::TimestampType>(arrow::TimeUnit::MICRO);

      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_NEWDATE:
        /* TODO: need to reconsider about data type for DATE field */
        return arrow::date32();

      case MYSQL_TYPE_TIME:
        /* TODO: need to reconsider about data type for TIME field */
        return std::make_shared<arrow::Time64Type>(arrow::TimeUnit::MICRO);

      case MYSQL_TYPE_DATETIME:
        /* TODO: need to reconsider about data type for DATETIME field */
        return std::make_shared<arrow::Time64Type>(arrow::TimeUnit::MICRO);

      case MYSQL_TYPE_YEAR:    /* YEAR: 1 byte */
        return arrow::uint16();

      case MYSQL_TYPE_STRING:     /* CHAR, BINARY */
      case MYSQL_TYPE_VAR_STRING: /* VARCHAR, VARBINARY */
      case MYSQL_TYPE_VARCHAR:
        return arrow::utf8();

      case MYSQL_TYPE_TINY_BLOB:   /* TINYBLOB, TINYTEXT */
      case MYSQL_TYPE_MEDIUM_BLOB: /* MEDIUMBLOB, MEDIUMTEXT */
      case MYSQL_TYPE_LONG_BLOB:   /* LONGBLOB, LONGTEXT */
      case MYSQL_TYPE_BLOB:        /* BLOB, TEXT */
        return arrow::utf8();

      case MYSQL_TYPE_SET:
      case MYSQL_TYPE_ENUM:
      case MYSQL_TYPE_GEOMETRY:
        /* TODO */
        return nullptr;

      case MYSQL_TYPE_NULL:
        return arrow::null();

      default:
        break;
    }

    return arrow::binary();
  }

  MYSQL_RES* result_;
  unsigned int num_fields_;
  MYSQL_FIELD* fields_;
  std::shared_ptr<arrow::Schema> schema_;
  rb_encoding* default_internal_enc_;
  rb_encoding* conn_enc;
};

VALUE
mysql2_result_to_arrow(int argc, VALUE* argv, VALUE self)
{
  using namespace internal;

  GET_RESULT(self);

  /* TODO: support prepared statement */
  if (wrapper->stmt_wrapper) {
    throw ruby::error(rb_eNotImpError, "Prepared statement is not supported");
  }

  if (wrapper->stmt_wrapper && wrapper->stmt_wrapper->closed) {
    throw ruby::error(ma_eMysql2Error, "Statement handle already closed");
  }

  VALUE defaults = rb_iv_get(self, "@query_options");
  Check_Type(defaults, T_HASH);

  VALUE opts;
  if (rb_scan_args(argc, argv, "01", &opts) == 1) {
    opts = rb_funcall(defaults, intern_merge, 1, opts);
  } else {
    opts = defaults;
  }

  int cacheRows = RTEST(rb_hash_aref(opts, sym_cache_rows));
  if (cacheRows) {
    rb_warn(":cache_rows is ignored in to_arrow method");
    cacheRows = 0;
  }

  if (wrapper->stmt_wrapper && !wrapper->is_streaming) {
    rb_warn("Rows are not cached in to_arrow method even for prepared statements (if not streaming)");
  }

  ResultWrapper res(wrapper);

  res.symbolizeKeys = RTEST(rb_hash_aref(opts, sym_symbolize_keys));
  res.asArray       = rb_hash_aref(opts, sym_as) == sym_array;
  res.castBool      = RTEST(rb_hash_aref(opts, sym_cast_booleans));
  res.cast          = RTEST(rb_hash_aref(opts, sym_cast));

  if (wrapper->stmt_wrapper && !res.cast) {
    rb_warn(":cast is forced for prepared statements");
  }

  VALUE dbTz = rb_hash_aref(opts, sym_database_timezone);
  if (dbTz == sym_local) {
    res.dbTimezone = Timezone::local;
  } else if (dbTz == sym_utc) {
    res.dbTimezone = Timezone::utc;
  } else {
    if (!NIL_P(dbTz)) {
      rb_warn(":database_timezone option must be :utc or :local - defaulting to :local");
    }
    res.dbTimezone = Timezone::local;
  }

  VALUE appTz = rb_hash_aref(opts, sym_application_timezone);
  if (appTz == sym_local) {
    res.appTimezone = Timezone::local;
  } else if (appTz == sym_utc) {
    res.appTimezone = Timezone::utc;
  } else {
    res.appTimezone = Timezone::unknown;
  }

  wrapper->numberOfRows = wrapper->stmt_wrapper
    ? mysql_stmt_num_rows(wrapper->stmt_wrapper->stmt)
    : mysql_num_rows(wrapper->result);

  using fetch_row_func_t = bool (internal::ResultWrapper::*)(
      std::unique_ptr<arrow::RecordBatchBuilder>&);
  fetch_row_func_t fetch_row_func;
  if (wrapper->stmt_wrapper) {
    fetch_row_func = &internal::ResultWrapper::fetch_row_stmt;
  } else {
    fetch_row_func = &internal::ResultWrapper::fetch_row;
  }

  auto schema = res.schema();
  auto num_fields = schema->num_fields();
  auto memory_pool = arrow::default_memory_pool();
  std::unique_ptr<arrow::RecordBatchBuilder> rbb;
  auto status = arrow::RecordBatchBuilder::Make(schema, memory_pool, &rbb);
  if (!status.ok()) {
    throw ruby::error(rb_eRuntimeError, status.message());
  }

  if (wrapper->is_streaming) {
    if (wrapper->rows == Qnil) {
      wrapper->rows = rb_ary_new();
    }

    if (!wrapper->streamingComplete) {
      while ((res.*fetch_row_func)(rbb));

      rb_mysql_result_free_result(wrapper);
      wrapper->streamingComplete = 1;

      // Check for errors, the connection might have gone out from under us
      // mysql_error returns an empty string if there is no error
      const char* errstr = mysql_error(wrapper->client_wrapper->client);
      if (errstr[0]) {
        throw ruby::error(ma_eMysql2Error, errstr);
      }
    } else {
      throw ruby::error(
          ma_eMysql2Error,
          "You have already fetched all the rows for this query and streaming is true. (to reiterate you must requery).");
    }
  }
  else { /* not streaming */
    for (unsigned long i = 0; i < wrapper->numberOfRows; i++) {
      (void)(res.*fetch_row_func)(rbb);
    }
  }

  std::shared_ptr<arrow::RecordBatch> batch;
  status = rbb->Flush(&batch);
  if (!status.ok()) {
    throw ruby::error(rb_eRuntimeError, status.message());
  }

  auto gobj_batch = GARROW_RECORD_BATCH(
      g_object_new(GARROW_TYPE_RECORD_BATCH,
                   "record-batch", &batch, nullptr));
  return GOBJ2RVAL(gobj_batch);
}

}  // namespace internal

static VALUE
mysql2_result_to_arrow(int argc, VALUE* argv, VALUE self)
{
  try {
    return internal::mysql2_result_to_arrow(argc, argv, self);
  } catch (ruby::error err) {
    rb_exc_raise(err.exception_object());
  }
}

extern "C" void
Init_mysql2_result_extension(void)
{
  VALUE mResultExtension;

  mResultExtension = rb_define_module_under(ma_mMysql2Arrow, "ResultExtension");

  rb_define_method(mResultExtension, "to_arrow",
                   reinterpret_cast<VALUE (*)(...)>(mysql2_result_to_arrow), -1);

  intern_utc          = rb_intern("utc");
  intern_local        = rb_intern("local");
  intern_merge        = rb_intern("merge");
  // intern_localtime    = rb_intern("localtime");
  // intern_local_offset = rb_intern("local_offset");
  // intern_civil        = rb_intern("civil");
  // intern_new_offset   = rb_intern("new_offset");
  // intern_BigDecimal   = rb_intern("BigDecimal");

  sym_symbolize_keys  = ID2SYM(rb_intern("symbolize_keys"));
  sym_as              = ID2SYM(rb_intern("as"));
  sym_array           = ID2SYM(rb_intern("array"));
  sym_local           = ID2SYM(rb_intern("local"));
  sym_utc             = ID2SYM(rb_intern("utc"));
  sym_cast_booleans   = ID2SYM(rb_intern("cast_booleans"));
  sym_database_timezone     = ID2SYM(rb_intern("database_timezone"));
  sym_application_timezone  = ID2SYM(rb_intern("application_timezone"));
  sym_cache_rows     = ID2SYM(rb_intern("cache_rows"));
  sym_cast           = ID2SYM(rb_intern("cast"));
  // sym_stream         = ID2SYM(rb_intern("stream"));
  // sym_name           = ID2SYM(rb_intern("name"));

  binaryEncoding = rb_enc_find("binary");
}
