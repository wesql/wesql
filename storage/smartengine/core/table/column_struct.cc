/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "table/column_struct.h"
#include "logger/log_module.h"
#ifdef MYSQL_SERVER
#include "my_byteorder.h"
#endif
#include "schema/record_format.h"
#include "util/data_buffer.h"

namespace smartengine
{
using namespace common;
using namespace memory;
using namespace schema;
using namespace util;

namespace table
{
Column::Column()
    : schema_(),
      buf_(nullptr),
      buf_size_(0)
{}

Column::Column(const ColumnSchema &schema)
    : schema_(schema),
      buf_(nullptr),
      buf_size_(0)
{
  assert(schema_.is_valid());
}

Column::~Column()
{}

void Column::reuse()
{
  buf_ = nullptr;
  buf_size_ = 0;
}

bool Column::is_valid() const
{
  return schema_.is_valid();
}

int Column::parse(ColumnParseCtx &parse_ctx)
{
  int ret = common::Status::kOk;
  int64_t origin_pos = parse_ctx.pos_;

  if (!parse_ctx.is_valid()) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(parse_ctx));
  } else if (FAILED(do_parse(parse_ctx))) {
    SE_LOG(WARN, "fail to do parse", K(ret));
  } else {
    if (PRIMARY_COLUMN != schema_.get_type()) {
      buf_ = parse_ctx.buf_ + origin_pos;
      buf_size_ = parse_ctx.pos_ - origin_pos;
    }
  }

  return ret;
}

void Column::assign(const common::Slice &buf)
{
  buf_ = const_cast<char *>(buf.data());
  buf_size_ = buf.size();
}

int Column::do_parse(ColumnParseCtx &parse_ctx)
{
  UNUSED(parse_ctx);
  return common::Status::kOk;
}

int Column::parse_bytes(int64_t bytes,
                        char *buf,
                        int64_t buf_size,
                        int64_t &pos,
                        int64_t &value)
{
  int ret = common::Status::kOk;

#ifdef MYSQL_SERVER
  switch (bytes) {
    case 1:
      value = (int64_t)buf[pos];
      pos += 1;
      break;
    case 2:
      value = uint2korr(buf + pos);
      pos += 2;
      break;
    case 3:
      value = uint3korr(buf + pos);
      pos += 3;
      break;
    case 4:
      value = uint4korr(buf + pos);
      pos += 4;
      break;
  default:
    ret = common::Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected size bytes", K(bytes));
  }
#else
  assert(0);
#endif

  return ret;
}

DEFINE_TO_STRING(Column, KV_(schema), KVP_(buf), KV_(buf_size))

RecordHeader::RecordHeader()
    : Column(),
      flag_(0),
      col_cnt_(0),
      null_bitmap_size_(0)
{}

RecordHeader::RecordHeader(const ColumnSchema &schema)
    : Column(schema),
      flag_(0),
      col_cnt_(0),
      null_bitmap_size_(0)
{
  assert(RECORD_HEADER == schema_.get_type());
}

RecordHeader::~RecordHeader()
{}

void RecordHeader::reuse()
{
  Column::reuse();
  flag_ = 0;
  col_cnt_ = 0;
  null_bitmap_size_ = 0;
}

bool RecordHeader::is_valid() const
{
  return Column::is_valid() && (col_cnt_ >= 0) && (null_bitmap_size_ >= 0);
}

DEFINE_TO_STRING(RecordHeader, KV_(schema), KVP_(buf), KV_(buf_size),
    KV_(flag), KV_(col_cnt), KV_(null_bitmap_size))

int RecordHeader::do_parse(ColumnParseCtx &parse_ctx)
{
  int ret = common::Status::kOk;

  if (!parse_ctx.is_valid()) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(parse_ctx));
  } else if ((RECORD_HEADER != schema_.get_type()) || !schema_.is_valid()) {
    ret = common::Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected column schema", K(ret), K_(schema));
  } else {
    flag_ = (parse_ctx.buf_ + parse_ctx.pos_)[0];
    parse_ctx.pos_ += RecordFormat::RECORD_HEADER_SIZE;

    if (schema_.is_instant()) {
      if (RecordFormat::RECORD_INSTANT_DDL_FLAG & flag_) {
        if (FAILED(parse_bytes(RecordFormat::RECORD_FIELD_NUMBER_SIZE,
                               parse_ctx.buf_,
                               parse_ctx.buf_size_,
                               parse_ctx.pos_,
                               col_cnt_))) {
          SE_LOG(WARN, "fail to parse column count", K(ret));
        } else if (FAILED(parse_bytes(RecordFormat::RECORD_NULL_BITMAP_SIZE_BYTES,
                                      parse_ctx.buf_,
                                      parse_ctx.buf_size_,
                                      parse_ctx.pos_,
                                      null_bitmap_size_))) {
          SE_LOG(WARN, "fail to parse null bitmap size", K(ret));
        } else {
          SE_LOG(DEBUG, "success to parse column count and null bitmap size", K_(col_cnt), K_(null_bitmap_size));
        }
      } else {
        col_cnt_ = schema_.get_original_column_count();
        null_bitmap_size_ = schema_.get_original_null_bitmap_size();
      }
    } else {
      col_cnt_ = schema_.get_column_count();
      null_bitmap_size_ = schema_.get_null_bitmap_size();
    }
  }

  if (SUCCED(ret)) {
    parse_ctx.rec_hdr_ = this;
  }

  return ret;
}

NullBitmap::NullBitmap()
    : null_bitmap_size_(0),
      null_bitmap_(nullptr)
{}

NullBitmap::NullBitmap(const ColumnSchema &schema)
    : Column(schema),
      null_bitmap_size_(0),
      null_bitmap_(nullptr)
{
  assert(NULL_BITMAP == schema_.get_type());
}

NullBitmap::~NullBitmap()
{}

void NullBitmap::reuse()
{
  Column::reuse();
  null_bitmap_size_ = 0;
  null_bitmap_ = nullptr;
}

bool NullBitmap::is_valid() const
{
  return Column::is_valid()
         && ((0 == null_bitmap_size_ && nullptr == null_bitmap_)
            || (null_bitmap_size_ > 0 && nullptr != null_bitmap_));
}

bool NullBitmap::check_null(uint32_t null_offset, uint8_t null_mask) const
{
  return (0 != (null_bitmap_[null_offset] & null_mask)); 
}

DEFINE_TO_STRING(NullBitmap, KV_(schema), KVP_(buf), KV_(buf_size),
    KV_(null_bitmap_size), KVP_(null_bitmap))

int NullBitmap::do_parse(ColumnParseCtx &parse_ctx)
{
  int ret = common::Status::kOk;

  if (!parse_ctx.is_valid()) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(parse_ctx));
  } else if ((NULL_BITMAP != schema_.get_type()) || !schema_.is_valid()) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected column schema", K(ret), K_(schema));
  } else {
    null_bitmap_size_ = parse_ctx.rec_hdr_->null_bitmap_size_;
    if (null_bitmap_size_ > 0) {
      null_bitmap_ = parse_ctx.buf_ + parse_ctx.pos_;
      parse_ctx.pos_ += null_bitmap_size_;
    }
  }

  if (SUCCED(ret)) {
    parse_ctx.null_bitmap_ = this;
  }

  return ret;
}

UnpackInfo::UnpackInfo()
    : flag_(0),
      unpack_info_size_(0),
      unpack_info_(nullptr)
{}

UnpackInfo::UnpackInfo(const ColumnSchema &schema)
    : Column(schema),
      flag_(0),
      unpack_info_size_(0),
      unpack_info_(nullptr)
{
  assert(UNPACK_INFO == schema_.get_type());
}

UnpackInfo::~UnpackInfo()
{}

void UnpackInfo::reuse()
{
  Column::reuse();
  flag_ = 0;
  unpack_info_size_ = 0;
  unpack_info_ = nullptr;
}

bool UnpackInfo::is_valid() const
{
  return Column::is_valid()
         && (unpack_info_size_ >= 0 && nullptr != unpack_info_);
}

DEFINE_TO_STRING(UnpackInfo, KV_(schema), KVP_(buf), KV_(buf_size),
    KV_(flag), KV_(unpack_info_size), KVP_(unpack_info))

int UnpackInfo::do_parse(ColumnParseCtx &parse_ctx) 
{
  int ret = common::Status::kOk;

  if (!parse_ctx.is_valid()) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(parse_ctx));
  } else if ((UNPACK_INFO == schema_.get_type()) || !schema_.is_valid()) {
    SE_LOG(WARN, "unexpected column schema", K(ret), K_(schema));
  } else {
    flag_ = (uint8_t)(parse_ctx.buf_ + parse_ctx.pos_)[0];
    parse_ctx.pos_ += RecordFormat::RECORD_UNPACK_DATA_FLAG_SIZE;

    //unpack_info_size_ = xdb_netbuf_to_uint16(buf);
    if (FAILED(parse_unpack_info_size(parse_ctx.buf_,
                                      parse_ctx.buf_size_,
                                      parse_ctx.pos_))) {
      SE_LOG(WARN, "fail to parse unpack info size", K(ret));
    } else {
      unpack_info_ = parse_ctx.buf_ + parse_ctx.pos_;
      /**unpack_info_size include unpack header and unpack info*/
      parse_ctx.pos_ += (unpack_info_size_ - RecordFormat::RECORD_UNPACK_HEADER_SIZE);
    }
  }

  return ret;
}

int UnpackInfo::parse_unpack_info_size(char *buf, int64_t buf_size, int64_t &pos)
{
  int ret = common::Status::kOk;

  if (IS_NULL(buf) || buf_size <= 0
      || (pos + RecordFormat::RECORD_UNPACK_DATA_SIZE_BYTES) >= buf_size) {
    ret = common::Status::kInvalidArgument;
  } else {
    // TODO(Zhao Dongsheng) : uniform with se_netbuf_to_uint16? 
    uint16_t val = 0;
    memcpy(&val, buf, sizeof(val));
    unpack_info_size_ = be16toh(val);
    pos += RecordFormat::RECORD_UNPACK_DATA_SIZE_BYTES;
  }

  return ret;
}

DataColumn::DataColumn()
    : Column(),
      is_null_(false),
      data_size_(0),
      data_(nullptr)
{}

DataColumn::DataColumn(const ColumnSchema &schema)
    : Column(schema),
      is_null_(false),
      data_size_(0),
      data_(nullptr)
{
  //assert(is_data_column(schema_.type_));
}

DataColumn::~DataColumn()
{}

void DataColumn::reuse()
{
  Column::reuse();
  is_null_ = false;
  data_size_ = 0;
  data_ = nullptr;
}

void DataColumn::assign(const common::Slice &buf)
{
  Column::assign(buf);
  data_size_ = buf.size();
  data_ = const_cast<char *>(buf.data());
}

bool DataColumn::is_valid() const
{
  //TODO (Zhao Dongsheng): The empty blob still has a 2byte data size
  //, and the data size is zero. The varchar has same action.
  return Column::is_valid()
      && ((is_null_ && (nullptr == data_) && (0 == data_size_))
      || (!is_null_ && (nullptr != data_)));
}

DEFINE_TO_STRING(DataColumn, KV_(schema), KVP_(buf), KV_(buf_size),
    KV_(is_null), KV_(data_size), KVP_(data))

int DataColumn::do_parse(ColumnParseCtx &parse_ctx)
{
  int ret = common::Status::kOk;

  if (!parse_ctx.is_valid()) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(parse_ctx));
  } else if (!is_data_column(schema_.get_type()) || !schema_.is_valid()) {
    SE_LOG(WARN, "unexpected column schema", K(ret), K_(schema));
  } else if (schema_.is_primary()) {
    //assert(0 == parse_ctx.pos_);
    int32_t size = 0; 
    BufferReader reader(parse_ctx.buf_, parse_ctx.buf_size_, parse_ctx.pos_);
    reader.read_pod(size);
    data_size_ = size;
    parse_ctx.pos_ = reader.pos();
    data_ = parse_ctx.buf_ + parse_ctx.pos_;
    buf_ = data_;
    buf_size_ = data_size_;
    parse_ctx.pos_ += data_size_;
#ifndef NDEBUG
    SE_LOG(INFO, "success to parse primary column", K(data_size_), "pos", parse_ctx.pos_);
#endif
  } else {
    if ((0 != parse_ctx.rec_hdr_->null_bitmap_size_)
        && parse_ctx.null_bitmap_->check_null(schema_.get_null_offset(), schema_.get_null_mask())) {
      is_null_ = true;
      data_ = nullptr;
      data_size_ = 0;
    } else {
      if (FAILED(parse_column_data_size(parse_ctx.buf_,
                                        parse_ctx.buf_size_,
                                        parse_ctx.pos_))) {
        SE_LOG(WARN, "fail to parse column data size", K(ret));
      } else {
        data_ = parse_ctx.buf_ + parse_ctx.pos_;
        parse_ctx.pos_ += data_size_;
      }
    }
  }

  if (SUCCED(ret)) {
    if (PRIMARY_COLUMN != schema_.get_type()) {
      ++parse_ctx.parsed_data_col_cnt_;
    }
  }

  return ret;
}

int DataColumn::parse_column_data_size(char *buf,
                                       int64_t buf_size,
                                       int64_t &pos)
{
  int ret = common::Status::kOk;
  if (schema_.is_fixed()) {
    data_size_ = schema_.get_data_size();
  } else if (FAILED(parse_bytes(schema_.get_data_size_bytes(),
                                buf,
                                buf_size,
                                pos,
                                data_size_))) {
    SE_LOG(WARN, "fail to parse bytes", K(ret));
  } else {
    //succeed
  }
  
  return ret;
}

ColumnParseCtx::ColumnParseCtx()
    : buf_(nullptr),
      buf_size_(0),
      pos_(0),
      undecoded_col_cnt_(0),
      parsed_data_col_cnt_(0),
      rec_hdr_(nullptr),
      null_bitmap_(nullptr)
{}

ColumnParseCtx::~ColumnParseCtx()
{}

void ColumnParseCtx::reset()
{
  buf_ = nullptr;
  buf_size_ = 0;
  pos_ = 0;
  undecoded_col_cnt_ = 0;
  parsed_data_col_cnt_ = 0;
  rec_hdr_ = nullptr;
  null_bitmap_ = nullptr;
}

void ColumnParseCtx::setup_buf(char *buf, int64_t buf_size, int64_t pos)
{
  buf_ = buf;
  buf_size_ = buf_size;
  pos_ = pos;
}

bool ColumnParseCtx::is_valid() const
{
  /**TODO:Zhao Dongsheng, buf_size > pos_?*/
  return IS_NOTNULL(buf_) && (buf_size_ >= pos_);
}

bool ColumnParseCtx::end() const
{
  assert(rec_hdr_);
#ifndef NDEBUG
  SE_LOG(INFO, "check if reach end", K_(parsed_data_col_cnt), K_(rec_hdr), K_(undecoded_col_cnt));
#endif
  return (parsed_data_col_cnt_ >= (rec_hdr_->col_cnt_ - undecoded_col_cnt_));
}

DEFINE_TO_STRING(ColumnParseCtx, KV_(buf), KV_(buf_size), KV_(pos),
    KV_(undecoded_col_cnt), KV_(parsed_data_col_cnt), KV_(rec_hdr),
    KV_(null_bitmap))

ColumnFactory &ColumnFactory::get_instance()
{
  static ColumnFactory factory;
  return factory;
}

int ColumnFactory::alloc_column(const ColumnSchema &column_schema, Column *&column)
{
  int ret = common::Status::kOk;
  column = nullptr;

  if (!column_schema.is_valid()) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(column_schema));
  } else {
    switch(column_schema.get_type()) {
      case RECORD_HEADER:
        if (IS_NULL(column = MOD_NEW_OBJECT(ModId::kColumn,
                                            RecordHeader,
                                            column_schema))) {
          ret = common::Status::kMemoryLimit;
          SE_LOG(WARN, "fail to allocate memory for SeRecordHeader", K(ret), K(column_schema));
        }
        break;
      case NULL_BITMAP:
        if (IS_NULL(column = MOD_NEW_OBJECT(ModId::kColumn,
                                            NullBitmap,
                                            column_schema))) {
          ret = common::Status::kMemoryLimit;
          SE_LOG(WARN, "fail to allocate memory for SeNullBitmap", K(ret), K(column_schema));
        }
        break;
      case UNPACK_INFO:
        if (IS_NULL(column = MOD_NEW_OBJECT(ModId::kColumn,
                                            UnpackInfo,
                                            column_schema))) {
          ret = common::Status::kMemoryLimit;
          SE_LOG(WARN, "fail to allocate memory for SeUnpackInfo", K(ret), K(column_schema));
        }
        break;
      case PRIMARY_COLUMN:
      case BLOB_COLUMN:
      case VARCHAR_COLUMN:
      case STRING_COLUMN:
      case FIXED_COLUMN:
        if (IS_NULL(column = MOD_NEW_OBJECT(ModId::kColumn,
                                            DataColumn,
                                            column_schema))) {
          ret = common::Status::kMemoryLimit;
          SE_LOG(WARN, "fail to allocate memory for data column", K(ret), K(column_schema));
        }
        break;
      default:
        ret = common::Status::kNotSupported;
        SE_LOG(WARN, "unsupported column type", K(ret), K(column_schema));
    }
  }

  return ret;
}

int ColumnFactory::free_column(Column *&column)
{
  int ret = common::Status::kOk;

  if (IS_NULL(column)) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(column));
  } else {
    switch (column->type()) {
      case RECORD_HEADER: {
        RecordHeader *record_header = reinterpret_cast<RecordHeader *>(column);
        MOD_DELETE_OBJECT(RecordHeader, record_header);
        column = nullptr;
        break;
      }
      case NULL_BITMAP: {
        NullBitmap *null_bitmap = reinterpret_cast<NullBitmap *>(column);
        MOD_DELETE_OBJECT(NullBitmap, null_bitmap);
        column = nullptr;
        break;
      }
      case UNPACK_INFO: {
        UnpackInfo *unpack_info = reinterpret_cast<UnpackInfo *>(column);
        MOD_DELETE_OBJECT(UnpackInfo, unpack_info);
        column = nullptr;
        break;
      }
      case PRIMARY_COLUMN:
      case BLOB_COLUMN:
      case VARCHAR_COLUMN:
      case STRING_COLUMN:
      case FIXED_COLUMN: {
        DataColumn *data_column = reinterpret_cast<DataColumn *>(column);
        MOD_DELETE_OBJECT(DataColumn, data_column);
        column = nullptr;
        break;
      }
      default: {
        ret = common::Status::kNotSupported;
        SE_LOG(WARN, "unsupported column type", K(ret), K(column->type()));
      }
    }
  }

  return ret;
}

} // namespace table
} // namespace smartengine