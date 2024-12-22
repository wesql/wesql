#include "table/column_block_writer.h"
#include "logger/log_module.h"

namespace smartengine
{
using namespace common;
using namespace memory;
using namespace schema;

namespace table
{

ColumnBlockWriter::ColumnBlockWriter()
    : is_inited_(false),
      table_schema_(),
      compress_type_(kNoCompression),
      parse_ctx_(),
      columns_(),
      column_unit_writers_(),
      row_count_(0),
      column_count_(0),
      block_buf_(ModId::kColumnBlockWriter)
{}

ColumnBlockWriter::~ColumnBlockWriter()
{
  destroy();
}

void ColumnBlockWriter::destroy()
{
  for (uint32_t i =0; i < columns_.size(); ++i) {
    ColumnFactory::get_instance().free_column(columns_[i]);
  }
  columns_.clear();

  for (uint32_t i = 0; i < column_unit_writers_.size(); ++i) {
    MOD_DELETE_OBJECT(ColumnUnitWriter, column_unit_writers_[i]);
  }
  column_unit_writers_.clear();
}

int ColumnBlockWriter::init(const TableSchema &table_schema, const CompressionType compress_type)
{
  int ret = common::Status::kOk;
  Column *column = nullptr;

  if (UNLIKELY(is_inited_)) {
    ret = common::Status::kInitTwice;
    SE_LOG(WARN, "ColumnBlockWriter has been inited", K(ret));
  } else if (UNLIKELY(!table_schema.is_valid())) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(table_schema));
  } else if (FAILED(init_columns(table_schema.get_column_schemas()))) {
    SE_LOG(WARN, "fail to init columns", K(ret));
  } else if (FAILED(init_column_unit_writers(table_schema.get_column_schemas(), compress_type))) {
    SE_LOG(WARN, "fail to init column unit writers", K(ret));
  } else {
    table_schema_ = table_schema;
    compress_type_ = compress_type;
    is_inited_ = true;

#ifndef NDEBUG
    SE_LOG(INFO, "success to init ColumnBlockWriter", "column_size", table_schema.get_column_schemas().size());
#endif
  }

  return ret;
}

void ColumnBlockWriter::reuse()
{
  parse_ctx_.reset();
  row_count_ = 0;
  column_count_ = 0;
  block_buf_.reuse();
  std::for_each(column_unit_writers_.begin(), column_unit_writers_.end(), [&](ColumnUnitWriter *column_unit_writer) -> void {
    column_unit_writer->reuse();
  });
}

int ColumnBlockWriter::append(const Slice &key, const Slice &value)
{
  int ret = common::Status::kOk;
  ColumnArray columns;

  if (UNLIKELY(!is_inited_)) {
    ret = common::Status::kNotInit;
    SE_LOG(WARN, "ColumnBlockWriter should been inited first", K(ret));
  } else if (UNLIKELY(key.empty())) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(key));
  } else if (FAILED(convert_to_columns(key, value, columns))) {
    SE_LOG(WARN, "fail to convert to columns", K(ret));
  } else if (FAILED(write_columns(columns))) {
    SE_LOG(WARN, "failed to write columns", K(ret));
  } else {
    ++row_count_;
    /**the maximum of column count among the rows in this block.*/
    column_count_ = std::max(static_cast<int64_t>(columns.size()), column_count_);

#ifndef  NDEBUG
    SE_LOG(DEBUG, "success to append row", K_(row_count), K_(column_count));
#endif
  }

  return ret;
}

int ColumnBlockWriter::build(Slice &block_data, BlockInfo &block_info)
{
  int ret = Status::kOk;
  Slice unit_data;
  ColumnUnitInfo unit_info;
  ColumnUnitWriter *unit_writer = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ColumnBlockWriter should be inited", K(ret));
  } else if (UNLIKELY(column_count_ > static_cast<int64_t>(column_unit_writers_.size()))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, the column count must not more than schema definition",
        K(ret), K_(column_count), "schema_column_count", column_unit_writers_.size());
  } else {
    for (int32_t i = 0; SUCCED(ret) && i < column_count_; ++i) {
      unit_data.clear();
      unit_info.reset();
      unit_writer = nullptr;
      unit_info.data_offset_ = block_buf_.size();
      if (IS_NULL(unit_writer = column_unit_writers_[i])) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "the column unit writer must not be nullptr", K(ret), K(i), KP(unit_writer));
      } else if (FAILED(unit_writer->build(unit_data, unit_info))) {
        SE_LOG(WARN, "fail to build column unit", K(ret), K(i));
      } else if (FAILED(block_buf_.write(unit_data))) {
        SE_LOG(WARN, "fail to write unit data", K(ret));
      } else {
        block_info.get_unit_infos().push_back(unit_info);
      }
    }

    if (SUCCED(ret)) {
      block_data.assign(block_buf_.data(), block_buf_.size());
      block_info.set_columnar_format();

#ifndef NDEBUG  
      SE_LOG(INFO, "success to build column block", K(ret), "size", block_data.size());
#endif
    }
  }

  return ret;
}

int64_t ColumnBlockWriter::current_size() const
{
  int64_t size = 0;
  for (int32_t i = 0; i < column_count_; ++i) {
    assert(IS_NOTNULL(column_unit_writers_[i]));
    size += column_unit_writers_[i]->current_size();
  }
  return size;
}

int64_t ColumnBlockWriter::future_size(const uint32_t key_size, const uint32_t value_size) const
{
  int64_t future_size = current_size();
  //TODO: Zhao Dongsheng, the size of uint32_t is the key size
  future_size += sizeof(uint32_t) + key_size + value_size;

  return future_size;
}

int ColumnBlockWriter::init_columns(const ColumnSchemaArray &column_schemas)
{
  int ret = Status::kOk;
  Column *column = nullptr;

  for (uint32_t i = 0; SUCCED(ret) && i < column_schemas.size(); ++i) {
    const ColumnSchema &column_schema = column_schemas[i];
    column = nullptr;
    if (UNLIKELY(!column_schema.is_valid())) {
      ret = Status::kInvalidArgument;
      SE_LOG(WARN, "invalid column schema", K(ret), K(column_schema));
    } else if (FAILED(ColumnFactory::get_instance().alloc_column(column_schema, column))) {
      SE_LOG(WARN, "fail to allocate column", K(ret), K(i), K(column_schema));
    } else {
      assert(IS_NOTNULL(column));
      columns_.push_back(column);
    }
  }

  // Resource clean
  if (FAILED(ret)) {
    if (IS_NOTNULL(column)) {
      ColumnFactory::get_instance().free_column(column);
      column = nullptr;
    }
    for (uint32_t i = 0; i < columns_.size(); ++i) {
      ColumnFactory::get_instance().free_column(columns_[i]);
    }
    columns_.clear();
  }

  return ret;
}

int ColumnBlockWriter::init_column_unit_writers(const ColumnSchemaArray &column_schemas, const CompressionType compress_type)
{
  int ret = common::Status::kOk;
  ColumnUnitWriter *column_unit_writer = nullptr;

  for (uint32_t i = 0; SUCCED(ret) && i < column_schemas.size(); ++i) {
    const ColumnSchema &column_schema = column_schemas[i];
    column_unit_writer = nullptr; 
    if (!column_schema.is_valid()) {
      ret = common::Status::kInvalidArgument;
      SE_LOG(WARN, "invalid argument", K(ret), K(i), K(column_schema));
    } else if (IS_NULL(column_unit_writer = MOD_NEW_OBJECT(ModId::kColumnUnitWriter, ColumnUnitWriter))) {
      ret = common::Status::kMemoryLimit;
      SE_LOG(WARN, "failed to allocate memory for SeColumnUnitWriter", K(ret), K(i), K(column_schema));
    } else if (FAILED(column_unit_writer->init(column_schema, compress_type))) {
      SE_LOG(WARN, "failed to init column writer", K(ret));
    } else {
      column_unit_writers_.push_back(column_unit_writer);
    }
  }

  if (FAILED(ret)) {
    if (IS_NOTNULL(column_unit_writer)) {
      MOD_DELETE_OBJECT(ColumnUnitWriter, column_unit_writer);
    }
    for (uint32_t i = 0; i < column_unit_writers_.size(); ++i) {
      MOD_DELETE_OBJECT(ColumnUnitWriter, column_unit_writers_[i]);
    }
    column_unit_writers_.clear();
  }

  return ret;
}

int ColumnBlockWriter::convert_to_columns(const Slice &key, const Slice &value, ColumnArray &columns)
{
  int ret = Status::kOk;
  Column *column = nullptr;
  int64_t pos = 0;
  columns.clear();
  parse_ctx_.reset();
  /**TODO: Zhao Dongsheng, the undecoded_col_cnt_ name should change?*/
  parse_ctx_.undecoded_col_cnt_ = table_schema_.get_packed_column_count();
  parse_ctx_.setup_buf(const_cast<char *>(value.data()), value.size(), pos);

  if (UNLIKELY(key.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret));
  } else if (UNLIKELY(columns_.empty())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "at least one column must exist", K(ret));
  } else {
    for (uint32_t i = 0; SUCCED(ret) && i < columns_.size(); ++i) {
      if (IS_NULL(column = columns_[i])) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "the column must not be nullptr", K(ret), K(i));
      } else {
        column->reuse();
        if (0 == i) {
          // The first column must be primary key.
          column->assign(key);
          columns.push_back(column);
        } else if (FAILED(column->parse(parse_ctx_))) {
          SE_LOG(WARN, "fail to parse column", K(ret), K(i));
        } else {
          columns.push_back(column);

          if (parse_ctx_.end()) {
            break;
          }
        }
      }
    }
  } 

  return ret;
}

int ColumnBlockWriter::write_columns(const ColumnArray &columns)
{
  int ret = common::Status::kOk;
  Column *column = nullptr;
  ColumnUnitWriter *unit_writer = nullptr;

  if (columns.size() > column_unit_writers_.size()) {
    ret = common::Status::kErrorUnexpected;
    SE_LOG(WARN, "the column count is not match with table schema", K(ret),
        "column_size", columns.size(), "writer_size", column_unit_writers_.size());
  } else {
    for (uint32_t i = 0; SUCCED(ret) && i < columns.size(); ++i) {
      column = nullptr;
      unit_writer = nullptr;
      if (IS_NULL(column = columns[i]) || IS_NULL(unit_writer = column_unit_writers_[i])) {
        ret = common::Status::kErrorUnexpected;
        SE_LOG(WARN, "neither column nor column writer should been nulltr", K(ret),
            K(i), KP(column), KP(unit_writer));
      } else if (FAILED(unit_writer->append(column))) {
        SE_LOG(WARN, "failed to append column", K(ret), K(i));
      } else {
        //succeed
      }
    }
  }

  return ret;
}

} // namespace table
} // namespace smartengine