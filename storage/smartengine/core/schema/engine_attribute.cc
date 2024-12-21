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

#include "schema/engine_attribute.h"
#include <cstring>

#ifdef MYSQL_SERVER
#include "my_rapidjson_size_t.h"
#include <rapidjson/document.h>
#endif // MYSQL_SERVER

#include "logger/log_module.h"
#include "util/status.h"
#include "options/options_helper.h"

namespace smartengine
{
using namespace common;

namespace schema
{
const char *EngineAttribute::ENGINE_ATTRIBUTE_NAMES[] = {"data_format",
                                                         "block_size",
                                                         "bloom_filter",
                                                         "compression"};

EngineAttribute::EngineAttribute()
    : data_format_(EngineAttribute::ROW_FORMAT),
      block_size_(DEFAULT_BLOCK_SIZE),
      use_bloom_filter_(true),
      compress_types_()
{
  compress_types_.push_back(kZSTD);
}

EngineAttribute::~EngineAttribute() {}

void EngineAttribute::reset()
{
  data_format_ = EngineAttribute::ROW_FORMAT;
  block_size_ = DEFAULT_BLOCK_SIZE;
  use_bloom_filter_ = false;
  compress_types_.clear();
  compress_types_.push_back(kZSTD);
}

bool EngineAttribute::is_valid() const
{
  return check_data_format_validate(data_format_) &&
         check_block_size_validate(block_size_) &&
         !compress_types_.empty();
}

int EngineAttribute::parse(const std::string &attributes_json)
{
  int ret = Status::kOk;

#ifdef MYSQL_SERVER
  rapidjson::Document attributes;

  if (attributes_json.empty()) {
    SE_LOG(INFO, "the user did not specify any attributes, use default values", K(*this));
  } else if (attributes.Parse(attributes_json.c_str()).HasParseError()) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "fail to parse attributes json format", K(ret), K(attributes_json));
  } else {
    for (auto iter = attributes.MemberBegin(); SUCCED(ret) && attributes.MemberEnd() != iter; ++iter) {
      std::string attribute_name(iter->name.GetString(), iter->name.GetStringLength());
      switch (get_attribute_type(attribute_name)) {
        case DATA_FORMAT_ATTRIBUTE: {
          if (!iter->value.IsString()) {
            ret = Status::kErrorUnexpected;
            SE_LOG(WARN, "the value type for data format is wrong", K(ret), K(attributes_json),
                K(attribute_name), "type", std::to_string(iter->value.GetType()));
          } else if (FAILED(parse_data_format_attribute(std::string(iter->value.GetString(), iter->value.GetStringLength())))) {
            SE_LOG(WARN, "fail to parse data format attribute", K(ret));
          } else {
            SE_LOG(INFO, "success to parse data format attribute", KE_(data_format));
          }
          break;
        }
        case BLOCK_SIZE_ATTRIBUTE: {
          if (!iter->value.IsInt64()) {
            ret = Status::kErrorUnexpected;
            SE_LOG(WARN, "the value type for block size is wrong", K(ret), K(attributes_json),
                K(attribute_name), "type", std::to_string(iter->value.GetType()));
          } else if (FAILED(parse_block_size_attribute(iter->value.GetInt64()))) {
            SE_LOG(WARN, "fail to parse block size attribute", K(ret), K(attributes_json),
                K(attribute_name), "value", iter->value.GetInt64());
          } else {
            SE_LOG(INFO, "success to parse block size attribute", K_(block_size));
          }
          break;
        }
        case BLOOM_FILTER_ATTRIBUTE: {
          if (!iter->value.IsBool()) {
            ret = Status::kErrorUnexpected;
            SE_LOG(WARN, "the value type for bloom filter attribute is wrong", K(ret), K(attributes_json),
                K(attribute_name), "type", std::to_string(iter->value.GetType()));
          } else {
            use_bloom_filter_ = iter->value.GetBool();
            SE_LOG(INFO, "success to parse bloom filter attribute", K_(use_bloom_filter));
          }
          break;
        }
        case COMPRESSION_ATTRIBUTE: {
          if (!iter->value.IsString()) {
            ret = Status::kErrorUnexpected;
            SE_LOG(WARN, "the value type for compression is wrong", K(ret), K(attributes_json),
                K(attribute_name), "type", std::to_string(iter->value.GetType()));
          } else if (FAILED(parse_compression_attribute(std::string(iter->value.GetString(), iter->value.GetStringLength())))) {
            SE_LOG(WARN, "fail to parse compression attribute", K(ret), K(attributes_json),
                K(attribute_name), "type", std::to_string(iter->value.GetType()));
          } else {
            SE_LOG(INFO, "success to parse compression attribute");
          }
          break;
        }
        default:
          ret = Status::kNotSupported;
          SE_LOG(WARN, "unsupported attribute", K(ret), K(attributes_json), K(attribute_name));
      }
    }
  }
#endif // MYSQL_SERVER

  return ret;
}

int64_t EngineAttribute::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  util::databuff_printf(buf, buf_len, pos, "{");
  util::databuff_print_kv_list(buf, buf_len, pos, "data_format", static_cast<int8_t>(data_format_));
  util::databuff_print_kv_list(buf, buf_len, pos, "block_size", block_size_);
  util::databuff_print_kv_list(buf, buf_len, pos, "use_bloom_filter", use_bloom_filter_);

  util::databuff_printf(buf, buf_len, pos, "{");
  util::databuff_printf(buf, buf_len, pos, "\"compress_types\":");
  for (int8_t i = 0; i < static_cast<int8_t>(compress_types_.size()); ++i) {
    util::databuff_print_obj(buf, buf_len, pos, static_cast<int8_t>(compress_types_[i]));
  }
  util::databuff_printf(buf, buf_len, pos, "}");

  util::databuff_printf(buf, buf_len, pos, "}"); 

  return pos;
}

int EngineAttribute::serialize(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = Status::kOk;
  int32_t header_size = get_serialize_size();
  int32_t header_version = ENGINE_ATTRIBUTE_VERSION;
  int8_t data_format_value = static_cast<int8_t>(data_format_);

  if (IS_NULL(buf) || UNLIKELY(buf_len <= 0) || UNLIKELY(pos >= buf_len)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (FAILED(util::encode_fixed_int32(buf, buf_len, pos, header_size))) {
    SE_LOG(WARN, "fail to encode header size", K(ret), K(header_size));
  } else if (FAILED(util::encode_fixed_int32(buf, buf_len, pos, header_version))) {
    SE_LOG(WARN, "fail to encode header version", K(ret), K(header_version));
  } else if (FAILED(util::encode_fixed_int8(buf, buf_len, pos, data_format_value))) {
    SE_LOG(WARN, "fail to encode data format", K(ret), KE_(data_format));
  } else if (FAILED(util::encode_fixed_int32(buf, buf_len, pos, block_size_))) {
    SE_LOG(WARN, "fail to encode block size", K(ret), K_(block_size));
  } else if (FAILED(util::encode_boolean(buf, buf_len, pos, use_bloom_filter_))) {
    SE_LOG(WARN, "fail to encode use bloom filter", K(ret), KE_(use_bloom_filter));
  } else {
    int8_t count = compress_types_.size();
    int8_t type = 0;
    if (FAILED(util::encode_fixed_int8(buf, buf_len, pos, count))) {
      SE_LOG(WARN, "fail to encode compress count", K(ret), K(count));
    } else {
      for (int8_t i = 0; SUCCED(ret) && i < count; ++i) {
        type = static_cast<int8_t>(compress_types_[i]);
        if (FAILED(util::encode_fixed_int8(buf, buf_len, pos, type))) {
          SE_LOG(WARN, "fail to encode compress type", K(ret), K(i), K(type));
        }
      }
    }
  }

  return ret;
}

int EngineAttribute::deserialize(const char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = Status::kOk;
  int32_t header_size = 0;
  int32_t header_version = 0;
  int8_t data_format_value = 0;

  if (IS_NULL(buf) || UNLIKELY(buf_len <= 0) || UNLIKELY(pos >= buf_len)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (FAILED(util::decode_fixed_int32(buf, buf_len, pos, header_size))) {
    SE_LOG(WARN, "fail to decode header size", K(ret));
  } else if (FAILED(util::decode_fixed_int32(buf, buf_len, pos, header_version))) {
    SE_LOG(WARN, "fail to decode header version", K(ret));
  } else if (FAILED(util::decode_fixed_int8(buf, buf_len, pos, data_format_value))) {
    SE_LOG(WARN, "fail to decode data format", K(ret));
  } else if (FAILED(util::decode_fixed_int32(buf, buf_len, pos, block_size_))) {
    SE_LOG(WARN, "fail to decode block size", K(ret));
  } else if (FAILED(util::decode_boolean(buf, buf_len, pos, use_bloom_filter_))) {
    SE_LOG(WARN, "fail to decode use bloom filter", K(ret));
  } else {
    data_format_ = static_cast<DataFormat>(data_format_value);

    int8_t count = 0;
    int8_t type = 0;
    compress_types_.clear();
    if (FAILED(util::decode_fixed_int8(buf, buf_len, pos, count))) {
      SE_LOG(WARN, "fail to decode compress count", K(ret));
    } else {
      for (int8_t i = 0; i < count; ++i) {
        if (FAILED(util::decode_fixed_int8(buf, buf_len, pos, type))) {
          SE_LOG(WARN, "fail to decode type", K(ret));
        } else {
          compress_types_.push_back(static_cast<CompressionType>(type));
        }
      }
    }
  }

  return ret;
}

int64_t EngineAttribute::get_serialize_size() const
{
  // header size and header version
  int64_t size = sizeof(int32_t) + sizeof(int32_t);
  size += sizeof(int8_t); // data_format_
  size += sizeof(block_size_); // block_size_
  size += sizeof(bool); // use_bloom_filter_
  size += sizeof(int8_t); // count of compress_types_
  size += sizeof(int8_t) * compress_types_.size(); // elements of compress_types_

  return size;
}

bool EngineAttribute::compare_str(const char *cstr, const std::string &str)
{
  return (strlen(cstr) == str.size()) && (0 == strcasecmp(cstr, str.c_str()));
}

bool EngineAttribute::is_data_format_attribute(const std::string &name)
{
  return compare_str(ENGINE_ATTRIBUTE_NAMES[0], name);
}

bool EngineAttribute::is_block_size_attribute(const std::string &name)
{
  return compare_str(ENGINE_ATTRIBUTE_NAMES[1], name);
}

bool EngineAttribute::is_bloom_filter_attribute(const std::string &name)
{
  return compare_str(ENGINE_ATTRIBUTE_NAMES[2], name);
}

bool EngineAttribute::is_compression_attribute(const std::string &name)
{
  return compare_str(ENGINE_ATTRIBUTE_NAMES[3], name);
}

bool EngineAttribute::check_data_format_validate(const DataFormat format) const
{
  return (ROW_FORMAT == format) || (COLUMNAR_FORMAT == format);
}

bool EngineAttribute::check_block_size_validate(const int32_t value) const
{
  return (value > 0) && (value <= (1 << BLOCK_SIZE_SHIFT_MAX));
}

EngineAttribute::EngineAttributeType EngineAttribute::get_attribute_type(const std::string &name)
{
  EngineAttributeType attribute_type = INVALID_ENGINE_ATTRIBUTE_TYPE;

  if (is_data_format_attribute(name)) {
    attribute_type = DATA_FORMAT_ATTRIBUTE;
  } else if (is_block_size_attribute(name)) {
    attribute_type = BLOCK_SIZE_ATTRIBUTE;
  } else if (is_bloom_filter_attribute(name)) {
    attribute_type = BLOOM_FILTER_ATTRIBUTE;
  } else if (is_compression_attribute(name)) {
    attribute_type = COMPRESSION_ATTRIBUTE;
  } else {
    SE_LOG(WARN, "unsupported attribute name", K(name));
  }

  return attribute_type;
}

int EngineAttribute::parse_data_format_attribute(const std::string &value)
{
  int ret = Status::kOk;

  if (UNLIKELY(value.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(value));
  } else if (compare_str("row", value)) {
    data_format_ = ROW_FORMAT;
  } else if (compare_str("columnar", value)) {
    data_format_ = COLUMNAR_FORMAT;
  } else {
    ret = Status::kNotSupported;
    SE_LOG(WARN, "unsupported data format type", K(ret), K(value));
  }

  return ret;
}

int EngineAttribute::parse_block_size_attribute(const int32_t value)
{
  int ret = Status::kOk;

  if (UNLIKELY(value <= 0)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(value));
  } else if (!check_block_size_validate(value)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the block size is invalid", K(ret), K(value));
  } else {
    block_size_ = value;
  }

  return ret;
}

int EngineAttribute::parse_compression_attribute(const std::string &value)
{
  int ret = Status::kOk;

  if (UNLIKELY(value.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(value));
  } else if (!GetVectorCompressionType(value, &compress_types_)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "fail to parse compression attribute", K(ret), K(value));
  } else {
    SE_LOG(INFO, "compression attribute", K(value));
  }

  return ret;
}

} // namespace schema
} // namespace smartengine