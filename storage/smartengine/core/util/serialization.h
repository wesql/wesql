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
 *
 * serialization.h is for packing all of integral types data into buffer stream.
 */

#ifndef SMARTENGINE_INCLUDE_SERIALIZATION_H_
#define SMARTENGINE_INCLUDE_SERIALIZATION_H_

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unordered_set>
#include <unordered_map>
#include "template_util.h"
#include "util/status.h"

#define ASSERT_BUFFER(length) \
  assert(nullptr != buffer && bufsiz - pos >= length)

#define ASSERT_FIXED_LENGTH_BUFFER(type) \
  assert(nullptr != buffer && bufsiz - pos >= (int64_t)sizeof(type))

#define DECLARE_SERIALIZATION()                                      \
  public: \
    int serialize(char *buffer, int64_t bufsiz, int64_t &pos) const;   \
    int deserialize(const char *buffer, int64_t bufsiz, int64_t &pos); \
    int64_t get_serialize_size() const;

#define DEFINE_SERIALIZATION(clz, ...)                                     \
  int clz::serialize(char *buffer, int64_t bufsiz, int64_t &pos) const {   \
    return util::serialize(buffer, bufsiz, pos, __VA_ARGS__);              \
  }                                                                        \
  int clz::deserialize(const char *buffer, int64_t bufsiz, int64_t &pos) { \
    return util::deserialize(buffer, bufsiz, pos, __VA_ARGS__);            \
  }                                                                        \
  int64_t clz::get_serialize_size() const {                                \
    return util::get_serialize_size(__VA_ARGS__);                          \
  }

/*-----------------macros for compactiple serialize and deserialize---------------------*/
//macros for declare compactiple serialize and deserialize
#define DECLARE_COMPACTIPLE_SERIALIZATION(VERSION) \
  public: \
    DECLARE_COMPACTIPLE_SERIALIZATION_ \
  private: \
    const static int64_t VERSION_ = VERSION;

#define DECLARE_COMPACTIPLE_SERIALIZATION_ \
  int serialize(char *buf, int64_t buf_len, int64_t &pos) const; \
  int serialize_(char *buf, int64_t buf_len, int64_t &pos) const; \
  int deserialize(const char *buf, int64_t buf_len, int64_t &pos); \
  int deserialize_(const char *buf, int64_t buf_len, int64_t &pos); \
  int64_t get_serialize_size() const; \
  int64_t get_serialize_size_() const;

//macros for declare compactiple serialize and deserialize with override
#define DECLARE_COMPACTIPLE_SERIALIZATION_OVERRIDE(VERSION) \
  public: \
    DECLARE_COMPACTIPLE_SERIALIZATION_OVERRIDE_ \
  private: \
    const static int64_t VERSION_ = VERSION;

#define DECLARE_COMPACTIPLE_SERIALIZATION_OVERRIDE_ \
  int serialize(char *buf, int64_t buf_len, int64_t &pos) const override; \
  int serialize_(char *buf, int64_t buf_len, int64_t &pos) const; \
  int deserialize(const char *buf, int64_t buf_len, int64_t &pos) override; \
  int deserialize_(const char *buf, int64_t buf_len, int64_t &pos); \
  int64_t get_serialize_size() const override; \
  int64_t get_serialize_size_() const;

//macros for serialize and deserialize header
#define SERIALIZE_HEADER(VERSION, LEN) \
  if (common::Status::kOk == ret) { \
    ret = util::serialize(buf, buf_len, pos, VERSION); \
  } \
  if (common::Status::kOk == ret) { \
    ret = util::serialize(buf, buf_len, pos, LEN); \
  }

#define CHECK_HEADER(VERSION, LEN) \
  if (common::Status::kOk == ret) { \
    if (VERSION_ != VERSION) { \
      ret = common::Status::kNotSupported; \
    } else if (LEN < 0) { \
      ret = common::Status::kCorruption; \
    } else if (buf_len < (pos + LEN)) { \
      ret = common::Status::kCorruption; \
    } \
  }

#define DESERIALIZE_HEADER(VERSION, LEN) \
  if (common::Status::kOk == ret) { \
    ret = util::deserialize(buf, buf_len, pos, VERSION); \
  } \
  if (common::Status::kOk == ret) { \
    ret = util::deserialize(buf, buf_len, pos, LEN); \
  }

#define HEADER_SERIALIZE_SIZE(VERSION, LEN) \
  LEN += util::get_serialize_size(LEN); \
  LEN += util::get_serialize_size(VERSION)

#define DEFINE_COMPACTIPLE_SERIALIZATION(CLZ, ...) \
  int CLZ::serialize(char *buf, int64_t buf_len, int64_t &pos) const \
  { \
    int ret = common::Status::kOk; \
    int64_t serialize_size = get_serialize_size_(); \
    SERIALIZE_HEADER(VERSION_, serialize_size); \
    if (common::Status::kOk == ret) { \
      ret = serialize_(buf, buf_len, pos); \
    } \
    return ret; \
  } \
  int CLZ::serialize_(char *buf, int64_t buf_len, int64_t &pos) const \
  { \
    return util::serialize_x(buf, buf_len, pos, ##__VA_ARGS__); \
  } \
  int CLZ::deserialize(const char *buf, int64_t buf_len, int64_t &pos) \
  { \
    int ret = common::Status::kOk; \
    int64_t version = 0; \
    int64_t deserialize_size = 0; \
    int64_t orig_pos = 0; \
    DESERIALIZE_HEADER(version, deserialize_size);\
    CHECK_HEADER(version, deserialize_size); \
    if (common::Status::kOk == ret) { \
      orig_pos = pos; \
      pos = 0; \
      ret = deserialize_(buf + orig_pos, deserialize_size, pos); \
      pos = orig_pos + deserialize_size; \
    } \
    return ret; \
  } \
  int CLZ::deserialize_(const char *buf, int64_t buf_len, int64_t &pos) \
  { \
    return util::deserialize_x(buf, buf_len, pos, ##__VA_ARGS__); \
  } \
  int64_t CLZ::get_serialize_size() const \
  { \
    int64_t serialize_size = get_serialize_size_(); \
    HEADER_SERIALIZE_SIZE(VERSION_, serialize_size); \
    return serialize_size; \
  } \
  int64_t CLZ::get_serialize_size_() const \
  { \
    return util::get_serialize_size_x(__VA_ARGS__); \
  }

// macros for declare pure virtual serialize and deserialize
#define DECLARE_PURE_VIRTUAL_SERIALIZATION() \
  virtual int serialize(char *buf, int64_t buf_len, int64_t &pos) const = 0; \
  virtual int deserialize(const char *buf, int64_t buf_len, int64_t &pos) = 0; \
  virtual int64_t get_serialize_size() const = 0

namespace smartengine {
namespace common {
class Status;
class Slice;
}  // namespace common

namespace util {

const uint64_t XE_MAX_V1B = (__UINT64_C(1) << 7) - 1;
const uint64_t XE_MAX_V2B = (__UINT64_C(1) << 14) - 1;
const uint64_t XE_MAX_V3B = (__UINT64_C(1) << 21) - 1;
const uint64_t XE_MAX_V4B = (__UINT64_C(1) << 28) - 1;
const uint64_t XE_MAX_V5B = (__UINT64_C(1) << 35) - 1;
const uint64_t XE_MAX_V6B = (__UINT64_C(1) << 42) - 1;
const uint64_t XE_MAX_V7B = (__UINT64_C(1) << 49) - 1;
const uint64_t XE_MAX_V8B = (__UINT64_C(1) << 56) - 1;
const uint64_t XE_MAX_V9B = (__UINT64_C(1) << 63) - 1;

const uint64_t XE_MAX_1B = (__UINT64_C(1) << 8) - 1;
const uint64_t XE_MAX_2B = (__UINT64_C(1) << 16) - 1;
const uint64_t XE_MAX_3B = (__UINT64_C(1) << 24) - 1;
const uint64_t XE_MAX_4B = (__UINT64_C(1) << 32) - 1;
const uint64_t XE_MAX_5B = (__UINT64_C(1) << 40) - 1;
const uint64_t XE_MAX_6B = (__UINT64_C(1) << 48) - 1;
const uint64_t XE_MAX_7B = (__UINT64_C(1) << 56) - 1;
const uint64_t XE_MAX_8B = UINT64_MAX;

const uint64_t XE_MAX_INT_1B = (__UINT64_C(23));
const uint64_t XE_MAX_INT_2B = (__UINT64_C(1) << 8) - 1;
const uint64_t XE_MAX_INT_3B = (__UINT64_C(1) << 16) - 1;
const uint64_t XE_MAX_INT_4B = (__UINT64_C(1) << 24) - 1;
const uint64_t XE_MAX_INT_5B = (__UINT64_C(1) << 32) - 1;
const uint64_t XE_MAX_INT_7B = (__UINT64_C(1) << 48) - 1;
const uint64_t XE_MAX_INT_9B = UINT64_MAX;

const int64_t XE_MAX_1B_STR_LEN = (__INT64_C(55));
const int64_t XE_MAX_2B_STR_LEN = (__INT64_C(1) << 8) - 1;
const int64_t XE_MAX_3B_STR_LEN = (__INT64_C(1) << 16) - 1;
const int64_t XE_MAX_4B_STR_LEN = (__INT64_C(1) << 24) - 1;
const int64_t XE_MAX_5B_STR_LEN = (__INT64_C(1) << 32) - 1;

const int8_t XE_INT_TYPE_BIT_POS = 7;
const int8_t XE_INT_OPERATION_BIT_POS = 6;
const int8_t XE_INT_SIGN_BIT_POS = 5;
const int8_t XE_DATETIME_OPERATION_BIT = 3;
const int8_t XE_DATETIME_SIGN_BIT = 2;
const int8_t XE_FLOAT_OPERATION_BIT_POS = 0;
const int8_t XE_DECIMAL_OPERATION_BIT_POS = 7;

const int8_t XE_INT_VALUE_MASK = 0x1f;
const int8_t XE_VARCHAR_LEN_MASK = 0x3f;
const int8_t XE_DATETIME_LEN_MASK = 0x3;

const int8_t XE_VARCHAR_TYPE = static_cast<int8_t>((0x1 << 7));
const int8_t XE_SEQ_TYPE = static_cast<int8_t>(0xc0);
const int8_t XE_DATETIME_TYPE = static_cast<int8_t>(0xd0);
const int8_t XE_PRECISE_DATETIME_TYPE = static_cast<int8_t>(0xe0);
const int8_t XE_MODIFYTIME_TYPE = static_cast<int8_t>(0xf0);
const int8_t XE_CREATETIME_TYPE = static_cast<int8_t>(0xf4);
const int8_t XE_FLOAT_TYPE = static_cast<int8_t>(0xf8);
const int8_t XE_DOUBLE_TYPE = static_cast<int8_t>(0xfa);
const int8_t XE_nullptr_TYPE = static_cast<int8_t>(0xfc);
const int8_t XE_BOOL_TYPE = static_cast<int8_t>(0xfd);
const int8_t XE_EXTEND_TYPE = static_cast<int8_t>(0xfe);
const int8_t XE_DECIMAL_TYPE = static_cast<int8_t>(0xff);
const int8_t XE_NUMBER_TYPE = XE_DECIMAL_TYPE;  // 2014number

/**
 * @brief Encode a byte into given buffer.
 *
 * @param buffer address of destination buffer
 * @param value the byte
 *
 */

inline int encode_fixed_int8(char *buffer, const int64_t bufsiz, int64_t &pos,
                             int8_t value) {
  ASSERT_FIXED_LENGTH_BUFFER(int8_t);
  *(buffer + pos++) = value;
  return common::Status::kOk;
}

inline int decode_fixed_int8(const char *buffer, const int64_t bufsiz,
                             int64_t &pos, int8_t &value) {
  ASSERT_FIXED_LENGTH_BUFFER(int8_t);
  value = *(buffer + pos++);
  return common::Status::kOk;
}

/**
 * @brief Enocde a 16-bit integer in big-endian order
 *
 * @param buffer address of destination buffer
 * @param value value to encode
 */
inline int encode_fixed_int16(char *buffer, const int64_t bufsiz, int64_t &pos,
                              int16_t value) {
  ASSERT_FIXED_LENGTH_BUFFER(int16_t);
  *(buffer + pos++) = static_cast<char>((((value) >> 8)) & 0xff);
  *(buffer + pos++) = static_cast<char>((value)&0xff);
  return common::Status::kOk;
}

inline int decode_fixed_int16(const char *buffer, const int64_t bufsiz,
                              int64_t &pos, int16_t &value) {
  ASSERT_FIXED_LENGTH_BUFFER(int16_t);
  value = static_cast<int16_t>(((*(buffer + pos++)) & 0xff) << 8);
  value = static_cast<int16_t>(value | (*(buffer + pos++) & 0xff));
  return common::Status::kOk;
}

inline int encode_fixed_int32(char *buffer, const int64_t bufsiz, int64_t &pos,
                              int32_t value) {
  ASSERT_FIXED_LENGTH_BUFFER(int32_t);
  *(buffer + pos++) = static_cast<char>(((value) >> 24) & 0xff);
  *(buffer + pos++) = static_cast<char>(((value) >> 16) & 0xff);
  *(buffer + pos++) = static_cast<char>(((value) >> 8) & 0xff);
  *(buffer + pos++) = static_cast<char>((value)&0xff);
  return common::Status::kOk;
}

inline int decode_fixed_int32(const char *buffer, const int64_t bufsiz,
                              int64_t &pos, int32_t &value) {
  ASSERT_FIXED_LENGTH_BUFFER(int32_t);
  value = ((static_cast<int32_t>(*(buffer + pos++))) & 0xff) << 24;
  value |= ((static_cast<int32_t>(*(buffer + pos++))) & 0xff) << 16;
  value |= ((static_cast<int32_t>(*(buffer + pos++))) & 0xff) << 8;
  value |= ((static_cast<int32_t>(*(buffer + pos++))) & 0xff);
  return common::Status::kOk;
}

inline int encode_fixed_int64(char *buffer, const int64_t bufsiz, int64_t &pos,
                              int64_t value) {
  ASSERT_FIXED_LENGTH_BUFFER(int64_t);
  *(buffer + pos++) = static_cast<char>(((value) >> 56) & 0xff);
  *(buffer + pos++) = static_cast<char>(((value) >> 48) & 0xff);
  *(buffer + pos++) = static_cast<char>(((value) >> 40) & 0xff);
  *(buffer + pos++) = static_cast<char>(((value) >> 32) & 0xff);
  *(buffer + pos++) = static_cast<char>(((value) >> 24) & 0xff);
  *(buffer + pos++) = static_cast<char>(((value) >> 16) & 0xff);
  *(buffer + pos++) = static_cast<char>(((value) >> 8) & 0xff);
  *(buffer + pos++) = static_cast<char>((value)&0xff);
  return common::Status::kOk;
}

inline int decode_fixed_int64(const char *buffer, const int64_t bufsiz,
                              int64_t &pos, int64_t &value) {
  ASSERT_FIXED_LENGTH_BUFFER(int64_t);
  value = ((static_cast<int64_t>((*(buffer + pos++))) & 0xff)) << 56;
  value |= ((static_cast<int64_t>((*(buffer + pos++))) & 0xff)) << 48;
  value |= ((static_cast<int64_t>((*(buffer + pos++))) & 0xff)) << 40;
  value |= ((static_cast<int64_t>((*(buffer + pos++))) & 0xff)) << 32;
  value |= ((static_cast<int64_t>((*(buffer + pos++))) & 0xff)) << 24;
  value |= ((static_cast<int64_t>((*(buffer + pos++))) & 0xff)) << 16;
  value |= ((static_cast<int64_t>((*(buffer + pos++))) & 0xff)) << 8;
  value |= ((static_cast<int64_t>((*(buffer + pos++))) & 0xff));
  return common::Status::kOk;
}

inline int encode_boolean(char *buffer, const int64_t bufsiz, int64_t &pos,
                          bool value) {
  ASSERT_FIXED_LENGTH_BUFFER(int8_t);
  *(buffer + pos++) = (value) ? 1 : 0;
  return common::Status::kOk;
}

inline int decode_boolean(const char *buffer, const int64_t bufsiz,
                          int64_t &pos, bool &value) {
  ASSERT_FIXED_LENGTH_BUFFER(char);
  int8_t v = 0;
  int ret = 0;
  if ((ret = decode_fixed_int8(buffer, bufsiz, pos, v)) ==
      common::Status::kOk) {
    value = (v != 0);
  }
  return ret;
}

inline int64_t encoded_length_var_int64(int64_t value) {
  uint64_t uv = static_cast<uint64_t>(value);
  int64_t need_bytes = 0;
  if (uv <= XE_MAX_V1B)
    need_bytes = 1;
  else if (uv <= XE_MAX_V2B)
    need_bytes = 2;
  else if (uv <= XE_MAX_V3B)
    need_bytes = 3;
  else if (uv <= XE_MAX_V4B)
    need_bytes = 4;
  else if (uv <= XE_MAX_V5B)
    need_bytes = 5;
  else if (uv <= XE_MAX_V6B)
    need_bytes = 6;
  else if (uv <= XE_MAX_V7B)
    need_bytes = 7;
  else if (uv <= XE_MAX_V8B)
    need_bytes = 8;
  else if (uv <= XE_MAX_V9B)
    need_bytes = 9;
  else
    need_bytes = 10;
  return need_bytes;
}

/**
 * @brief Encode a integer (up to 64bit) in variable length encoding
 *
 * @param buffer pointer to the destination buffer
 * @param end the end pointer to the destination buffer
 * @param value value to encode
 *
 * @return true - success, false - failed
 */
inline int encode_var_int64(char *buffer, const int64_t bufsiz, int64_t &pos,
                            int64_t value) {
  uint64_t uv = static_cast<uint64_t>(value);
  ASSERT_BUFFER(encoded_length_var_int64(uv));
  while (uv > XE_MAX_V1B) {
    *(buffer + pos++) = static_cast<int8_t>((uv) | 0x80);
    uv >>= 7;
  }
  if (uv <= XE_MAX_V1B) {
    *(buffer + pos++) = static_cast<int8_t>((uv)&0x7f);
  }
  return common::Status::kOk;
}

inline int decode_var_int64(const char *buffer, const int64_t bufsiz,
                            int64_t &pos, int64_t &value) {
  uint64_t uv = 0;
  uint32_t shift = 0;
  int64_t tmp_pos = pos;
  int ret = common::Status::kOk;
  while ((*(buffer + tmp_pos)) & 0x80) {
    if (bufsiz - tmp_pos < 1) {
      ret = common::Status::kNoSpace;
      break;
    }
    uv |= (static_cast<uint64_t>(*(buffer + tmp_pos++)) & 0x7f) << shift;
    shift += 7;
  }
  if (common::Status::kOk == ret) {
    if (bufsiz - tmp_pos < 1) {
      ret = common::Status::kNoSpace;
    } else {
      uv |= ((static_cast<uint64_t>(*(buffer + tmp_pos++)) & 0x7f) << shift);
      value = static_cast<int64_t>(uv);
      pos = tmp_pos;
    }
  }
  return ret;
}

inline int64_t encoded_length_var_int32(int32_t value) {
  uint32_t uv = static_cast<uint64_t>(value);
  int64_t need_bytes = 0;
  if (uv <= XE_MAX_V1B)
    need_bytes = 1;
  else if (uv <= XE_MAX_V2B)
    need_bytes = 2;
  else if (uv <= XE_MAX_V3B)
    need_bytes = 3;
  else if (uv <= XE_MAX_V4B)
    need_bytes = 4;
  else
    need_bytes = 5;
  return need_bytes;
}

/**
 * @brief Encode a integer (up to 32bit) in variable length encoding
 *
 * @param buffer pointer to the destination buffer
 * @param end the end pointer to the destination buffer
 * @param value value to encode
 *
 * @return true - success, false - failed
 */

inline int encode_var_int32(char *buffer, const int64_t bufsiz, int64_t &pos,
                            int32_t value) {
  uint32_t uv = static_cast<uint32_t>(value);
  ASSERT_BUFFER(encoded_length_var_int32(value));
  while (uv > XE_MAX_V1B) {
    *(buffer + pos++) = static_cast<int8_t>((uv) | 0x80);
    uv >>= 7;
  }
  if (uv <= XE_MAX_V1B) {
    *(buffer + pos++) = static_cast<int8_t>((uv)&0x7f);
  }
  return common::Status::kOk;
}

inline int decode_var_int32(const char *buffer, const int64_t bufsiz,
                            int64_t &pos, int32_t &value) {
  uint32_t uv = 0;
  uint32_t shift = 0;
  int ret = common::Status::kOk;
  int64_t tmp_pos = pos;
  while ((*(buffer + tmp_pos)) & 0x80) {
    if (bufsiz - tmp_pos < 1) {
      ret = common::Status::kNoSpace;
      break;
    }
    uv |= (static_cast<uint32_t>(*(buffer + tmp_pos++)) & 0x7f) << shift;
    shift += 7;
  }
  if (common::Status::kOk == ret) {
    if (bufsiz - tmp_pos < 1)
      ret = common::Status::kNoSpace;
    else {
      uv |= (static_cast<uint32_t>(*(buffer + tmp_pos++)) & 0x7f) << shift;
      value = static_cast<int32_t>(uv);
      pos = tmp_pos;
    }
  }
  return ret;
}

inline int64_t encoded_length_var_str(int64_t len) {
  return encoded_length_var_int64(len) + len;
}

/**
 * @brief Encode a buffer as vstr(int64,data,null)
 *
 * @param buffer pointer to the destination buffer
 * @param vbuf pointer to the start of the input buffer
 * @param len length of the input buffer
 */
inline int encode_var_str(char *buffer, const int64_t bufsiz, int64_t &pos,
                          const void *str, int64_t len) {
  int ret = 0;
  ASSERT_BUFFER(encoded_length_var_str(len));
  if (common::Status::kOk == ret) {
    /**
     * even through it's a null string, we can serialize it with
     * lenght 0, and following a '\0'
     */
    ret = encode_var_int64(buffer, bufsiz, pos, len);
    if (common::Status::kOk == ret && len > 0 && nullptr != str) {
      memcpy(buffer + pos, str, len);
      pos += len;
    }
  }
  return ret;
}

inline int encode_var_str(char *buffer, const int64_t bufsiz, int64_t &pos,
                          const common::Slice &slice) {
  return encode_var_str(buffer, bufsiz, pos, slice.data(), slice.size());
}

// return deserialized string pointer to input buffer;
// return nullptr if error occurs;
inline const char *decode_var_str(const char *buffer, const int64_t bufsiz,
                                  int64_t &pos, int64_t &length) {
  const char *str = 0;
  int64_t tmp_pos = pos;

  length = -1;
  if ((nullptr == buffer) || (bufsiz < 0) || (pos < 0)) {
  } else if (decode_var_int64(buffer, bufsiz, tmp_pos, length) !=
             common::Status::kOk) {
  } else if (length >= 0) {
    if (bufsiz - tmp_pos >= length) {
      str = buffer + tmp_pos;
      tmp_pos += length;
      pos = tmp_pos;
    }
  }
  return str;
}

// copy deserialized string into @dest[@length]
inline int decode_var_str(const char *buffer, const int64_t bufsiz,
                          int64_t &pos, char *dest, int64_t &length) {
  int ret = 0;
  int64_t tmp_len = 0;
  int64_t tmp_pos = pos;
  if ((nullptr == buffer) || (bufsiz < 0) || (pos < 0) || (nullptr == dest) ||
      length < 0) {
    ret = common::Status::kNoSpace;
  } else if (decode_var_int64(buffer, bufsiz, tmp_pos, tmp_len) != 0 ||
             tmp_len > bufsiz || tmp_len > length) {
    ret = common::Status::kNoSpace;
  } else if (tmp_len >= 0) {
    if (bufsiz - tmp_pos >= tmp_len) {
      length = tmp_len;
      memcpy(dest, buffer + tmp_pos, tmp_len);
      tmp_pos += tmp_len;
      pos = tmp_pos;
    }
  }
  return ret;
}

// setup @slice's data point to serialized buffer;
// WARNING : @slice does not own the buffer, it will be unavialable
// when @buffer is no longer live.
inline int decode_var_str(const char *buffer, const int64_t bufsiz,
                          int64_t &pos, common::Slice &slice) {
  int64_t length = 0;
  slice.data_ = decode_var_str(buffer, bufsiz, pos, length);
  slice.size_ = static_cast<size_t>(length);
  if (nullptr == slice.data_ || length < 0) {
    return common::Status::kNoSpace;
  }
  return common::Status::kOk;
}

inline int decode_var_str(const char *buffer, const int64_t bufsiz,
                          int64_t &pos, std::string &value) {
  int64_t length = 0;
  const char *data = decode_var_str(buffer, bufsiz, pos, length);
  if (nullptr == data || length < 0) {
    return common::Status::kNoSpace;
  }
  value.assign(data, length);
  return common::Status::kOk;
}

// -----------------------------------------------------------------
// serialize & deserialize template functions;
// -----------------------------------------------------------------

inline int serialize(char *buffer, const int64_t bufsiz, int64_t &pos,
                     int64_t value) {
  return encode_var_int64(buffer, bufsiz, pos, value);
}
inline int serialize(char *buffer, const int64_t bufsiz, int64_t &pos,
                     uint64_t value) {
  return encode_var_int64(buffer, bufsiz, pos, static_cast<int64_t>(value));
}
inline int serialize(char *buffer, const int64_t bufsiz, int64_t &pos,
                     int32_t value) {
  return encode_var_int32(buffer, bufsiz, pos, value);
}
inline int serialize(char *buffer, const int64_t bufsiz, int64_t &pos,
                     uint32_t value) {
  return encode_var_int32(buffer, bufsiz, pos, static_cast<int32_t>(value));
}
inline int serialize(char *buffer, const int64_t bufsiz, int64_t &pos,
                     int16_t value) {
  return encode_fixed_int16(buffer, bufsiz, pos, value);
}
inline int serialize(char *buffer, const int64_t bufsiz, int64_t &pos,
                     uint16_t value) {
  return encode_fixed_int16(buffer, bufsiz, pos, static_cast<int16_t>(value));
}
inline int serialize(char *buffer, const int64_t bufsiz, int64_t &pos,
                     int8_t value) {
  return encode_fixed_int8(buffer, bufsiz, pos, value);
}
inline int serialize(char *buffer, const int64_t bufsiz, int64_t &pos,
                     uint8_t value) {
  return encode_fixed_int8(buffer, bufsiz, pos, static_cast<int8_t>(value));
}
inline int serialize(char *buffer, const int64_t bufsiz, int64_t &pos,
                     const char *value) {
  return encode_var_str(buffer, bufsiz, pos, value, strlen(value));
}
inline int serialize(char *buffer, const int64_t bufsiz, int64_t &pos,
                     const common::Slice &value) {
  return encode_var_str(buffer, bufsiz, pos, value);
}
inline int serialize(char *buffer, const int64_t bufsiz, int64_t &pos,
                     const std::string &value) {
  return encode_var_str(buffer, bufsiz, pos, value.data(), value.size());
}

template <typename T>
int serialize(char *buffer, const int64_t bufsiz, int64_t &pos,
              const T &value) {
  return value.serialize(buffer, bufsiz, pos);
}

template <typename T>
int serialize_x(char *buf, const int64_t buf_len, int64_t &pos, const T &value);

template <typename T, typename... Args>
int serialize_x(char *buf, const int64_t buf_len, int64_t &pos, const T &head, const Args &... rest);

template <typename T>
int deserialize_x(const char *buf, int64_t buf_len, int64_t &pos, T &value);

template <typename T, typename... Args>
int deserialize_x(const char *buf, int64_t buf_len, int64_t &pos, T &head, Args &... rest);

template <typename T>
int get_serialize_size_x(const T &value);

template <typename T, typename... Args>
int get_serialize_size_x(const T &head, const Args &... rest);

template <typename T>
int serialize_v(char *buffer, const int64_t bufsiz, int64_t &pos,
                const autovector<T> &values) {
  int ret = 0;
  if (common::Status::kOk !=
      (ret = encode_var_int64(buffer, bufsiz, pos, values.size()))) {
  } else {
    for (auto &value : values) {
      if (common::Status::kOk !=
          (ret = serialize(buffer, bufsiz, pos, value))) {
        break;
      }
    }
  }
  return ret;
}

template <typename T>
int serialize_v(char *buf, const int64_t buf_len, int64_t &pos, const std::vector<T> &values)
{
  int ret = common::Status::kOk;
  if (common::Status::kOk != (ret = encode_var_int64(buf, buf_len, pos, values.size()))) {
  } else {
    for (auto &value : values) {
      if (common::Status::kOk != (ret = serialize_x(buf, buf_len, pos, value))) {
        break;
      }
    }
  }
  return ret;
}

template <typename T>
int serialize_v(char *buf, const int64_t buf_len, int64_t &pos, const std::unordered_set<T> &values)
{
  int ret = common::Status::kOk;
  if (common::Status::kOk != (ret = encode_var_int64(buf, buf_len, pos, values.size()))) {
  } else {
    for (auto &value : values) {
      if (common::Status::kOk != (ret = serialize(buf, buf_len, pos, value))) {
        break;
      }
    }
  }
  return ret;
}

template <typename T>
int serialize_x(char *buf, const int64_t buf_len, int64_t &pos, const T &value);

template <typename T,typename E>
int serialize_v(char *buf, const int64_t buf_len, int64_t &pos, const std::unordered_map<T,E> &values)
{
  int ret = common::Status::kOk;
  if (common::Status::kOk != (ret = encode_var_int64(buf, buf_len, pos, values.size()))) {
  } else {
    for (auto &value : values) {
      if (common::Status::kOk != (ret = serialize_x(buf, buf_len, pos, value.first))) {
        break;
      }
      if(common::Status::kOk != (ret = serialize_x(buf, buf_len, pos, value.second)) ) {
        break;
      }
      
    }
  }
  return ret;
}

template <typename E, template <typename, typename...> class Container,
          typename... Args>
int serialize_v(char *buffer, const int64_t bufsiz, int64_t &pos,
                const Container<E, Args...> &values) {
  int ret = 0;
  if (common::Status::kOk !=
      (ret = encode_var_int64(buffer, bufsiz, pos, values.size()))) {
  } else {
    for (auto &value : values) {
      if (common::Status::kOk !=
          (ret = serialize(buffer, bufsiz, pos, value))) {
        break;
      }
    }
  }
  return ret;
}

inline int deserialize(const char *buffer, const int64_t bufsiz, int64_t &pos,
                       int64_t &value) {
  return decode_var_int64(buffer, bufsiz, pos, value);
}
inline int deserialize(const char *buffer, const int64_t bufsiz, int64_t &pos,
                       uint64_t &value) {
  return decode_var_int64(buffer, bufsiz, pos,
                          reinterpret_cast<int64_t &>(value));
}
inline int deserialize(const char *buffer, const int64_t bufsiz, int64_t &pos,
                       int32_t &value) {
  return decode_var_int32(buffer, bufsiz, pos, value);
}
inline int deserialize(const char *buffer, const int64_t bufsiz, int64_t &pos,
                       uint32_t &value) {
  return decode_var_int32(buffer, bufsiz, pos,
                          reinterpret_cast<int32_t &>(value));
}
inline int deserialize(const char *buffer, const int64_t bufsiz, int64_t &pos,
                       int16_t &value) {
  return decode_fixed_int16(buffer, bufsiz, pos, value);
}
inline int deserialize(const char *buffer, const int64_t bufsiz, int64_t &pos,
                       uint16_t &value) {
  return decode_fixed_int16(buffer, bufsiz, pos,
                            reinterpret_cast<int16_t &>(value));
}
inline int deserialize(const char *buffer, const int64_t bufsiz, int64_t &pos,
                       int8_t &value) {
  return decode_fixed_int8(buffer, bufsiz, pos, value);
}
inline int deserialize(const char *buffer, const int64_t bufsiz, int64_t &pos,
                       uint8_t &value) {
  return decode_fixed_int8(buffer, bufsiz, pos,
                           reinterpret_cast<int8_t &>(value));
}
// make sure @value has enough space, otherwise it will be segment fault.
inline int deserialize(const char *buffer, const int64_t bufsiz, int64_t &pos,
                       char *value) {
  int64_t length = bufsiz;
  int ret = decode_var_str(buffer, bufsiz, pos, value, length);
  if (common::Status::kOk == ret) {
    value[length] = 0;
  }
  return ret;
}

inline int deserialize(const char *buffer, const int64_t bufsiz, int64_t &pos,
                       common::Slice &value) {
  return decode_var_str(buffer, bufsiz, pos, value);
}

inline int deserialize(const char *buffer, const int64_t bufsiz, int64_t &pos,
                       std::string &value) {
  return decode_var_str(buffer, bufsiz, pos, value);
}

template <typename T>
int deserialize(const char *buffer, const int64_t bufsiz, int64_t &pos,
                T &value) {
  return value.deserialize(buffer, bufsiz, pos);
}

template <typename E, template <typename, typename...> class Container,
          typename... Args>
int deserialize_v(const char *buffer, const int64_t bufsiz, int64_t &pos,
                  Container<E, Args...> &values) {
  int ret = 0;
  int64_t size = 0;
  if (common::Status::kOk !=
      (ret = decode_var_int64(buffer, bufsiz, pos, size))) {
  } else {
    E value;
    for (int64_t i = 0; i < size; ++i) {
      if (common::Status::kOk !=
          (ret = deserialize(buffer, bufsiz, pos, value))) {
        break;
      } else {
        values.push_back(value);
      }
    }
  }
  return ret;
}

template <typename E>
int deserialize_v(const char *buffer, const int64_t bufsiz, int64_t &pos,
                  autovector<E> &values) {
  int ret = 0;
  int64_t size = 0;
  if (common::Status::kOk !=
      (ret = decode_var_int64(buffer, bufsiz, pos, size))) {
  } else {
    E value;
    for (int64_t i = 0; i < size; ++i) {
      if (common::Status::kOk !=
          (ret = deserialize(buffer, bufsiz, pos, value))) {
        break;
      } else {
        values.emplace_back(value);
      }
    }
  }
  return ret;
}

template <typename T>
int deserialize_v(const char *buf, const int64_t buf_len, int64_t &pos, std::vector<T> &values)
{
  int ret = common::Status::kOk;
  int64_t size = 0;
  if (common::Status::kOk != (ret = decode_var_int64(buf, buf_len, pos, size))) {
  } else {
    T value;
    for (int64_t i = 0; i < size; ++i) {
      if (common::Status::kOk != (ret = deserialize_x(buf, buf_len, pos, value))) {
        break;
      } else {
        values.emplace_back(value);
      }
    }
  }
  return ret;
}

template <typename T>
int deserialize_v(const char *buf, const int64_t buf_len, int64_t &pos, std::unordered_set<T> &values)
{
  int ret = common::Status::kOk;
  int64_t size = 0;
  T value;
  if (common::Status::kOk != (ret = decode_var_int64(buf, buf_len, pos, size))) {
  } else {
    for (int64_t i = 0; i < size; ++i)  {
      if (common::Status::kOk != (ret = deserialize(buf, buf_len, pos, value))) {
        break;
      } else {
        values.emplace(value);
      }
    }
  }
  return ret;
}

template <typename T>
int deserialize_x(const char *buf, int64_t buf_len, int64_t &pos, T &value);

template <typename T, typename E>
int deserialize_v(const char *buf, const int64_t buf_len, int64_t &pos, std::unordered_map<T,E> &values)
{
  int ret = common::Status::kOk;
  int64_t size = 0;
  if (common::Status::kOk != (ret = decode_var_int64(buf, buf_len, pos, size))) {
  } else {
    for (int64_t i = 0; i < size; ++i)  {
      T first;
      E second;
      if (common::Status::kOk != (ret = deserialize_x(buf, buf_len, pos, first))) {
        break;
      } else if (common::Status::kOk != (ret = deserialize_x(buf, buf_len, pos, second))) {
        break;
      } else {
        values[first] = second;
      }
    }
  }
  return ret;
}

inline int64_t get_serialize_size(int64_t value) {
  return encoded_length_var_int64(value);
}
inline int64_t get_serialize_size(uint64_t value) {
  return encoded_length_var_int64(static_cast<int64_t>(value));
}
inline int64_t get_serialize_size(int32_t value) {
  return encoded_length_var_int32(value);
}
inline int64_t get_serialize_size(uint32_t value) {
  return encoded_length_var_int32(static_cast<int32_t>(value));
}
inline int64_t get_serialize_size(int16_t value) { return sizeof(value); }
inline int64_t get_serialize_size(uint16_t value) { return sizeof(value); }
inline int64_t get_serialize_size(int8_t value) { return sizeof(value); }
inline int64_t get_serialize_size(uint8_t value) { return sizeof(value); }
inline int64_t get_serialize_size(const char *value) {
  return encoded_length_var_str(nullptr == value ? 0 : strlen(value));
}
inline int64_t get_serialize_size(const common::Slice &value) {
  return encoded_length_var_str(value.size());
}
inline int64_t get_serialize_size(const std::string &value) {
  return encoded_length_var_str(value.size());
}

template <typename T>
int64_t get_serialize_size(const T &value) {
  return value.get_serialize_size();
}

template <typename E>
int get_serialize_v_size(const autovector<E> &values) {
  int64_t ret = get_serialize_size(values.size());
  for (auto &value : values) {
    ret += get_serialize_size(value);
  }
  return ret;
}

template <typename T>
int get_serialize_v_size(const std::vector<T> &values)
{
  int64_t value_size = values.size();
  int64_t ret_size = get_serialize_size(value_size);
  for (auto &value : values) {
    ret_size += get_serialize_size_x(value);
  }
  return ret_size;
}

template <typename T>
int get_serialize_v_size(const std::unordered_set<T> &values)
{
  int64_t size = get_serialize_size(values.size());
  for (auto &value : values) {
    size += get_serialize_size(value);
  }
  return size;
}

template <typename T>
int get_serialize_size_x(const T &value);

template <typename T, typename E>
int get_serialize_v_size(const std::unordered_map<T,E> &values)
{
  int64_t size = get_serialize_size(values.size());
  for (auto &value : values) {
    size += get_serialize_size_x(value.first);
    size += get_serialize_size_x(value.second);
  }
  return size;
}

template <typename E, template <typename, typename...> class Container,
          typename... Args>
int get_serialize_v_size(const Container<E, Args...> &values) {
  int64_t ret = get_serialize_size(values.size());
  for (auto &value : values) {
    ret += get_serialize_size(value);
  }
  return ret;
}

// generic template (variant arguments)
template <typename T, typename... Args>
int serialize(char *buffer, const int64_t bufsiz, int64_t &pos, const T &head,
              const Args &... rest) {
  int ret = 0;
  if (common::Status::kOk != (ret = serialize(buffer, bufsiz, pos, head))) {
  } else if (common::Status::kOk !=
             (ret = serialize(buffer, bufsiz, pos, rest...))) {
  }
  return ret;
}

template <typename T, typename... Args>
int deserialize(const char *buffer, const int64_t bufsiz, int64_t &pos, T &head,
                Args &... rest) {
  int ret = 0;
  if (common::Status::kOk != (ret = deserialize(buffer, bufsiz, pos, head))) {
  } else if (common::Status::kOk !=
             (ret = deserialize(buffer, bufsiz, pos, rest...))) {
  }
  return ret;
}

template <typename T, typename... Args>
int64_t get_serialize_size(const T &head, const Args &... rest) {
  return get_serialize_size(head) + get_serialize_size(rest...);
}

// generic template (variant arguments), for compactiple serialize and deserialize
template <bool> struct ContainerSerializeWrap;
template <>
struct ContainerSerializeWrap<true>
{
  template <typename T>
  int operator()(char *buf, int64_t buf_len, int64_t &pos, const T &t)
  {
    return util::serialize_v(buf, buf_len, pos, t);
  }
};
template <>
struct ContainerSerializeWrap<false>
{
  template <typename T>
  int operator()(char *buf, int64_t buf_len, int64_t &pos, const T &t)
  {
    return util::serialize(buf, buf_len, pos, t);
  }
};

template <bool> struct ContainerDeserializeWrap;
template <>
struct ContainerDeserializeWrap<true>
{
  template <typename T>
  int operator()(const char *buf, int64_t buf_len, int64_t &pos, T &t)
  {
    return util::deserialize_v(buf, buf_len, pos, t);
  }
};

template <>
struct ContainerDeserializeWrap<false>
{
  template <typename T>
  int operator()(const char *buf, int64_t buf_len, int64_t &pos, T &t)
  {
    return util::deserialize(buf, buf_len, pos, t);
  }
};

template <bool> struct ContainerGetSerializeSizeWrap;
template <>
struct ContainerGetSerializeSizeWrap<true>
{
  template <typename T>
  int operator()(const T &t)
  {
    return util::get_serialize_v_size(t);
  }
};
template <>
struct ContainerGetSerializeSizeWrap<false>
{
  template <typename T>
  int operator()(const T &t)
  {
    return util::get_serialize_size(t);
  }
};

template <bool> struct SerializeWrap;
template <>
struct SerializeWrap<true>
{
  template <typename T>
  int operator()(char *buf, int64_t buf_len, int64_t &pos, const T &t)
  {
    return  t.serialize(buf, buf_len, pos);
  }
};
template <>
struct SerializeWrap<false>
{
  template <typename T>
  int operator()(char *buf, int64_t buf_len, int64_t &pos, const T &t)
  {
    typedef ContainerSerializeWrap<IS_CONTAINER(T)> Wrap;
    return Wrap()(buf, buf_len, pos, t);
  }
};

template <bool> struct DeserializeWrap;
template <>
struct DeserializeWrap<true>
{
  template <typename T>
  int operator()(const char *buf, int64_t buf_len, int64_t &pos, T &t)
  {
    return t.deserialize(buf, buf_len, pos);
  }
};
template <>
struct DeserializeWrap<false>
{
  template <typename T>
  int operator()(const char *buf, int64_t buf_len, int64_t &pos, T &t)
  {
    typedef ContainerDeserializeWrap<IS_CONTAINER(T)> Wrap;
    return Wrap()(buf, buf_len, pos, t);
  }
};

template <bool> struct GetSerializeSizeWrap;
template <>
struct GetSerializeSizeWrap<true>
{
  template <typename T>
  int64_t operator()(const T &t)
  {
    return t.get_serialize_size();
  }
};
template <>
struct GetSerializeSizeWrap<false>
{
  template <typename T>
  int64_t operator()(const T &t)
  {
    typedef ContainerGetSerializeSizeWrap<IS_CONTAINER(T)> Wrap;
    return Wrap()(t);
    //return util::get_serialize_size(t);
  }
};

template <typename T>
int serialize_x(char *buf, const int64_t buf_len, int64_t &pos, const T &value)
{
  typedef SerializeWrap<HAS_MEMBER(T, serialize)> Wrap;
  return Wrap()(buf, buf_len, pos, value);
}

template <typename T, typename... Args>
int serialize_x(char *buf, const int64_t buf_len, int64_t &pos, const T &head, const Args &... rest)
{
  int ret = common::Status::kOk;
  typedef SerializeWrap<HAS_MEMBER(T, serialize)> Wrap;
  if (common::Status::kOk != (ret = Wrap()(buf, buf_len, pos, head))) {
  } else if (common::Status::kOk != (ret = serialize_x(buf, buf_len, pos, rest...))) {
  }
  return ret;
}

template <typename T>
int deserialize_x(const char *buf, int64_t buf_len, int64_t &pos, T &value)
{
  typedef DeserializeWrap<HAS_MEMBER(T, deserialize)> Wrap;
  return Wrap()(buf, buf_len, pos, value);
}
template <typename T, typename... Args>
int deserialize_x(const char *buf, int64_t buf_len, int64_t &pos, T &head, Args &... rest)
{
  int ret = common::Status::kOk;
  typedef DeserializeWrap<HAS_MEMBER(T, deserialize)> Wrap;
  if ((pos < buf_len) && common::Status::kOk != (ret = Wrap()(buf, buf_len, pos, head))) {
  } else if ((pos < buf_len) && common::Status::kOk != (ret = deserialize_x(buf, buf_len, pos, rest...))) {
  }
  return ret;
}

template <typename T>
int get_serialize_size_x(const T &value)
{
  typedef GetSerializeSizeWrap<HAS_MEMBER(T, get_serialize_size)> Wrap;
  return Wrap()(value);
}
template <typename T, typename... Args>
int get_serialize_size_x(const T &head, const Args &... rest)
{
  typedef GetSerializeSizeWrap<HAS_MEMBER(T, get_serialize_size)> Wrap;
  return Wrap()(head) + get_serialize_size_x(rest...);
}
} /* util */
} /* smartengine*/

#endif  // SMARTENGINE_SERIALIZATION_H_
