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

#include <vector>
#ifndef IS_PRINT_UTIL_H_
#define IS_PRINT_UTIL_H_ 1
#include <stdint.h>
#include <atomic>
#include <string>

namespace smartengine {
namespace util {

struct MyVoid
{
};

int32_t hex_to_str(const void *in_data, const int32_t data_length, void *buff,
                   const int32_t buff_size);
int32_t str_to_hex(const void *in_data, const int32_t data_length, void *buff,
                   const int32_t buff_size);
inline int char_to_hex(const char &input, char &output);
int unhex(const char *input, int64_t input_len, char *output,
          int64_t output_len, int64_t &write_len);

////////////////////////////////////////////////////////////////
// databuff stuff
////////////////////////////////////////////////////////////////

int databuff_printf(char *buf, const int64_t buf_len, int64_t &pos,
                    const char *fmt, ...) __attribute__((format(printf, 4, 5)));

/// print object with to_string members
template <class T>
void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                        const T &obj) {
  //assert(pos >= 0);
  pos += obj.to_string(buf + pos, buf_len - pos);
}
template<class T>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const std::atomic<T> &obj) {
  databuff_print_obj(buf, buf_len, pos, obj.load());
}
template <class T>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                        T *obj) {
  databuff_print_obj(buf, buf_len, pos, *obj);
}
template <class T>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                        const T *obj) {
  databuff_print_obj(buf, buf_len, pos, *obj);
}
template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const MyVoid *obj) {
  databuff_printf(buf, buf_len, pos, "%p", obj);
}
template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const uint64_t &obj) {
  databuff_printf(buf, buf_len, pos, "%lu", obj);
}
template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const int64_t &obj) {
  databuff_printf(buf, buf_len, pos, "%ld", obj);
}
template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const uint32_t &obj) {
  databuff_printf(buf, buf_len, pos, "%u", obj);
}
template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const int32_t &obj) {
  databuff_printf(buf, buf_len, pos, "%d", obj);
}
template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const uint16_t &obj) {
  databuff_printf(buf, buf_len, pos, "%u", obj);
}
template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const int16_t &obj) {
  databuff_printf(buf, buf_len, pos, "%d", obj);
}
template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const uint8_t &obj) {
  databuff_printf(buf, buf_len, pos, "%hhu", obj);
}
template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const int8_t &obj) {
  databuff_printf(buf, buf_len, pos, "%d", obj);
}
template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const float &obj) {
  databuff_printf(buf, buf_len, pos, "%f", obj);
}
template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const double &obj) {
  databuff_printf(buf, buf_len, pos, "%.12f", obj);
}
template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const bool &obj) {
  databuff_printf(buf, buf_len, pos, "%s", obj ? "true" : "false");
}
template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const char &obj) {
  databuff_printf(buf, buf_len, pos, "%c", obj);
}
template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const char *const &obj) {
  if (nullptr == obj) {
    databuff_printf(buf, buf_len, pos, "null");
  } else {
    databuff_printf(buf, buf_len, pos, "\"%s\"", obj);
  }
}
template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const char *obj) {
  if (nullptr == obj) {
    databuff_printf(buf, buf_len, pos, "null");
  } else {
    databuff_printf(buf, buf_len, pos, "\"%s\"", obj);
  }
}

template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               char *obj) {
  if (nullptr == obj) {
    databuff_printf(buf, buf_len, pos, "null");
  } else {
    databuff_printf(buf, buf_len, pos, "\"%s\"", obj);
  }
}

template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const std::string &obj)
{
  databuff_printf(buf, buf_len, pos, "\"%s\"", obj.c_str());
}

template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const std::string_view &obj)
{
  databuff_printf(buf, buf_len, pos, "\"%s\"", std::string(obj).c_str());
}

template <class T>
void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const std::vector<T> &obj_vec)
{
  for (uint32_t i = 0; i < obj_vec.size(); ++i) {
    databuff_printf(buf, buf_len, pos, "[%u]", i);
    databuff_print_obj(buf, buf_len, pos, obj_vec[i]);
    databuff_printf(buf, buf_len, pos, ",");
  }
}
/*
template <>
inline void databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const common::Slice &obj) {
  databuff_printf(buf, buf_len, pos, "\"%.*s\"", (int32_t)obj.size(),
                  obj.data());
}
*/

/// print JSON-style key-value pair
template <class T>
void databuff_print_json_kv(char *buf, const int64_t buf_len, int64_t &pos,
                            const char *key, const T &obj) {
  databuff_printf(buf, buf_len, pos, "\"%s\":", key);
  databuff_print_obj(buf, buf_len, pos, obj);
}
/// print array of objects
template <class T>
void databuff_print_obj_array(char *buf, const int64_t buf_len, int64_t &pos,
                              const T *obj, const int64_t size) {
  databuff_printf(buf, buf_len, pos, "[");
  for (int64_t i = 0; i < size; ++i) {
    databuff_print_obj(buf, buf_len, pos, obj[i]);
    if (i != size - 1) databuff_printf(buf, buf_len, pos, ", ");
  }
  databuff_printf(buf, buf_len, pos, "]");
}
/// Utility tempalte class to adapt C array to an object, which can be used with
/// databuff_print_obj
template <typename T>
struct ArrayWrap {
  ArrayWrap(const T *objs, const int64_t num) : objs_(objs), num_(num){}
  int64_t to_string(char *buf, const int64_t buf_len) const {
    int64_t pos = 0;
    databuff_print_obj_array(buf, buf_len, pos, objs_, num_);
    return pos;
  }

 private:
  const T *objs_;
  int64_t num_;
};

template <typename T>
void databuff_print_json_wrapped_kv(char *buf, const int64_t buf_len,
                                    int64_t &pos, const char *key,
                                    const T &obj) {
  databuff_printf(buf, buf_len, pos, "{");
  databuff_print_json_kv(buf, buf_len, pos, key, obj);
  databuff_printf(buf, buf_len, pos, "}");
}

template <typename T>
void databuff_print_kv_list(char *buffer, const int64_t bufsiz, int64_t &pos,
                            const char *key, const T &item) {
  databuff_print_json_wrapped_kv(buffer, bufsiz, pos, key, item);
}

template <typename T, typename... Args>
void databuff_print_kv_list(char *buffer, const int64_t bufsiz, int64_t &pos,
                            const char *key, const T &item, Args &... rest) {
  databuff_print_json_wrapped_kv(buffer, bufsiz, pos, key, item);
  databuff_printf(buffer, bufsiz, pos, ",");
  databuff_print_kv_list(buffer, bufsiz, pos, rest...);
}

template <typename T>
void databuff_print_log_kv_list(char *buf, const int64_t buf_len, int64_t &pos,
                                const char *key, const T &value)
{
  databuff_printf(buf, buf_len, pos, "%s", key);
  databuff_printf(buf, buf_len, pos, "%s", "=");
  databuff_print_obj(buf, buf_len, pos, value);
}

template <typename T, typename... Args>
void databuff_print_log_kv_list(char *buf, const int64_t buf_len, int64_t &pos,
                                const char *key, const T &value, Args &... rest)
{
  databuff_print_log_kv_list(buf, buf_len, pos, key, value);
  databuff_printf(buf, buf_len, pos, ", ");
  databuff_print_log_kv_list(buf, buf_len, pos, rest...);
}

/**
template <typename T, int64_t BUFFER_NUM = 5>
const char *to_cstring(const T &obj) {
  static const int64_t BUFFER_SIZE = 1024;
  //static __thread char buffers[BUFFER_NUM][BUFFER_SIZE];
  static __thread char* buffers = new char[BUFFER_NUM * BUFFER_SIZE];
  static __thread uint64_t i = 0;
  //char *buffer = buffers[i++ % BUFFER_NUM];
  char *buffer = &buffers[(i++)*BUFFER_SIZE];
  obj.to_string(buffer, BUFFER_SIZE);
  return buffer;
}
*/
}  // end namespace util
}  // end namespace smartengine

#define DECLARE_TO_STRING() \
public: \
  int64_t to_string(char *buf, const int64_t buf_len) const;

#define DECLARE_VIRTUAL_TO_STRING() \
public: \
  virtual int64_t to_string(char *buf, const int64_t buf_len) const;

#define DECLARE_OVERRIDE_TO_STRING() \
public: \
  virtual int64_t to_string(char *buf, const int64_t buf_len) const override;

#define DEFINE_TO_STRING(clz, args...) \
  int64_t clz::to_string(char *buf, const int64_t buf_len) const { \
    int64_t pos = 0;                                               \
    util::databuff_printf(buf, buf_len, pos, "{");                 \
    util::databuff_print_kv_list(buf, buf_len, pos, ##args);  \
    util::databuff_printf(buf, buf_len, pos, "}");                 \
    return pos;                                                    \
  }

#define DECLARE_AND_DEFINE_TO_STRING(args...)                          \
public: \
  int64_t to_string(char *buf, const int64_t buf_len) const {      \
    int64_t pos = 0;                                               \
    util::databuff_printf(buf, buf_len, pos, "{");                 \
    util::databuff_print_kv_list(buf, buf_len, pos, ##args);  \
    util::databuff_printf(buf, buf_len, pos, "}");                 \
    return pos;                                                    \
  }

#define KV(obj) #obj, obj
#define KV_(obj) #obj, obj##_
#define KVE(obj) #obj, ((const uint32_t&)(obj))
#define KVE_(obj) #obj, ((const unsigned int &)(obj##_))
#define KVP(obj) #obj, (const util::MyVoid*&)obj
#define KVP_(obj) #obj, (const util::MyVoid*&)obj##_
#define VP(obj) (const util::MyVoid*&)obj
#define VP_(obj) (const util::MyVoid*&)obj##_

#endif /* IS_PRINT_UTIL_H_ */
