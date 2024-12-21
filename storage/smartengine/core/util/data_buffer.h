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

#pragma once

#include "logger/log_module.h"
#include "util/status.h"

namespace smartengine {
namespace util {
class BufferHolder {
public:
  BufferHolder() : data_(nullptr),
                   capacity_(0),
                   pos_(0)
  {}

  BufferHolder(char *buf, int64_t buf_length, int64_t pos)
      : data_(buf),
        capacity_(buf_length),
        pos_(pos)
  {}

  ~BufferHolder()
  {}

  bool is_valid() const
  {
    return (nullptr != data_) && (capacity_ > 0) && (pos_ >= 0) && (pos_ <= capacity_);
  }

  void assign(char *buf, int64_t buf_length, int64_t pos)
  {
    data_ = buf;
    capacity_ = buf_length;
    pos_ = pos;
    assert(is_valid());
  }

  void reuse()
  {
    pos_ = 0;
  }

  void advance(int64_t size)
  {
    assert((pos_ + size) <= capacity_);
    pos_ += size;
  }

  inline char *data()
  {
    return data_;
  }

  inline int64_t size() const
  {
    return pos_; 
  }

  inline int64_t pos() const
  {
    return pos_; 
  }

  inline char *current() 
  {
    return data_ + pos_;
  }

  inline int64_t remain() const
  {
    return (capacity_ - pos_);
  }

  inline int64_t capacity() const
  {
    return capacity_;
  }

protected:
  char *data_;
  int64_t capacity_;
  int64_t pos_;
};

class BufferWriter : public BufferHolder {
public:
  BufferWriter() : BufferHolder()
  {}

  BufferWriter(char *buf, const int64_t buf_length, const int64_t pos)
      : BufferHolder(buf, buf_length, pos)
  {}

  virtual ~BufferWriter()
  {}

  int write(common::Slice data)
  {
    return write(data.data(), data.size());
  }

  int write(const char *buf, const int64_t size)
  {
    int ret = common::Status::kOk;

    if (remain() >= size) {
      do_write(buf, size);
    } else if (SUCCED(expand(size))) {
      do_write(buf, size);
    }

    return ret;
  }

  template <typename T>
  int write_pod(const T &value)
  {
    int ret = common::Status::kOk;
    int64_t size = static_cast<int64_t>(sizeof(value));

    if (remain() >= size) {
      do_write(value);
    } else if (SUCCED(expand(size))) {
      do_write(value);
    }

    return ret;
  }

protected:
  virtual int expand(const int64_t size)
  {
    UNUSED(size) ;
    return common::Status::kNoSpace;
  }

private:
  void do_write(const char *buf, const int64_t size)
  {
    memcpy(data_ + pos_, buf, size);
    advance(size);
  }

  template <typename T>
  void do_write(const T &value)
  {
    *((T *)(data_ + pos_)) = value;
    advance(sizeof(value));
  }
};

class AutoBufferWriter : public BufferWriter {
public:
  AutoBufferWriter()
      : BufferWriter(),
        mod_id_(memory::ModId::kDefaultMod),
        aligned_size_(0)
  {}

  AutoBufferWriter(int mod_id)
      : BufferWriter(),
        mod_id_(mod_id),
        aligned_size_(0)
  {}

  AutoBufferWriter(int mod_id, int64_t aligned_size)
      : BufferWriter(),
        mod_id_(mod_id),
        aligned_size_(aligned_size)
  {}

  virtual ~AutoBufferWriter() override
  {
    free();
  }

  int reserve(const int64_t size)
  {
    int ret = common::Status::kOk;
    int expected_size = pos_ + size;
    char *new_data = nullptr;

    if (expected_size <= 0 || capacity_ >= expected_size) {
      //do nothing
    } else if (IS_NULL(new_data = alloc(expected_size))) {
      ret = common::Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for new data", K(ret), K(size));
    } else {
      if (nullptr != data_) {
        memcpy(new_data, data_, pos_);
        free();
      }
      data_ = new_data;
      capacity_ = expected_size;
    }

    return ret;
  }

protected:
  virtual int expand(const int64_t size) override
  {
    int64_t new_size = std::max(capacity_ * 2, size + capacity_);
    return reserve(new_size);
  }

private:
  char *alloc(const int64_t size)
  {
    char *buf = nullptr;
    if (0 != aligned_size_) {
      buf = reinterpret_cast<char *>(memory::base_memalign(aligned_size_, size, mod_id_));
    } else {
      buf = reinterpret_cast<char *>(memory::base_malloc(size, mod_id_));
    }

    return buf;
  }

  void free()
  {
    if (nullptr != data_) {
      if (0 != aligned_size_) {
        memory::base_memalign_free(data_);
        data_ = nullptr;
      } else {
        memory::base_free(data_);
        data_ = nullptr;
      }
    }
  }

private:
  int mod_id_;
  int64_t aligned_size_;
};

class BufferReader : public BufferHolder {
public:
  BufferReader() : BufferHolder()
  {}

  BufferReader(char *buf, int64_t buf_size, int64_t pos)
      : BufferHolder(buf, buf_size, pos)
  {}

  virtual ~BufferReader()
  {}

  template <typename T>
  int read_pod(T &value)
  {
    int ret = common::Status::kOk;
    int64_t size = static_cast<int64_t>(sizeof(value));

    if (remain() < size) {
      ret = common::Status::kNoSpace;
      SE_LOG(WARN, "there hasn't enough space", K(ret), "remain_size", remain(), K(size));
    } else {
      value = *(reinterpret_cast<T *>(data_ + pos_));
      advance(size);
    }

    return ret;
  }

  int read(char *&buf, const int64_t size)
  {
    int ret = common::Status::kOk;

    if (remain() < size) {
      ret = common::Status::kNoSpace;
      SE_LOG(WARN, "there hasn't enough space", K(ret), "remain_size", remain(), K(size));
    } else {
      buf = data_ + pos_;
      advance(size);
    }

    return ret;
  }
};

} // namespace util
} // namespace smartengine