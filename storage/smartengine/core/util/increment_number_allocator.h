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

#ifndef SMARTENGINE_INCLUDE_INCREMENT_NUMBER_GENERATOR_H_
#define SMARTENGINE_INCLUDE_INCREMENT_NUMBER_GENERATOR_H_

#include <atomic>

namespace smartengine
{
namespace util
{
#define DEFINE_INCREMENT_NUMBER_ALLOCATOR(generator_name) \
  class generator_name##Allocator \
  { \
  public: \
    static generator_name##Allocator &get_instance() \
    { \
      static generator_name##Allocator allocator; \
      return allocator; \
    } \
  public: \
    inline int64_t alloc() \
    { \
      return next_number_++;  \
    } \
    inline void set(int64_t next_number) \
    { \
      next_number_.store(next_number);  \
    } \
    inline int64_t get() \
    { \
      return next_number_.load(); \
    } \
   \
  private:  \
    generator_name##Allocator() : next_number_(0) \
    { \
    } \
    ~generator_name##Allocator() {} \
    \
  private:  \
    std::atomic<int64_t> next_number_;  \
  };
  
DEFINE_INCREMENT_NUMBER_ALLOCATOR(Test)
DEFINE_INCREMENT_NUMBER_ALLOCATOR(UniqueId)
DEFINE_INCREMENT_NUMBER_ALLOCATOR(FileNumber)

} //namespace util
} //namespace smartengine

#endif
