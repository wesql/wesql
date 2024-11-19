/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef IS_LOGGER_V2_H_
#define IS_LOGGER_V2_H_

#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <string>
#include <string.h>
#include <pthread.h>
#include <atomic>
#include "util/to_string.h"

namespace smartengine
{
namespace logger
{
enum InfoLogLevel : unsigned char {
  DEBUG_LEVEL = 0,
  INFO_LEVEL,
  WARN_LEVEL,
  ERROR_LEVEL,
  SYSTEM_LEVEL, // system-level logs should always be printed
  NUM_INFO_LOG_LEVELS,
};

enum InfoLogModule : long {
  SE_MOD = 0,
  SE_COMPACTION_MOD,
  SE_FLUSH_MOD,
  SE_HANDLER_MOD,
  COMMON_MOD,
  NUM_INFO_LOG_MODULES,
};

class Logger
{
private:
  static const int64_t MAX_LOG_SIZE = 16 * 1024; //16KB
  static const int64_t DEFAULT_LOG_FILE_SIZE = 256 * 1024 * 1024; //256MB
  static const int64_t MAX_LOG_FILE_NAME_SIZE = 256;
  static const mode_t LOG_FILE_MODE = 0644;
  static const int LOG_FILE_SWITCH_CHECK_RATE = 1;
  struct LogLocation
  {
    const char *file_name_;
    const char *function_name_;
    const int32_t line_num_;

    LogLocation(const char *file_name, const char *function_name, const int32_t line_num)
        : file_name_(file_name),
          function_name_(function_name),
          line_num_(line_num)
    {
    }
    ~LogLocation() {}
    inline bool is_valid() const
    {
      return nullptr != file_name_ && nullptr != function_name_ && line_num_ >= 0;
    }
  };
  struct LogBuffer
  {
    int64_t pos_;
    char buf_[MAX_LOG_SIZE];

    LogBuffer() : pos_(0)
    {
      buf_[0] = '\0';
    }
    ~LogBuffer() {}
    inline char *current() { return buf_ + pos_; }
    inline int64_t remain() { return MAX_LOG_SIZE - pos_; }
    inline void advance(int64_t len) 
    {
      if(len >= 0) {
        if (pos_ + len > MAX_LOG_SIZE) {
          pos_ = MAX_LOG_SIZE;
        } else {
          pos_ += len;
        }
      }
    }
    inline void set_pos(int64_t pos) { pos_ = pos; }
    inline bool is_valid()
    {
      return pos_ >= 0;
    }
  };
  struct log_file_entry {
    std::string log_file_path;
    std::string log_file_name;
  };
public:
  ~Logger()
  {
    pthread_mutex_destroy(&file_mutex_);
  }

  static inline Logger &get_log();
  int init(const char *file_path,
           InfoLogLevel log_level = INFO_LEVEL,
           int64_t max_log_size = DEFAULT_LOG_FILE_SIZE);
  // init with existing opened fd
  int init(int fd, InfoLogLevel log_level = INFO_LEVEL);
  int reinit(int fd, InfoLogLevel log_level = INFO_LEVEL);
  inline void set_max_log_size(int64_t max_log_size) { max_log_size_ = max_log_size; }
  void set_log_level(InfoLogLevel log_level);

  void set_log_level_mod(InfoLogModule log_mod, InfoLogLevel log_level);
  InfoLogLevel get_log_level_mod(InfoLogModule log_mod) {
    return log_level_mod_[log_mod];
  }
  bool need_print(int32_t level)
  {
    return level >= log_level_;
  }
  bool need_print_mod(InfoLogModule log_mod, int32_t log_level);

  /*
   * print log by kv form.
   * Step1: print log header
   * Step2: print info_string
   * Step3: print kv list
   * Step4: print log tail
   * Step5: write log to log file
   */
  template <typename ...Args>
  void print_log_kv(const char *mod_name,
                    const int32_t level,
                    const char *file_name,
                    const char *function_name,
                    const int32_t line_num,
                    const char *info_string,
                    const Args &... args);
  void print_log_kv(const char *mod_name,
                    const int32_t level,
                    const char *file_name,
                    const char *function_name,
                    const int32_t line_num,
                    const char *info_string);

  /*
   * print log by fmt form
   * Step1: print log header
   * Step2: print fmt string
   * Step3: write log to log file
   */
  template <typename ...Args>
  void print_log_fmt(const char *mod_name,
                     const int32_t level,
                     const char *file_name,
                     const char *function_name,
                     const int32_t line_num,
                     const char *fmt,
                     const Args &... args);
  /*
   * print log for compactibility smartengine's log
   */
  void print_log_fmt(const char *mod_name,
                     const int32_t level,
                     const char *file_name,
                     const char *function_name,
                     const int32_t line_num,
                     const char *fmt,
                     va_list ap) __attribute__((format(printf, 7, 0)));
  bool is_inited() const { return is_inited_; }
#ifndef NDEBUG 
  void reset() { is_inited_ = false; } 
#endif
private:
  Logger() : is_inited_(false),
             fd_(fileno(stderr)),
             log_level_(InfoLogLevel::DEBUG_LEVEL),
             reserved_days_(-1),
             reserved_file_num_(-1),
             curr_log_size_(0),
             max_log_size_(0),
             force_switch_log_(true)
  {
    pthread_mutex_init(&file_mutex_, nullptr);
    memset(file_path_, 0, MAX_LOG_FILE_NAME_SIZE);
    for (int idx = 0; idx < InfoLogModule::NUM_INFO_LOG_MODULES; idx++) {
      log_level_mod_[idx] = log_level_;
    }
  }
  /*
   * print log common header
   * [year-month-day hour:minutes:seconds] LOG_LEVEL [thread_id] function_name(file_name:line_num)
   * [1990-01-01 00:00:00] INFO [3456] print_log_header(logger_v2.h:120)
   */
  void print_log_header(const char *mod_name,
                        const int32_t level,
                        const LogLocation &log_location,
                        LogBuffer &log_buffer);
  /*
   * add '\n' to log tail
   */
  void print_log_tail(LogBuffer &log_buffer);
  inline void print_info_string(const char *info_string, LogBuffer &log_buffer)
  {
    int64_t len = snprintf(log_buffer.current(), log_buffer.remain(), "%s, ", info_string);
    log_buffer.advance(len);
  }

  void write_log(LogBuffer &log_buffer);
  void swith_log_file();
  inline bool file_exist(const char *file_path);
  void clear_log_file();
  static bool log_file_time_comp_by_name(log_file_entry &entry_a, log_file_entry &entry_b);

public:
  int get_log_reserved_days() {
    return reserved_days_;
  }
  int get_log_reserved_file_num() {
    return reserved_file_num_;
  }
  int64_t get_log_file_size() {
    return max_log_size_;
  }
  void set_log_reserved_days(int days) {
    pthread_mutex_lock(&file_mutex_);
    reserved_days_ = days;
    pthread_mutex_unlock(&file_mutex_); 
  }
  void set_log_reserved_file_num(int num) {
    pthread_mutex_lock(&file_mutex_);
    reserved_file_num_ = num;
    pthread_mutex_unlock(&file_mutex_); 
  }

private:
  bool external_log_;
  bool is_inited_;
  static const char *const err_str_[];
  int32_t fd_;
  char file_path_[MAX_LOG_FILE_NAME_SIZE];
  InfoLogLevel log_level_;
  InfoLogLevel log_level_mod_[NUM_INFO_LOG_MODULES];
  int reserved_days_; //log file earlier than this will be deleted, -1 means disable this feature, set by sql client.
  int reserved_file_num_; //keep reserved_file_num_ log files. double threshold control, -1 means disable, set by sql client.
  static int num_log_file_switch_;
  std::atomic<int64_t> curr_log_size_;
  int64_t max_log_size_;
  bool force_switch_log_; //force to switch log file when restart
  pthread_mutex_t file_mutex_;
};

inline Logger& Logger::get_log()
{
  static Logger log;
  return log;
}

template <typename ...Args>
void Logger::print_log_kv(const char *mod_name,
                          const int32_t level,
                          const char *file_name,
                          const char *function_name,
                          const int32_t line_num,
                          const char *info_string,
                          const Args &...args)
{
  LogLocation log_location(file_name, function_name, line_num);
  if (nullptr != mod_name
      && level >= InfoLogLevel::DEBUG_LEVEL
      && level < InfoLogLevel::NUM_INFO_LOG_LEVELS
      && log_location.is_valid()
      && nullptr != info_string)  {
    LogBuffer log_buffer;
    print_log_header(mod_name, level, log_location, log_buffer);
    print_info_string(info_string, log_buffer);
    util::databuff_print_log_kv_list(log_buffer.buf_, MAX_LOG_SIZE, log_buffer.pos_, args...) ;
    print_log_tail(log_buffer);
    write_log(log_buffer);
  }
}

template <typename ...Args>
void Logger::print_log_fmt(const char *mod_name,
                           const int32_t level,
                           const char *file_name,
                           const char *function_name,
                           const int32_t line_num,
                           const char *fmt,
                           const Args &...args)
{
  LogLocation log_location(file_name, function_name, line_num);
  if (nullptr != mod_name
      && level >= InfoLogLevel::DEBUG_LEVEL
      && level < InfoLogLevel::NUM_INFO_LOG_LEVELS
      && log_location.is_valid()
      && nullptr != fmt)  {
    LogBuffer log_buffer;
    print_log_header(mod_name, level, log_location, log_buffer);
    util::databuff_printf(log_buffer.buf_, MAX_LOG_SIZE, log_buffer.pos_, fmt, args...);
    print_log_tail(log_buffer);
    write_log(log_buffer);
  }
}

} // namespace logger
} // namespace smartengine

#endif
