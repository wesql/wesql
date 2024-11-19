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
#include <algorithm>
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <unistd.h>
#include <vector>
#include <assert.h>
#include "logger.h"

#define GETTID() static_cast<int64_t>(syscall(__NR_gettid))
namespace smartengine
{
namespace logger
{
const char *const Logger::err_str_[] = {"DEBUG", "INFO", "WARN", "ERROR", "SYSTEM"};
int Logger::num_log_file_switch_ = 0;

void Logger::set_log_level(InfoLogLevel log_level)
{
  //SE_LOG(INFO, "set log level", K(log_level));
  // BACKTRACE(INFO, "set log level");
  log_level_ = log_level;
  for (int idx = 0; idx < InfoLogModule::NUM_INFO_LOG_MODULES; idx++) {
    log_level_mod_[idx] = log_level_;
  }
}

void Logger::set_log_level_mod (InfoLogModule log_mod, InfoLogLevel log_level) {
  if (log_mod >= 0 && log_mod < InfoLogModule::NUM_INFO_LOG_MODULES) {
    if (log_level < InfoLogLevel::NUM_INFO_LOG_LEVELS) {
      log_level_mod_[log_mod] = log_level;
    }
  }
}

bool Logger::need_print_mod(InfoLogModule log_mod, int32_t log_level){
  // TODO check the log_mod correctness
  if (log_mod >= 0 && log_mod < InfoLogModule::NUM_INFO_LOG_MODULES) {
    return log_level >= log_level_mod_[log_mod];
  } else {
    return false;
  }
}

int Logger::init(const char *file_path, InfoLogLevel log_level, int64_t max_log_size)
{
  int ret = 0;
  int32_t tmp_fd = 0;
  assert(!is_inited_);

  if (nullptr == file_path
      || (log_level >= NUM_INFO_LOG_LEVELS)
      || max_log_size <= 0) {
    ret = -1;
  } else {
    memcpy(file_path_, file_path, strlen(file_path));
    log_level_ = log_level; 
    max_log_size_ = max_log_size;
    if (file_exist(file_path_)) {
      swith_log_file();
    } else {
      if (-1 == (tmp_fd = open(file_path, O_RDWR | O_CREAT | O_APPEND | O_LARGEFILE, LOG_FILE_MODE))) {
        fd_ = -1;
        ret = -1;
      } else {
        //dup2(tmp_fd, fileno(stdout));
        //dup2(tmp_fd, fileno(stderr));
        /*
        if (tmp_fd != fd_) {
          close(tmp_fd);
        }
        */
        fd_ = tmp_fd;
      }
    }
    if (fd_ >= 0) {
      for (int idx = 0; idx < InfoLogModule::NUM_INFO_LOG_MODULES; idx++) {
        log_level_mod_[idx] = log_level_;
      }
      is_inited_ = true;
      force_switch_log_ = false;
    }
  }
  return ret;
}

// init with existing opened fd
int Logger::init(int fd, InfoLogLevel log_level/* = INFO_LEVEL*/)
{
  assert(!is_inited_);

  int tmp_fd;
  if ((fd < 0) || (tmp_fd = dup(fd)) <0 || (log_level >= NUM_INFO_LOG_LEVELS)) {
    return -1;
  }
  log_level_ = log_level;
  for (int idx = 0; idx < InfoLogModule::NUM_INFO_LOG_MODULES; idx++) {
    log_level_mod_[idx] = log_level_;
  }
  fd_ = tmp_fd;
  external_log_ = true;
  is_inited_ = true;
  return 0;
}

int Logger::reinit(int fd, InfoLogLevel log_level/* = INFO_LEVEL*/)
{
  assert(is_inited_);

  if (fd_ >= 0) {
    fsync(fd_);
    close(fd_);
  }
  is_inited_ = false;
  return init(fd, log_level);
}

void Logger::print_log_kv(const char *mod_name,
                          const int32_t level,
                          const char *file_name,
                          const char *function_name,
                          const int32_t line_num,
                          const char *info_string)
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
    print_log_tail(log_buffer);
    write_log(log_buffer);
  }
}

void Logger::print_log_fmt(const char *mod_name,
                           const int32_t level,
                           const char *file_name,
                           const char *function_name,
                           const int32_t line_num,
                           const char *fmt,
                           va_list ap)
{
  LogLocation log_location(file_name, function_name, line_num);
  int64_t len = 0;
  if (nullptr != mod_name
      && level >= InfoLogLevel::DEBUG_LEVEL
      && level < InfoLogLevel::NUM_INFO_LOG_LEVELS
      && log_location.is_valid()
      && nullptr != fmt) {
    LogBuffer log_buffer;
    print_log_header(mod_name, level, log_location, log_buffer);
    len = vsnprintf(log_buffer.current(), log_buffer.remain(), fmt, ap);
    log_buffer.advance(len);
    print_log_tail(log_buffer);
    write_log(log_buffer);
  }
}

void Logger::print_log_header(const char *mod_name,
                              const int32_t level,
                              const LogLocation &log_location,
                              LogBuffer &log_buffer)
{
  //for performce，skip params check, caller should guarantee the params valid
  //remove the prefix dir
  const char *file_name = strrchr(log_location.file_name_, '/');
  file_name = (nullptr != file_name) ? (file_name + 1) : log_location.file_name_;
  //get current time, and transform to 1990-01-01 00:00:00
  struct timeval tv;
  if (0 == gettimeofday(&tv, nullptr)) {
    struct tm time;
    if (nullptr != (::localtime_r(const_cast<const time_t *>(&tv.tv_sec), &time))) {
      int64_t len = snprintf(log_buffer.current(), log_buffer.remain(),
                             "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] %-6s [%ld] %s %s(%s:%d) ",
                             time.tm_year + 1900, time.tm_mon + 1, time.tm_mday, time.tm_hour, time.tm_min, time.tm_sec, tv.tv_usec,
                             err_str_[level], GETTID(), mod_name, log_location.function_name_, file_name, log_location.line_num_);
      log_buffer.advance(len);
    }
  }
}

void Logger::print_log_tail(LogBuffer &log_buffer)
{
  if (log_buffer.is_valid()) {
    if (log_buffer.pos_ < MAX_LOG_SIZE) {
      if ('\n' != log_buffer.buf_[log_buffer.pos_ - 1]) {
        log_buffer.buf_[log_buffer.pos_] = '\n';
        log_buffer.advance(1);
      }
    } else {
      if ('\n' != log_buffer.buf_[MAX_LOG_SIZE - 1]) {
        log_buffer.buf_[MAX_LOG_SIZE - 1] = '\n';
        log_buffer.set_pos(MAX_LOG_SIZE);
      }
    }
  }
}

void Logger::write_log(LogBuffer &log_buffer)
{
  if (-1 != fd_) {
    if (external_log_) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
      write(fd_, log_buffer.buf_, log_buffer.pos_);
#pragma GCC diagnostic pop
      // skip rotation for external log
      return;
    }
    ssize_t log_size = write(fd_, log_buffer.buf_, log_buffer.pos_);
    if (log_size > 0 && log_size == log_buffer.pos_) {
      curr_log_size_ += log_size;
    }
    if (fd_ != fileno(stdout) && fd_ != fileno(stderr) && curr_log_size_ > max_log_size_) {
      swith_log_file();
    }
  }
}

void Logger::swith_log_file()
{
  if (0 == pthread_mutex_trylock(&file_mutex_)) {
    //double check, log may have been switched by other thread
    if (curr_log_size_ > max_log_size_ || force_switch_log_) {
      char old_file_path[MAX_LOG_FILE_NAME_SIZE];
      memset(old_file_path, 0, MAX_LOG_FILE_NAME_SIZE);
      time_t t = 0;
      time(&t);
      struct tm tm;
      localtime_r(&t, &tm);
      do {
#ifdef __GNUC__
#ifndef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"
#endif //__clang__
#endif //__GNUC__
        if (snprintf(old_file_path, sizeof(old_file_path), "%s.%04d%02d%02d%02d%02d%02d", file_path_, tm.tm_year + 1900,
            tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec) < 0) {
          // error
        }
#ifdef __GNUC__
#ifndef __clang__
#pragma GCC diagnostic pop
#endif //__clang__
#endif //__GNUC__
        tm.tm_sec += 1;
      } while (file_exist(old_file_path));
      rename(file_path_, old_file_path);
      int32_t tmp_fd = open(file_path_, O_RDWR | O_CREAT | O_APPEND | O_LARGEFILE, LOG_FILE_MODE);
      //dup2(tmp_fd, fileno(stdout));
      //dup2(tmp_fd, fileno(stderr));
      if (fileno(stdout) != fd_ && fileno(stderr) != fd_) {
        close(fd_);
      }
      fd_ = tmp_fd;
      curr_log_size_ = 0;
      num_log_file_switch_++;
      if (num_log_file_switch_ % LOG_FILE_SWITCH_CHECK_RATE == 0) {
        clear_log_file();
      }
    }
    pthread_mutex_unlock(&file_mutex_);
  }
}

bool Logger::file_exist(const char *file_path)
{
  return (0 == access(file_path, F_OK)) ? true : false;
}

void Logger::clear_log_file() {
  if (reserved_days_ == -1 && reserved_file_num_ == -1) {
    return;
  }
  char log_file_dir[MAX_LOG_FILE_NAME_SIZE];
  char pSearch[MAX_LOG_FILE_NAME_SIZE];
  memset(pSearch, 0, sizeof pSearch);
  char *dirPos = strrchr(file_path_, '/');
  int rc_copy = 0;
  if (dirPos == NULL) {
    //TODO means the relative path, need check this carefully 
    if (!getcwd(log_file_dir, MAX_LOG_FILE_NAME_SIZE)) {
      return;
    }
    rc_copy = snprintf(pSearch, MAX_LOG_FILE_NAME_SIZE, "%s.", file_path_);
    if (rc_copy < 0) {
      return;
    }
  }
  else {
    rc_copy = snprintf(log_file_dir, dirPos-file_path_+1, "%s", file_path_);
    if (rc_copy < 0) {
      return;
    }
    rc_copy = snprintf(pSearch, MAX_LOG_FILE_NAME_SIZE, "%s.", dirPos+1);
    if (rc_copy < 0) {
      return;
    }
  }
  // if pSearch only has '.', then the below logic will have huge risk, just return.
  if (1 == strlen(pSearch)) {
    return;
  }
  DIR *log_dir_src = opendir(log_file_dir);
  if (log_dir_src == NULL) {
    return;
  }
  struct dirent *ptr = NULL;
  const int64_t reserved_second = reserved_days_>=0 ? reserved_days_ * 24 * 60 * 60:(int64_t(1)<<60);
  int log_file_num = 0;
  time_t curTime;
  time(&curTime);
  char file_name_src[MAX_LOG_FILE_NAME_SIZE];
  std::vector<log_file_entry> vec_log_entry;
  int ret_remove = 0;
  while ( (ptr=readdir(log_dir_src)) != NULL ) {
    if (strstr(ptr->d_name, pSearch) != ptr->d_name) {
      continue;
    }
    struct stat file_stat;
    rc_copy = snprintf(file_name_src, MAX_LOG_FILE_NAME_SIZE, "%s/%s", log_file_dir, ptr->d_name);
    if (rc_copy < 0) {
      closedir(log_dir_src);
      return;
    }
    int ret = stat(file_name_src, &file_stat);
    if (ret == -1) {
      closedir(log_dir_src);
      return;
    }
    if (S_ISDIR(file_stat.st_mode)) {
      continue;
    }
    // time threshold
    if (curTime - file_stat.st_mtime > reserved_second) {
      ret_remove = remove(file_name_src);
      if (ret_remove) {
        closedir(log_dir_src);
        return;
      }
    }
    else {
      log_file_num++;
      vec_log_entry.push_back({file_name_src, ptr->d_name});
    }
  }
  // only keep the latest reserved_file_num_ th log file
  if (reserved_file_num_!=-1 && log_file_num>reserved_file_num_) {
    std::vector<log_file_entry>::iterator it;
    std::sort(vec_log_entry.begin(), vec_log_entry.end(), log_file_time_comp_by_name);
    for (it = vec_log_entry.begin()+reserved_file_num_; it!=vec_log_entry.end();it++) {
      ret_remove = remove(it->log_file_path.c_str());
      if (ret_remove) {
        closedir(log_dir_src);
        return;
      }
    }
  }
  closedir(log_dir_src);
}

// max sort, log file name monotonically increasing
bool Logger::log_file_time_comp_by_name(log_file_entry &entry_a, log_file_entry &entry_b) {
  return entry_a.log_file_name > entry_b.log_file_name;
}

} // namespace logger
} // namespace smartengine
