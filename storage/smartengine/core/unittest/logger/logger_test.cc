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

#include "logger/log_module.h"
#include <gtest/gtest.h>
#include <thread>

namespace smartengine
{
namespace logger
{
  // Warning: set these values to large (e.g. at the scale of 100 or 1000)
  // will result this test running to the heat death of the universe.
  // Increase both for stress test purposes; by default set both value
  // reasonably small as this is just a unit test :)
  const int64_t LOOP_COUNT = 10;
  const int64_t THREAD_COUNT = 10;


void func()
{
  int ret = 0;
  bool var_bool = true;
  int8_t var_int8 = 8;
  uint8_t var_uint8 = 8;
  int16_t var_int16 = 16;
  uint16_t var_uint16 = 16;
  int32_t var_int32 = 32;
  uint32_t var_uint32 = 32;
  int64_t var_int64 = 64;
  uint64_t var_uint64 = 64;
  double var_double = 1.0;
  float var_float = 2.0;
  const char *var_str = "se_db";
  
  for (int64_t i = 0; i < LOOP_COUNT; ++i) {
    SE_LOG(DEBUG, "test print debug log without vars.");
    SE_LOG(DEBUG, "test print debug log with vars by K", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
    SE_LOG(DEBUG, "test print debug log with vars by K", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
    __SE_LOG(DEBUG, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);
    SE_LOG(INFO, "test print debug log without vars");
    SE_LOG(INFO, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
    SE_LOG(INFO, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
    __SE_LOG(INFO, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);
    SE_LOG(WARN, "test print debug log without vars.");
    SE_LOG(WARN, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
    SE_LOG(WARN, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
    __SE_LOG(WARN, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);
    SE_LOG(ERROR, "test print debug log without vars.");
    SE_LOG(ERROR, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
    SE_LOG(ERROR, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
    __SE_LOG(ERROR, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);
  }
}
#if 1
TEST(TestLogger, test_log_print_multi_thread)
{
  int ret = 0;
  const char *log_file_name = "LOG";
#ifndef NDEBUG
  Logger::get_log().reset();
#endif
	int stdout_fd = 1000;
  int stderr_fd = 1001;
  dup2(fileno(stdout), stdout_fd);
  dup2(fileno(stderr), stderr_fd);
  ret = Logger::get_log().init(log_file_name);
  Logger::get_log().set_log_level(DEBUG_LEVEL);
  //Logger::get_log().set_max_log_size(64 * 1024 * 1024);
  ASSERT_EQ(0, ret);

  std::thread threads[1000];
  for (int64_t i = 0; i < THREAD_COUNT; ++i) {
    threads[i] = std::thread(func);
  }

  for (int64_t i = 0; i < THREAD_COUNT; ++i) {
    threads[i].join();
  }
  dup2(stdout_fd, fileno(stdout));
  dup2(stderr_fd, fileno(stderr));
	std::cout<<"test_log_print_multi_thread over"<<std::endl;
}
#endif

#if 1
TEST(TestLogger, test_log_print)
{
  int ret = 0;
  const char *log_file_name = "LOG";
#ifndef NDEBUG
	Logger::get_log().reset();
#endif
	int stdout_fd = 1000;
  int stderr_fd = 1001;
  dup2(fileno(stdout), stdout_fd);
  dup2(fileno(stderr), stderr_fd);
  ret = Logger::get_log().init(log_file_name);
#if 1
	Logger::get_log().set_log_level(INFO_LEVEL);
	Logger::get_log().set_log_level_mod(LOG_MOD_SUBMOD(SE, FLUSH), LOG_LEVEL(INFO));
	Logger::get_log().set_log_level_mod(LOG_MOD(SE), LOG_LEVEL(WARN));
#endif
	ASSERT_EQ(0, ret);
  bool var_bool = true;
  int8_t var_int8 = 8;
  uint8_t var_uint8 = 8;
  int16_t var_int16 = 16;
  uint16_t var_uint16 = 16;
  int32_t var_int32 = 32;
  uint32_t var_uint32 = 32;
  int64_t var_int64 = 64;
  uint64_t var_uint64 = 64;
  double var_double = 1.0;
  float var_float = 2.0;
  const char *var_str = "se_db";

  SE_LOG(DEBUG, "test print debug log without vars.");
  SE_LOG(DEBUG, "test print debuglog with vars by K", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
  SE_LOG(DEBUG, "test print debug log with vars by K", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
  __SE_LOG(DEBUG, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);
  SE_LOG(INFO, "test print debug log without vars");
  SE_LOG(INFO, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
  SE_LOG(INFO, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
  __SE_LOG(INFO, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);
  SE_LOG(WARN, "test print debug log without vars.");
  SE_LOG(WARN, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
  SE_LOG(WARN, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
  __SE_LOG(WARN, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);
  SE_LOG(ERROR, "test print debug log without vars.");
  SE_LOG(ERROR, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
  SE_LOG(ERROR, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
  __SE_LOG(ERROR, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);

	COMPACTION_LOG(DEBUG, "test print debug log without vars.");
	COMPACTION_LOG(DEBUG, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	COMPACTION_LOG(DEBUG, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	__COMPACTION_LOG(DEBUG, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);
	
	COMPACTION_LOG(INFO, "test print debug log without vars.");
	COMPACTION_LOG(INFO, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	COMPACTION_LOG(INFO, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	__COMPACTION_LOG(INFO, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);
	
	COMPACTION_LOG(WARN, "test print debug log without vars.");
	COMPACTION_LOG(WARN, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	COMPACTION_LOG(WARN, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	__COMPACTION_LOG(WARN, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);

	COMPACTION_LOG(ERROR, "test print debug log without vars.");
	COMPACTION_LOG(ERROR, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	COMPACTION_LOG(ERROR, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	__COMPACTION_LOG(ERROR, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);

	COMPACTION_LOG(SYSTEM, "test print debug log without vars.");
	COMPACTION_LOG(SYSTEM, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	COMPACTION_LOG(SYSTEM, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	__COMPACTION_LOG(SYSTEM, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);

	FLUSH_LOG(DEBUG, "test print debug log without vars.");
	FLUSH_LOG(DEBUG, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	FLUSH_LOG(DEBUG, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	__FLUSH_LOG(DEBUG, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);

	FLUSH_LOG(WARN, "test print debug log without vars.");
	FLUSH_LOG(WARN, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	FLUSH_LOG(WARN, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	__FLUSH_LOG(WARN, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);

	FLUSH_LOG(SYSTEM, "test print debug log without vars.");
	FLUSH_LOG(SYSTEM, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	FLUSH_LOG(SYSTEM, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	__FLUSH_LOG(SYSTEM, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);

	HANDLER_LOG(DEBUG, "test print debug log without vars.");
	HANDLER_LOG(DEBUG, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	HANDLER_LOG(DEBUG, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	__HANDLER_LOG(DEBUG, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);

	HANDLER_LOG(WARN, "test print debug log without vars.");
	HANDLER_LOG(WARN, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	HANDLER_LOG(WARN, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	__HANDLER_LOG(WARN, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);

	HANDLER_LOG(SYSTEM, "test print debug log without vars.");
	HANDLER_LOG(SYSTEM, "test print debug log with vars by K.", K(ret), K(var_bool), K(var_int8), K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	HANDLER_LOG(SYSTEM, "test print debug log with vars by K.", K(ret), "var_bool", var_bool, "var_int8", var_int8, K(var_uint8), K(var_int16), K(var_uint16), K(var_int32), K(var_uint32), K(var_int64), K(var_uint64), K(var_double), K(var_float), K(var_str));
	__HANDLER_LOG(SYSTEM, "test print debug log with vars by fmt. ret = %d, var_bool = %d, var_int8 = %d, var_str = %s\n", ret, var_bool, var_int8, var_str);
  dup2(stdout_fd, fileno(stdout));
  dup2(stderr_fd, fileno(stderr));
	std::cout<<"test_log_print over"<<std::endl;
}
#endif

void log_file_control_test(const char *log_file_name) {
#ifndef NDEBUG
  Logger::get_log().reset();
#endif 
  int stdout_fd = 1000;
  int stderr_fd = 1001;
  dup2(fileno(stdout), stdout_fd);
  dup2(fileno(stderr), stderr_fd);
  Logger::get_log().init(log_file_name);
  int reserved_days = 3;
  int reserved_file_num = 3;
  int log_size = 1000;
  int ret_days = LOG.get_log_reserved_days();
  int ret_file_num = LOG.get_log_reserved_file_num();
  int ret_file_size = LOG.get_log_file_size();
  ASSERT_EQ(ret_days, -1);
  ASSERT_EQ(ret_file_num, -1);
  ASSERT_EQ(ret_file_size, (1024*1024*256));
  LOG.set_log_reserved_days(reserved_days);
  LOG.set_log_reserved_file_num(reserved_file_num);
  LOG.set_max_log_size(log_size);
  ret_days = LOG.get_log_reserved_days();
  ret_file_num = LOG.get_log_reserved_file_num();
  ret_file_size = LOG.get_log_file_size();
  ASSERT_EQ(ret_days, reserved_days);
  ASSERT_EQ(ret_file_num, reserved_file_num);
  ASSERT_EQ(ret_file_size, log_size);
  func();
  LOG.set_log_reserved_days(-1);
  LOG.set_log_reserved_file_num(-1);
  LOG.set_max_log_size(1024*1024*256);
  dup2(stdout_fd, fileno(stdout));
  dup2(stderr_fd, fileno(stderr));
}

TEST(TestLogger, log_file_control) {
  std::string log_file_name = "LOG";
  //log_file_control_test(log_file_name.c_str());
  //std::string log_file_name_relative = "~/LOG";
  //log_file_control_test(log_file_name_relative.c_str());
  //std::string log_file_name_abs = "/u01/zxj/workstation/tmp/ist/logdata/LOG";
  log_file_control_test(log_file_name.c_str());
}
} //namespace logger
} //namespace smartengine

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
