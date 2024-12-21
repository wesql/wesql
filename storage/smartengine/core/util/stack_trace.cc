/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
*/

#include "util/stack_trace.h"
#include <cxxabi.h>
#include <execinfo.h>
#include <sstream>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include "util/macro_utils.h"

namespace smartengine
{
namespace util
{

std::string StackTrace::backtrace_fast()
{
  std::ostringstream backtrace_stream;
  int32_t frame_deepth = 0;
  void *frames[MAX_FRAME_DEEPTH] = {nullptr};

  frame_deepth = backtrace(frames, MAX_FRAME_DEEPTH);
  for (int64_t i = 0; i < frame_deepth; ++i) {
    backtrace_stream << "#" << i << " " << frames[i] << '\n';
  }

  return backtrace_stream.str();
}

std::string StackTrace::backtrace_slow()
{
  std::ostringstream backtrace_stream;
  int32_t frame_deepth = 0;
  void *frames[MAX_FRAME_DEEPTH] = {nullptr};
  char **symbols = nullptr;

  frame_deepth = backtrace(frames, MAX_FRAME_DEEPTH);
  symbols = backtrace_symbols(frames, frame_deepth);
  if (IS_NOTNULL(symbols)) {
    for (int32_t i = 0; i < frame_deepth; ++i) {
      backtrace_stream << "#" << i << " ";
      demangle_symbol(symbols[i], backtrace_stream);
    }

    free(symbols);
  }

  return backtrace_stream.str();
}

void StackTrace::install_handler()
{
  // just use the plain old signal as it's simple and sufficient
  // for this use case
  signal(SIGILL, handler);
  signal(SIGSEGV, handler);
  signal(SIGBUS, handler);
  signal(SIGABRT, handler);
}
void StackTrace::demangle_symbol(char *symbol, std::ostringstream &backtrace_stream)
{
  int status = 0;
  char *demangled = nullptr;
  char *left = nullptr;
  char *right = nullptr;

  if (IS_NOTNULL(symbol)) {
    // The format of the original symbol is as follows:
    // "/usr/bin/mysqld(_Z21mysql_execute_commandP3THDb+0x24aa) [0x401cfa7]“.
    // The substring that needs be demangled is between '(' and '+'.
    left = strchr(symbol, '(');
    right = (nullptr != left) ? strchr(left, '+') : nullptr;

    if (IS_NOTNULL(left) && IS_NOTNULL(right)) {
      *left++ = '\0';
      *right++ = '\0';
      demangled = abi::__cxa_demangle(left, nullptr, nullptr, &status);

      if (IS_NULL(demangled) || (0 != status)) {
        // If demangling fails, the symbol needs to be reverted to its original format.
        *(left - 1) = '(';
        *(right - 1) = '+';

        backtrace_stream << symbol << '\n';
      } else {
        backtrace_stream << symbol << '(' << demangled << '+' << right << '\n';
      }

      if (IS_NOTNULL(demangled)) {
        free(demangled);
        demangled = nullptr;
      }
    } else {
      backtrace_stream << symbol << '\n';
    }
  }
}

void StackTrace::handler(int signal_num)
{
  // reset to default handler
  signal(signal_num, SIG_DFL);
  fprintf(stderr, "Received signal %d (%s)\n", signal_num, strsignal(signal_num));
  // skip the top three signal handler related frames
  fprintf(stderr, "%s", backtrace_slow().c_str());
  // re-signal to default handler (so we still get core dump if needed...)
  raise(signal_num);
}

} // namespace util
} // namespace smartengine
