/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <functional>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "options/options.h"
#include "storage/storage_log_entry.h"
#include "storage/storage_logger.h"
#include "tools/ldb_cmd_execute_result.h"
#include "tools/ldb_tool.h"

namespace smartengine {
using namespace storage;
using namespace db;
using namespace common;

namespace tools {

class LDBCommand {
 public:
  // Command-line arguments
  static const std::string ARG_DB;
  static const std::string ARG_PATH;
  static const std::string ARG_HEX;
  static const std::string ARG_KEY_HEX;
  static const std::string ARG_VALUE_HEX;
  static const std::string ARG_CF_NAME;
  static const std::string ARG_TTL;
  static const std::string ARG_TTL_START;
  static const std::string ARG_TTL_END;
  static const std::string ARG_TIMESTAMP;
  static const std::string ARG_FROM;
  static const std::string ARG_TO;
  static const std::string ARG_MAX_KEYS;
  static const std::string ARG_BLOOM_BITS;
  static const std::string ARG_COMPRESSION_TYPE;
  static const std::string ARG_COMPRESSION_MAX_DICT_BYTES;
  static const std::string ARG_BLOCK_SIZE;
  static const std::string ARG_AUTO_COMPACTION;
  static const std::string ARG_DB_WRITE_BUFFER_SIZE;
  static const std::string ARG_WRITE_BUFFER_SIZE;
  static const std::string ARG_FILE_SIZE;
  static const std::string ARG_CREATE_IF_MISSING;
  static const std::string ARG_NO_VALUE;

  struct ParsedParams {
    std::string cmd;
    std::vector<std::string> cmd_params;
    std::map<std::string, std::string> option_map;
    std::vector<std::string> flags;
  };

  static LDBCommand* SelectCommand(const ParsedParams& parsed_parms);

  static LDBCommand* InitFromCmdLineArgs(
      const std::vector<std::string>& args, const common::Options& options,
      const LDBOptions& ldb_options,
      const std::vector<db::ColumnFamilyDescriptor>* column_families,
      const std::function<LDBCommand*(const ParsedParams&)>& selector =
          SelectCommand);

  static LDBCommand* InitFromCmdLineArgs(
      int argc, char** argv, const common::Options& options,
      const LDBOptions& ldb_options,
      const std::vector<db::ColumnFamilyDescriptor>* column_families);

  bool ValidateCmdLineOptions();

  virtual common::Options PrepareOptionsForOpenDB();

  virtual void SetDBOptions(common::Options options) { options_ = options; }

  virtual void SetColumnFamilies(
      const std::vector<db::ColumnFamilyDescriptor>* column_families) {
    if (column_families != nullptr) {
      column_families_ = *column_families;
    } else {
      column_families_.clear();
    }
  }

  void SetLDBOptions(const LDBOptions& ldb_options) {
    ldb_options_ = ldb_options;
  }

  virtual bool NoDBOpen() { return false; }

  virtual ~LDBCommand() { CloseDB(); }

  /* Run the command, and return the execute result. */
  void Run();

  virtual void DoCommand() = 0;

  LDBCommandExecuteResult GetExecuteState() { return exec_state_; }

  void ClearPreviousRunState() { exec_state_.Reset(); }

  // Consider using smartengine::common::Slice::DecodeHex directly instead if you
  // don't need the
  // 0x prefix
  static std::string HexToString(const std::string& str);

  // Consider using smartengine::common::Slice::ToString(true) directly instead if
  // you don't need the 0x prefix
  static std::string StringToHex(const std::string& str);

  static const char* DELIM;

 protected:
  LDBCommandExecuteResult exec_state_;
  std::string db_path_;
  std::string column_family_name_;
  db::DB* db_;
  std::map<std::string, db::ColumnFamilyHandle*> cf_handles_;

  /** If true, the key is input/output as hex in get/put/scan/delete etc. */
  bool is_key_hex_;

  /** If true, the value is input/output as hex in get/put/scan/delete etc. */
  bool is_value_hex_;


  // If true, the kvs are output with their insert/modify timestamp in a ttl db
  bool timestamp_;

  /**
   * Map of options passed on the command-line.
   */
  const std::map<std::string, std::string> option_map_;

  /**
   * Flags passed on the command-line.
   */
  const std::vector<std::string> flags_;

  /** List of command-line options valid for this command */
  const std::vector<std::string> valid_cmd_line_options_;

  bool ParseKeyValue(const std::string& line, std::string* key,
                     std::string* value, bool is_key_hex, bool is_value_hex);

  LDBCommand(const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags,
             const std::vector<std::string>& valid_cmd_line_options);

  void OpenDB();

  void CloseDB();

  db::ColumnFamilyHandle* GetCfHandle();

  static std::string PrintKeyValue(const std::string& key,
                                   const std::string& value, bool is_key_hex,
                                   bool is_value_hex);

  static std::string PrintKeyValue(const std::string& key,
                                   const std::string& value, bool is_hex);

  /**
   * Return true if the specified flag is present in the specified flags vector
   */
  static bool IsFlagPresent(const std::vector<std::string>& flags,
                            const std::string& flag) {
    return (std::find(flags.begin(), flags.end(), flag) != flags.end());
  }

  static std::string HelpRangeCmdArgs();

  /**
   * A helper function that returns a list of command line options
   * used by this command.  It includes the common options and the ones
   * passed in.
   */
  static std::vector<std::string> BuildCmdLineOptions(
      std::vector<std::string> options);

  bool ParseIntOption(const std::map<std::string, std::string>& options,
                      const std::string& option, int& value,
                      LDBCommandExecuteResult& exec_state);

  bool ParseStringOption(const std::map<std::string, std::string>& options,
                         const std::string& option, std::string* value);

  common::Options options_;
  std::vector<db::ColumnFamilyDescriptor> column_families_;
  LDBOptions ldb_options_;

 private:
  /**
   * Interpret command line options and flags to determine if the key
   * should be input/output in hex.
   */
  bool IsKeyHex(const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  /**
   * Interpret command line options and flags to determine if the value
   * should be input/output in hex.
   */
  bool IsValueHex(const std::map<std::string, std::string>& options,
                  const std::vector<std::string>& flags);

  /**
   * Returns the value of the specified option as a boolean.
   * default_val is used if the option is not found in options.
   * Throws an exception if the value of the option is not
   * "true" or "false" (case insensitive).
   */
  bool ParseBooleanOption(const std::map<std::string, std::string>& options,
                          const std::string& option, bool default_val);

  /**
   * Converts val to a boolean.
   * val must be either true or false (case insensitive).
   * Otherwise an exception is thrown.
   */
  bool StringToBool(std::string val);
};

class LDBCommandRunner {
 public:
  static void PrintHelp(const LDBOptions& ldb_options, const char* exec_name);

  static void RunCommand(
      int argc, char** argv, common::Options options,
      const LDBOptions& ldb_options,
      const std::vector<db::ColumnFamilyDescriptor>* column_families);
};

class LogEntryIterator {
public:
  LogEntryIterator()
    : env_(util::Env::Default()),
      manifest_log_path_(),
      reader_(nullptr),
      record_(),
      scratch_(),
      curr_log_buf_()
  {
  }
  ~LogEntryIterator()
  {
  }

  int init(std::string manifest_file_path);
  int get_next_log_entry(ManifestLogEntryHeader &log_entry_header, char *&log_data, int64_t &log_length);

private:
  int create_log_reader();
  int get_next_record();

private:
  util::Env *env_;
  std::string manifest_log_path_;
  log::Reader *reader_;
  Slice record_;
  std::string scratch_;
  StorageLoggerBuffer curr_log_buf_;
};


}  // namespace tools
}  // namespace smartengine