//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "tools/ldb_cmd.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>


#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/log_reader.h"
#include "memory/base_malloc.h"
#include "logger/log_module.h"
#include "port/dirent.h"
#include "table/scoped_arena_iterator.h"
#include "table/table.h"
#include "tools/ldb_cmd_impl.h"
#include "util/coding.h"
#include "util/file_name.h"
#include "util/file_reader_writer.h"
#include "util/string_util.h"
#include "storage/extent_space_manager.h"
#include "write_batch/write_batch.h"
#include "write_batch/write_batch_internal.h"

namespace smartengine {
using namespace common;
using namespace util;
using namespace db;
using namespace table;
using namespace cache;
using namespace storage;
using namespace memory;

namespace tools {

const std::string LDBCommand::ARG_DB = "db";
const std::string LDBCommand::ARG_PATH = "path";
const std::string LDBCommand::ARG_HEX = "hex";
const std::string LDBCommand::ARG_KEY_HEX = "key_hex";
const std::string LDBCommand::ARG_VALUE_HEX = "value_hex";
const std::string LDBCommand::ARG_CF_NAME = "column_family";
const std::string LDBCommand::ARG_TTL = "ttl";
const std::string LDBCommand::ARG_TTL_START = "start_time";
const std::string LDBCommand::ARG_TTL_END = "end_time";
const std::string LDBCommand::ARG_TIMESTAMP = "timestamp";
const std::string LDBCommand::ARG_FROM = "from";
const std::string LDBCommand::ARG_TO = "to";
const std::string LDBCommand::ARG_MAX_KEYS = "max_keys";
const std::string LDBCommand::ARG_BLOOM_BITS = "bloom_bits";
const std::string LDBCommand::ARG_COMPRESSION_MAX_DICT_BYTES =
    "compression_max_dict_bytes";
const std::string LDBCommand::ARG_BLOCK_SIZE = "block_size";
const std::string LDBCommand::ARG_AUTO_COMPACTION = "auto_compaction";
const std::string LDBCommand::ARG_DB_WRITE_BUFFER_SIZE = "db_write_buffer_size";
const std::string LDBCommand::ARG_WRITE_BUFFER_SIZE = "write_buffer_size";
const std::string LDBCommand::ARG_CREATE_IF_MISSING = "create_if_missing";
const std::string LDBCommand::ARG_NO_VALUE = "no_value";

const char* LDBCommand::DELIM = " ==> ";

namespace {

void DumpWalFile(std::string wal_file, bool print_header, bool print_values,
                 LDBCommandExecuteResult* exec_state);

//void DumpSstFile(std::string filename, bool output_hex, bool show_properties);
}

LDBCommand* LDBCommand::InitFromCmdLineArgs(
    int argc, char** argv, const Options& options,
    const LDBOptions& ldb_options,
    const std::vector<ColumnFamilyDescriptor>* column_families) {
  std::vector<std::string> args;
  for (int i = 1; i < argc; i++) {
    args.push_back(argv[i]);
  }
  return InitFromCmdLineArgs(args, options, ldb_options, column_families,
                             SelectCommand);
}

/**
 * Parse the command-line arguments and create the appropriate LDBCommand2
 * instance.
 * The command line arguments must be in the following format:
 * ./ldb --db=PATH_TO_DB [--commonOpt1=commonOpt1Val] ..
 *        COMMAND <PARAM1> <PARAM2> ... [-cmdSpecificOpt1=cmdSpecificOpt1Val] ..
 * This is similar to the command line format used by HBaseClientTool.
 * Command name is not included in args.
 * Returns nullptr if the command-line cannot be parsed.
 */
LDBCommand* LDBCommand::InitFromCmdLineArgs(
    const std::vector<std::string>& args, const Options& options,
    const LDBOptions& ldb_options,
    const std::vector<ColumnFamilyDescriptor>* column_families,
    const std::function<LDBCommand*(const ParsedParams&)>& selector) {
  // --x=y command line arguments are added as x->y map entries in
  // parsed_params.option_map.
  //
  // Command-line arguments of the form --hex end up in this array as hex to
  // parsed_params.flags
  ParsedParams parsed_params;

  // Everything other than option_map and flags. Represents commands
  // and their parameters.  For eg: put key1 value1 go into this vector.
  std::vector<std::string> cmdTokens;

  const std::string OPTION_PREFIX = "--";

  for (const auto& arg : args) {
    if (arg[0] == '-' && arg[1] == '-') {
      std::vector<std::string> splits = StringSplit(arg, '=');
      if (splits.size() == 2) {
        std::string optionKey = splits[0].substr(OPTION_PREFIX.size());
        parsed_params.option_map[optionKey] = splits[1];
      } else {
        std::string optionKey = splits[0].substr(OPTION_PREFIX.size());
        parsed_params.flags.push_back(optionKey);
      }
    } else {
      cmdTokens.push_back(arg);
    }
  }

  if (cmdTokens.size() < 1) {
    fprintf(stderr, "Command not specified!");
    return nullptr;
  }

  parsed_params.cmd = cmdTokens[0];
  parsed_params.cmd_params.assign(cmdTokens.begin() + 1, cmdTokens.end());

  LDBCommand* command = selector(parsed_params);

  if (command) {
    command->SetDBOptions(options);
    command->SetLDBOptions(ldb_options);
  }
  return command;
}

LDBCommand* LDBCommand::SelectCommand(const ParsedParams& parsed_params) {
  if (parsed_params.cmd == GetCommand::Name()) {
    return new GetCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  } else if (parsed_params.cmd == PutCommand::Name()) {
    return new PutCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  } else if (parsed_params.cmd == BatchPutCommand::Name()) {
    return new BatchPutCommand(parsed_params.cmd_params,
                               parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == DeleteCommand::Name()) {
    return new DeleteCommand(parsed_params.cmd_params, parsed_params.option_map,
                             parsed_params.flags);
  } else if (parsed_params.cmd == ApproxSizeCommand::Name()) {
    return new ApproxSizeCommand(parsed_params.cmd_params,
                                 parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == DBQuerierCommand::Name()) {
    return new DBQuerierCommand(parsed_params.cmd_params,
                                parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == WALDumperCommand::Name()) {
    return new WALDumperCommand(parsed_params.cmd_params,
                                parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == ManifestDumpCommand::Name()) {
    return new ManifestDumpCommand(parsed_params.cmd_params,
                                   parsed_params.option_map,
                                   parsed_params.flags);
  } else if (parsed_params.cmd == CheckpointDumpCommand::Name()) {
    return new CheckpointDumpCommand(parsed_params.cmd_params,
                                   parsed_params.option_map,
                                   parsed_params.flags);
  } else if (parsed_params.cmd == ListColumnFamiliesCommand::Name()) {
    return new ListColumnFamiliesCommand(parsed_params.cmd_params,
                                         parsed_params.option_map,
                                         parsed_params.flags);
  } else if (parsed_params.cmd == CreateColumnFamilyCommand::Name()) {
    return new CreateColumnFamilyCommand(parsed_params.cmd_params,
                                         parsed_params.option_map,
                                         parsed_params.flags);
  }
  return nullptr;
}

/* Run the command, and return the execute result. */
void LDBCommand::Run() {
  if (!exec_state_.IsNotStarted()) {
    return;
  }

  if (db_ == nullptr && !NoDBOpen()) {
    OpenDB();
  }

  // We'll intentionally proceed even if the DB can't be opened because users
  // can also specify a filename, not just a directory.
  DoCommand();

  if (exec_state_.IsNotStarted()) {
    exec_state_ = LDBCommandExecuteResult::Succeed("");
  }

  if (db_ != nullptr) {
    CloseDB();
  }
}

LDBCommand::LDBCommand(const std::map<std::string,
                       std::string>& options,
                       const std::vector<std::string>& flags,
                       const std::vector<std::string>& valid_cmd_line_options)
    : db_(nullptr),
      is_key_hex_(false),
      is_value_hex_(false),
      timestamp_(false),
      option_map_(options),
      flags_(flags),
      valid_cmd_line_options_(valid_cmd_line_options) {
  std::map<std::string, std::string>::const_iterator itr = options.find(ARG_DB);
  if (itr != options.end()) {
    db_path_ = itr->second;
  }

  itr = options.find(ARG_CF_NAME);
  if (itr != options.end()) {
    column_family_name_ = itr->second;
  } else {
    column_family_name_ = kDefaultColumnFamilyName;
  }

  is_key_hex_ = IsKeyHex(options, flags);
  is_value_hex_ = IsValueHex(options, flags);
  timestamp_ = IsFlagPresent(flags, ARG_TIMESTAMP);
}

void LDBCommand::OpenDB() {
  Options opt = PrepareOptionsForOpenDB();
  if (!exec_state_.IsNotStarted()) {
    return;
  }
  // Open the DB.
  Status st;
  std::vector<ColumnFamilyHandle*> handles_opened;
  if (column_families_.empty()) {
    // Try to figure out column family lists
    std::vector<std::string> cf_list;
    // There is possible the DB doesn't exist yet, for "create if not
    // "existing case". The failure is ignored here. We rely on DB::Open()
    // to give us the correct error message for problem with opening
    // existing DB.
    if (st.ok() && cf_list.size() > 1) {
      // Ignore single column family DB.
      //for (auto cf_name : cf_list) {
      //  column_families_.emplace_back(cf_name, opt);
      //}
    }
  }
  if (column_families_.empty()) {
    //st = DB::Open(opt, db_path_, &db_);
  } else {
    st = DB::Open(opt, db_path_, &handles_opened, &db_);
  }
  
  if (!st.ok()) {
    std::string msg = st.ToString();
    exec_state_ = LDBCommandExecuteResult::Failed(msg);
  } else if (!handles_opened.empty()) {
    assert(handles_opened.size() == column_families_.size());
    bool found_cf_name = false;
    //for (size_t i = 0; i < handles_opened.size(); i++) {
    //  cf_handles_[column_families_[i].name] = handles_opened[i];
    //  if (column_family_name_ == column_families_[i].name) {
    //    found_cf_name = true;
    //  }
    //}
    if (!found_cf_name) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "Non-existing sub table " + column_family_name_);
      CloseDB();
    }
  } else {
    // We successfully opened DB in single sub table mode.
    assert(column_families_.empty());
    if (column_family_name_ != kDefaultColumnFamilyName) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "Non-existing sub table " + column_family_name_);
      CloseDB();
    }
  }

  options_ = opt;
}

void LDBCommand::CloseDB() {
  if (db_ != nullptr) {
    for (auto& pair : cf_handles_) {
      delete pair.second;
    }
    delete db_;
    db_ = nullptr;
  }
}

ColumnFamilyHandle* LDBCommand::GetCfHandle() {
  if (!cf_handles_.empty()) {
    auto it = cf_handles_.find(column_family_name_);
    if (it == cf_handles_.end()) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "Cannot find sub table " + column_family_name_);
    } else {
      return it->second;
    }
  }
  return db_->DefaultColumnFamily();
}

std::vector<std::string> LDBCommand::BuildCmdLineOptions(
    std::vector<std::string> options) {
  std::vector<std::string> ret = {ARG_DB,
                                  ARG_BLOOM_BITS,
                                  ARG_BLOCK_SIZE,
                                  ARG_AUTO_COMPACTION,
                                  ARG_COMPRESSION_MAX_DICT_BYTES,
                                  ARG_WRITE_BUFFER_SIZE,
                                  ARG_CF_NAME};
  ret.insert(ret.end(), options.begin(), options.end());
  return ret;
}

/**
 * Parses the specific integer option and fills in the value.
 * Returns true if the option is found.
 * Returns false if the option is not found or if there is an error parsing the
 * value.  If there is an error, the specified exec_state is also
 * updated.
 */
bool LDBCommand::ParseIntOption(
    const std::map<std::string, std::string>& options,
    const std::string& option, int& value,
    LDBCommandExecuteResult& exec_state) {
  std::map<std::string, std::string>::const_iterator itr =
      option_map_.find(option);
  if (itr != option_map_.end()) {
    try {
#if defined(CYGWIN)
      value = strtol(itr->second.c_str(), 0, 10);
#else
      value = std::stoi(itr->second);
#endif
      return true;
    } catch (const std::invalid_argument&) {
      exec_state =
          LDBCommandExecuteResult::Failed(option + " has an invalid value.");
    } catch (const std::out_of_range&) {
      exec_state = LDBCommandExecuteResult::Failed(
          option + " has a value out-of-range.");
    }
  }
  return false;
}

/**
 * Parses the specified option and fills in the value.
 * Returns true if the option is found.
 * Returns false otherwise.
 */
bool LDBCommand::ParseStringOption(
    const std::map<std::string, std::string>& options,
    const std::string& option, std::string* value) {
  auto itr = option_map_.find(option);
  if (itr != option_map_.end()) {
    *value = itr->second;
    return true;
  }
  return false;
}

Options LDBCommand::PrepareOptionsForOpenDB() {
  Options opt = options_;

  std::map<std::string, std::string>::const_iterator itr;

  BlockBasedTableOptions table_options;
  int block_size;
  if (ParseIntOption(option_map_, ARG_BLOCK_SIZE, block_size, exec_state_)) {
    if (block_size > 0) {
      table_options.block_size = block_size;
    } else {
      exec_state_ =
          LDBCommandExecuteResult::Failed(ARG_BLOCK_SIZE + " must be > 0.");
    }
  }


  itr = option_map_.find(ARG_AUTO_COMPACTION);
  if (itr != option_map_.end()) {
    opt.disable_auto_compactions = !StringToBool(itr->second);
  }

  int compression_max_dict_bytes;
  if (ParseIntOption(option_map_, ARG_COMPRESSION_MAX_DICT_BYTES,
                     compression_max_dict_bytes, exec_state_)) {
    if (compression_max_dict_bytes >= 0) {
      opt.compression_opts.max_dict_bytes = compression_max_dict_bytes;
    } else {
      exec_state_ = LDBCommandExecuteResult::Failed(
          ARG_COMPRESSION_MAX_DICT_BYTES + " must be >= 0.");
    }
  }

  int db_write_buffer_size;
  if (ParseIntOption(option_map_, ARG_DB_WRITE_BUFFER_SIZE,
                     db_write_buffer_size, exec_state_)) {
    if (db_write_buffer_size >= 0) {
      opt.db_write_buffer_size = db_write_buffer_size;
    } else {
      exec_state_ = LDBCommandExecuteResult::Failed(ARG_DB_WRITE_BUFFER_SIZE +
                                                    " must be >= 0.");
    }
  }

  int write_buffer_size;
  if (ParseIntOption(option_map_, ARG_WRITE_BUFFER_SIZE, write_buffer_size,
                     exec_state_)) {
    if (write_buffer_size > 0) {
      opt.write_buffer_size = write_buffer_size;
    } else {
      exec_state_ = LDBCommandExecuteResult::Failed(ARG_WRITE_BUFFER_SIZE +
                                                    " must be > 0.");
    }
  }

  if (opt.db_paths.size() == 0) {
    opt.db_paths.emplace_back(db_path_, std::numeric_limits<uint64_t>::max());
  }

  return opt;
}

bool LDBCommand::ParseKeyValue(const std::string& line, std::string* key,
                               std::string* value, bool is_key_hex,
                               bool is_value_hex) {
  size_t pos = line.find(DELIM);
  if (pos != std::string::npos) {
    *key = line.substr(0, pos);
    *value = line.substr(pos + strlen(DELIM));
    if (is_key_hex) {
      *key = HexToString(*key);
    }
    if (is_value_hex) {
      *value = HexToString(*value);
    }
    return true;
  } else {
    return false;
  }
}

/**
 * Make sure that ONLY the command-line options and flags expected by this
 * command are specified on the command-line.  Extraneous options are usually
 * the result of user error.
 * Returns true if all checks pass.  Else returns false, and prints an
 * appropriate error msg to stderr.
 */
bool LDBCommand::ValidateCmdLineOptions() {
  for (std::map<std::string, std::string>::const_iterator itr =
           option_map_.begin();
       itr != option_map_.end(); ++itr) {
    if (std::find(valid_cmd_line_options_.begin(),
                  valid_cmd_line_options_.end(),
                  itr->first) == valid_cmd_line_options_.end()) {
      fprintf(stderr, "Invalid command-line option %s\n", itr->first.c_str());
      return false;
    }
  }

  for (std::vector<std::string>::const_iterator itr = flags_.begin();
       itr != flags_.end(); ++itr) {
    if (std::find(valid_cmd_line_options_.begin(),
                  valid_cmd_line_options_.end(),
                  *itr) == valid_cmd_line_options_.end()) {
      fprintf(stderr, "Invalid command-line flag %s\n", itr->c_str());
      return false;
    }
  }

  if (!NoDBOpen() && option_map_.find(ARG_DB) == option_map_.end() &&
      option_map_.find(ARG_PATH) == option_map_.end()) {
    fprintf(stderr, "Either %s or %s must be specified.\n", ARG_DB.c_str(),
            ARG_PATH.c_str());
    return false;
  }

  return true;
}

std::string LDBCommand::HexToString(const std::string& str) {
  std::string result;
  std::string::size_type len = str.length();
  if (len < 2 || str[0] != '0' || str[1] != 'x') {
    fprintf(stderr, "Invalid hex input %s.  Must start with 0x\n", str.c_str());
    throw "Invalid hex input";
  }
  if (!Slice(str.data() + 2, len - 2).DecodeHex(&result)) {
    throw "Invalid hex input";
  }
  return result;
}

std::string LDBCommand::StringToHex(const std::string& str) {
  std::string result("0x");
  result.append(Slice(str).ToString(true));
  return result;
}

std::string LDBCommand::PrintKeyValue(const std::string& key,
                                      const std::string& value, bool is_key_hex,
                                      bool is_value_hex) {
  std::string result;
  result.append(is_key_hex ? StringToHex(key) : key);
  result.append(DELIM);
  result.append(is_value_hex ? StringToHex(value) : value);
  return result;
}

std::string LDBCommand::PrintKeyValue(const std::string& key,
                                      const std::string& value, bool is_hex) {
  return PrintKeyValue(key, value, is_hex, is_hex);
}

std::string LDBCommand::HelpRangeCmdArgs() {
  std::ostringstream str_stream;
  str_stream << " ";
  str_stream << "[--" << ARG_FROM << "] ";
  str_stream << "[--" << ARG_TO << "] ";
  return str_stream.str();
}

bool LDBCommand::IsKeyHex(const std::map<std::string, std::string>& options,
                          const std::vector<std::string>& flags) {
  return (IsFlagPresent(flags, ARG_HEX) || IsFlagPresent(flags, ARG_KEY_HEX) ||
          ParseBooleanOption(options, ARG_HEX, false) ||
          ParseBooleanOption(options, ARG_KEY_HEX, false));
}

bool LDBCommand::IsValueHex(const std::map<std::string, std::string>& options,
                            const std::vector<std::string>& flags) {
  return (IsFlagPresent(flags, ARG_HEX) ||
          IsFlagPresent(flags, ARG_VALUE_HEX) ||
          ParseBooleanOption(options, ARG_HEX, false) ||
          ParseBooleanOption(options, ARG_VALUE_HEX, false));
}

bool LDBCommand::ParseBooleanOption(
    const std::map<std::string, std::string>& options,
    const std::string& option, bool default_val) {
  std::map<std::string, std::string>::const_iterator itr = options.find(option);
  if (itr != options.end()) {
    std::string option_val = itr->second;
    return StringToBool(itr->second);
  }
  return default_val;
}

bool LDBCommand::StringToBool(std::string val) {
  std::transform(val.begin(), val.end(), val.begin(),
                 [](char ch) -> char { return (char)::tolower(ch); });

  if (val == "true") {
    return true;
  } else if (val == "false") {
    return false;
  } else {
    throw "Invalid value for boolean argument";
  }
}

int LogEntryIterator::init(std::string manifest_file_path)
{
  int ret = Status::kOk;
  manifest_log_path_ = manifest_file_path;

  if (FAILED(create_log_reader())) {
    fprintf(stderr, "fail to create log reader. %d\n", ret);
  } else if (FAILED(get_next_record())) {
    fprintf(stderr, "fail to get next record. %d\n", ret);
  }
  return ret;
}

int LogEntryIterator::get_next_log_entry(ManifestLogEntryHeader &log_entry_header, char *&log_data, int64_t &log_length)
{
  int ret = Status::kOk;

  if (FAILED(curr_log_buf_.read_log(log_entry_header, log_data, log_length))) {
    if (Status::kIterEnd == ret) {
      if (FAILED(get_next_record())) {
        if (Status::kIterEnd != ret) {
          fprintf(stderr, "fail to get next record.%d\n", ret);
        }
      } else if (FAILED(curr_log_buf_.read_log(log_entry_header, log_data, log_length))) {
        fprintf(stderr, "fail to read log. %d\n", ret);
        abort();
      }
    } else {
      fprintf(stderr, "fail to read log. %d\n", ret);
      abort();
    }
  }
  return ret;
}
int LogEntryIterator::get_next_record()
{
  int ret = Status::kOk;
  LogHeader log_header;
  char *log_buf = nullptr;
  int64_t log_buf_length = 0;
  int64_t pos = 0;

  if (!reader_->ReadRecord(&record_, &scratch_)) {
    ret = Status::kIterEnd;
  } else {
    log_buf = const_cast<char *>(record_.data());
    log_buf_length = record_.size();
    if (FAILED(log_header.deserialize(log_buf, log_buf_length, pos))) {
      fprintf(stderr, "fail to deserialzie log entry header\n");
    } else {
      curr_log_buf_.assign(log_buf + pos, log_header.log_length_);
    }
  }
  return ret;
}
int LogEntryIterator::create_log_reader()
{
  int ret = Status::kOk;
  util::SequentialFileReader *file_reader = nullptr;
//  std::unique_ptr<SequentialFile> manifest_file;
//  std::unique_ptr<SequentialFileReader> manifest_file_reader;
  SequentialFile *manifest_file = nullptr;
//  SequentialFileReader *manifest_file_reader = nullptr;
  util::EnvOptions env_options;
  util::Env *env = util::Env::Default();
  int32_t start = 0;
  
  if (FAILED(env_->FileExists(manifest_log_path_).code())) {
    fprintf(stderr, "file not exist. %d, %s\n", ret, manifest_log_path_.c_str());
  } else if (FAILED(env_->NewSequentialFile(manifest_log_path_, manifest_file, env_options).code())) {
    fprintf(stderr, "fail to open manifest file. %d, %s\n", ret, manifest_log_path_.c_str());
  } else if (nullptr == (file_reader = new SequentialFileReader(manifest_file))) {
    fprintf(stderr, "fail to creader file reader.\n");
  } else {
//    manifest_file_reader.reset(file_reader);
    // todo destruct file
    if (nullptr == (reader_ = MOD_NEW_OBJECT(ModId::kStorageLogger,
                                             db::log::Reader,
                                             file_reader,
                                             nullptr,
                                             true,
                                             start,
                                             0))) {
      fprintf(stderr, "fail to create log reader.\n");
    }
  }
  
  return ret;
}

int parse_and_print_log_entry(ManifestLogEntryHeader &log_entry_header, char *log_data, int64_t log_length)
{
  int ret = Status::kOk;
  int64_t pos = 0;
  const int64_t buf_length = 2 * 1024 * 1024;
  char buf[buf_length];
  int64_t to_string_pos = 0;

  if ((nullptr == log_data || 0 >= log_length) && (REDO_LOG_BEGIN != log_entry_header.log_entry_type_ && REDO_LOG_COMMIT != log_entry_header.log_entry_type_)) {
    fprintf(stderr, "invalid argument\n");
  } else {
    log_entry_header.to_string(const_cast<char *>(buf), buf_length);
    fprintf(stderr, "log_entry_header = %s,", buf);
    memset(buf, 0, buf_length);
    switch (log_entry_header.log_entry_type_) {
      case REDO_LOG_BEGIN:
         fprintf(stderr, "begin\n");
         break;
      case REDO_LOG_COMMIT:
         fprintf(stderr, "commit\n");
         break;
      case REDO_LOG_ADD_SSTABLE:
      case REDO_LOG_REMOVE_SSTABLE:
        {
         ChangeSubTableLogEntry change_subtable_log_entry;
         if (FAILED(change_subtable_log_entry.deserialize(log_data, log_length, pos))) {
           fprintf(stderr, "fail to deserialize change partition group log entry. %d, %ld\n", ret, log_entry_header.log_entry_type_);
         } else {
           change_subtable_log_entry.to_string(const_cast<char *>(buf), buf_length);
           fprintf(stderr, "%s\n", buf);
         }
         break;
        }
      case REDO_LOG_MODIFY_SSTABLE:
        {
         ChangeInfo change_info;
         ModifySubTableLogEntry modify_subtable_log_entry(change_info);
         if (FAILED(modify_subtable_log_entry.deserialize(log_data, log_length, pos))) {
           fprintf(stderr, "fail to deserialize change partition group log entry. %d, %ld\n", ret, log_entry_header.log_entry_type_);
         } else {
           modify_subtable_log_entry.to_string(const_cast<char *>(buf), buf_length);
           fprintf(stderr, "%s\n", buf);
         }
         break;
        }
      case REDO_LOG_MODIFY_EXTENT_META:
        {
         ModifyExtentMetaLogEntry modify_extent_meta_log_entry;
         if (FAILED(modify_extent_meta_log_entry.deserialize(log_data, log_length, pos))) {
         } else {
           modify_extent_meta_log_entry.to_string(const_cast<char *>(buf), buf_length);
           fprintf(stderr, "%s\n", buf);
         }
         break;
        }
      default:
         fprintf(stderr, "not support log type\n");
    }
  }

  return ret;
}

void dump_manifest_file(std::string file_path)
{
  int ret = Status::kOk;
  log::Reader *reader = nullptr;
  int32_t log_type = 0;
  ManifestLogEntryHeader log_entry_header;
  char *log_data = nullptr;
  int64_t log_length = 0;
  LogEntryIterator log_entry_iterator;

  fprintf(stderr, "manifest file path, %s\n", file_path.c_str());
  if (FAILED(log_entry_iterator.init(file_path))) {
    fprintf(stderr, "fail to init log entry itarator. %d\n", ret);
  } else {
    while(Status::kOk == ret) {
      if (FAILED(log_entry_iterator.get_next_log_entry(log_entry_header, log_data, log_length))) {
        if (Status::kIterEnd != ret) {
          fprintf(stderr, "fail to get next log_entry. %d\n", ret);
        }
      } else {
        parse_and_print_log_entry(log_entry_header, log_data, log_length);
      }
    }
  }
}

const std::string ManifestDumpCommand::ARG_VERBOSE = "verbose";
const std::string ManifestDumpCommand::ARG_JSON = "json";
const std::string ManifestDumpCommand::ARG_PATH = "path";

void ManifestDumpCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(ManifestDumpCommand::Name());
  ret.append(" [--" + ARG_VERBOSE + "]");
  ret.append(" [--" + ARG_JSON + "]");
  ret.append(" [--" + ARG_PATH + "=<path_to_manifest_file>]");
  ret.append("\n");
}

ManifestDumpCommand::ManifestDumpCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options,
                 flags,
                 BuildCmdLineOptions({ARG_VERBOSE, ARG_PATH, ARG_HEX, ARG_JSON})),
      verbose_(false),
      json_(false),
      path_("") {
  verbose_ = IsFlagPresent(flags, ARG_VERBOSE);
  json_ = IsFlagPresent(flags, ARG_JSON);

  std::map<std::string, std::string>::const_iterator itr =
      options.find(ARG_PATH);
  if (itr != options.end()) {
    path_ = itr->second;
    if (path_.empty()) {
      exec_state_ = LDBCommandExecuteResult::Failed("--path: missing pathname");
    }
  }
}


void ManifestDumpCommand::DoCommand()
{
  std::string manifest_file_path;
  if (!path_.empty()) {
    manifest_file_path = path_;
    dump_manifest_file(manifest_file_path);
  }
}
// ----------------------------------------------------------------------------

namespace {

/*
int dump_meta_array(StorageManager::MetaDB &header, 
                    util::SequentialFile *checkpoint_read,
                    bool print) {
  Slice result;
  Status s;
  int64_t pos = 0;
  int32_t meta_items = 0;         
  std::unique_ptr<char[]> print_buf(new char[StorageManager::BUFFER_SIZE]); 
  std::unique_ptr<char[]> buf(new char[StorageManager::BUFFER_SIZE]);

  ExtentMeta tmp;
  do {
    if (meta_items >= header.meta_num_) {
      break;  // read all extents
    }
    memset(buf.get(), 0, StorageManager::BUFFER_SIZE);
    s = checkpoint_read->Read(StorageManager::BUFFER_SIZE, &result, buf.get());
    if (!s.ok()) {
      fprintf(stderr, "Read checkpoint file failed: %d", s.code());
      break;
    }
    if (result.size() <= 0) {
      break;
    }
    pos = 0;
    while (Status::kOk == tmp.deserialize(buf.get(), StorageManager::BUFFER_SIZE, pos)) {
      meta_items++;
      if (print) {
        memset(print_buf.get(), 0, StorageManager::BUFFER_SIZE);
        tmp.to_string(print_buf.get(), StorageManager::BUFFER_SIZE);
        printf("%s\n", print_buf.get());
      }
    }
  } while (true);  // FIXME: some error may block here

  if (meta_items != header.meta_num_) {
    fprintf(stderr, "The meta array in checkpoint file is wrong.\n \
                     read %d items is not equal to header record %ld\n",
                     meta_items, header.meta_num_);
    return Status::kCorruption;
  }

  return Status::kOk;
}

int dump_large_object_array(StorageManager::MetaDB &header, 
                            util::SequentialFile *checkpoint_read,  
                            bool print) {
  Slice result;
  Status s;
  int64_t pos = 0;
  int64_t large_object_items = 0;
  std::unique_ptr<char[]> buf(new char[StorageManager::BUFFER_SIZE]);

  do {
    if (large_object_items >= header.large_object_num_) {
      break;  // read all extents
    }
    memset(buf.get(), 0, StorageManager::BUFFER_SIZE);
    s = checkpoint_read->Read(StorageManager::BUFFER_SIZE, &result, buf.get());
    if (!s.ok()) {
      fprintf(stderr, "Read the large object array failed %d\n", s.code());
      return s.code();
    }
    if (result.size() <= 0) {
      break;
    }

    std::string key;
    util::autovector<storage::ExtentId> oob_extents;
    pos = 0;
    do {
      int ret = Status::kOk;
      if (FAILED(util::deserialize(buf.get(), StorageManager::BUFFER_SIZE, pos, key))) {
        if (pos >= StorageManager::BUFFER_SIZE) {
          fprintf(stdout, "finish deserialize one block of large object: %ld\n", pos);
          break;
        } else {
          fprintf(stderr, "fail to deserialize large object: %d\n", ret);
          return ret;
        }
      } else if (key.empty()) {
        fprintf(stdout, "finish deserialize one block of large object: %ld\n", pos);
        // reaching the padding 0, or key won't be empty
        break;
      } else if (FAILED(util::deserialize_v(buf.get(), StorageManager::BUFFER_SIZE, pos, oob_extents))) {
        if (pos >= StorageManager::BUFFER_SIZE) {
          fprintf(stdout, "finish deserialize one block of large object: %ld\n", pos);
          break;
        } else {
          fprintf(stderr, "fail to deserialize large object: %d\n", ret);
          return ret;
        }
      }
      
      if (print) {
        printf("one large object - key: %s, extents: ", util::to_cstring(Slice(key)));
        for (auto& extent : oob_extents) {
          printf("[%d, %d] ", extent.file_number, extent.offset);
        }
        printf("\n");
      }
      oob_extents.clear();
      large_object_items++;
      if (large_object_items >= header.large_object_num_) {
        break;  // read all extents
      }
    } while (true);
  } while (true);

  return Status::kOk;  
}

int dump_last_meta_version(StorageManager::MetaDB &header,
                      util::SequentialFile *checkpoint_read,
                      std::unordered_map<Slice, std::pair<Slice, int64_t>, 
                      SliceHasher, SliceComparator> *extents) {
  int64_t i = 0;
  ParsedInternalKey ikey;
  Slice key;
  Slice value;
  Slice result;
  Status s;
  MetaKey meta_key;
  MetaValue meta_value;
  int64_t pos = 0;
  int ret = Status::kOk;
  int32_t offset = -1;
  std::unique_ptr<char[]> print_buf(new char[StorageManager::BUFFER_SIZE]);
  std::unique_ptr<char[]> buf(new char[StorageManager::BUFFER_SIZE]);
  char *p = nullptr;
  char *end = buf.get() + StorageManager::BUFFER_SIZE;
  char *key_value_buf = nullptr;
  Slice tmp_key;
  Slice tmp_value;
  bool print = (extents == nullptr);

  do {
    if (i >= header.entry_num_) {
      break;  // read all entries
    }
    memset(buf.get(), 0, StorageManager::BUFFER_SIZE);
    s = checkpoint_read->Read(StorageManager::BUFFER_SIZE, &result, buf.get());
    if (!s.ok()) {
      fprintf(stderr, "Read the checkpoint faild %s\n", s.ToString().c_str());
      return s.code();
    }
    if (result.size() <= 0) {
      break;  // no more data finished
    }
    p = buf.get();
    // parse one block
    while (p < end && i < header.entry_num_) {
      key = GetLengthPrefixedSlice(p);
      if (key.empty()) {
        break;  // eof of one block
      }
      if (ParseInternalKey(key, &ikey)) {
        value = GetLengthPrefixedSlice(key.data() + key.size());
        pos = 0;
        ret = meta_key.deserialize(ikey.user_key.data(),
                                   static_cast<int64_t>(ikey.user_key.size()), pos);
        if (ret != Status::kOk) {
          fprintf(stderr, "Deserialize the meta_key of checkpoint failed %d\n", ret);
          return ret;
        }
        pos = 0;
        if (print) {
          memset(print_buf.get(), 0, StorageManager::BUFFER_SIZE);
          meta_key.to_string(print_buf.get(), StorageManager::BUFFER_SIZE);
          printf("MetaKey: %s\n", print_buf.get());
        }
        ret = meta_value.deserialize(value.data(),
                                     static_cast<int64_t>(value.size()), pos);
        if (ret != Status::kOk) {
          fprintf(stderr, "Deserialize the meta_value of checkpoint failed %d\n", ret);
          return ret;
        }
        if (print) {
          memset(print_buf.get(), 0, StorageManager::BUFFER_SIZE);
          meta_value.to_string(print_buf.get(), StorageManager::BUFFER_SIZE);
          printf("MetaValue: %s\n", print_buf.get());
        }
        if (extents != nullptr) {
          key_value_buf = static_cast<char*>(base_malloc(meta_key.largest_key_.size() +
                          meta_value.smallest_key_.size(), 
                          ModId::kBackupCheck));
          if (key_value_buf == nullptr) {
            fprintf(stderr, "malloc key value buff failed\n");
            return Status::kMemoryLimit;
          }
          memcpy(key_value_buf, meta_key.largest_key_.data(), meta_key.largest_key_.size());
          memcpy(key_value_buf + meta_key.largest_key_.size(), meta_value.smallest_key_.data(),
                 meta_value.smallest_key_.size());
          tmp_key = Slice(key_value_buf, meta_key.largest_key_.size());
          tmp_value = Slice(key_value_buf + meta_key.largest_key_.size(), meta_value.smallest_key_.size());
          (*extents)[tmp_key] =
                     std::make_pair(tmp_value, meta_value.extent_id_.id());
        }
        i++;
      } else {
        fprintf(stderr, "Parse checkpoint internal key failed.");
        return Status::kCorruption;
      }
      p = const_cast<char *>(value.data() + value.size());
    } 
  } while (true);

  if (i != header.entry_num_) {
    fprintf(stderr, "The entries of checkpoint meta is not equal to the header\n \
                     read %ld items vs header record %ld\n", i, header.entry_num_);
    return Status::kCorruption;
  }
  
  return Status::kOk;
}
*/

}  // namespace

/*
int CheckpointDumpCommand::dump_checkpoint_file(std::string ckpname, bool verbose, 
                                                bool hex, bool json,
                                                std::unordered_map<Slice, std::pair<Slice, int64_t>, 
                                                SliceHasher, SliceComparator> *extents) {
  Status s;
  EnvOptions opt_env_opts;
  opt_env_opts.use_direct_reads = false;
  std::unique_ptr<util::SequentialFile> checkpoint_read;
  Options options;
  bool print = (extents == nullptr);

  s = options.env->NewSequentialFile(
                    ckpname, &checkpoint_read, opt_env_opts);
  if (!s.ok()) {
    fprintf(stderr, "Open checkpoint file for read failed %d\n", s.code());
    return s.code();
  }

  StorageManager::MetaDB header;
  int ret = Status::kOk;
  do {
    ret = StorageManager::read_checkpoint_header(header, checkpoint_read.get());
    if (ret != Status::kOk) {
      fprintf(stderr, "Read checkpoint header failed %d", ret);
      return ret;
    }
    if (-1 == header.cfd_id_) {
      break; // finished
    }

    printf("-----------------------------dump extent meta array %d-----------------------\n", header.cfd_id_);
    if (header.meta_num_ > 0) {
      ret = dump_meta_array(header, checkpoint_read.get(), print);
      if (ret != Status::kOk) {
        fprintf(stderr, "Dump the meta array failed code %d\n", ret);
        return ret;
      } 
    }
    printf("-----------------------------dump large object %d----------------------------\n", header.cfd_id_);
    if (header.large_object_num_ > 0) {
      ret = dump_large_object_array(header, checkpoint_read.get(), print);
      if (ret != Status::kOk) {
        fprintf(stderr, "Dump the large object array failed code %d\n", ret);
        return ret;
      }
    }
    printf("-----------------------------dump last meta version %d-----------------------\n", header.cfd_id_);
    if (header.entry_num_ > 0) {
      ret = dump_last_meta_version(header, checkpoint_read.get(), extents);
      if (ret != Status::kOk) {
        fprintf(stderr, "Dump the last meta version failed code %d\n", ret);
        return ret;
      }
    }
  } while (true);

  return ret;
}
*/

int CheckpointDumpCommand::dump_checkpoint_file()
{
  int ret = Status::kOk;
//  std::unique_ptr<util::RandomAccessFile> checkpoint_reader;
  RandomAccessFile *checkpoint_reader = nullptr;
  util::EnvOptions env_options;
  util::Env *env = util::Env::Default();
  CheckpointHeader *header = nullptr;
  int64_t header_buffer_size = DEFAULT_BUFFER_SIZE;
  char *header_buffer = new char[header_buffer_size]; // todo
  memset(header_buffer, 0, header_buffer_size);
  header = reinterpret_cast<CheckpointHeader *>(header_buffer);
  Slice result;

  if (FAILED(env->FileExists(path_).code())) {
    fprintf(stderr, "file not exist. %d, %s\n", ret, path_.c_str());
  } else if (FAILED(env->NewRandomAccessFile(path_, checkpoint_reader, env_options).code())) {
    fprintf(stderr, "fail to open checkpoint file. %d, %s\n", ret, path_.c_str());
  } else if (FAILED(checkpoint_reader->Read(0 /*file_offset*/, header_buffer_size, &result, header_buffer).code())) {
    fprintf(stderr, "fail to reader checkpoint header. %d\n", ret);
  } else if (FAILED(dump_extent_space_mgr_checkpoint(checkpoint_reader, header))) {
    fprintf(stderr, "fail to dump extent space manager checkpoint. %d\n", ret);
  } else if (FAILED(dump_version_set_checkpoint(checkpoint_reader, header))) {
    fprintf(stderr, "fail to dump version set checkpoint. %d\n", ret);
  }
  MOD_DELETE_OBJECT(RandomAccessFile, checkpoint_reader);
  return ret;
}

int CheckpointDumpCommand::dump_extent_space_mgr_checkpoint(util::RandomAccessFile *checkpoint_reader, CheckpointHeader *header)
{
  int ret = Status::kOk;
  char *buf = nullptr;
  int64_t buf_size = DEFAULT_BUFFER_SIZE;
  char *string_buf = nullptr;
  int64_t string_buf_size = DEFAULT_TO_STRING_SIZE;
  int64_t pos = 0;
  int64_t string_buf_pos = 0;
  CheckpointBlockHeader *block_header = nullptr;
  Slice result;
  int64_t block_index = 0;
  int64_t offset = header->extent_meta_block_offset_;

  fprintf(stderr, "begin dump extent space mgr checkpoint\n");
  if (nullptr == checkpoint_reader || nullptr == header) {
    ret = Status::kInvalidArgument;
    fprintf(stderr, "invalid argument. %p, %p\n", checkpoint_reader, header);
  } else if (nullptr == (buf = new char[buf_size])) {
    ret = Status::kMemoryLimit;
    fprintf(stderr, "fail to allocate memory for buf. %d\n", ret);
  } else if (nullptr == (string_buf = new char[string_buf_size])) {
    ret = Status::kMemoryLimit;
    fprintf(stderr, "fail to allocate memory for string bug. %d\n", ret);
  } else {
    while ((Status::kOk == ret) && block_index < header->extent_meta_block_count_) {
      pos = 0;
      if (FAILED(checkpoint_reader->Read(offset, buf_size, &result, buf).code())) {
        fprintf(stderr, "fail to read buf. %d\n", ret);
      } else {
        block_header = reinterpret_cast<CheckpointBlockHeader *>(buf);
        pos = block_header->data_offset_;
        for (int64_t i = 0; SUCCED(ret) && i < block_header->entry_count_; ++i) {
          ExtentMeta extent_meta;
          if (FAILED(extent_meta.deserialize(buf, buf_size, pos))) {
            SE_LOG(WARN, "fail to deserialize extent meta", K(ret));
          } else {
            string_buf_pos = extent_meta.to_string(string_buf, string_buf_size);
            string_buf[string_buf_pos] = '\0';
            fprintf(stderr, "%s\n", string_buf);
          }
        }
        ++block_index;
        offset += buf_size;
      }
    }
  }

  return ret;
}

int CheckpointDumpCommand::dump_version_set_checkpoint(util::RandomAccessFile *checkpoint_reader, CheckpointHeader *header)
{
  int ret = Status::kOk;
  char *buf = nullptr;
  int64_t buf_size = DEFAULT_BUFFER_SIZE;
  char *string_buf = nullptr;
  int64_t string_buf_size = DEFAULT_TO_STRING_SIZE;
  int64_t string_pos = 0;
  int64_t pos = 0;
  int64_t block_index = 0;
  int64_t offset = header->sub_table_meta_block_offset_;
  CheckpointBlockHeader *block_header = nullptr;
  common::Options options;
  common::ColumnFamilyOptions cf_options;
  CreateSubTableArgs dummy_args(cf_options);
  Slice result;

  if (nullptr == checkpoint_reader || nullptr == header) {
    ret = Status::kInvalidArgument;
    fprintf(stderr, "invalid argument. %d, %p, %p\n", ret, checkpoint_reader, header);
  } else if (nullptr == (buf = new char[buf_size])) {
    ret = Status::kMemoryLimit;
    fprintf(stderr, "fail to allocate memory for buf. %d\n", ret);
  } else if (nullptr == (string_buf = new char[string_buf_size])) {
    ret = Status::kMemoryLimit;
    fprintf(stderr, "fail to allocate memory for string bug. %d\n", ret);
  } else {
    memset(buf, '\0', buf_size);
    memset(string_buf, '\0', string_buf_size);
    while (Status::kOk == ret && block_index < header->sub_table_meta_block_count_) {
      pos = 0;
      if (FAILED(checkpoint_reader->Read(offset, buf_size, &result, buf).code())) {
        fprintf(stderr, "fail to read buf. %d\n", ret);
      } else {
        block_header = reinterpret_cast<CheckpointBlockHeader *>(buf);
        pos = block_header->data_offset_;
        if (block_header->block_size_ > buf_size) {
          if (FAILED(read_big_subtable(checkpoint_reader, block_header->block_size_, offset))) {
            fprintf(stderr, "fail to read big subtable. %d\n", ret);
          }
        } else {
          for (int64_t i = 0; Status::kOk == ret && i < block_header->entry_count_; ++i) {
            ColumnFamilyData sub_table(options);
            string_pos = 0;
            if (FAILED(sub_table.deserialize_and_dump(buf, buf_size, pos, string_buf, string_buf_size, string_pos))) {
              fprintf(stderr, "fail to deserialize subtable. %d\n", ret);
            } else {
              string_buf[string_pos] = '\0';
              fprintf(stderr, "%s\n", string_buf);
            }
          }
          offset += buf_size;
        }
      }
      ++block_index;
    }
  }

  if (nullptr != buf) {
    delete[] buf;
    buf = nullptr;
  }

  return ret;
}

int CheckpointDumpCommand::read_big_subtable(util::RandomAccessFile *checkpoint_reader,
                                             int64_t block_size,
                                             int64_t &file_offset)
{
  int ret = Status::kOk;
  char *buf = nullptr;
  int64_t buf_size = block_size;
  char *string_buf = nullptr;
  int64_t string_buf_size = block_size;
  CheckpointBlockHeader *block_header = nullptr;
  Slice result;
  int64_t pos = 0;
  int64_t string_buf_pos = 0;
  common::Options options;
  ColumnFamilyData sub_table(options);

  if (nullptr == checkpoint_reader || block_size <= 0 || file_offset <= 0) {
    ret = Status::kOk;
    fprintf(stderr, "invalid argument. %d\n", ret);
  } else if (nullptr  == (buf = new char[buf_size])) {
    ret = Status::kMemoryLimit;
    fprintf(stderr, "fail to allocate memory for buf. %d\n", ret);
  } else if (nullptr  == (string_buf = new char[string_buf_size])) {
    ret = Status::kMemoryLimit;
    fprintf(stderr, "fail to allocate memory for string buf. %d\n", ret);
  } else if (FAILED(checkpoint_reader->Read(file_offset, buf_size, &result, buf).code())) {
    fprintf(stderr, "fail to read buf. %d\n", ret);
  } else {
    block_header = reinterpret_cast<CheckpointBlockHeader *>(buf);
    pos = block_header->data_offset_;
    if (1 != block_header->entry_count_) {
      ret = Status::kCorruption;
      fprintf(stderr, "entry count not 1. %d, %ld\n", ret, block_header->entry_count_);
    } else if (FAILED(sub_table.deserialize(buf, buf_size, pos))) {
      fprintf(stderr, "fail to deserialize subtable. %d\n", ret);
    } else {
      string_buf_pos = sub_table.to_string(string_buf, string_buf_size);
      string_buf[string_buf_pos] = '\0';
      fprintf(stderr, "%s\n", string_buf);
      file_offset += buf_size;
    }
  }

  if (nullptr != buf) {
    delete[] buf;
    buf = nullptr;
  }

  return ret;
}
const std::string CheckpointDumpCommand::ARG_VERBOSE = "verbose";
const std::string CheckpointDumpCommand::ARG_JSON = "json";
const std::string CheckpointDumpCommand::ARG_PATH = "path";

void CheckpointDumpCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(CheckpointDumpCommand::Name());
  //ret.append(" [--" + ARG_VERBOSE + "]");
  //ret.append(" [--" + ARG_JSON + "]");
  ret.append(" [--" + ARG_PATH + "=<path_to_manifest_file>]");
  ret.append("\n");
}

CheckpointDumpCommand::CheckpointDumpCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options,
                 flags,
                 BuildCmdLineOptions({ARG_VERBOSE, ARG_PATH, ARG_HEX, ARG_JSON})),
      verbose_(false),
      json_(false),
      path_("") {
  verbose_ = IsFlagPresent(flags, ARG_VERBOSE);
  json_ = IsFlagPresent(flags, ARG_JSON);

  std::map<std::string, std::string>::const_iterator itr =
      options.find(ARG_PATH);
  if (itr != options.end()) {
    path_ = itr->second;
    if (path_.empty()) {
      exec_state_ = LDBCommandExecuteResult::Failed("--path: missing pathname");
    }
  }
}

void CheckpointDumpCommand::DoCommand() {
  std::string checkpointfile;

  fprintf(stderr, "dongsheng dump checkpoint file %s\n", path_.c_str());
  if (!path_.empty()) {
    checkpointfile = path_;
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(
                  "No checkpoint file specify; use --path to set one");
    return;
  }

  if (verbose_) {
    printf("Processing Manifest file %s\n", checkpointfile.c_str());
  }

  //TODO:yuanfeng
  //dump_checkpoint_file(checkpointfile, verbose_, is_key_hex_, json_);
  dump_checkpoint_file();

  if (verbose_) {
    printf("Processing Manifest file %s done\n", checkpointfile.c_str());
  }
}

// ----------------------------------------------------------------------------

void ListColumnFamiliesCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(ListColumnFamiliesCommand::Name());
  ret.append(" full_path_to_db_directory ");
  ret.append("\n");
}

ListColumnFamiliesCommand::ListColumnFamiliesCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, {}) {
  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "dbname must be specified for the list_column_families command");
  } else {
    dbname_ = params[0];
  }
}

void ListColumnFamiliesCommand::DoCommand() {
  std::vector<std::string> column_families;
  printf("Column families in %s: \n{", dbname_.c_str());
  bool first = true;
  for (auto cf : column_families) {
    if (!first) {
      printf(", ");
    }
    first = false;
    printf("%s", cf.c_str());
  }
  printf("}\n");
}

void CreateColumnFamilyCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(CreateColumnFamilyCommand::Name());
  ret.append(" --db=<db_path> <new_column_family_name>");
  ret.append("\n");
}

CreateColumnFamilyCommand::CreateColumnFamilyCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, {ARG_DB}) {
  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "new sub table name must be specified");
  } else {
    new_cf_name_ = params[0];
  }
}

void CreateColumnFamilyCommand::DoCommand() {
  //TODO:yuanfeng
  /*
  ColumnFamilyHandle* new_cf_handle;
  Status st = db_->CreateColumnFamily(options_, new_cf_name_, &new_cf_handle);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "Fail to create new sub table: " + st.ToString());
  }
  delete new_cf_handle;
  CloseDB();
  */
}

namespace {

struct StdErrReporter : public log::Reader::Reporter {
  virtual void Corruption(size_t bytes, const Status& s) override {
    std::cerr << "Corruption detected in log file " << s.ToString() << "\n";
  }
};

class InMemoryHandler : public WriteBatch::Handler {
 public:
  InMemoryHandler(std::stringstream& row, bool print_values)
      : Handler(), row_(row) {
    print_values_ = print_values;
  }

  void commonPutMerge(const Slice& key, const Slice& value) {
    std::string k = LDBCommand::StringToHex(key.ToString());
    if (print_values_) {
      std::string v = LDBCommand::StringToHex(value.ToString());
      row_ << k << " : ";
      row_ << v << " ";
    } else {
      row_ << k << " ";
    }
  }

  virtual Status PutCF(uint32_t cf, const Slice& key,
                       const Slice& value) override {
    row_ << "PUT(" << cf << ") : ";
    commonPutMerge(key, value);
    return Status::OK();
  }

  virtual Status DeleteCF(uint32_t cf, const Slice& key) override {
    row_ << "DELETE(" << cf << ") : ";
    row_ << LDBCommand::StringToHex(key.ToString()) << " ";
    return Status::OK();
  }

  virtual Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
    row_ << "SINGLE_DELETE(" << cf << ") : ";
    row_ << LDBCommand::StringToHex(key.ToString()) << " ";
    return Status::OK();
  }

  virtual Status MarkBeginPrepare() override {
    row_ << "BEGIN_PREARE ";
    return Status::OK();
  }

  virtual Status MarkEndPrepare(const Slice& xid, SequenceNumber) override {
    row_ << "END_PREPARE(";
    row_ << LDBCommand::StringToHex(xid.ToString()) << ") ";
    return Status::OK();
  }

  virtual Status MarkRollback(const Slice& xid, SequenceNumber) override {
    row_ << "ROLLBACK(";
    row_ << LDBCommand::StringToHex(xid.ToString()) << ") ";
    return Status::OK();
  }

  virtual Status MarkCommit(const Slice& xid, SequenceNumber) override {
    row_ << "COMMIT(";
    row_ << LDBCommand::StringToHex(xid.ToString()) << ") ";
    return Status::OK();
  }

  virtual ~InMemoryHandler() override {}

 private:
  std::stringstream& row_;
  bool print_values_;
};

void DumpWalFile(std::string wal_file, bool print_header, bool print_values,
                 LDBCommandExecuteResult* exec_state) {
  Env* env_ = Env::Default();
  EnvOptions soptions;
  unique_ptr<SequentialFileReader> wal_file_reader_ptr;
  unique_ptr<SequentialFile, ptr_destruct_delete<SequentialFile>> file_ptr;
  SequentialFileReader *wal_file_reader = nullptr;
  Status status;
  {
    SequentialFile *file = nullptr;
    status = env_->NewSequentialFile(wal_file, file, soptions);
    file_ptr.reset(file);
    if (status.ok()) {
//      wal_file_reader.reset(new SequentialFileReader(std::move(file)));
      wal_file_reader = new SequentialFileReader(file);
      wal_file_reader_ptr.reset(wal_file_reader);
    }
  }
  if (!status.ok()) {
    if (exec_state) {
      *exec_state = LDBCommandExecuteResult::Failed("Failed to open WAL file " +
                                                    status.ToString());
    } else {
      std::cerr << "Error: Failed to open WAL file " << status.ToString()
                << std::endl;
    }
  } else {
    StdErrReporter reporter;
    int64_t log_number = 0;
    FileType type = util::kInvalidFileType;

    // we need the log number, but ParseFilename expects dbname/NNN.log.
    std::string sanitized = wal_file;
    size_t lastslash = sanitized.rfind('/');
    if (lastslash != std::string::npos)
      sanitized = sanitized.substr(lastslash + 1);
    if (Status::kOk != FileNameUtil::parse_file_name(sanitized, log_number, type)) {
      // bogus input, carry on as best we can
      log_number = 0;
    }
    DBOptions db_options;
    log::Reader reader(wal_file_reader, &reporter, true, 0, log_number);
    std::string scratch;
    WriteBatch batch;
    Slice record;
    std::stringstream row;
    if (print_header) {
      std::cout << "Sequence,Count,ByteSize,Physical Offset,Key(s)";
      if (print_values) {
        std::cout << " : value ";
      }
      std::cout << "\n";
    }
    while (reader.ReadRecord(&record, &scratch)) {
      row.str("");
      if (record.size() < WriteBatchInternal::kHeader) {
        reporter.Corruption(record.size(),
                            Status::Corruption("log record too small"));
      } else {
        WriteBatchInternal::SetContents(&batch, record);
        row << WriteBatchInternal::Sequence(&batch) << ",";
        row << WriteBatchInternal::Count(&batch) << ",";
        row << WriteBatchInternal::ByteSize(&batch) << ",";
        row << reader.LastRecordOffset() << ",";
        InMemoryHandler handler(row, print_values);
        batch.Iterate(&handler);
        row << "\n";
      }
      std::cout << row.str();
    }
  }
}

}  // namespace

const std::string WALDumperCommand::ARG_WAL_FILE = "walfile";
const std::string WALDumperCommand::ARG_PRINT_VALUE = "print_value";
const std::string WALDumperCommand::ARG_PRINT_HEADER = "header";

WALDumperCommand::WALDumperCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options,
                 flags,
                 BuildCmdLineOptions({ARG_WAL_FILE, ARG_PRINT_HEADER, ARG_PRINT_VALUE})),
      print_header_(false),
      print_values_(false) {
  wal_file_.clear();

  std::map<std::string, std::string>::const_iterator itr =
      options.find(ARG_WAL_FILE);
  if (itr != options.end()) {
    wal_file_ = itr->second;
  }

  print_header_ = IsFlagPresent(flags, ARG_PRINT_HEADER);
  print_values_ = IsFlagPresent(flags, ARG_PRINT_VALUE);
  if (wal_file_.empty()) {
    exec_state_ = LDBCommandExecuteResult::Failed("Argument " + ARG_WAL_FILE +
                                                  " must be specified.");
  }
}

void WALDumperCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(WALDumperCommand::Name());
  ret.append(" --" + ARG_WAL_FILE + "=<write_ahead_log_file_path>");
  ret.append(" [--" + ARG_PRINT_HEADER + "] ");
  ret.append(" [--" + ARG_PRINT_VALUE + "] ");
  ret.append("\n");
}

void WALDumperCommand::DoCommand() {
  DumpWalFile(wal_file_, print_header_, print_values_, &exec_state_);
}

// ----------------------------------------------------------------------------

GetCommand::GetCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options,
                 flags,
                 BuildCmdLineOptions({ARG_TTL, ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX})) {
  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "<key> must be specified for the get command");
  } else {
    key_ = params.at(0);
  }

  if (is_key_hex_) {
    key_ = HexToString(key_);
  }
}

void GetCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(GetCommand::Name());
  ret.append(" <key>");
  ret.append(" [--" + ARG_TTL + "]");
  ret.append("\n");
}

void GetCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  std::string value;
  Status st = db_->Get(ReadOptions(), GetCfHandle(), key_, &value);
  if (st.ok()) {
    fprintf(stdout, "%s\n",
            (is_value_hex_ ? StringToHex(value) : value).c_str());
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
  }
}

// ----------------------------------------------------------------------------

ApproxSizeCommand::ApproxSizeCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options,
                 flags,
                 BuildCmdLineOptions(
                     {ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX, ARG_FROM, ARG_TO})) {
  if (options.find(ARG_FROM) != options.end()) {
    start_key_ = options.find(ARG_FROM)->second;
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(
        ARG_FROM + " must be specified for approxsize command");
    return;
  }

  if (options.find(ARG_TO) != options.end()) {
    end_key_ = options.find(ARG_TO)->second;
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(
        ARG_TO + " must be specified for approxsize command");
    return;
  }

  if (is_key_hex_) {
    start_key_ = HexToString(start_key_);
    end_key_ = HexToString(end_key_);
  }
}

void ApproxSizeCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(ApproxSizeCommand::Name());
  ret.append(HelpRangeCmdArgs());
  ret.append("\n");
}

void ApproxSizeCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  db::Range ranges[1];
  ranges[0] = db::Range(start_key_, end_key_);
  uint64_t sizes[1];
  db_->GetApproximateSizes(GetCfHandle(), ranges, 1, sizes);
  fprintf(stdout, "%lu\n", (unsigned long)sizes[0]);
  /* Weird that GetApproximateSizes() returns void, although documentation
   * says that it returns a Status object.
  if (!st.ok()) {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
  }
  */
}

// ----------------------------------------------------------------------------

BatchPutCommand::BatchPutCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options,
                 flags,
                 BuildCmdLineOptions({ARG_TTL, ARG_HEX, ARG_KEY_HEX,
                                      ARG_VALUE_HEX, ARG_CREATE_IF_MISSING})) {
  if (params.size() < 2) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "At least one <key> <value> pair must be specified batchput.");
  } else if (params.size() % 2 != 0) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "Equal number of <key>s and <value>s must be specified for batchput.");
  } else {
    for (size_t i = 0; i < params.size(); i += 2) {
      std::string key = params.at(i);
      std::string value = params.at(i + 1);
      key_values_.push_back(std::pair<std::string, std::string>(
          is_key_hex_ ? HexToString(key) : key,
          is_value_hex_ ? HexToString(value) : value));
    }
  }
}

void BatchPutCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(BatchPutCommand::Name());
  ret.append(" <key> <value> [<key> <value>] [..]");
  ret.append(" [--" + ARG_TTL + "]");
  ret.append("\n");
}

void BatchPutCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  WriteBatch batch;

  for (std::vector<std::pair<std::string, std::string>>::const_iterator itr =
           key_values_.begin();
       itr != key_values_.end(); ++itr) {
    batch.Put(GetCfHandle(), itr->first, itr->second);
  }
  Status st = db_->Write(WriteOptions(), &batch);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
  }
}

Options BatchPutCommand::PrepareOptionsForOpenDB() {
  Options opt = LDBCommand::PrepareOptionsForOpenDB();
  return opt;
}

DeleteCommand::DeleteCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options,
                 flags,
                 BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX})) {
  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "KEY must be specified for the delete command");
  } else {
    key_ = params.at(0);
    if (is_key_hex_) {
      key_ = HexToString(key_);
    }
  }
}

void DeleteCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(DeleteCommand::Name() + " <key>");
  ret.append("\n");
}

void DeleteCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  Status st = db_->Delete(WriteOptions(), GetCfHandle(), key_);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
  }
}

PutCommand::PutCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options,
                 flags,
                 BuildCmdLineOptions({ARG_TTL, ARG_HEX, ARG_KEY_HEX,
                                      ARG_VALUE_HEX, ARG_CREATE_IF_MISSING})) {
  if (params.size() != 2) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "<key> and <value> must be specified for the put command");
  } else {
    key_ = params.at(0);
    value_ = params.at(1);
  }

  if (is_key_hex_) {
    key_ = HexToString(key_);
  }

  if (is_value_hex_) {
    value_ = HexToString(value_);
  }
}

void PutCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(PutCommand::Name());
  ret.append(" <key> <value> ");
  ret.append(" [--" + ARG_TTL + "]");
  ret.append("\n");
}

void PutCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  Status st = db_->Put(WriteOptions(), GetCfHandle(), key_, value_);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
  }
}

Options PutCommand::PrepareOptionsForOpenDB() {
  Options opt = LDBCommand::PrepareOptionsForOpenDB();
  return opt;
}

// ----------------------------------------------------------------------------

const char* DBQuerierCommand::HELP_CMD = "help";
const char* DBQuerierCommand::GET_CMD = "get";
const char* DBQuerierCommand::PUT_CMD = "put";
const char* DBQuerierCommand::DELETE_CMD = "delete";

DBQuerierCommand::DBQuerierCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options,
                 flags,
                 BuildCmdLineOptions({ARG_TTL, ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX})) {

}

void DBQuerierCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(DBQuerierCommand::Name());
  ret.append(" [--" + ARG_TTL + "]");
  ret.append("\n");
  ret.append(
      "    Starts a REPL shell.  Type help for list of available "
      "commands.");
  ret.append("\n");
}

void DBQuerierCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }

  ReadOptions read_options;
  WriteOptions write_options;

  std::string line;
  std::string key;
  std::string value;
  while (getline(std::cin, line, '\n')) {
    // Parse line into std::vector<std::string>
    std::vector<std::string> tokens;
    size_t pos = 0;
    while (true) {
      size_t pos2 = line.find(' ', pos);
      if (pos2 == std::string::npos) {
        break;
      }
      tokens.push_back(line.substr(pos, pos2 - pos));
      pos = pos2 + 1;
    }
    tokens.push_back(line.substr(pos));

    const std::string& cmd = tokens[0];

    if (cmd == HELP_CMD) {
      fprintf(stdout,
              "get <key>\n"
              "put <key> <value>\n"
              "delete <key>\n");
    } else if (cmd == DELETE_CMD && tokens.size() == 2) {
      key = (is_key_hex_ ? HexToString(tokens[1]) : tokens[1]);
      db_->Delete(write_options, GetCfHandle(), Slice(key));
      fprintf(stdout, "Successfully deleted %s\n", tokens[1].c_str());
    } else if (cmd == PUT_CMD && tokens.size() == 3) {
      key = (is_key_hex_ ? HexToString(tokens[1]) : tokens[1]);
      value = (is_value_hex_ ? HexToString(tokens[2]) : tokens[2]);
      db_->Put(write_options, GetCfHandle(), Slice(key), Slice(value));
      fprintf(stdout, "Successfully put %s %s\n", tokens[1].c_str(),
              tokens[2].c_str());
    } else if (cmd == GET_CMD && tokens.size() == 2) {
      key = (is_key_hex_ ? HexToString(tokens[1]) : tokens[1]);
      if (db_->Get(read_options, GetCfHandle(), Slice(key), &value).ok()) {
        fprintf(stdout, "%s\n",
                PrintKeyValue(key, value, is_key_hex_, is_value_hex_).c_str());
      } else {
        fprintf(stdout, "Not found %s\n", tokens[1].c_str());
      }
    } else {
      fprintf(stdout, "Unknown command %s\n", line.c_str());
    }
  }
}

}  // tools
}  // namespace smartengine