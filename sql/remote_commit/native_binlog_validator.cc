/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/materializer.h"

#include <filesystem>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "my_inttypes.h"
#include "sql/binlog_istream.h"
#include "sql/binlog_reader.h"
#include "sql/log_event.h"
#include "mysql/binlog/event/trx_boundary_parser.h"

namespace wesql::remote_commit {
namespace {

namespace fs = std::filesystem;

bool reject(std::string *error, std::string detail) {
  if (error != nullptr) *error = std::move(detail);
  return false;
}

bool exact_rotate_target(const Rotate_log_event &rotate,
                         const fs::path &next_file) {
  if (rotate.new_log_ident == nullptr || rotate.ident_len == 0 ||
      rotate.pos != BIN_LOG_HEADER_SIZE)
    return false;
  const std::string target(rotate.new_log_ident, rotate.ident_len);
  const fs::path target_path(target);
  return target_path == target_path.filename() &&
         target == next_file.filename().string();
}

}  // namespace

bool NativeBinlogImageValidator::validate(
    const std::vector<fs::path> &files, const Cursor &durable_cursor,
    std::string *error) {
  if (error != nullptr) error->clear();
  if (max_event_bytes_ == 0 || files.empty() || durable_cursor.file.empty())
    return reject(error, "empty native binlog validation input");

  std::set<fs::path> unique_files;
  mysql::binlog::event::Transaction_boundary_parser parser(
      mysql::binlog::event::Transaction_boundary_parser::
          TRX_BOUNDARY_PARSER_RECEIVER);

  for (size_t file_index = 0; file_index < files.size(); ++file_index) {
    const fs::path &file = files[file_index];
    std::error_code filesystem_error;
    if (!unique_files.insert(file.lexically_normal()).second ||
        !fs::is_regular_file(file, filesystem_error) || filesystem_error)
      return reject(error,
                    "native binlog path is duplicated or not a regular file: " +
                        file.string());

    Binlog_file_reader reader(true, max_event_bytes_);
    if (reader.open(file.c_str()))
      return reject(error, "cannot open reconstructed native binlog: " +
                               file.string() + ": " +
                               reader.get_error_str());

    bool first_event = true;
    bool saw_rotate = false;
    my_off_t prior_position = reader.position();
    while (Log_event *raw_event = reader.read_event_object()) {
      std::unique_ptr<Log_event> event(raw_event);
      const my_off_t event_end = reader.position();
      if (event_end <= prior_position || !event->is_valid())
        return reject(error,
                      "native binlog contains a non-progressing or invalid "
                      "event: " +
                          file.string());
      prior_position = event_end;

      const auto event_type = event->get_type_code();
      if (first_event) {
        first_event = false;
        if (event_type != mysql::binlog::event::FORMAT_DESCRIPTION_EVENT)
          return reject(error,
                        "native binlog does not begin with an FDE: " +
                            file.string());
      } else if (saw_rotate) {
        return reject(error,
                      "native binlog contains an event after its terminal "
                      "Rotate event: " +
                          file.string());
      }

      if (event_type == mysql::binlog::event::ANONYMOUS_GTID_LOG_EVENT)
        return reject(error,
                      "native binlog contains an anonymous GTID event: " +
                          file.string());

      if (event_type == mysql::binlog::event::ROTATE_EVENT) {
        if (!parser.is_not_inside_transaction() ||
            file_index + 1 == files.size())
          return reject(error,
                        "native binlog Rotate event is not a file boundary: " +
                            file.string());
        const auto *rotate = dynamic_cast<const Rotate_log_event *>(event.get());
        if (rotate == nullptr ||
            !exact_rotate_target(*rotate, files[file_index + 1]))
          return reject(error,
                        "native binlog Rotate target does not match the next "
                        "file: " +
                            file.string());
        saw_rotate = true;
      }

      bool info_error = false;
      mysql::binlog::event::Log_event_basic_info event_info;
      std::tie(info_error, event_info) =
          extract_log_event_basic_info(event.get());
      if (info_error || parser.feed_event(event_info, false))
        return reject(error,
                      "native binlog violates transaction event ordering: " +
                          file.string());
    }

    if (first_event ||
        reader.get_error_type() != Binlog_read_error::READ_EOF)
      return reject(error, "native binlog is truncated or unreadable: " +
                               file.string() + ": " +
                               reader.get_error_str());
    if (parser.is_error() || parser.is_inside_transaction())
      return reject(error,
                    "native binlog ends inside an incomplete transaction: " +
                        file.string());
    if (file_index + 1 < files.size() && !saw_rotate)
      return reject(error,
                    "native binlog file has no terminal Rotate event: " +
                        file.string());

    const my_off_t file_size = reader.ifile()->length();
    if (reader.position() != file_size)
      return reject(error,
                    "native binlog reader did not consume the exact file: " +
                        file.string());
    if (file_index + 1 == files.size() &&
        (file.filename().string() != durable_cursor.file ||
         static_cast<uint64_t>(file_size) != durable_cursor.pos))
      return reject(error,
                    "native binlog endpoint differs from HEAD durable cursor");
  }

  return true;
}

}  // namespace wesql::remote_commit
