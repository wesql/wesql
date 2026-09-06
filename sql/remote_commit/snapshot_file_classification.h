/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_SNAPSHOT_FILE_CLASSIFICATION_INCLUDED
#define SQL_REMOTE_COMMIT_SNAPSHOT_FILE_CLASSIFICATION_INCLUDED

#include <charconv>
#include <cstdint>
#include <filesystem>
#include <string>

namespace wesql::remote_commit {

inline bool is_mysql_dictionary_snapshot_file(
    const std::filesystem::path &relative) {
  if (relative.empty() || relative.is_absolute()) return false;
  for (const auto &part : relative)
    if (part == "." || part == "..") return false;
  const auto encoded = relative.generic_string();
  const auto first = relative.begin()->generic_string();
  if (encoded == "auto.cnf" || encoded == "mysql.ibd" ||
      encoded == "mysql_upgrade_history" || first == "mysql" ||
      first == "sys" || first == "performance_schema")
    return true;

  // dd::sdi_file stores non-tablespace SDI as schema/table-prefix_ID.sdi.
  if (!relative.has_parent_path() ||
      relative.parent_path().has_parent_path() || relative.extension() != ".sdi")
    return false;
  const std::string stem = relative.stem().string();
  const auto separator = stem.rfind('_');
  if (separator == std::string::npos || separator == 0 ||
      separator + 1 == stem.size())
    return false;
  const char *start = stem.data() + separator + 1;
  const char *end = stem.data() + stem.size();
  uint64_t id = 0;
  const auto parsed = std::from_chars(start, end, id);
  return parsed.ec == std::errc() && parsed.ptr == end && id != 0 &&
         *start != '0';
}

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_SNAPSHOT_FILE_CLASSIFICATION_INCLUDED
