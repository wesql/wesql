/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_PERSISTENT_ENGINE_POLICY_INCLUDED
#define SQL_REMOTE_COMMIT_PERSISTENT_ENGINE_POLICY_INCLUDED

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace wesql::remote_commit {

// Temporary and internal tables are outside the remote durable-state contract.
// Persistent user tables must be materializable from the remote snapshot.
enum class TableEngineScope : std::uint8_t {
  PERSISTENT_USER,
  USER_TEMPORARY,
  INTERNAL,
};

constexpr char ascii_lower(char value) {
  return value >= 'A' && value <= 'Z' ? static_cast<char>(value - 'A' + 'a')
                                      : value;
}

constexpr bool ascii_case_equal(std::string_view left,
                                std::string_view right) {
  if (left.size() != right.size()) return false;
  for (std::size_t index = 0; index < left.size(); ++index) {
    if (ascii_lower(left[index]) != ascii_lower(right[index])) return false;
  }
  return true;
}

constexpr bool supported_persistent_user_engine(std::string_view engine) {
  if (ascii_case_equal(engine, "InnoDB")) return true;
#ifdef WITH_SMARTENGINE
  // Startup evidence separately requires the SmartEngine provider to export
  // the exact immutable object-extent-v2 live set before admission can open.
#ifdef WITH_XENGINE_COMPATIBLE_MODE
  return ascii_case_equal(engine, "XENGINE");
#else
  return ascii_case_equal(engine, "SMARTENGINE");
#endif
#else
  return false;
#endif
}

constexpr bool table_engine_allowed(TableEngineScope scope,
                                    std::string_view engine) {
  return scope != TableEngineScope::PERSISTENT_USER ||
         supported_persistent_user_engine(engine);
}

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_PERSISTENT_ENGINE_POLICY_INCLUDED
