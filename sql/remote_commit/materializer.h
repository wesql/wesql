/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_MATERIALIZER_INCLUDED
#define SQL_REMOTE_COMMIT_MATERIALIZER_INCLUDED

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include "objstore.h"
#include "sql/remote_commit/recovery.h"

namespace wesql::remote_commit {

enum class PayloadReadOutcome : uint8_t {
  APPLIED,
  ABSENT,
  BLOCKED,
  PERMANENT_ERROR,
};

struct PayloadReadResult {
  PayloadReadOutcome outcome{PayloadReadOutcome::PERMANENT_ERROR};
  std::string detail;
};

class PayloadIo {
 public:
  virtual ~PayloadIo() = default;
  virtual PayloadReadResult download(
      std::string_view key, const std::filesystem::path &destination) = 0;
};

class ObjectStorePayloadIo final : public PayloadIo {
 public:
  ObjectStorePayloadIo(objstore::ObjectStore *object_store, std::string bucket)
      : object_store_(object_store), bucket_(std::move(bucket)) {}

  PayloadReadResult download(
      std::string_view key,
      const std::filesystem::path &destination) override;

  static PayloadReadResult classify_exact_file_result(
      const objstore::ExactFileResult &result,
      const std::filesystem::path &destination);

 private:
  objstore::ObjectStore *object_store_;
  std::string bucket_;
};

class BinlogImageValidator {
 public:
  virtual ~BinlogImageValidator() = default;
  virtual bool validate(const std::vector<std::filesystem::path> &files,
                        const Cursor &durable_cursor,
                        std::string *error) = 0;
};

class NativeBinlogImageValidator final : public BinlogImageValidator {
 public:
  explicit NativeBinlogImageValidator(uint32_t max_event_bytes)
      : max_event_bytes_(max_event_bytes) {}

  bool validate(const std::vector<std::filesystem::path> &files,
                const Cursor &durable_cursor,
                std::string *error) override;

 private:
  uint32_t max_event_bytes_;
};

enum class MaterializeOutcome : uint8_t {
  READY,
  BLOCKED,
  CORRUPT,
  LOCAL_IO_ERROR,
};

struct MaterializeResult {
  MaterializeOutcome outcome{MaterializeOutcome::CORRUPT};
  std::string detail;

  bool ready() const { return outcome == MaterializeOutcome::READY; }
};

struct MaterializeOptions {
  std::filesystem::path temp_root;
  std::filesystem::path binlog_index_relative_path{"binlog.index"};
  uint64_t max_object_bytes{4ULL * 1024 * 1024 * 1024};
  uint64_t max_total_payload_bytes{64ULL * 1024 * 1024 * 1024};
};

struct MaterializedRoot {
  std::vector<std::filesystem::path> binlog_files;
  std::filesystem::path binlog_index;
  uint64_t verified_payload_bytes{0};
};

// Materializes only into a caller-selected non-existent root. Provider or
// validation failures leave that root in place for first-failure evidence.
class RecoveryMaterializer {
 public:
  RecoveryMaterializer(PayloadIo *payload_io,
                       BinlogImageValidator *binlog_validator)
      : payload_io_(payload_io), binlog_validator_(binlog_validator) {}

  MaterializeResult materialize(const RecoveryPlan &plan,
                                const MaterializeOptions &options,
                                MaterializedRoot *root);

 private:
  PayloadIo *payload_io_;
  BinlogImageValidator *binlog_validator_;
};

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_MATERIALIZER_INCLUDED
