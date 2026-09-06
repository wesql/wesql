/*
 * Copyright (c) 2026, ApeCloud Inc Holding Limited
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef SMARTENGINE_INCLUDE_REMOTE_EXTENT_H_
#define SMARTENGINE_INCLUDE_REMOTE_EXTENT_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "objstore.h"
#include "storage/storage_common.h"

namespace smartengine {
namespace db {
class Snapshot;
}
namespace storage {
struct ExtentMeta;

namespace remote_extent {

constexpr uint64_t kJsonSafeIntegerMax = 9007199254740991ULL;
constexpr size_t kMaximumObjectKeyBytes = 1024;

struct RuntimeConfig {
  std::string cluster_object_prefix;
  std::string stream_sha256;
  uint64_t writer_epoch{0};
  ::objstore::ObjectStore *object_store{nullptr};
  std::string bucket;

  bool operator==(const RuntimeConfig &other) const;
};

// SmartEngine core and its standalone tools must not link Server objects.
// The statically linked plugin installs this provider when Server support is
// available; tools leave it absent and therefore keep remote mode disabled.
struct RuntimeProvider {
  bool (*enabled)() = nullptr;
  bool (*load_runtime)(RuntimeConfig *config) = nullptr;
  bool (*is_fenced)() = nullptr;
  void (*fence)(std::string_view reason) = nullptr;

  bool operator==(const RuntimeProvider &other) const = default;
};

struct ParsedPrefix {
  uint64_t writer_epoch{0};
  uint64_t allocation_seq{0};
  std::string database_name_hex;
  std::string database_name;
  uint64_t index_id{0};
};

struct LiveExtentRef {
  uint64_t ordinal{0};
  uint64_t writer_epoch{0};
  uint64_t allocation_seq{0};
  std::string database_name_hex;
  uint64_t index_id{0};
  uint64_t object_id{0};
  std::string key;
  uint64_t size{0};
  std::string sha256;

  bool operator==(const LiveExtentRef &other) const;
};

enum class ImmutableWriteOutcome : uint8_t {
  VERIFIED,
  BLOCKED,
  FENCED,
  PERMANENT_ERROR,
};

struct ImmutableWriteResult {
  ImmutableWriteOutcome outcome{ImmutableWriteOutcome::PERMANENT_ERROR};
  std::string detail;

  bool verified() const { return outcome == ImmutableWriteOutcome::VERIFIED; }
};

class ConditionalIo {
 public:
  virtual ~ConditionalIo() = default;
  virtual ::objstore::ExactObjectResult get(std::string_view key) = 0;
  virtual ::objstore::ConditionalPutResult create(std::string_view key,
                                                   std::string_view body) = 0;
};

class ObjectStoreConditionalIo final : public ConditionalIo {
 public:
  ObjectStoreConditionalIo(::objstore::ObjectStore *object_store,
                           std::string bucket)
      : object_store_(object_store), bucket_(std::move(bucket)) {}

  ::objstore::ExactObjectResult get(std::string_view key) override;
  ::objstore::ConditionalPutResult create(std::string_view key,
                                           std::string_view body) override;

 private:
  ::objstore::ObjectStore *object_store_;
  std::string bucket_;
};

bool enabled();
bool runtime_config(RuntimeConfig *config, std::string *error);
bool is_fenced();
std::string fence_reason();
void enter_fenced(std::string_view reason);

bool install_runtime_provider(const RuntimeProvider &provider,
                              std::string *error);
void clear_runtime_provider();

// Standalone SmartEngine tests do not link the MySQL remote-commit runtime.
bool install_test_runtime(const RuntimeConfig &config, std::string *error);
void clear_test_runtime();

bool sha256_hex(std::string_view body, std::string *digest,
                std::string *error);
bool build_prefix(const RuntimeConfig &config, std::string_view database_name,
                  uint64_t index_id, uint64_t allocation_seq,
                  std::string *prefix, std::string *error);
bool allocate_prefix(std::string_view database_name, int64_t index_id,
                     std::string *prefix, std::string *error);
bool parse_prefix(const RuntimeConfig &config, std::string_view prefix,
                  ParsedPrefix *parsed, std::string *error);
bool object_key(const RuntimeConfig &config, std::string_view prefix,
                const ExtentId &extent_id, std::string *key,
                ParsedPrefix *parsed, std::string *error);
bool object_key(std::string_view prefix, const ExtentId &extent_id,
                std::string *key, std::string *error);

// max_steps == 0 means that transient read-back keeps the caller blocked.
ImmutableWriteResult create_and_verify(ConditionalIo *io,
                                       std::string_view key,
                                       std::string_view intended_body,
                                       size_t max_steps = 0,
                                       uint32_t retry_delay_ms = 100);

enum class MetadataValidationMode : uint8_t {
  EXISTING_LIVE,
  NEW_REFERENCE,
};

bool remember_verified(std::string_view key, uint64_t size,
                       std::string_view sha256, std::string *error);
bool validate_metadata(const ExtentMeta &meta, MetadataValidationMode mode,
                       LiveExtentRef *ref, std::string *error);
bool verify_metadata_object(const ExtentMeta &meta, LiveExtentRef *ref,
                            std::string *error, size_t max_steps = 0,
                            uint32_t retry_delay_ms = 100);

bool canonicalize_live_set(std::vector<LiveExtentRef> *refs,
                           std::string *error);
bool export_snapshot_live_set(
    const std::unordered_set<db::Snapshot *> &snapshots,
    std::vector<LiveExtentRef> *refs, std::string *error);

}  // namespace remote_extent
}  // namespace storage
}  // namespace smartengine

#endif  // SMARTENGINE_INCLUDE_REMOTE_EXTENT_H_
