/*
 * Copyright (c) 2026, ApeCloud Inc Holding Limited
 * Licensed under the Apache License, Version 2.0.
 */

#include "objstore/remote_extent.h"

#include <openssl/evp.h>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <map>
#include <mutex>
#include <optional>
#include <thread>
#include <tuple>
#include <utility>

#include "db/snapshot.h"
#include "storage/multi_version_extent_meta_layer.h"
#include "storage/storage_meta_struct.h"
#include "util/increment_number_allocator.h"

namespace smartengine {
namespace storage {
namespace remote_extent {
namespace {

struct VerifiedObject {
  uint64_t size{0};
  std::string sha256;
};

std::mutex g_runtime_mutex;
std::optional<RuntimeConfig> g_runtime;
std::optional<RuntimeProvider> g_runtime_provider;
bool g_test_runtime{false};
std::map<std::string, VerifiedObject> g_verified_objects;
std::atomic<bool> g_fenced{false};
std::string g_fence_reason;

bool fail(std::string *error, std::string message) {
  if (error != nullptr) *error = std::move(message);
  return false;
}

bool is_lower_hex(std::string_view value, size_t expected_size) {
  if (value.size() != expected_size) return false;
  for (const unsigned char ch : value) {
    if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')))
      return false;
  }
  return true;
}

bool valid_utf8(std::string_view value) {
  const auto *data = reinterpret_cast<const unsigned char *>(value.data());
  size_t pos = 0;
  while (pos < value.size()) {
    const unsigned char lead = data[pos++];
    if (lead <= 0x7f) continue;

    uint32_t codepoint = 0;
    size_t continuation_count = 0;
    if (lead >= 0xc2 && lead <= 0xdf) {
      codepoint = lead & 0x1f;
      continuation_count = 1;
    } else if (lead >= 0xe0 && lead <= 0xef) {
      codepoint = lead & 0x0f;
      continuation_count = 2;
    } else if (lead >= 0xf0 && lead <= 0xf4) {
      codepoint = lead & 0x07;
      continuation_count = 3;
    } else {
      return false;
    }
    if (continuation_count > value.size() - pos) return false;
    for (size_t index = 0; index < continuation_count; ++index) {
      const unsigned char next = data[pos++];
      if ((next & 0xc0) != 0x80) return false;
      codepoint = (codepoint << 6) | (next & 0x3f);
    }
    if ((continuation_count == 2 && codepoint < 0x800) ||
        (continuation_count == 3 && codepoint < 0x10000) ||
        (codepoint >= 0xd800 && codepoint <= 0xdfff) ||
        codepoint > 0x10ffff) {
      return false;
    }
  }
  return true;
}

bool is_ascii_component(std::string_view value) {
  if (value.empty() || value.size() > 48 || value == "." || value == "..")
    return false;
  const auto alphanumeric = [](unsigned char ch) {
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= '0' && ch <= '9');
  };
  if (!alphanumeric(static_cast<unsigned char>(value.front()))) return false;
  for (const unsigned char ch : value) {
    if (!alphanumeric(ch) && ch != '.' && ch != '_' && ch != '-')
      return false;
  }
  return true;
}

bool valid_cluster_prefix(std::string_view value) {
  if (value.empty() || value.front() == '/' || value.back() == '/')
    return false;
  size_t begin = 0;
  while (begin < value.size()) {
    const size_t slash = value.find('/', begin);
    const size_t end = slash == std::string_view::npos ? value.size() : slash;
    if (!is_ascii_component(value.substr(begin, end - begin))) return false;
    if (slash == std::string_view::npos) return true;
    begin = slash + 1;
  }
  return false;
}

bool validate_identity(const RuntimeConfig &config, std::string *error) {
  if (!valid_cluster_prefix(config.cluster_object_prefix))
    return fail(error, "remote extent cluster prefix is not canonical");
  if (!is_lower_hex(config.stream_sha256, 64))
    return fail(error, "remote extent stream SHA-256 is not canonical");
  if (config.writer_epoch == 0 ||
      config.writer_epoch > kJsonSafeIntegerMax)
    return fail(error, "remote extent writer epoch is invalid");
  return true;
}

bool validate_runtime(const RuntimeConfig &config, std::string *error) {
  if (!validate_identity(config, error)) return false;
  if (config.object_store == nullptr || config.bucket.empty())
    return fail(error, "remote extent object-store runtime is incomplete");
  if (!config.object_store->conditional_capabilities()
           .supports_remote_commit_io())
    return fail(error, "remote extent conditional object I/O is unavailable");
  return true;
}

std::string hex_encode(std::string_view value) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string encoded;
  encoded.reserve(value.size() * 2);
  for (const unsigned char ch : value) {
    encoded.push_back(kHex[ch >> 4]);
    encoded.push_back(kHex[ch & 0x0f]);
  }
  return encoded;
}

int hex_nibble(char ch) {
  if (ch >= '0' && ch <= '9') return ch - '0';
  if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
  return -1;
}

bool hex_decode_utf8(std::string_view encoded, std::string *decoded,
                     std::string *error) {
  if (decoded == nullptr || encoded.empty() || encoded.size() % 2 != 0 ||
      !is_lower_hex(encoded, encoded.size()))
    return fail(error, "database name hex is not canonical");
  decoded->clear();
  decoded->reserve(encoded.size() / 2);
  for (size_t pos = 0; pos < encoded.size(); pos += 2) {
    const int high = hex_nibble(encoded[pos]);
    const int low = hex_nibble(encoded[pos + 1]);
    if (high < 0 || low < 0)
      return fail(error, "database name hex contains an invalid byte");
    decoded->push_back(static_cast<char>((high << 4) | low));
  }
  return valid_utf8(*decoded)
             ? true
             : fail(error, "database name hex does not decode to UTF-8");
}

bool parse_decimal(std::string_view value, uint64_t *parsed,
                   std::string *error) {
  if (parsed == nullptr || value.empty() ||
      (value.size() > 1 && value.front() == '0'))
    return fail(error, "decimal path component is not canonical");
  uint64_t number = 0;
  const auto converted =
      std::from_chars(value.data(), value.data() + value.size(), number);
  if (converted.ec != std::errc() ||
      converted.ptr != value.data() + value.size())
    return fail(error, "decimal path component is invalid or overflowing");
  *parsed = number;
  return true;
}

std::string extent_root(const RuntimeConfig &config) {
  return config.cluster_object_prefix + "/smartengine/v2/extents/s" +
         config.stream_sha256;
}

void retry_delay(uint32_t milliseconds) {
  if (milliseconds != 0)
    std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
}

ImmutableWriteResult terminal(ImmutableWriteOutcome outcome,
                              std::string detail) {
  return {outcome, std::move(detail)};
}

bool same_identity(const LiveExtentRef &left, const LiveExtentRef &right) {
  return left.writer_epoch == right.writer_epoch &&
         left.allocation_seq == right.allocation_seq &&
         left.database_name_hex == right.database_name_hex &&
         left.index_id == right.index_id && left.object_id == right.object_id &&
         left.key == right.key;
}

bool same_payload(const LiveExtentRef &left, const LiveExtentRef &right) {
  return same_identity(left, right) && left.size == right.size &&
         left.sha256 == right.sha256;
}

bool verified_locked(std::string_view key, uint64_t size,
                     std::string_view sha256) {
  const auto found = g_verified_objects.find(std::string(key));
  return found != g_verified_objects.end() && found->second.size == size &&
         found->second.sha256 == sha256;
}

bool collect_layer(const ExtentLayer *layer, std::vector<LiveExtentRef> *refs,
                   std::string *error) {
  if (layer == nullptr) return fail(error, "snapshot contains a null extent layer");
  const auto collect = [&](const ExtentLayer::ExtentMetaSortedVector &metas) {
    for (auto iter = metas.begin(); iter != metas.end(); ++iter) {
      if (*iter == nullptr)
        return fail(error, "snapshot contains a null extent metadata entry");
      LiveExtentRef ref;
      if (!validate_metadata(**iter, MetadataValidationMode::EXISTING_LIVE,
                             &ref, error))
        return false;
      refs->push_back(std::move(ref));
    }
    return true;
  };
  return collect(layer->extent_meta_arr_) && collect(layer->lob_extent_arr_);
}

}  // namespace

bool RuntimeConfig::operator==(const RuntimeConfig &other) const {
  return cluster_object_prefix == other.cluster_object_prefix &&
         stream_sha256 == other.stream_sha256 &&
         writer_epoch == other.writer_epoch &&
         object_store == other.object_store && bucket == other.bucket;
}

bool LiveExtentRef::operator==(const LiveExtentRef &other) const {
  return ordinal == other.ordinal && same_payload(*this, other);
}

::objstore::ExactObjectResult ObjectStoreConditionalIo::get(
    std::string_view key) {
  if (object_store_ == nullptr) return ::objstore::ExactObjectResult::unsupported();
  return object_store_->get_object_exact(bucket_, key);
}

::objstore::ConditionalPutResult ObjectStoreConditionalIo::create(
    std::string_view key, std::string_view body) {
  if (object_store_ == nullptr)
    return ::objstore::ConditionalPutResult::unsupported();
  return object_store_->put_object_conditional(
      bucket_, key, body, ::objstore::ConditionalPutCondition::create_only());
}

bool enabled() {
  RuntimeProvider provider;
  {
    std::lock_guard<std::mutex> guard(g_runtime_mutex);
    if (g_test_runtime) return true;
    if (!g_runtime_provider.has_value()) return false;
    provider = *g_runtime_provider;
  }
  return provider.enabled();
}

bool runtime_config(RuntimeConfig *config, std::string *error) {
  if (config == nullptr) return fail(error, "null remote extent runtime result");
  RuntimeProvider provider;
  {
    std::lock_guard<std::mutex> guard(g_runtime_mutex);
    if (g_test_runtime && g_runtime.has_value()) {
      *config = *g_runtime;
      return true;
    }
    if (!g_runtime_provider.has_value())
      return fail(error, "remote extent runtime provider is not installed");
    provider = *g_runtime_provider;
  }

  RuntimeConfig observed;
  if (!provider.load_runtime(&observed))
    return fail(error, "remote extent runtime is not initialized");
  if (!validate_runtime(observed, error)) return false;

  bool changed = false;
  {
    std::lock_guard<std::mutex> guard(g_runtime_mutex);
    if (!g_runtime.has_value()) {
      g_runtime = observed;
    } else if (!(*g_runtime == observed)) {
      changed = true;
    }
    if (!changed) *config = *g_runtime;
  }
  if (changed) {
    enter_fenced("remote extent runtime identity changed during the process");
    return fail(error, "remote extent runtime identity changed");
  }
  return true;
}

bool is_fenced() {
  if (g_fenced.load(std::memory_order_acquire)) return true;
  RuntimeProvider provider;
  {
    std::lock_guard<std::mutex> guard(g_runtime_mutex);
    if (!g_runtime_provider.has_value()) return false;
    provider = *g_runtime_provider;
  }
  return provider.is_fenced();
}

std::string fence_reason() {
  std::lock_guard<std::mutex> guard(g_runtime_mutex);
  return g_fence_reason;
}

void enter_fenced(std::string_view reason) {
  if (reason.empty()) reason = "remote extent invariant failure";
  RuntimeProvider provider;
  bool has_provider = false;
  {
    std::lock_guard<std::mutex> guard(g_runtime_mutex);
    if (!g_fenced.exchange(true, std::memory_order_acq_rel))
      g_fence_reason.assign(reason);
    if (g_runtime_provider.has_value()) {
      provider = *g_runtime_provider;
      has_provider = true;
    }
  }
  if (has_provider && provider.enabled()) provider.fence(reason);
}

bool install_runtime_provider(const RuntimeProvider &provider,
                              std::string *error) {
  if (provider.enabled == nullptr || provider.load_runtime == nullptr ||
      provider.is_fenced == nullptr || provider.fence == nullptr)
    return fail(error, "remote extent runtime provider is incomplete");
  std::lock_guard<std::mutex> guard(g_runtime_mutex);
  if (g_test_runtime)
    return fail(error, "test runtime is already installed");
  if (g_runtime_provider.has_value() &&
      !(*g_runtime_provider == provider))
    return fail(error, "a different remote extent provider is installed");
  g_runtime_provider = provider;
  return true;
}

void clear_runtime_provider() {
  std::lock_guard<std::mutex> guard(g_runtime_mutex);
  if (g_test_runtime) return;
  g_runtime_provider.reset();
  g_runtime.reset();
  g_verified_objects.clear();
  g_fence_reason.clear();
  g_fenced.store(false, std::memory_order_release);
}

bool install_test_runtime(const RuntimeConfig &config, std::string *error) {
  if (!validate_runtime(config, error)) return false;
  std::lock_guard<std::mutex> guard(g_runtime_mutex);
  if (g_runtime.has_value() && !(*g_runtime == config))
    return fail(error, "a different remote extent runtime is already installed");
  g_runtime = config;
  g_test_runtime = true;
  g_fenced.store(false, std::memory_order_release);
  g_fence_reason.clear();
  g_verified_objects.clear();
  return true;
}

void clear_test_runtime() {
  std::lock_guard<std::mutex> guard(g_runtime_mutex);
  if (!g_test_runtime) return;
  g_runtime.reset();
  g_test_runtime = false;
  g_verified_objects.clear();
  g_fence_reason.clear();
  g_fenced.store(false, std::memory_order_release);
}

bool sha256_hex(std::string_view body, std::string *digest,
                std::string *error) {
  if (digest == nullptr) return fail(error, "null SHA-256 result");
  EVP_MD_CTX *context = EVP_MD_CTX_new();
  if (context == nullptr) return fail(error, "cannot allocate SHA-256 context");
  unsigned char bytes[EVP_MAX_MD_SIZE];
  unsigned int byte_count = 0;
  const bool ok = EVP_DigestInit_ex(context, EVP_sha256(), nullptr) == 1 &&
                  EVP_DigestUpdate(context, body.data(), body.size()) == 1 &&
                  EVP_DigestFinal_ex(context, bytes, &byte_count) == 1;
  EVP_MD_CTX_free(context);
  if (!ok || byte_count != 32)
    return fail(error, "cannot calculate SHA-256");
  static constexpr char kHex[] = "0123456789abcdef";
  digest->clear();
  digest->reserve(64);
  for (unsigned int index = 0; index < byte_count; ++index) {
    digest->push_back(kHex[bytes[index] >> 4]);
    digest->push_back(kHex[bytes[index] & 0x0f]);
  }
  return true;
}

bool build_prefix(const RuntimeConfig &config, std::string_view database_name,
                  uint64_t index_id, uint64_t allocation_seq,
                  std::string *prefix, std::string *error) {
  if (prefix == nullptr) return fail(error, "null remote extent prefix result");
  if (!validate_identity(config, error)) return false;
  if (database_name.empty() || !valid_utf8(database_name))
    return fail(error, "remote extent database name is not non-empty UTF-8");
  *prefix = extent_root(config) + "/e" +
            std::to_string(config.writer_epoch) + "/a" +
            std::to_string(allocation_seq) + "/db=" +
            hex_encode(database_name) + "/idx=" + std::to_string(index_id) +
            "/data/";
  if (prefix->size() + 20 > kMaximumObjectKeyBytes)
    return fail(error, "remote extent object key exceeds its size limit");
  ParsedPrefix parsed;
  return parse_prefix(config, *prefix, &parsed, error);
}

bool allocate_prefix(std::string_view database_name, int64_t index_id,
                     std::string *prefix, std::string *error) {
  if (index_id < 0) return fail(error, "remote extent index id is negative");
  RuntimeConfig config;
  if (!runtime_config(&config, error)) return false;
  const int64_t allocated = util::UniqueIdAllocator::get_instance().alloc();
  if (allocated < 0)
    return fail(error, "remote extent allocation sequence overflowed");
  return build_prefix(config, database_name, static_cast<uint64_t>(index_id),
                      static_cast<uint64_t>(allocated), prefix, error);
}

bool parse_prefix(const RuntimeConfig &config, std::string_view prefix,
                  ParsedPrefix *parsed, std::string *error) {
  if (parsed == nullptr) return fail(error, "null parsed extent prefix result");
  if (!validate_identity(config, error)) return false;
  const std::string root = extent_root(config) + "/e";
  if (prefix.size() <= root.size() || prefix.substr(0, root.size()) != root)
    return fail(error, "remote extent prefix has the wrong stream root");

  size_t begin = root.size();
  const auto consume = [&](std::string_view delimiter, std::string_view *value,
                           size_t *position) {
    const size_t end = prefix.find(delimiter, *position);
    if (end == std::string_view::npos || end == *position) return false;
    *value = prefix.substr(*position, end - *position);
    *position = end + delimiter.size();
    return true;
  };

  std::string_view epoch_text;
  std::string_view allocation_text;
  std::string_view database_hex;
  std::string_view index_text;
  if (!consume("/a", &epoch_text, &begin) ||
      !consume("/db=", &allocation_text, &begin) ||
      !consume("/idx=", &database_hex, &begin) ||
      !consume("/data/", &index_text, &begin) || begin != prefix.size())
    return fail(error, "remote extent prefix shape is not canonical");

  ParsedPrefix result;
  if (!parse_decimal(epoch_text, &result.writer_epoch, error) ||
      result.writer_epoch == 0 || result.writer_epoch > kJsonSafeIntegerMax ||
      !parse_decimal(allocation_text, &result.allocation_seq, error) ||
      !parse_decimal(index_text, &result.index_id, error) ||
      !hex_decode_utf8(database_hex, &result.database_name, error))
    return false;
  if (result.writer_epoch > config.writer_epoch)
    return fail(error, "remote extent prefix belongs to a future writer epoch");
  result.database_name_hex.assign(database_hex);
  *parsed = std::move(result);
  return true;
}

bool object_key(const RuntimeConfig &config, std::string_view prefix,
                const ExtentId &extent_id, std::string *key,
                ParsedPrefix *parsed, std::string *error) {
  if (key == nullptr) return fail(error, "null remote extent object key result");
  ParsedPrefix local;
  if (!parse_prefix(config, prefix, &local, error)) return false;
  const uint64_t object_id = assemble_objid_by_fdfn(
      extent_id.file_number, extent_id.offset);
  *key = std::string(prefix) + std::to_string(object_id);
  if (key->size() > kMaximumObjectKeyBytes)
    return fail(error, "remote extent object key exceeds its size limit");
  if (parsed != nullptr) *parsed = std::move(local);
  return true;
}

bool object_key(std::string_view prefix, const ExtentId &extent_id,
                std::string *key, std::string *error) {
  if (key == nullptr) return fail(error, "null extent object key result");
  if (!enabled()) {
    *key = std::string(prefix) + std::to_string(assemble_objid_by_fdfn(
                                       extent_id.file_number,
                                       extent_id.offset));
    return true;
  }
  RuntimeConfig config;
  return runtime_config(&config, error) &&
         object_key(config, prefix, extent_id, key, nullptr, error);
}

ImmutableWriteResult create_and_verify(ConditionalIo *io,
                                       std::string_view key,
                                       std::string_view intended_body,
                                       size_t max_steps,
                                       uint32_t retry_delay_ms) {
  if (io == nullptr || key.empty() || intended_body.empty())
    return terminal(ImmutableWriteOutcome::PERMANENT_ERROR,
                    "invalid immutable extent create input");

  enum class ReadbackReason { SUCCESS, EXPLICIT_CONFLICT, TRANSPORT_UNKNOWN };
  bool issue_create = true;
  ReadbackReason reason = ReadbackReason::TRANSPORT_UNKNOWN;
  size_t steps = 0;
  while (max_steps == 0 || steps++ < max_steps) {
    if (issue_create) {
      const ::objstore::ConditionalPutResult put = io->create(key, intended_body);
      issue_create = false;
      switch (put.outcome()) {
        case ::objstore::ConditionalPutOutcome::APPLIED:
          reason = ReadbackReason::SUCCESS;
          break;
        case ::objstore::ConditionalPutOutcome::CONFLICT_409:
        case ::objstore::ConditionalPutOutcome::PRECONDITION_FAILED_412:
          reason = ReadbackReason::EXPLICIT_CONFLICT;
          break;
        case ::objstore::ConditionalPutOutcome::TRANSPORT_UNKNOWN:
          reason = ReadbackReason::TRANSPORT_UNKNOWN;
          break;
        case ::objstore::ConditionalPutOutcome::PERMANENT_ERROR:
          return terminal(ImmutableWriteOutcome::PERMANENT_ERROR,
                          "immutable extent create failed permanently");
        case ::objstore::ConditionalPutOutcome::UNSUPPORTED:
          return terminal(ImmutableWriteOutcome::PERMANENT_ERROR,
                          "immutable extent create is unsupported");
      }
    }

    const ::objstore::ExactObjectResult read = io->get(key);
    switch (read.outcome()) {
      case ::objstore::ExactObjectOutcome::FOUND:
        if (read.size() == intended_body.size() &&
            read.body() == intended_body)
          return terminal(ImmutableWriteOutcome::VERIFIED, {});
        return terminal(ImmutableWriteOutcome::FENCED,
                        "immutable extent read-back diverged");
      case ::objstore::ExactObjectOutcome::NOT_FOUND_404:
        if (reason == ReadbackReason::TRANSPORT_UNKNOWN) {
          issue_create = true;
          retry_delay(retry_delay_ms);
          break;
        }
        return terminal(ImmutableWriteOutcome::FENCED,
                        reason == ReadbackReason::SUCCESS
                            ? "successful immutable extent create read back 404"
                            : "immutable extent conflict read back 404");
      case ::objstore::ExactObjectOutcome::TRANSIENT_UNAVAILABLE:
        retry_delay(retry_delay_ms);
        break;
      case ::objstore::ExactObjectOutcome::PERMANENT_ERROR:
        return terminal(ImmutableWriteOutcome::PERMANENT_ERROR,
                        "immutable extent read-back failed permanently");
      case ::objstore::ExactObjectOutcome::UNSUPPORTED:
        return terminal(ImmutableWriteOutcome::PERMANENT_ERROR,
                        "immutable extent exact read-back is unsupported");
    }
  }
  return terminal(ImmutableWriteOutcome::BLOCKED,
                  "immutable extent read-back remains unavailable");
}

bool remember_verified(std::string_view key, uint64_t size,
                       std::string_view sha256, std::string *error) {
  if (key.empty() || size == 0 || !is_lower_hex(sha256, 64))
    return fail(error, "invalid verified remote extent identity");
  std::lock_guard<std::mutex> guard(g_runtime_mutex);
  const auto found = g_verified_objects.find(std::string(key));
  if (found != g_verified_objects.end()) {
    if (found->second.size != size || found->second.sha256 != sha256)
      return fail(error, "verified remote extent identity changed");
    return true;
  }
  g_verified_objects.emplace(std::string(key),
                             VerifiedObject{size, std::string(sha256)});
  return true;
}

bool validate_metadata(const ExtentMeta &meta, MetadataValidationMode mode,
                       LiveExtentRef *ref, std::string *error) {
  if (!enabled()) return true;
  if (is_fenced()) return fail(error, "remote extent runtime is fenced");
  RuntimeConfig config;
  if (!runtime_config(&config, error)) return false;
  if (meta.extent_space_type_ != OBJECT_EXTENT_SPACE)
    return fail(error, "remote mode metadata references a file extent");
  if (meta.attr_ != ExtentMeta::F_NORMAL_EXTENT &&
      meta.attr_ != ExtentMeta::F_LARGE_OBJECT_EXTENT)
    return fail(error, "remote mode metadata has an invalid extent kind");
  if (meta.table_schema_.get_index_id() < 0)
    return fail(error, "remote mode metadata has a negative index id");
  if (meta.object_size_ != static_cast<uint64_t>(MAX_EXTENT_SIZE) ||
      !is_lower_hex(meta.object_sha256_, 64))
    return fail(error, "remote mode metadata lacks the exact object digest");

  std::string key;
  ParsedPrefix parsed;
  if (!object_key(config, meta.prefix_, meta.extent_id_, &key, &parsed,
                  error))
    return false;
  if (parsed.database_name != meta.table_schema_.get_database_name() ||
      parsed.index_id !=
          static_cast<uint64_t>(meta.table_schema_.get_index_id()))
    return fail(error, "remote extent prefix and metadata tuple disagree");
  if (mode == MetadataValidationMode::NEW_REFERENCE &&
      parsed.writer_epoch != config.writer_epoch)
    return fail(error, "new remote extent does not belong to this writer epoch");

  if (mode == MetadataValidationMode::NEW_REFERENCE) {
    std::lock_guard<std::mutex> guard(g_runtime_mutex);
    if (!verified_locked(key, meta.object_size_, meta.object_sha256_))
      return fail(error, "remote extent metadata precedes verified upload");
  }

  if (ref != nullptr) {
    ref->ordinal = 0;
    ref->writer_epoch = parsed.writer_epoch;
    ref->allocation_seq = parsed.allocation_seq;
    ref->database_name_hex = std::move(parsed.database_name_hex);
    ref->index_id = parsed.index_id;
    ref->object_id = assemble_objid_by_fdfn(meta.extent_id_.file_number,
                                            meta.extent_id_.offset);
    ref->key = std::move(key);
    ref->size = meta.object_size_;
    ref->sha256 = meta.object_sha256_;
  }
  return true;
}

bool verify_metadata_object(const ExtentMeta &meta, LiveExtentRef *ref,
                            std::string *error, size_t max_steps,
                            uint32_t retry_delay_ms) {
  if (!enabled()) return true;
  LiveExtentRef candidate;
  if (!validate_metadata(meta, MetadataValidationMode::EXISTING_LIVE,
                         &candidate, error))
    return false;
  {
    std::lock_guard<std::mutex> guard(g_runtime_mutex);
    if (verified_locked(candidate.key, candidate.size, candidate.sha256)) {
      if (ref != nullptr) *ref = std::move(candidate);
      return true;
    }
  }

  RuntimeConfig config;
  if (!runtime_config(&config, error)) return false;
  size_t steps = 0;
  while (max_steps == 0 || steps++ < max_steps) {
    const ::objstore::ExactObjectResult read =
        config.object_store->get_object_exact(config.bucket, candidate.key);
    if (read.outcome() == ::objstore::ExactObjectOutcome::FOUND) {
      std::string digest;
      if (!sha256_hex(read.body(), &digest, error)) return false;
      if (read.size() != candidate.size || digest != candidate.sha256)
        return fail(error, "live remote extent body does not match metadata");
      if (!remember_verified(candidate.key, candidate.size, candidate.sha256,
                             error))
        return false;
      if (ref != nullptr) *ref = std::move(candidate);
      return true;
    }
    if (read.outcome() == ::objstore::ExactObjectOutcome::TRANSIENT_UNAVAILABLE) {
      retry_delay(retry_delay_ms);
      continue;
    }
    return fail(error,
                read.outcome() == ::objstore::ExactObjectOutcome::NOT_FOUND_404
                    ? "live remote extent is missing"
                    : "live remote extent exact GET failed");
  }
  return fail(error, "live remote extent verification remains blocked");
}

bool canonicalize_live_set(std::vector<LiveExtentRef> *refs,
                           std::string *error) {
  if (refs == nullptr) return fail(error, "null remote extent live set");
  std::sort(refs->begin(), refs->end(),
            [](const LiveExtentRef &left, const LiveExtentRef &right) {
              return std::tie(left.writer_epoch, left.allocation_seq,
                              left.database_name_hex, left.index_id,
                              left.object_id, left.key) <
                     std::tie(right.writer_epoch, right.allocation_seq,
                              right.database_name_hex, right.index_id,
                              right.object_id, right.key);
            });

  std::vector<LiveExtentRef> canonical;
  canonical.reserve(refs->size());
  std::map<std::string, LiveExtentRef> by_key;
  for (const LiveExtentRef &candidate : *refs) {
    const auto keyed = by_key.find(candidate.key);
    if (keyed != by_key.end() && !same_payload(keyed->second, candidate))
      return fail(error, "remote extent live set has a conflicting key");
    if (keyed == by_key.end()) by_key.emplace(candidate.key, candidate);

    if (!canonical.empty() && same_identity(canonical.back(), candidate)) {
      if (!same_payload(canonical.back(), candidate))
        return fail(error, "remote extent live set has a conflicting tuple");
      continue;
    }
    canonical.push_back(candidate);
  }
  for (size_t index = 0; index < canonical.size(); ++index)
    canonical[index].ordinal = index;
  *refs = std::move(canonical);
  return true;
}

bool export_snapshot_live_set(
    const std::unordered_set<db::Snapshot *> &snapshots,
    std::vector<LiveExtentRef> *refs, std::string *error) {
  if (refs == nullptr) return fail(error, "null snapshot live-set result");
  refs->clear();
  if (!enabled()) return true;
  for (const db::Snapshot *snapshot : snapshots) {
    if (snapshot == nullptr) return fail(error, "snapshot set contains null");
    for (int64_t level = 0; level < MAX_TIER_COUNT; ++level) {
      const ExtentLayerVersion *version =
          snapshot->get_extent_layer_version(level);
      if (version == nullptr) continue;
      for (auto iter = version->extent_layer_arr_.begin();
           iter != version->extent_layer_arr_.end(); ++iter) {
        if (!collect_layer(*iter, refs, error)) return false;
      }
      if (version->dump_extent_layer_ != nullptr &&
          !collect_layer(version->dump_extent_layer_, refs, error))
        return false;
    }
  }
  if (!canonicalize_live_set(refs, error)) return false;

  // The snapshot remains pinned while every referenced immutable body is
  // checked, so the exported set cannot race metadata reclamation.
  for (const LiveExtentRef &ref : *refs) {
    std::lock_guard<std::mutex> guard(g_runtime_mutex);
    if (!verified_locked(ref.key, ref.size, ref.sha256))
      return fail(error, "snapshot contains an unverified remote extent");
  }
  return true;
}

}  // namespace remote_extent
}  // namespace storage
}  // namespace smartengine
