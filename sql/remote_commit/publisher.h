/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_PUBLISHER_INCLUDED
#define SQL_REMOTE_COMMIT_PUBLISHER_INCLUDED

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "objstore.h"
#include "sql/remote_commit/policy.h"
#include "sql/remote_commit/protocol_codec.h"

namespace wesql::remote_commit {

enum class PublishOutcome : uint8_t {
  APPLIED,
  ABSENT,
  BLOCKED,
  REFIX_REQUIRED,
  FENCED,
  PERMANENT_ERROR,
};

struct PublishedBytes {
  std::string body;
  std::string etag;

  bool operator==(const PublishedBytes &) const = default;
};

struct PublishResult {
  PublishOutcome outcome{PublishOutcome::PERMANENT_ERROR};
  std::string detail;
  std::optional<PublishedBytes> object;

  bool applied() const { return outcome == PublishOutcome::APPLIED; }
};

class ConditionalIo {
 public:
  virtual ~ConditionalIo() = default;

  virtual objstore::ExactObjectResult get(std::string_view key,
                                          uint64_t max_bytes) = 0;
  virtual objstore::ConditionalPutResult put(
      std::string_view key, std::string_view body,
      const objstore::ConditionalPutCondition &condition) = 0;
};

class ObjectStoreConditionalIo final : public ConditionalIo {
 public:
  ObjectStoreConditionalIo(objstore::ObjectStore *object_store,
                           std::string bucket)
      : object_store_(object_store), bucket_(std::move(bucket)) {}

  objstore::ExactObjectResult get(std::string_view key,
                                  uint64_t max_bytes) override;
  objstore::ConditionalPutResult put(
      std::string_view key, std::string_view body,
      const objstore::ConditionalPutCondition &condition) override;

 private:
  objstore::ObjectStore *object_store_;
  std::string bucket_;
};

class ProtocolStore {
 public:
  explicit ProtocolStore(ConditionalIo *io, size_t logical_attempt_limit = 2)
      : io_(io), logical_attempt_limit_(logical_attempt_limit) {}

  PublishResult read(
      std::string_view key,
      uint64_t max_bytes = static_cast<uint64_t>(kSnapshotManifestMaxBytes));
  PublishResult create_immutable(std::string_view key, std::string_view body);
  PublishResult create_fixed(std::string_view key, std::string_view body);
  PublishResult compare_and_swap(std::string_view key, std::string_view body,
                                 const PublishedBytes &prior);

 private:
  PublishResult conditionally_put(
      std::string_view key, std::string_view body,
      const objstore::ConditionalPutCondition &condition,
      const std::optional<PublishedBytes> &prior, bool immutable);
  PublishResult classify_readback(
      std::string_view key, std::string_view intended,
      const std::optional<PublishedBytes> &prior, bool may_retry,
      bool immutable, bool *retry);

  ConditionalIo *io_;
  size_t logical_attempt_limit_;
};

struct PublisherState {
  LifecycleState lifecycle{LifecycleState::OFF};
  std::optional<PublishedBytes> epoch_object;
  std::optional<WriterEpoch> epoch;
  std::optional<PublishedBytes> head_object;
  std::optional<Head> head;
};

class HeadPublisher {
 public:
  HeadPublisher(ConditionalIo *io, StreamIdentity stream,
                size_t logical_attempt_limit = 2);

  const StreamIdentity &stream() const { return stream_; }
  const PublisherState &state() const { return state_; }

  // HEAD is deliberately read before WRITER_EPOCH. An existing stream must
  // never be routed through EMPTY_SOURCE bootstrap checks.
  PublishResult probe();
  PublishResult acquire_epoch(std::string_view writer_id);

  // Bind the bounded reader's post-acquisition candidate before publishing
  // the takeover snapshot. Retains local ownership and verifies both objects.
  PublishResult bind_takeover_candidate(
      const PublishedBytes &expected_epoch_object,
      const WriterEpoch &expected_epoch,
      const PublishedBytes &expected_head_object, const Head &expected_head);

  // Re-establishes same-writer ownership after a same-binary re-exec. Both
  // fixed objects are re-read and must equal the caller's canonical bytes,
  // ETags, and parsed values. This performs no remote write and installs the
  // local writer id only after every comparison succeeds.
  PublishResult adopt_epoch(const PublishedBytes &expected_epoch_object,
                            const WriterEpoch &expected_epoch,
                            const PublishedBytes &expected_head_object,
                            const Head &expected_head);

  // Bootstrap snapshot workers adopt an exact epoch while HEAD is still
  // absent. The read order remains HEAD then WRITER_EPOCH and no write is
  // issued. An adopted publisher stays read-only until final installed-root
  // activation.
  PublishResult adopt_epoch_without_head(
      const PublishedBytes &expected_epoch_object,
      const WriterEpoch &expected_epoch);

  // Re-verifies the installed READY HEAD and exact epoch, then converts one
  // read-only adoption into normal RUNNING publisher ownership. Recovery and
  // bootstrap snapshot workers never call this method.
  PublishResult activate_adopted_epoch(const Head &installed_head);

  // The caller supplies a fully formed manifest and intended HEAD. This
  // method verifies their exact relationship, publishes the immutable
  // manifest, conditionally advances HEAD, and rechecks epoch ownership.
  PublishResult publish(const TransitionManifest &manifest,
                        const Head &intended_head);

  // Used immediately before engine COMMIT and again before public cursor/OK.
  PublishResult verify_decision(const Head &intended_head);

  // Verifies an exact adopted HEAD and WRITER_EPOCH without promoting the
  // publisher. Installed-root re-exec uses this while recovery is still
  // running with CLOSED admission; the adoption remains read-only and its
  // lifecycle stays RECOVERING until activate_adopted_epoch().
  PublishResult verify_adopted_head_read_only(const Head &intended_head);

  void mark_blocked(std::string detail);
  void fence(std::string detail);
  const std::string &last_error() const { return last_error_; }

 private:
  PublishResult read_epoch();
  PublishResult read_head();
  PublishResult check_epoch_owner();
  PublishResult verify_decision_impl(const Head &intended_head,
                                     bool promote_running);
  PublishResult validate_transition(const TransitionManifest &manifest,
                                    const Head &head,
                                    std::string_view manifest_body,
                                    std::string_view manifest_sha,
                                    std::string_view manifest_key) const;
  PublishResult terminal(PublishOutcome outcome, std::string detail);

  StreamIdentity stream_;
  ProtocolStore store_;
  PublisherState state_;
  std::string local_writer_id_;
  bool adopted_read_only_{false};
  std::string last_error_;
};

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_PUBLISHER_INCLUDED
