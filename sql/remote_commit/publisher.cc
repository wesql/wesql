/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/publisher.h"
#include "sql/remote_commit/fault_injection.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace wesql::remote_commit {
namespace {

PublishResult result(PublishOutcome outcome, std::string detail = {},
                     std::optional<PublishedBytes> object = std::nullopt) {
  return {outcome, std::move(detail), std::move(object)};
}

std::string status_detail(const objstore::Status &status) {
  return std::string(status.error_message());
}

bool same_object(const PublishedBytes &left, const PublishedBytes &right) {
  return left.body == right.body && left.etag == right.etag;
}

ObjectRef manifest_object_ref(const Head &head) { return head.manifest; }

}  // namespace

objstore::ExactObjectResult ObjectStoreConditionalIo::get(
    std::string_view key, uint64_t max_bytes) {
  if (object_store_ == nullptr) return objstore::ExactObjectResult::unsupported();
  return object_store_->get_object_exact(bucket_, key, max_bytes);
}

objstore::ConditionalPutResult ObjectStoreConditionalIo::put(
    std::string_view key, std::string_view body,
    const objstore::ConditionalPutCondition &condition) {
  if (object_store_ == nullptr)
    return objstore::ConditionalPutResult::unsupported();
  return object_store_->put_object_conditional(bucket_, key, body, condition);
}

PublishResult ProtocolStore::read(std::string_view key, uint64_t max_bytes) {
  if (max_bytes == 0)
    return result(PublishOutcome::PERMANENT_ERROR,
                  "metadata byte limit must be positive");
  auto object = io_->get(key, max_bytes);
  switch (object.outcome()) {
    case objstore::ExactObjectOutcome::FOUND:
      if (object.size() != object.body().size())
        return result(PublishOutcome::FENCED,
                      "exact GET content length does not match body");
      return result(PublishOutcome::APPLIED, {},
                    PublishedBytes{object.body(), object.etag()});
    case objstore::ExactObjectOutcome::NOT_FOUND_404:
      return result(PublishOutcome::ABSENT, status_detail(object.status()));
    case objstore::ExactObjectOutcome::TRANSIENT_UNAVAILABLE:
      return result(PublishOutcome::BLOCKED, status_detail(object.status()));
    case objstore::ExactObjectOutcome::PERMANENT_ERROR:
    case objstore::ExactObjectOutcome::UNSUPPORTED:
      return result(PublishOutcome::PERMANENT_ERROR,
                    status_detail(object.status()));
  }
  return result(PublishOutcome::PERMANENT_ERROR,
                "unknown exact GET outcome");
}

PublishResult ProtocolStore::create_immutable(std::string_view key,
                                              std::string_view body) {
  return conditionally_put(key, body,
                           objstore::ConditionalPutCondition::create_only(),
                           std::nullopt, true);
}

PublishResult ProtocolStore::create_fixed(std::string_view key,
                                          std::string_view body) {
  return conditionally_put(key, body,
                           objstore::ConditionalPutCondition::create_only(),
                           std::nullopt, false);
}

PublishResult ProtocolStore::compare_and_swap(std::string_view key,
                                              std::string_view body,
                                              const PublishedBytes &prior) {
  return conditionally_put(
      key, body, objstore::ConditionalPutCondition::match_etag(prior.etag),
      prior, false);
}

PublishResult ProtocolStore::conditionally_put(
    std::string_view key, std::string_view body,
    const objstore::ConditionalPutCondition &condition,
    const std::optional<PublishedBytes> &prior, bool immutable) {
  if (logical_attempt_limit_ == 0)
    return result(PublishOutcome::PERMANENT_ERROR,
                  "logical attempt limit must be positive");

  for (size_t attempt = 0; attempt < logical_attempt_limit_; ++attempt) {
    const auto put_result = io_->put(key, body, condition);
    bool may_retry = false;
    switch (put_result.outcome()) {
      case objstore::ConditionalPutOutcome::APPLIED:
        may_retry = false;
        break;
      case objstore::ConditionalPutOutcome::TRANSPORT_UNKNOWN:
        may_retry = true;
        break;
      case objstore::ConditionalPutOutcome::CONFLICT_409:
      case objstore::ConditionalPutOutcome::PRECONDITION_FAILED_412:
        may_retry = false;
        break;
      case objstore::ConditionalPutOutcome::PERMANENT_ERROR:
      case objstore::ConditionalPutOutcome::UNSUPPORTED:
        return result(PublishOutcome::PERMANENT_ERROR,
                      status_detail(put_result.status()));
    }

    bool retry = false;
    auto classified = classify_readback(key, body, prior, may_retry, immutable,
                                        &retry);
    if (!retry) return classified;
    if (attempt + 1 == logical_attempt_limit_)
      return result(PublishOutcome::BLOCKED,
                    "conditional PUT remains undecided after exact prior "
                    "read-back");
  }
  return result(PublishOutcome::PERMANENT_ERROR,
                "unreachable conditional PUT state");
}

PublishResult ProtocolStore::classify_readback(
    std::string_view key, std::string_view intended,
    const std::optional<PublishedBytes> &prior, bool may_retry, bool immutable,
    bool *retry) {
  *retry = false;
  uint64_t max_bytes = static_cast<uint64_t>(intended.size());
  if (prior.has_value()) {
    max_bytes = std::max(max_bytes,
                         static_cast<uint64_t>(prior->body.size()));
  }
  if (max_bytes < std::numeric_limits<uint64_t>::max()) ++max_bytes;
  auto readback = read(key, max_bytes);
  if (readback.outcome == PublishOutcome::BLOCKED) return readback;
  if (readback.outcome == PublishOutcome::PERMANENT_ERROR) return readback;
  if (readback.outcome == PublishOutcome::APPLIED) {
    if (readback.object->body == intended) return readback;
    if (may_retry && !immutable && prior.has_value() &&
        same_object(*readback.object, *prior)) {
      *retry = true;
      return result(PublishOutcome::BLOCKED);
    }
    return result(PublishOutcome::FENCED,
                  "conditional object read-back contains divergent bytes");
  }

  // A success/conflict followed by 404 violates the strong-consistency
  // contract. Only a transport-unknown create may be retried after absence.
  if (may_retry && !prior.has_value()) {
    *retry = true;
    return result(PublishOutcome::BLOCKED);
  }
  return result(PublishOutcome::FENCED,
                "conditional object disappeared during read-back");
}

HeadPublisher::HeadPublisher(ConditionalIo *io, StreamIdentity stream,
                             size_t logical_attempt_limit)
    : stream_(std::move(stream)), store_(io, logical_attempt_limit) {}

PublishResult HeadPublisher::terminal(PublishOutcome outcome,
                                      std::string detail) {
  if (outcome == PublishOutcome::FENCED) fence(detail);
  if (outcome == PublishOutcome::BLOCKED) mark_blocked(detail);
  return result(outcome, std::move(detail));
}

void HeadPublisher::mark_blocked(std::string detail) {
  if (state_.lifecycle == LifecycleState::FENCED) return;
  state_.lifecycle = LifecycleState::BLOCKED;
  last_error_ = std::move(detail);
}

void HeadPublisher::fence(std::string detail) {
  state_.lifecycle = LifecycleState::FENCED;
  last_error_ = std::move(detail);
}

PublishResult HeadPublisher::read_head() {
  auto read =
      store_.read(stream_.remote_prefix + "/HEAD", kHeadMaxBytes);
  if (read.outcome == PublishOutcome::APPLIED) {
    Head parsed;
    std::string error;
    if (!parse_head(read.object->body, stream_, &parsed, &error))
      return terminal(PublishOutcome::FENCED,
                      "invalid remote HEAD: " + error);
    state_.head_object = *read.object;
    state_.head = std::move(parsed);
  } else if (read.outcome == PublishOutcome::ABSENT) {
    state_.head_object.reset();
    state_.head.reset();
  }
  return read;
}

PublishResult HeadPublisher::read_epoch() {
  auto read = store_.read(stream_.remote_prefix + "/WRITER_EPOCH",
                          kWriterEpochMaxBytes);
  if (read.outcome == PublishOutcome::APPLIED) {
    WriterEpoch parsed;
    std::string error;
    if (!parse_writer_epoch(read.object->body, stream_, &parsed, &error))
      return terminal(PublishOutcome::FENCED,
                      "invalid remote WRITER_EPOCH: " + error);
    state_.epoch_object = *read.object;
    state_.epoch = std::move(parsed);
  } else if (read.outcome == PublishOutcome::ABSENT) {
    state_.epoch_object.reset();
    state_.epoch.reset();
  }
  return read;
}

PublishResult HeadPublisher::probe() {
  if (state_.lifecycle == LifecycleState::FENCED)
    return result(PublishOutcome::FENCED, last_error_);
  state_.lifecycle = LifecycleState::INITIALIZING;

  auto head_result = read_head();
  if (head_result.outcome != PublishOutcome::APPLIED &&
      head_result.outcome != PublishOutcome::ABSENT)
    return terminal(head_result.outcome, head_result.detail);
  auto epoch_result = read_epoch();
  if (epoch_result.outcome != PublishOutcome::APPLIED &&
      epoch_result.outcome != PublishOutcome::ABSENT)
    return terminal(epoch_result.outcome, epoch_result.detail);

  if (state_.head.has_value() && !state_.epoch.has_value())
    return terminal(PublishOutcome::FENCED,
                    "HEAD exists while WRITER_EPOCH is absent");
  if (state_.head.has_value() &&
      (state_.head->writer.epoch == 0 ||
       state_.head->writer.epoch > state_.epoch->epoch))
    return terminal(PublishOutcome::FENCED,
                    "HEAD writer epoch is outside the admitted epoch");
  return result(PublishOutcome::APPLIED);
}

PublishResult HeadPublisher::acquire_epoch(std::string_view writer_id) {
  if (state_.lifecycle == LifecycleState::FENCED)
    return result(PublishOutcome::FENCED, last_error_);
  if (writer_id.size() != 32 ||
      writer_id.find_first_not_of("0123456789abcdef") != std::string_view::npos)
    return terminal(PublishOutcome::PERMANENT_ERROR,
                    "writer id must be 32 lowercase hexadecimal bytes");

  WriterEpoch intended;
  intended.writer_id.assign(writer_id);
  if (state_.epoch.has_value()) {
    if (state_.epoch->epoch >= kJsonSafeIntegerMax)
      return terminal(PublishOutcome::PERMANENT_ERROR,
                      "writer epoch exhausted");
    intended.epoch = state_.epoch->epoch + 1;
    intended.previous_epoch = state_.epoch->epoch;
  } else {
    intended.epoch = 1;
    intended.previous_epoch = 0;
  }

  std::string body;
  std::string error;
  if (!serialize_writer_epoch(stream_, intended, &body, &error))
    return terminal(PublishOutcome::PERMANENT_ERROR,
                    "cannot serialize WRITER_EPOCH: " + error);
  PublishResult write =
      state_.epoch_object.has_value()
          ? store_.compare_and_swap(stream_.remote_prefix + "/WRITER_EPOCH",
                                    body, *state_.epoch_object)
          : store_.create_fixed(stream_.remote_prefix + "/WRITER_EPOCH", body);
  if (!write.applied()) return terminal(write.outcome, write.detail);

  state_.epoch = intended;
  state_.epoch_object = *write.object;
  local_writer_id_.assign(writer_id);
  return write;
}

PublishResult HeadPublisher::bind_takeover_candidate(
    const PublishedBytes &expected_epoch_object,
    const WriterEpoch &expected_epoch,
    const PublishedBytes &expected_head_object, const Head &expected_head) {
  if (state_.lifecycle == LifecycleState::FENCED)
    return result(PublishOutcome::FENCED, last_error_);
  if (local_writer_id_.empty() || adopted_read_only_ ||
      state_.lifecycle == LifecycleState::RUNNING ||
      !state_.epoch.has_value() || !state_.epoch_object.has_value() ||
      !state_.head.has_value() || !state_.head_object.has_value() ||
      *state_.epoch != expected_epoch ||
      !same_object(*state_.epoch_object, expected_epoch_object) ||
      local_writer_id_ != expected_epoch.writer_id)
    return terminal(PublishOutcome::PERMANENT_ERROR,
                    "takeover candidate lacks locally acquired epoch");

  Head parsed;
  std::string error;
  if (expected_head_object.etag.empty() ||
      !parse_head(expected_head_object.body, stream_, &parsed, &error) ||
      parsed != expected_head ||
      expected_head.generation < state_.head->generation ||
      (expected_head.generation == state_.head->generation &&
       !same_object(*state_.head_object, expected_head_object)) ||
      expected_head.writer.epoch > expected_epoch.epoch ||
      (expected_head.writer.epoch == expected_epoch.epoch &&
       expected_head.writer.id != expected_epoch.writer_id))
    return terminal(PublishOutcome::FENCED,
                    "takeover candidate is invalid or regresses cached HEAD");

  const PublishResult head_read =
      store_.read(stream_.remote_prefix + "/HEAD", kHeadMaxBytes);
  if (!head_read.applied())
    return terminal(head_read.outcome == PublishOutcome::ABSENT
                        ? PublishOutcome::FENCED
                        : head_read.outcome,
                    "cannot verify takeover candidate: " + head_read.detail);
  if (!same_object(*head_read.object, expected_head_object))
    return terminal(PublishOutcome::FENCED,
                    "HEAD changed after takeover candidate selection");
  const PublishResult ownership = check_epoch_owner();
  if (!ownership.applied()) return ownership;
  if (!same_object(*ownership.object, expected_epoch_object))
    return terminal(PublishOutcome::FENCED,
                    "takeover epoch object identity changed");

  state_.head = expected_head;
  state_.head_object = expected_head_object;
  last_error_.clear();
  return result(PublishOutcome::APPLIED, {}, expected_head_object);
}

PublishResult HeadPublisher::adopt_epoch(
    const PublishedBytes &expected_epoch_object,
    const WriterEpoch &expected_epoch,
    const PublishedBytes &expected_head_object, const Head &expected_head) {
  if (state_.lifecycle == LifecycleState::FENCED)
    return result(PublishOutcome::FENCED, last_error_);
  if (!local_writer_id_.empty())
    return terminal(PublishOutcome::PERMANENT_ERROR,
                    "writer epoch ownership is already local");
  if (expected_epoch_object.body.empty() ||
      expected_epoch_object.etag.empty() || expected_head_object.body.empty() ||
      expected_head_object.etag.empty())
    return terminal(PublishOutcome::PERMANENT_ERROR,
                    "epoch adoption proof is incomplete");

  WriterEpoch canonical_epoch;
  Head canonical_head;
  std::string error;
  if (!parse_writer_epoch(expected_epoch_object.body, stream_,
                          &canonical_epoch, &error) ||
      canonical_epoch != expected_epoch ||
      !parse_head(expected_head_object.body, stream_, &canonical_head, &error) ||
      canonical_head != expected_head || expected_epoch.epoch == 0 ||
      expected_epoch.writer_id.empty() ||
      expected_head.writer.epoch > expected_epoch.epoch ||
      (expected_head.writer.epoch == expected_epoch.epoch &&
       expected_head.writer.id != expected_epoch.writer_id)) {
    std::string detail = "epoch adoption proof is not canonical";
    if (!error.empty()) detail.append(": ").append(error);
    return terminal(PublishOutcome::PERMANENT_ERROR, std::move(detail));
  }

  PublishResult head_read =
      store_.read(stream_.remote_prefix + "/HEAD", kHeadMaxBytes);
  if (!head_read.applied() || !head_read.object.has_value())
    return terminal(head_read.outcome,
                    "cannot read exact HEAD during epoch adoption: " +
                        head_read.detail);
  Head remote_head;
  if (!parse_head(head_read.object->body, stream_, &remote_head, &error))
    return terminal(PublishOutcome::FENCED,
                    "invalid HEAD during epoch adoption: " + error);

  PublishResult epoch_read = store_.read(
      stream_.remote_prefix + "/WRITER_EPOCH", kWriterEpochMaxBytes);
  if (!epoch_read.applied() || !epoch_read.object.has_value())
    return terminal(epoch_read.outcome,
                    "cannot read exact WRITER_EPOCH during adoption: " +
                        epoch_read.detail);
  WriterEpoch remote_epoch;
  if (!parse_writer_epoch(epoch_read.object->body, stream_, &remote_epoch,
                          &error))
    return terminal(PublishOutcome::FENCED,
                    "invalid WRITER_EPOCH during adoption: " + error);

  if (!same_object(*head_read.object, expected_head_object) ||
      remote_head != expected_head ||
      !same_object(*epoch_read.object, expected_epoch_object) ||
      remote_epoch != expected_epoch) {
    return terminal(PublishOutcome::FENCED,
                    "remote HEAD or WRITER_EPOCH changed before adoption");
  }

  state_.head_object = *head_read.object;
  state_.head = std::move(remote_head);
  state_.epoch_object = *epoch_read.object;
  state_.epoch = std::move(remote_epoch);
  local_writer_id_ = expected_epoch.writer_id;
  adopted_read_only_ = true;
  state_.lifecycle = LifecycleState::RECOVERING;
  last_error_.clear();
  return result(PublishOutcome::APPLIED, {}, *epoch_read.object);
}

PublishResult HeadPublisher::adopt_epoch_without_head(
    const PublishedBytes &expected_epoch_object,
    const WriterEpoch &expected_epoch) {
  if (state_.lifecycle == LifecycleState::FENCED)
    return result(PublishOutcome::FENCED, last_error_);
  if (!local_writer_id_.empty())
    return terminal(PublishOutcome::PERMANENT_ERROR,
                    "writer epoch ownership is already local");
  if (expected_epoch_object.body.empty() ||
      expected_epoch_object.etag.empty() || expected_epoch.epoch == 0 ||
      expected_epoch.writer_id.empty())
    return terminal(PublishOutcome::PERMANENT_ERROR,
                    "HEAD-absent epoch adoption proof is incomplete");

  WriterEpoch canonical_epoch;
  std::string error;
  if (!parse_writer_epoch(expected_epoch_object.body, stream_,
                          &canonical_epoch, &error) ||
      canonical_epoch != expected_epoch) {
    std::string detail =
        "HEAD-absent epoch adoption proof is not canonical";
    if (!error.empty()) detail.append(": ").append(error);
    return terminal(PublishOutcome::PERMANENT_ERROR, std::move(detail));
  }

  const PublishResult head_read =
      store_.read(stream_.remote_prefix + "/HEAD", kHeadMaxBytes);
  if (head_read.outcome != PublishOutcome::ABSENT) {
    const PublishOutcome outcome =
        head_read.applied() ? PublishOutcome::FENCED : head_read.outcome;
    return terminal(outcome,
                    head_read.applied()
                        ? "HEAD appeared before bootstrap epoch adoption"
                        : "cannot prove HEAD absent during epoch adoption: " +
                              head_read.detail);
  }

  PublishResult epoch_read = store_.read(
      stream_.remote_prefix + "/WRITER_EPOCH", kWriterEpochMaxBytes);
  if (!epoch_read.applied() || !epoch_read.object.has_value())
    return terminal(epoch_read.outcome,
                    "cannot read exact WRITER_EPOCH during adoption: " +
                        epoch_read.detail);
  WriterEpoch remote_epoch;
  if (!parse_writer_epoch(epoch_read.object->body, stream_, &remote_epoch,
                          &error))
    return terminal(PublishOutcome::FENCED,
                    "invalid WRITER_EPOCH during adoption: " + error);
  if (!same_object(*epoch_read.object, expected_epoch_object) ||
      remote_epoch != expected_epoch)
    return terminal(PublishOutcome::FENCED,
                    "remote WRITER_EPOCH changed before HEAD-absent adoption");

  state_.head.reset();
  state_.head_object.reset();
  state_.epoch_object = *epoch_read.object;
  state_.epoch = std::move(remote_epoch);
  local_writer_id_ = expected_epoch.writer_id;
  adopted_read_only_ = true;
  state_.lifecycle = LifecycleState::INITIALIZING;
  last_error_.clear();
  return result(PublishOutcome::APPLIED, {}, *epoch_read.object);
}

PublishResult HeadPublisher::activate_adopted_epoch(
    const Head &installed_head) {
  if (!adopted_read_only_)
    return terminal(PublishOutcome::PERMANENT_ERROR,
                    "writer epoch was not adopted read-only");
  PublishResult verified = verify_decision(installed_head);
  if (!verified.applied()) return verified;
  adopted_read_only_ = false;
  return verified;
}

PublishResult HeadPublisher::validate_transition(
    const TransitionManifest &manifest, const Head &head,
    std::string_view manifest_body, std::string_view manifest_sha,
    std::string_view manifest_key) const {
  if (!state_.epoch.has_value() || local_writer_id_.empty())
    return result(PublishOutcome::PERMANENT_ERROR,
                  "writer epoch has not been acquired");
  const Writer local_writer{local_writer_id_, state_.epoch->epoch};
  if (!(manifest.writer == local_writer) || !(head.writer == local_writer))
    return result(PublishOutcome::FENCED,
                  "transition writer does not own WRITER_EPOCH");
  if (manifest.generation != head.generation ||
      manifest.recovery_window != head.recovery_window ||
      manifest.segment_tip != head.segment_tip ||
      manifest.snapshot != head.snapshot ||
      manifest.base_cursor != head.base_cursor ||
      manifest.durable_cursor != head.durable_cursor)
    return result(PublishOutcome::PERMANENT_ERROR,
                  "manifest and intended HEAD fields diverge");
  if (head.manifest.key != manifest_key ||
      head.manifest.size != manifest_body.size() ||
      head.manifest.sha256 != manifest_sha)
    return result(PublishOutcome::PERMANENT_ERROR,
                  "intended HEAD does not reference exact manifest bytes");

  if (!state_.head.has_value()) {
    if (manifest.kind != ManifestKind::BOOTSTRAP || head.generation != 1 ||
        manifest.head_parent.has_value() || manifest.previous.has_value() ||
        head.parent.has_value())
      return result(PublishOutcome::PERMANENT_ERROR,
                    "invalid BOOTSTRAP transition");
    return result(PublishOutcome::APPLIED);
  }

  if (manifest.kind == ManifestKind::BOOTSTRAP ||
      state_.head->generation >= kJsonSafeIntegerMax ||
      head.generation != state_.head->generation + 1)
    return result(PublishOutcome::PERMANENT_ERROR,
                  "invalid non-bootstrap generation");
  if (!manifest.head_parent.has_value() || !manifest.previous.has_value() ||
      !head.parent.has_value() ||
      !(manifest.head_parent == head.parent))
    return result(PublishOutcome::PERMANENT_ERROR,
                  "transition parent tuple is missing or divergent");

  std::string prior_sha;
  std::string error;
  if (!sha256_hex(state_.head_object->body, &prior_sha, &error))
    return result(PublishOutcome::PERMANENT_ERROR,
                  "cannot hash cached HEAD: " + error);
  const HeadParent expected_parent{state_.head->generation,
                                   state_.head_object->etag, prior_sha};
  if (!(head.parent == expected_parent))
    return result(PublishOutcome::FENCED,
                  "transition does not use exact cached HEAD parent");
  const ObjectRef prior_manifest = manifest_object_ref(*state_.head);
  if (manifest.previous->generation != state_.head->generation ||
      manifest.previous->key != prior_manifest.key ||
      manifest.previous->size != prior_manifest.size ||
      manifest.previous->sha256 != prior_manifest.sha256)
    return result(PublishOutcome::PERMANENT_ERROR,
                  "transition does not reference the prior manifest");
  return result(PublishOutcome::APPLIED);
}

PublishResult HeadPublisher::publish(const TransitionManifest &manifest,
                                     const Head &intended_head) {
  if (adopted_read_only_)
    return result(PublishOutcome::PERMANENT_ERROR,
                  "read-only adopted epoch cannot publish remote state");
  if (state_.lifecycle == LifecycleState::FENCED)
    return result(PublishOutcome::FENCED, last_error_);
  std::string manifest_body;
  std::string manifest_sha;
  std::string manifest_key;
  std::string head_body;
  std::string error;
  if (!serialize_transition_manifest(stream_, manifest, &manifest_body,
                                     &error) ||
      !sha256_hex(manifest_body, &manifest_sha, &error) ||
      !transition_manifest_key(stream_, manifest.writer, manifest.generation,
                               manifest_sha, &manifest_key, &error) ||
      !serialize_head(stream_, intended_head, &head_body, &error))
    return terminal(PublishOutcome::PERMANENT_ERROR,
                    "cannot build transition: " + error);

  auto valid = validate_transition(manifest, intended_head, manifest_body,
                                   manifest_sha, manifest_key);
  if (!valid.applied()) return terminal(valid.outcome, valid.detail);

  auto manifest_write =
      store_.create_immutable(manifest_key, manifest_body);
  if (!manifest_write.applied())
    return terminal(manifest_write.outcome, manifest_write.detail);

  // Revalidate both fixed objects immediately before the HEAD CAS. The
  // immutable manifest may become an orphan, but a stale writer must never
  // issue a CAS after losing its epoch or after HEAD moved.
  auto owner = check_epoch_owner();
  if (!owner.applied()) return owner;
  auto current_head =
      store_.read(stream_.remote_prefix + "/HEAD", kHeadMaxBytes);
  if (current_head.applied() && current_head.object.has_value() &&
      current_head.object->body == head_body) {
    state_.head = intended_head;
    state_.head_object = *current_head.object;
    owner = check_epoch_owner();
    if (!owner.applied()) return owner;
    state_.lifecycle = LifecycleState::RUNNING;
    last_error_.clear();
    return current_head;
  }
  if (state_.head_object.has_value()) {
    if (current_head.outcome == PublishOutcome::ABSENT)
      return terminal(PublishOutcome::FENCED,
                      "prior HEAD disappeared before transition CAS");
    if (!current_head.applied())
      return terminal(current_head.outcome,
                      "cannot revalidate prior HEAD before CAS: " +
                          current_head.detail);
    if (!same_object(*current_head.object, *state_.head_object))
      return terminal(PublishOutcome::FENCED,
                      "HEAD changed before transition CAS");
  } else if (current_head.outcome != PublishOutcome::ABSENT) {
    if (current_head.applied())
      return terminal(PublishOutcome::FENCED,
                      "HEAD appeared before bootstrap CAS");
    return terminal(current_head.outcome,
                    "cannot revalidate absent HEAD before bootstrap CAS: " +
                        current_head.detail);
  }

  if (manifest.kind == ManifestKind::LOG_TRANSITION)
    production_fault_point("remote_commit_pause_after_read_head_before_cas");
  auto head_write =
      state_.head_object.has_value()
          ? store_.compare_and_swap(stream_.remote_prefix + "/HEAD", head_body,
                                    *state_.head_object)
          : store_.create_fixed(stream_.remote_prefix + "/HEAD", head_body);
  if (!head_write.applied())
    return terminal(head_write.outcome, head_write.detail);

  state_.head = intended_head;
  state_.head_object = *head_write.object;
  owner = check_epoch_owner();
  if (!owner.applied()) return owner;
  state_.lifecycle = LifecycleState::RUNNING;
  last_error_.clear();
  return head_write;
}

PublishResult HeadPublisher::check_epoch_owner() {
  auto current = store_.read(stream_.remote_prefix + "/WRITER_EPOCH",
                             kWriterEpochMaxBytes);
  if (!current.applied())
    return terminal(current.outcome,
                    "cannot verify WRITER_EPOCH ownership: " + current.detail);
  WriterEpoch parsed;
  std::string error;
  if (!parse_writer_epoch(current.object->body, stream_, &parsed, &error))
    return terminal(PublishOutcome::FENCED,
                    "invalid WRITER_EPOCH during ownership check: " + error);
  if (!state_.epoch.has_value() || !(parsed == *state_.epoch) ||
      parsed.writer_id != local_writer_id_)
    return terminal(PublishOutcome::FENCED, "writer epoch ownership was lost");
  state_.epoch = parsed;
  state_.epoch_object = *current.object;
  return current;
}

PublishResult HeadPublisher::verify_decision_impl(const Head &intended_head,
                                                  bool promote_running) {
  if (state_.lifecycle == LifecycleState::FENCED)
    return result(PublishOutcome::FENCED, last_error_);
  std::string intended_body;
  std::string error;
  if (!serialize_head(stream_, intended_head, &intended_body, &error))
    return terminal(PublishOutcome::PERMANENT_ERROR,
                    "cannot serialize intended HEAD: " + error);
  auto current =
      store_.read(stream_.remote_prefix + "/HEAD", kHeadMaxBytes);
  if (!current.applied())
    return terminal(current.outcome,
                    "cannot verify HEAD decision: " + current.detail);
  if (current.object->body != intended_body)
    return terminal(PublishOutcome::FENCED,
                    "HEAD no longer equals the intended decision");
  state_.head = intended_head;
  state_.head_object = *current.object;
  PublishResult owner = check_epoch_owner();
  if (!owner.applied()) return owner;
  if (promote_running) state_.lifecycle = LifecycleState::RUNNING;
  last_error_.clear();
  return owner;
}

PublishResult HeadPublisher::verify_decision(const Head &intended_head) {
  return verify_decision_impl(intended_head, true);
}

PublishResult HeadPublisher::verify_adopted_head_read_only(
    const Head &intended_head) {
  if (state_.lifecycle == LifecycleState::FENCED)
    return result(PublishOutcome::FENCED, last_error_);
  if (!adopted_read_only_)
    return terminal(PublishOutcome::PERMANENT_ERROR,
                    "writer epoch was not adopted read-only");
  return verify_decision_impl(intended_head, false);
}

}  // namespace wesql::remote_commit
