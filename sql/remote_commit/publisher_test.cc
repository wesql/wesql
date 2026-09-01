/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/publisher.h"

#include <algorithm>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

namespace rc = wesql::remote_commit;

namespace {

objstore::Status status(objstore::Errors code, const char *message) {
  return objstore::Status(code, 0, message);
}

void expect(bool condition, const char *message) {
  if (!condition) {
    std::cerr << "publisher test failed: " << message << '\n';
    std::exit(EXIT_FAILURE);
  }
}

class FakeIo final : public rc::ConditionalIo {
 public:
  struct PutCall {
    std::string key;
    std::string body;
    objstore::ConditionalPutMode mode;
    std::string etag;
  };

  objstore::ExactObjectResult get(std::string_view key,
                                  uint64_t max_bytes) override {
    operations.emplace_back("GET " + std::string(key));
    get_keys.emplace_back(key);
    get_max_bytes.push_back(max_bytes);
    expect(!gets.empty(), "unexpected exact GET");
    auto value = std::move(gets.front());
    gets.pop_front();
    if (value.is_found() && value.size() > max_bytes) {
      return objstore::ExactObjectResult::permanent_error(
          status(objstore::SE_UNEXPECTED, "bounded exact GET exceeded"));
    }
    return value;
  }

  objstore::ConditionalPutResult put(
      std::string_view key, std::string_view body,
      const objstore::ConditionalPutCondition &condition) override {
    operations.emplace_back("PUT " + std::string(key));
    puts.push_back({std::string(key), std::string(body), condition.mode(),
                    condition.etag()});
    expect(!put_results.empty(), "unexpected conditional PUT");
    auto value = std::move(put_results.front());
    put_results.pop_front();
    return value;
  }

  std::deque<objstore::ExactObjectResult> gets;
  std::deque<objstore::ConditionalPutResult> put_results;
  std::vector<std::string> get_keys;
  std::vector<uint64_t> get_max_bytes;
  std::vector<PutCall> puts;
  std::vector<std::string> operations;
};

std::string hash(std::string_view bytes) {
  std::string digest;
  std::string error;
  expect(rc::sha256_hex(bytes, &digest, &error), "cannot hash fixture bytes");
  return digest;
}

struct PublishFixture {
  rc::StreamIdentity stream;
  rc::WriterEpoch epoch1;
  rc::WriterEpoch epoch2;
  rc::Head prior_head;
  std::string prior_head_body;
  std::string prior_head_etag{"\"head-1\""};
  rc::TransitionManifest transition;
  std::string transition_body;
  rc::Head intended_head;
  std::string intended_head_body;
};

rc::SnapshotRef snapshot_ref(const rc::StreamIdentity &stream,
                             std::string id, rc::Cursor cursor,
                             std::string_view body) {
  rc::SnapshotRef ref;
  ref.id = std::move(id);
  ref.cursor = std::move(cursor);
  ref.manifest_size = body.size();
  ref.manifest_sha256 = hash(body);
  std::string error;
  expect(rc::snapshot_manifest_key(stream, ref.id, ref.manifest_sha256,
                                   &ref.manifest_key, &error),
         "cannot derive fixture snapshot key");
  return ref;
}

rc::SegmentTip snapshot_tip(const rc::SnapshotRef &snapshot) {
  rc::SegmentTip tip;
  tip.kind = rc::SegmentTipKind::SNAPSHOT_ROOT;
  tip.snapshot_id = snapshot.id;
  tip.cursor = snapshot.cursor;
  return tip;
}

PublishFixture make_publish_fixture() {
  PublishFixture fixture;
  std::string error;
  expect(rc::build_stream_identity("repo-1", "branch-1", "cluster/main",
                                   &fixture.stream, &error),
         "cannot build publisher fixture stream");
  const rc::Writer writer1{"11111111111111111111111111111111", 1};
  const rc::Writer writer2{"22222222222222222222222222222222", 2};
  fixture.epoch1 = {1, writer1.id, 0};
  fixture.epoch2 = {2, writer2.id, 1};
  const rc::Cursor cursor{"binlog.000001", 4};
  const rc::SnapshotRef first_snapshot = snapshot_ref(
      fixture.stream, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", cursor,
      "snapshot-one");

  rc::TransitionManifest bootstrap;
  bootstrap.kind = rc::ManifestKind::BOOTSTRAP;
  bootstrap.generation = 1;
  bootstrap.writer = writer1;
  bootstrap.recovery_window = {1, 1, 0};
  bootstrap.segment_tip = snapshot_tip(first_snapshot);
  bootstrap.snapshot = first_snapshot;
  bootstrap.base_cursor = cursor;
  bootstrap.durable_cursor = cursor;
  std::string bootstrap_body;
  expect(rc::stabilize_transition_manifest(fixture.stream, 0, &bootstrap,
                                           &bootstrap_body, &error),
         "cannot stabilize fixture bootstrap");
  const std::string bootstrap_sha = hash(bootstrap_body);
  std::string bootstrap_key;
  expect(rc::transition_manifest_key(fixture.stream, writer1, 1, bootstrap_sha,
                                     &bootstrap_key, &error),
         "cannot derive fixture bootstrap key");

  fixture.prior_head.generation = 1;
  fixture.prior_head.writer = writer1;
  fixture.prior_head.manifest =
      {bootstrap_key, bootstrap_body.size(), bootstrap_sha};
  fixture.prior_head.recovery_window = bootstrap.recovery_window;
  fixture.prior_head.segment_tip = bootstrap.segment_tip;
  fixture.prior_head.snapshot = first_snapshot;
  fixture.prior_head.base_cursor = cursor;
  fixture.prior_head.durable_cursor = cursor;
  expect(rc::serialize_head(fixture.stream, fixture.prior_head,
                            &fixture.prior_head_body, &error),
         "cannot serialize fixture prior HEAD");

  const rc::SnapshotRef next_snapshot = snapshot_ref(
      fixture.stream, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", cursor,
      "snapshot-two");
  const rc::HeadParent parent{1, fixture.prior_head_etag,
                              hash(fixture.prior_head_body)};
  fixture.transition.kind = rc::ManifestKind::SNAPSHOT;
  fixture.transition.generation = 2;
  fixture.transition.writer = writer2;
  fixture.transition.head_parent = parent;
  fixture.transition.previous = rc::ManifestRef{
      1, bootstrap_key, bootstrap_body.size(), bootstrap_sha};
  fixture.transition.recovery_window = {2, 1, 0};
  fixture.transition.segment_tip = snapshot_tip(next_snapshot);
  fixture.transition.snapshot = next_snapshot;
  fixture.transition.base_cursor = cursor;
  fixture.transition.durable_cursor = cursor;
  expect(rc::stabilize_transition_manifest(
             fixture.stream, bootstrap_body.size(), &fixture.transition,
             &fixture.transition_body, &error),
         "cannot stabilize fixture transition");
  const std::string transition_sha = hash(fixture.transition_body);
  std::string transition_key;
  expect(rc::transition_manifest_key(fixture.stream, writer2, 2,
                                     transition_sha, &transition_key, &error),
         "cannot derive fixture transition key");

  fixture.intended_head.generation = 2;
  fixture.intended_head.writer = writer2;
  fixture.intended_head.parent = parent;
  fixture.intended_head.manifest =
      {transition_key, fixture.transition_body.size(), transition_sha};
  fixture.intended_head.recovery_window = fixture.transition.recovery_window;
  fixture.intended_head.segment_tip = fixture.transition.segment_tip;
  fixture.intended_head.snapshot = next_snapshot;
  fixture.intended_head.base_cursor = cursor;
  fixture.intended_head.durable_cursor = cursor;
  expect(rc::serialize_head(fixture.stream, fixture.intended_head,
                            &fixture.intended_head_body, &error),
         "cannot serialize fixture intended HEAD");
  return fixture;
}

void test_success_requires_readback() {
  FakeIo io;
  io.put_results.push_back(objstore::ConditionalPutResult::applied("ignored"));
  io.gets.push_back(objstore::ExactObjectResult::found("body", "\"v1\""));
  rc::ProtocolStore store(&io);
  const auto result = store.create_fixed("HEAD", "body");
  expect(result.applied(), "successful PUT and read-back");
  expect(result.object->etag == "\"v1\"", "read-back ETag is authoritative");
  expect(io.puts.size() == 1 && io.get_keys.size() == 1,
         "one PUT and one read-back");
  expect(io.get_max_bytes.front() == 5,
         "conditional read-back did not use intended size plus sentinel");
}

void test_explicit_read_cap_is_forwarded() {
  FakeIo io;
  io.gets.push_back(objstore::ExactObjectResult::found("body", "\"v1\""));
  rc::ProtocolStore store(&io);
  const auto result = store.read("HEAD", 7);
  expect(result.applied() && io.get_max_bytes.size() == 1 &&
             io.get_max_bytes.front() == 7,
         "ProtocolStore changed the exact GET byte cap");
}

void test_success_then_missing_fences() {
  FakeIo io;
  io.put_results.push_back(objstore::ConditionalPutResult::applied());
  io.gets.push_back(objstore::ExactObjectResult::not_found(
      status(objstore::SE_NO_SUCH_KEY, "missing")));
  rc::ProtocolStore store(&io);
  const auto result = store.create_fixed("HEAD", "body");
  expect(result.outcome == rc::PublishOutcome::FENCED,
         "success followed by 404 fences");
}

void test_conflict_disambiguation() {
  {
    FakeIo io;
    io.put_results.push_back(objstore::ConditionalPutResult::conflict_409(
        status(objstore::SE_OBJECT_CONFLICT, "conflict")));
    io.gets.push_back(
        objstore::ExactObjectResult::found("intended", "\"v2\""));
    rc::ProtocolStore store(&io);
    const auto result = store.create_fixed("HEAD", "intended");
    expect(result.applied(), "conflict with intended read-back is success");
    expect(io.puts.size() == 1, "explicit conflict is not retried");
  }
  {
    FakeIo io;
    io.put_results.push_back(
        objstore::ConditionalPutResult::precondition_failed_412(
            status(objstore::SE_OBJECT_PRECONDITION_FAILED,
                   "precondition")));
    io.gets.push_back(objstore::ExactObjectResult::found("prior", "\"p\""));
    rc::ProtocolStore store(&io);
    const rc::PublishedBytes prior{"prior", "\"p\""};
    const auto result = store.compare_and_swap("HEAD", "intended", prior);
    expect(result.outcome == rc::PublishOutcome::FENCED,
           "explicit precondition failure with prior fences");
    expect(io.puts.size() == 1, "412 is never retried");
  }
}

void test_transport_unknown_retries_frozen_cas() {
  FakeIo io;
  io.put_results.push_back(objstore::ConditionalPutResult::transport_unknown(
      status(objstore::CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED,
             "timeout")));
  io.put_results.push_back(objstore::ConditionalPutResult::applied());
  io.gets.push_back(objstore::ExactObjectResult::found("prior", "\"p\""));
  io.gets.push_back(
      objstore::ExactObjectResult::found("intended", "\"next\""));
  rc::ProtocolStore store(&io, 2);
  const rc::PublishedBytes prior{"prior", "\"p\""};
  const auto result = store.compare_and_swap("HEAD", "intended", prior);
  expect(result.applied(), "transport-unknown retries after exact prior");
  expect(io.puts.size() == 2, "exactly two logical attempts");
  expect(io.puts[0].key == io.puts[1].key &&
             io.puts[0].body == io.puts[1].body &&
             io.puts[0].etag == io.puts[1].etag &&
             io.puts[0].mode == io.puts[1].mode,
         "retry freezes key/body/condition/ETag");
}

void test_transport_unknown_create_after_404() {
  FakeIo io;
  io.put_results.push_back(objstore::ConditionalPutResult::transport_unknown(
      status(objstore::CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED,
             "timeout")));
  io.put_results.push_back(objstore::ConditionalPutResult::applied());
  io.gets.push_back(objstore::ExactObjectResult::not_found(
      status(objstore::SE_NO_SUCH_KEY, "missing")));
  io.gets.push_back(
      objstore::ExactObjectResult::found(std::string("a\0b", 3), "\"e\""));
  rc::ProtocolStore store(&io, 2);
  const std::string body("a\0b", 3);
  const auto result = store.create_immutable("segment", body);
  expect(result.applied(), "create retries after transport-unknown and 404");
  expect(result.object->body.size() == 3,
         "immutable read-back remains binary safe");
  expect(io.puts.size() == 2 &&
             io.puts[0].mode == objstore::ConditionalPutMode::CREATE_ONLY &&
             io.puts[1].mode == objstore::ConditionalPutMode::CREATE_ONLY,
         "immutable retry preserves create-only condition");
}

void test_readback_unavailable_blocks_without_rewrite() {
  FakeIo io;
  io.put_results.push_back(objstore::ConditionalPutResult::transport_unknown(
      status(objstore::CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED,
             "timeout")));
  io.gets.push_back(objstore::ExactObjectResult::transient_unavailable(
      status(objstore::CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED,
             "unavailable")));
  rc::ProtocolStore store(&io, 2);
  const auto result = store.create_fixed("HEAD", "body");
  expect(result.outcome == rc::PublishOutcome::BLOCKED,
         "unavailable read-back blocks");
  expect(io.puts.size() == 1, "blocked read-back does not rewrite");
}

void test_divergent_immutable_fences() {
  FakeIo io;
  io.put_results.push_back(objstore::ConditionalPutResult::conflict_409(
      status(objstore::SE_OBJECT_CONFLICT, "conflict")));
  io.gets.push_back(
      objstore::ExactObjectResult::found("different", "\"v2\""));
  rc::ProtocolStore store(&io);
  const auto result = store.create_immutable("segment", "intended");
  expect(result.outcome == rc::PublishOutcome::FENCED,
         "immutable key collision fences");
}

void test_publish_revalidates_epoch_and_exact_head_before_cas() {
  const PublishFixture fixture = make_publish_fixture();
  std::string epoch1_body;
  std::string epoch2_body;
  std::string error;
  expect(rc::serialize_writer_epoch(fixture.stream, fixture.epoch1,
                                    &epoch1_body, &error) &&
             rc::serialize_writer_epoch(fixture.stream, fixture.epoch2,
                                        &epoch2_body, &error),
         "cannot serialize publisher fixture epochs");

  FakeIo io;
  io.gets.push_back(objstore::ExactObjectResult::found(
      fixture.prior_head_body, fixture.prior_head_etag));
  io.gets.push_back(
      objstore::ExactObjectResult::found(epoch1_body, "\"epoch-1\""));
  rc::HeadPublisher publisher(&io, fixture.stream);
  expect(publisher.probe().applied(), "cannot probe existing fixture stream");
  expect(io.get_max_bytes.size() == 2 &&
             io.get_max_bytes[0] == rc::kHeadMaxBytes &&
             io.get_max_bytes[1] == rc::kWriterEpochMaxBytes,
         "publisher probe did not bound HEAD and WRITER_EPOCH reads");

  io.put_results.push_back(objstore::ConditionalPutResult::applied());
  io.gets.push_back(
      objstore::ExactObjectResult::found(epoch2_body, "\"epoch-2\""));
  expect(publisher.acquire_epoch(fixture.epoch2.writer_id).applied(),
         "cannot acquire fixture writer epoch");

  io.operations.clear();
  io.puts.clear();
  io.get_keys.clear();
  io.put_results.push_back(objstore::ConditionalPutResult::applied());
  io.gets.push_back(objstore::ExactObjectResult::found(
      fixture.transition_body, "\"manifest-2\""));
  io.gets.push_back(
      objstore::ExactObjectResult::found(epoch2_body, "\"epoch-2\""));
  io.gets.push_back(objstore::ExactObjectResult::found(
      fixture.prior_head_body, fixture.prior_head_etag));
  io.put_results.push_back(objstore::ConditionalPutResult::applied());
  io.gets.push_back(objstore::ExactObjectResult::found(
      fixture.intended_head_body, "\"head-2\""));
  io.gets.push_back(
      objstore::ExactObjectResult::found(epoch2_body, "\"epoch-2\""));

  expect(publisher
             .publish(fixture.transition, fixture.intended_head)
             .applied(),
         "publisher transition failed");
  const std::string prefix = fixture.stream.remote_prefix;
  const std::vector<std::string> expected{
      "PUT " + fixture.intended_head.manifest.key,
      "GET " + fixture.intended_head.manifest.key,
      "GET " + prefix + "/WRITER_EPOCH",
      "GET " + prefix + "/HEAD",
      "PUT " + prefix + "/HEAD",
      "GET " + prefix + "/HEAD",
      "GET " + prefix + "/WRITER_EPOCH",
  };
  expect(io.operations == expected,
         "epoch/HEAD revalidation is not immediately before the HEAD CAS");
  expect(io.puts.size() == 2 &&
             io.puts.back().mode ==
                 objstore::ConditionalPutMode::MATCH_ETAG &&
             io.puts.back().etag == fixture.prior_head_etag,
         "HEAD CAS did not preserve the exact probed ETag");
}

void test_publish_retry_resolves_already_applied_intended_head() {
  const PublishFixture fixture = make_publish_fixture();
  std::string epoch1_body;
  std::string epoch2_body;
  std::string error;
  expect(rc::serialize_writer_epoch(fixture.stream, fixture.epoch1,
                                    &epoch1_body, &error) &&
             rc::serialize_writer_epoch(fixture.stream, fixture.epoch2,
                                        &epoch2_body, &error),
         "cannot serialize retry fixture epochs");

  FakeIo io;
  io.gets.push_back(objstore::ExactObjectResult::found(
      fixture.prior_head_body, fixture.prior_head_etag));
  io.gets.push_back(
      objstore::ExactObjectResult::found(epoch1_body, "\"epoch-1\""));
  rc::HeadPublisher publisher(&io, fixture.stream);
  expect(publisher.probe().applied(), "cannot probe retry fixture stream");
  io.put_results.push_back(objstore::ConditionalPutResult::applied());
  io.gets.push_back(
      objstore::ExactObjectResult::found(epoch2_body, "\"epoch-2\""));
  expect(publisher.acquire_epoch(fixture.epoch2.writer_id).applied(),
         "cannot acquire retry fixture epoch");

  io.operations.clear();
  io.puts.clear();
  io.put_results.push_back(objstore::ConditionalPutResult::applied());
  io.gets.push_back(objstore::ExactObjectResult::found(
      fixture.transition_body, "\"manifest-2\""));
  io.gets.push_back(
      objstore::ExactObjectResult::found(epoch2_body, "\"epoch-2\""));
  io.gets.push_back(objstore::ExactObjectResult::found(
      fixture.prior_head_body, fixture.prior_head_etag));
  io.put_results.push_back(objstore::ConditionalPutResult::transport_unknown(
      status(objstore::SE_IO_ERROR, "HEAD result unknown")));
  io.gets.push_back(objstore::ExactObjectResult::transient_unavailable(
      status(objstore::SE_IO_ERROR, "HEAD readback unavailable")));

  const rc::PublishResult first =
      publisher.publish(fixture.transition, fixture.intended_head);
  expect(first.outcome == rc::PublishOutcome::BLOCKED &&
             publisher.state().lifecycle == rc::LifecycleState::BLOCKED,
         "transport-unknown HEAD did not retain retryable blocked state");

  io.put_results.push_back(objstore::ConditionalPutResult::conflict_409(
      status(objstore::SE_OBJECT_CONFLICT, "manifest already exists")));
  io.gets.push_back(objstore::ExactObjectResult::found(
      fixture.transition_body, "\"manifest-2\""));
  io.gets.push_back(
      objstore::ExactObjectResult::found(epoch2_body, "\"epoch-2\""));
  io.gets.push_back(objstore::ExactObjectResult::found(
      fixture.intended_head_body, "\"head-2\""));
  io.gets.push_back(
      objstore::ExactObjectResult::found(epoch2_body, "\"epoch-2\""));

  const rc::PublishResult retried =
      publisher.publish(fixture.transition, fixture.intended_head);
  const std::string head_key = fixture.stream.remote_prefix + "/HEAD";
  const size_t head_puts = static_cast<size_t>(std::count_if(
      io.puts.begin(), io.puts.end(), [&](const FakeIo::PutCall &put) {
        return put.key == head_key;
      }));
  expect(retried.applied() && head_puts == 1 &&
             publisher.state().head == fixture.intended_head &&
             publisher.state().lifecycle == rc::LifecycleState::RUNNING &&
             publisher.last_error().empty(),
         "retry did not adopt the exact intended HEAD without a second PUT");

  publisher.mark_blocked("temporary verification failure");
  io.gets.push_back(objstore::ExactObjectResult::found(
      fixture.intended_head_body, "\"head-2\""));
  io.gets.push_back(
      objstore::ExactObjectResult::found(epoch2_body, "\"epoch-2\""));
  expect(publisher.verify_decision(fixture.intended_head).applied() &&
             publisher.state().lifecycle == rc::LifecycleState::RUNNING &&
             publisher.last_error().empty(),
         "successful exact verification did not clear blocked lifecycle state");
}

void test_adopt_epoch_requires_exact_read_only_proof() {
  const PublishFixture fixture = make_publish_fixture();
  std::string epoch1_body;
  std::string epoch2_body;
  std::string error;
  expect(rc::serialize_writer_epoch(fixture.stream, fixture.epoch1,
                                    &epoch1_body, &error) &&
             rc::serialize_writer_epoch(fixture.stream, fixture.epoch2,
                                        &epoch2_body, &error),
         "cannot serialize epoch adoption fixtures");
  const rc::PublishedBytes epoch_object{epoch2_body, "\"epoch-2\""};
  const rc::PublishedBytes head_object{fixture.intended_head_body,
                                       "\"head-2\""};

  {
    FakeIo io;
    io.gets.push_back(objstore::ExactObjectResult::found(
        head_object.body, head_object.etag));
    io.gets.push_back(objstore::ExactObjectResult::found(
        epoch_object.body, epoch_object.etag));
    rc::HeadPublisher publisher(&io, fixture.stream);
    expect(publisher
               .adopt_epoch(epoch_object, fixture.epoch2, head_object,
                            fixture.intended_head)
               .applied(),
           "exact epoch adoption failed");
    expect(io.puts.empty() && io.operations.size() == 2 &&
               io.operations[0] ==
                   "GET " + fixture.stream.remote_prefix + "/HEAD" &&
               io.operations[1] ==
                   "GET " + fixture.stream.remote_prefix + "/WRITER_EPOCH",
           "epoch adoption was not a HEAD-first read-only operation");

    const size_t operations_before_publish = io.operations.size();
    expect(publisher
               .publish(fixture.transition, fixture.intended_head)
               .outcome == rc::PublishOutcome::PERMANENT_ERROR &&
               io.operations.size() == operations_before_publish,
           "read-only adoption issued a remote publication");

    io.gets.push_back(objstore::ExactObjectResult::found(
        head_object.body, head_object.etag));
    io.gets.push_back(objstore::ExactObjectResult::found(
        epoch_object.body, epoch_object.etag));
    expect(publisher.verify_decision(fixture.intended_head).applied(),
           "adoption did not restore local writer ownership");

    io.gets.push_back(objstore::ExactObjectResult::found(
        head_object.body, head_object.etag));
    io.gets.push_back(objstore::ExactObjectResult::found(
        epoch_object.body, epoch_object.etag));
    expect(publisher.activate_adopted_epoch(fixture.intended_head).applied(),
           "installed-root activation did not promote adopted ownership");
  }

  const auto expect_remote_rejected = [&](std::string remote_head_body,
                                          std::string remote_head_etag,
                                          std::string remote_epoch_body,
                                          std::string remote_epoch_etag,
                                          const char *message) {
    FakeIo io;
    io.gets.push_back(objstore::ExactObjectResult::found(
        std::move(remote_head_body), std::move(remote_head_etag)));
    io.gets.push_back(objstore::ExactObjectResult::found(
        std::move(remote_epoch_body), std::move(remote_epoch_etag)));
    rc::HeadPublisher publisher(&io, fixture.stream);
    expect(!publisher
                .adopt_epoch(epoch_object, fixture.epoch2, head_object,
                             fixture.intended_head)
                .applied(),
           message);
    expect(io.puts.empty() && !publisher.state().head.has_value() &&
               !publisher.state().epoch.has_value(),
           "failed adoption installed local ownership state");
  };

  expect_remote_rejected(fixture.prior_head_body, fixture.prior_head_etag,
                         epoch_object.body, epoch_object.etag,
                         "changed HEAD was adopted");
  expect_remote_rejected(head_object.body, "\"changed-head-etag\"",
                         epoch_object.body, epoch_object.etag,
                         "changed HEAD ETag was adopted");
  expect_remote_rejected(head_object.body, head_object.etag, epoch1_body,
                         epoch_object.etag,
                         "changed epoch body/value was adopted");
  expect_remote_rejected(head_object.body, head_object.etag, epoch_object.body,
                         "\"changed-epoch-etag\"",
                         "changed epoch ETag was adopted");

  {
    FakeIo io;
    rc::HeadPublisher publisher(&io, fixture.stream);
    expect(!publisher
                .adopt_epoch(epoch_object, fixture.epoch1, head_object,
                             fixture.intended_head)
                .applied(),
           "proof body/value mismatch was adopted");
    expect(io.operations.empty() && io.puts.empty() &&
               !publisher.state().head.has_value() &&
               !publisher.state().epoch.has_value(),
           "noncanonical proof mutated local ownership state");
  }
}

void test_takeover_adopts_older_candidate_read_only() {
  const PublishFixture fixture = make_publish_fixture();
  std::string epoch2_body;
  std::string error;
  expect(rc::serialize_writer_epoch(fixture.stream, fixture.epoch2,
                                    &epoch2_body, &error),
         "cannot serialize takeover epoch");
  const rc::PublishedBytes epoch_object{epoch2_body, "\"epoch-2\""};
  const rc::PublishedBytes head_object{fixture.prior_head_body,
                                       fixture.prior_head_etag};

  FakeIo io;
  io.gets.push_back(objstore::ExactObjectResult::found(
      head_object.body, head_object.etag));
  io.gets.push_back(objstore::ExactObjectResult::found(
      epoch_object.body, epoch_object.etag));
  rc::HeadPublisher publisher(&io, fixture.stream);
  expect(publisher
             .adopt_epoch(epoch_object, fixture.epoch2, head_object,
                          fixture.prior_head)
             .applied(),
         "takeover worker rejected an exact older-writer candidate HEAD");
  expect(io.puts.empty(), "takeover adoption performed a remote write");
  const size_t operations = io.operations.size();
  expect(publisher
             .publish(fixture.transition, fixture.intended_head)
             .outcome == rc::PublishOutcome::PERMANENT_ERROR &&
             io.operations.size() == operations,
         "takeover worker could publish with a read-only adoption");
}

void test_bootstrap_adopts_epoch_while_head_absent() {
  const PublishFixture fixture = make_publish_fixture();
  std::string epoch2_body;
  std::string error;
  expect(rc::serialize_writer_epoch(fixture.stream, fixture.epoch2,
                                    &epoch2_body, &error),
         "cannot serialize bootstrap epoch");
  const rc::PublishedBytes epoch_object{epoch2_body, "\"epoch-2\""};

  {
    FakeIo io;
    io.gets.push_back(objstore::ExactObjectResult::not_found(
        status(objstore::SE_NO_SUCH_KEY, "missing")));
    io.gets.push_back(objstore::ExactObjectResult::found(
        epoch_object.body, epoch_object.etag));
    rc::HeadPublisher publisher(&io, fixture.stream);
    expect(publisher
               .adopt_epoch_without_head(epoch_object, fixture.epoch2)
               .applied(),
           "bootstrap snapshot worker could not adopt an exact epoch");
    expect(io.puts.empty() && io.operations.size() == 2 &&
               io.operations[0] ==
                   "GET " + fixture.stream.remote_prefix + "/HEAD" &&
               io.operations[1] ==
                   "GET " + fixture.stream.remote_prefix + "/WRITER_EPOCH",
           "HEAD-absent adoption was not read-only and HEAD-first");
    const size_t operations = io.operations.size();
    expect(publisher
               .publish(fixture.transition, fixture.intended_head)
               .outcome == rc::PublishOutcome::PERMANENT_ERROR &&
               io.operations.size() == operations,
           "bootstrap snapshot worker could publish remote state");
  }

  {
    FakeIo io;
    io.gets.push_back(objstore::ExactObjectResult::found(
        fixture.prior_head_body, fixture.prior_head_etag));
    rc::HeadPublisher publisher(&io, fixture.stream);
    expect(publisher
               .adopt_epoch_without_head(epoch_object, fixture.epoch2)
               .outcome == rc::PublishOutcome::FENCED,
           "bootstrap adoption accepted a newly appeared HEAD");
    expect(io.puts.empty() && io.operations.size() == 1,
           "appeared HEAD triggered another operation or write");
  }
}

}  // namespace

int main() {
  test_success_requires_readback();
  test_explicit_read_cap_is_forwarded();
  test_success_then_missing_fences();
  test_conflict_disambiguation();
  test_transport_unknown_retries_frozen_cas();
  test_transport_unknown_create_after_404();
  test_readback_unavailable_blocks_without_rewrite();
  test_divergent_immutable_fences();
  test_publish_revalidates_epoch_and_exact_head_before_cas();
  test_publish_retry_resolves_already_applied_intended_head();
  test_adopt_epoch_requires_exact_read_only_proof();
  test_takeover_adopts_older_candidate_read_only();
  test_bootstrap_adopts_epoch_while_head_absent();
  std::cout << "remote commit publisher tests passed\n";
  return EXIT_SUCCESS;
}
