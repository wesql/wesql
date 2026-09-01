/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/runtime_snapshot_coordinator.h"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace rc = wesql::remote_commit;

namespace {

static_assert(std::is_move_constructible_v<rc::RuntimeSnapshotAcquisition>);
static_assert(!std::is_copy_constructible_v<rc::RuntimeSnapshotAcquisition>);

void expect(bool condition, const char *message) {
  if (!condition) {
    std::cerr << "runtime snapshot coordinator test failed: " << message
              << '\n';
    std::exit(EXIT_FAILURE);
  }
}

rc::PublishResult publish_result(rc::PublishOutcome outcome,
                                 std::string detail = {}) {
  return {outcome, std::move(detail), std::nullopt};
}

rc::RuntimeSnapshotRequest request(
    uint64_t request_id, rc::RuntimeSnapshotRequestReason reason) {
  rc::RuntimeSnapshotRequest value;
  value.request_id = request_id;
  value.reason = reason;
  return value;
}

class TrackingLease final : public rc::RuntimeSnapshotSourceLease {
 public:
  TrackingLease(int *release_count, std::vector<std::string> *events,
                uint64_t lease_id)
      : release_count_(release_count), events_(events), lease_id_(lease_id) {}

  ~TrackingLease() override {
    ++*release_count_;
    events_->push_back("release:" + std::to_string(lease_id_));
  }

 private:
  int *release_count_;
  std::vector<std::string> *events_;
  uint64_t lease_id_;
};

class FakeControl final : public rc::RuntimeSnapshotControl {
 public:
  explicit FakeControl(std::vector<std::string> *events) : events_(events) {}

  bool take(rc::RuntimeSnapshotRequest *value) override {
    events_->push_back("take");
    if (next_request_ == requests.size()) return false;
    *value = requests[next_request_++];
    live_request_ = *value;
    return true;
  }

  bool wait(rc::RuntimeSnapshotRequest *value) override {
    events_->push_back("wait");
    if (wait_returns_shutdown) return true;
    if (next_request_ == requests.size()) return true;
    *value = requests[next_request_++];
    live_request_ = *value;
    return false;
  }

  rc::RuntimeSnapshotRequestResult refresh(uint64_t request_id) override {
    events_->push_back("refresh:" + std::to_string(request_id));
    refreshed_request_ids.push_back(request_id);
    const size_t index = refresh_calls++;
    if (hard_on_refresh_call.has_value() &&
        *hard_on_refresh_call == refresh_calls && live_request_.has_value()) {
      live_request_->reason = rc::RuntimeSnapshotRequestReason::HARD_LIMIT;
    }
    const rc::RuntimeSnapshotRequestOutcome outcome =
        index < refresh_outcomes.size()
            ? refresh_outcomes[index]
            : rc::RuntimeSnapshotRequestOutcome::ACTIVE;
    rc::RuntimeSnapshotRequestResult result;
    result.outcome = outcome;
    result.detail = "injected refresh outcome";
    if (outcome == rc::RuntimeSnapshotRequestOutcome::ACTIVE)
      result.request = live_request_;
    return result;
  }

  bool reserve_hard_gate(uint64_t request_id, std::string *error) override {
    events_->push_back("reserve:" + std::to_string(request_id));
    reserved_request_ids.push_back(request_id);
    const size_t index = reserve_calls++;
    const bool failed = index < reserve_failures.size() &&
                        reserve_failures[index];
    if (failed && error != nullptr) *error = "injected reserve failure";
    return failed;
  }

  rc::RuntimeSnapshotRequestResult complete(uint64_t request_id) override {
    events_->push_back("complete:" + std::to_string(request_id));
    completed_request_ids.push_back(request_id);
    const size_t index = complete_calls++;
    if (hard_before_complete && live_request_.has_value())
      live_request_->reason = rc::RuntimeSnapshotRequestReason::HARD_LIMIT;
    if (live_request_.has_value())
      completed_reasons.push_back(live_request_->reason);
    const rc::RuntimeSnapshotRequestOutcome outcome =
        index < complete_outcomes.size()
            ? complete_outcomes[index]
            : rc::RuntimeSnapshotRequestOutcome::COMPLETED;
    rc::RuntimeSnapshotRequestResult result;
    result.outcome = outcome;
    result.detail = "injected completion outcome";
    if (outcome == rc::RuntimeSnapshotRequestOutcome::ACTIVE)
      result.request = live_request_;
    if (outcome == rc::RuntimeSnapshotRequestOutcome::COMPLETED)
      live_request_.reset();
    return result;
  }

  void mark_terminal(uint64_t request_id,
                     rc::RuntimeSnapshotRequestOutcome outcome,
                     std::string_view) override {
    events_->push_back(
        std::string(outcome == rc::RuntimeSnapshotRequestOutcome::FENCED
                        ? "terminal-fenced:"
                        : "terminal-permanent:") +
        std::to_string(request_id));
    terminal_request_ids.push_back(request_id);
    terminal_outcomes.push_back(outcome);
  }

  std::vector<rc::RuntimeSnapshotRequest> requests;
  std::vector<rc::RuntimeSnapshotRequestOutcome> refresh_outcomes;
  std::vector<rc::RuntimeSnapshotRequestOutcome> complete_outcomes;
  std::vector<bool> reserve_failures;
  std::vector<uint64_t> refreshed_request_ids;
  std::vector<uint64_t> reserved_request_ids;
  std::vector<uint64_t> completed_request_ids;
  std::vector<rc::RuntimeSnapshotRequestReason> completed_reasons;
  std::vector<uint64_t> terminal_request_ids;
  std::vector<rc::RuntimeSnapshotRequestOutcome> terminal_outcomes;
  std::optional<size_t> hard_on_refresh_call;
  size_t refresh_calls{0};
  size_t reserve_calls{0};
  size_t complete_calls{0};
  bool hard_before_complete{false};
  bool wait_returns_shutdown{false};

 private:
  std::vector<std::string> *events_;
  size_t next_request_{0};
  std::optional<rc::RuntimeSnapshotRequest> live_request_;
};

class FakeAcquirer final : public rc::RuntimeSnapshotAcquirer {
 public:
  explicit FakeAcquirer(std::vector<std::string> *events) : events_(events) {}

  rc::RuntimeSnapshotAcquisitionResult acquire(
      const rc::RuntimeSnapshotRequest &value) override {
    events_->push_back("acquire:" + std::to_string(value.request_id));
    request_ids.push_back(value.request_id);
    const size_t index = acquire_calls++;
    const rc::RuntimeSnapshotAcquisitionOutcome outcome =
        index < outcomes.size()
            ? outcomes[index]
            : rc::RuntimeSnapshotAcquisitionOutcome::FIXED;

    rc::RuntimeSnapshotAcquisitionResult result;
    result.outcome = outcome;
    if (outcome != rc::RuntimeSnapshotAcquisitionOutcome::FIXED) {
      result.detail = "injected acquisition outcome";
      return result;
    }

    result.acquisition.emplace();
    rc::RuntimeSnapshotAcquisition &acquisition = *result.acquisition;
    acquisition.request_id = returned_request_id.value_or(value.request_id);
    acquisition.authority.request_id =
        returned_authority_id.value_or(value.request_id);
    acquisition.cut.writer.id = "cut-" + std::to_string(acquire_calls);
    acquisition.cut.writer.epoch = acquire_calls;
    if (!omit_source_lease) {
      ++leases_created;
      acquisition.source_lease = std::make_unique<TrackingLease>(
          &leases_released, events_, acquire_calls);
    }
    return result;
  }

  std::vector<rc::RuntimeSnapshotAcquisitionOutcome> outcomes;
  std::vector<uint64_t> request_ids;
  std::optional<uint64_t> returned_request_id;
  std::optional<uint64_t> returned_authority_id;
  size_t acquire_calls{0};
  int leases_created{0};
  int leases_released{0};
  bool omit_source_lease{false};

 private:
  std::vector<std::string> *events_;
};

class FakePublication final : public rc::RuntimeSnapshotPublicationDriver {
 public:
  explicit FakePublication(std::vector<std::string> *events)
      : events_(events) {}

  rc::PublishResult prepare(
      const rc::RuntimeSnapshotAcquisition &acquisition,
      rc::PreparedSnapshotPublication *prepared) override {
    events_->push_back("prepare:" + acquisition.cut.writer.id);
    prepared_cut_ids.push_back(acquisition.cut.writer.id);
    const size_t index = prepare_calls++;
    const rc::PublishOutcome outcome =
        index < prepare_outcomes.size() ? prepare_outcomes[index]
                                        : rc::PublishOutcome::APPLIED;
    if (outcome == rc::PublishOutcome::APPLIED)
      prepared->writer = acquisition.cut.writer;
    return publish_result(outcome, "injected preparation outcome");
  }

  rc::PublishResult publish(
      uint64_t request_id, const rc::PreparedSnapshotPublication &prepared,
      rc::SnapshotPublication *publication) override {
    events_->push_back("publish:" + prepared.writer.id);
    published_request_ids.push_back(request_id);
    published_cut_ids.push_back(prepared.writer.id);
    const size_t index = publish_calls++;
    const rc::PublishOutcome outcome =
        index < publish_outcomes.size() ? publish_outcomes[index]
                                        : rc::PublishOutcome::APPLIED;
    if (outcome == rc::PublishOutcome::APPLIED) {
      publication->head.generation = publish_calls;
      publication->head.writer = prepared.writer;
    }
    return publish_result(outcome, "injected publication outcome");
  }

  std::vector<rc::PublishOutcome> prepare_outcomes;
  std::vector<rc::PublishOutcome> publish_outcomes;
  std::vector<std::string> prepared_cut_ids;
  std::vector<std::string> published_cut_ids;
  std::vector<uint64_t> published_request_ids;
  size_t prepare_calls{0};
  size_t publish_calls{0};

 private:
  std::vector<std::string> *events_;
};

struct Fixture {
  Fixture()
      : control(&events),
        acquirer(&events),
        publication(&events),
        coordinator(&control, &acquirer, &publication) {}

  void initialize() {
    const rc::RuntimeSnapshotCoordinatorResult result =
        coordinator.initialize();
    expect(result.outcome == rc::RuntimeSnapshotCoordinatorOutcome::READY,
           "coordinator did not initialize to READY");
    expect(result.state == rc::RuntimeSnapshotCoordinatorState::READY,
           "initialized coordinator state is not READY");
  }

  std::vector<std::string> events;
  FakeControl control;
  FakeAcquirer acquirer;
  FakePublication publication;
  rc::RuntimeSnapshotCoordinator coordinator;
};

void test_initialize_then_idle() {
  Fixture fixture;
  const rc::RuntimeSnapshotCoordinatorResult before = fixture.coordinator.poll();
  expect(before.outcome ==
             rc::RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
         "poll before initialize did not fail");
  expect(fixture.coordinator.state() ==
             rc::RuntimeSnapshotCoordinatorState::UNINITIALIZED,
         "pre-initialize poll changed coordinator state");

  fixture.initialize();
  const rc::RuntimeSnapshotCoordinatorResult idle = fixture.coordinator.poll();
  expect(idle.outcome == rc::RuntimeSnapshotCoordinatorOutcome::IDLE,
         "empty nonblocking poll was not IDLE");
  expect(idle.request_id == 0, "IDLE result carried a request ID");
  expect(fixture.control.reserve_calls == 0,
         "IDLE poll reserved the hard gate");
  expect(fixture.acquirer.acquire_calls == 0, "IDLE poll acquired a cut");
}

void test_soft_happy_path_preserves_request_id() {
  Fixture fixture;
  fixture.control.requests.push_back(
      request(41, rc::RuntimeSnapshotRequestReason::SOFT_LIMIT));
  fixture.initialize();

  const rc::RuntimeSnapshotCoordinatorResult result = fixture.coordinator.poll();
  expect(result.published(), "soft request was not published");
  expect(result.request_id == 41, "published result lost its request ID");
  expect(result.state == rc::RuntimeSnapshotCoordinatorState::READY,
         "successful soft request did not return to READY");
  expect(fixture.control.reserve_calls == 0,
         "soft request reserved the hard gate");
  expect(fixture.control.complete_calls == 1,
         "soft request skipped atomic request completion");
  expect(fixture.acquirer.acquire_calls == 1,
         "soft request did not acquire exactly once");
  expect(fixture.publication.prepare_calls == 1,
         "soft request did not prepare exactly once");
  expect(fixture.publication.publish_calls == 1,
         "soft request did not publish exactly once");
  expect(fixture.publication.published_request_ids ==
             std::vector<uint64_t>{41},
         "soft publication received the wrong request ID");
  expect(fixture.acquirer.leases_released == 1,
         "soft request did not release its source lease");
  expect(fixture.coordinator.last_publication().has_value(),
         "successful publication was not retained");
}

void test_hard_happy_path_orders_gate_around_publication() {
  Fixture fixture;
  fixture.control.requests.push_back(
      request(42, rc::RuntimeSnapshotRequestReason::HARD_LIMIT));
  fixture.initialize();

  const rc::RuntimeSnapshotCoordinatorResult result = fixture.coordinator.poll();
  expect(result.published(), "hard request was not published");
  expect(result.request_id == 42, "hard result lost its request ID");
  expect(fixture.control.reserved_request_ids ==
             std::vector<uint64_t>{42},
         "hard request was not reserved exactly once");
  expect(fixture.control.completed_request_ids ==
             std::vector<uint64_t>{42},
         "hard request was not completed exactly once");
  expect(fixture.events ==
             std::vector<std::string>{
                 "take",          "refresh:42",   "reserve:42",
                 "acquire:42",    "refresh:42",   "prepare:cut-1",
                 "refresh:42",    "publish:cut-1", "complete:42",
                 "release:1"},
         "hard request lifecycle was out of order");
}

void test_prepare_blocked_retains_same_acquisition() {
  Fixture fixture;
  fixture.control.requests.push_back(
      request(43, rc::RuntimeSnapshotRequestReason::SOFT_LIMIT));
  fixture.publication.prepare_outcomes = {rc::PublishOutcome::BLOCKED,
                                          rc::PublishOutcome::APPLIED};
  fixture.initialize();

  const rc::RuntimeSnapshotCoordinatorResult blocked = fixture.coordinator.poll();
  expect(blocked.outcome == rc::RuntimeSnapshotCoordinatorOutcome::BLOCKED,
         "prepare BLOCKED was not surfaced");
  expect(blocked.request_id == 43, "prepare BLOCKED lost request authority");
  expect(fixture.acquirer.acquire_calls == 1,
         "prepare BLOCKED reacquired immediately");
  expect(fixture.acquirer.leases_released == 0,
         "prepare BLOCKED released its fixed source");

  const rc::RuntimeSnapshotCoordinatorResult published = fixture.coordinator.poll();
  expect(published.published(), "prepare BLOCKED retry did not publish");
  expect(fixture.acquirer.acquire_calls == 1,
         "prepare BLOCKED retry reacquired the cut");
  expect(fixture.publication.prepared_cut_ids ==
             std::vector<std::string>{"cut-1", "cut-1"},
         "prepare BLOCKED retry changed its fixed acquisition");
  expect(fixture.acquirer.leases_released == 1,
         "prepare BLOCKED success did not release its source lease");
}

void test_publish_blocked_retains_prepared_snapshot() {
  Fixture fixture;
  fixture.control.requests.push_back(
      request(44, rc::RuntimeSnapshotRequestReason::SOFT_LIMIT));
  fixture.publication.publish_outcomes = {rc::PublishOutcome::BLOCKED,
                                          rc::PublishOutcome::APPLIED};
  fixture.initialize();

  const rc::RuntimeSnapshotCoordinatorResult blocked = fixture.coordinator.poll();
  expect(blocked.outcome == rc::RuntimeSnapshotCoordinatorOutcome::BLOCKED,
         "publish BLOCKED was not surfaced");
  expect(fixture.acquirer.acquire_calls == 1,
         "publish BLOCKED reacquired immediately");
  expect(fixture.publication.prepare_calls == 1,
         "publish BLOCKED repeated preparation immediately");
  expect(fixture.acquirer.leases_released == 0,
         "publish BLOCKED released its fixed source");

  const rc::RuntimeSnapshotCoordinatorResult published = fixture.coordinator.poll();
  expect(published.published(), "publish BLOCKED retry did not publish");
  expect(fixture.acquirer.acquire_calls == 1,
         "publish BLOCKED retry reacquired the cut");
  expect(fixture.publication.prepare_calls == 1,
         "publish BLOCKED retry repeated preparation");
  expect(fixture.publication.published_cut_ids ==
             std::vector<std::string>{"cut-1", "cut-1"},
         "publish BLOCKED retry changed its prepared snapshot");
  expect(fixture.acquirer.leases_released == 1,
         "publish BLOCKED success did not release its source lease");
}

void test_prepare_refix_releases_and_reacquires() {
  Fixture fixture;
  fixture.control.requests.push_back(
      request(45, rc::RuntimeSnapshotRequestReason::SOFT_LIMIT));
  fixture.publication.prepare_outcomes = {rc::PublishOutcome::REFIX_REQUIRED,
                                          rc::PublishOutcome::APPLIED};
  fixture.initialize();

  const rc::RuntimeSnapshotCoordinatorResult refix = fixture.coordinator.poll();
  expect(refix.outcome ==
             rc::RuntimeSnapshotCoordinatorOutcome::REFIX_REQUIRED,
         "prepare REFIX_REQUIRED was not surfaced");
  expect(refix.request_id == 45, "prepare REFIX changed request authority");
  expect(fixture.acquirer.leases_released == 1,
         "prepare REFIX did not release the old source lease");
  expect(fixture.publication.publish_calls == 0,
         "prepare REFIX reached ordered publication");

  const rc::RuntimeSnapshotCoordinatorResult published = fixture.coordinator.poll();
  expect(published.published(), "prepare REFIX retry did not publish");
  expect(fixture.acquirer.request_ids == std::vector<uint64_t>{45, 45},
         "prepare REFIX retry changed request ID");
  expect(fixture.publication.prepared_cut_ids ==
             std::vector<std::string>{"cut-1", "cut-2"},
         "prepare REFIX retry reused the old fixed acquisition");
  expect(fixture.acquirer.leases_released == 2,
         "prepare REFIX retry leaked a source lease");
}

void test_publish_refix_keeps_hard_reservation_and_reacquires() {
  Fixture fixture;
  fixture.control.requests.push_back(
      request(46, rc::RuntimeSnapshotRequestReason::HARD_LIMIT));
  fixture.publication.publish_outcomes = {
      rc::PublishOutcome::REFIX_REQUIRED, rc::PublishOutcome::APPLIED};
  fixture.initialize();

  const rc::RuntimeSnapshotCoordinatorResult refix = fixture.coordinator.poll();
  expect(refix.outcome ==
             rc::RuntimeSnapshotCoordinatorOutcome::REFIX_REQUIRED,
         "publish REFIX_REQUIRED was not surfaced");
  expect(refix.request_id == 46, "publish REFIX changed request authority");
  expect(fixture.control.reserve_calls == 1,
         "publish REFIX did not retain the hard reservation");
  expect(fixture.acquirer.leases_released == 1,
         "publish REFIX did not release the old source lease");

  const rc::RuntimeSnapshotCoordinatorResult published = fixture.coordinator.poll();
  expect(published.published(), "publish REFIX retry did not publish");
  expect(fixture.control.reserve_calls == 1,
         "publish REFIX retry reserved the hard gate again");
  expect(fixture.control.complete_calls == 1,
         "publish REFIX retry did not complete the hard gate");
  expect(fixture.acquirer.request_ids == std::vector<uint64_t>{46, 46},
         "publish REFIX retry changed request ID");
  expect(fixture.publication.prepared_cut_ids ==
             std::vector<std::string>{"cut-1", "cut-2"},
         "publish REFIX retry did not reprepare from a new cut");
  expect(fixture.acquirer.leases_released == 2,
         "publish REFIX retry leaked a source lease");
}

void test_hard_completion_blocked_does_not_republish() {
  Fixture fixture;
  fixture.control.requests.push_back(
      request(47, rc::RuntimeSnapshotRequestReason::HARD_LIMIT));
  fixture.control.complete_outcomes = {
      rc::RuntimeSnapshotRequestOutcome::BLOCKED,
      rc::RuntimeSnapshotRequestOutcome::COMPLETED};
  fixture.initialize();

  const rc::RuntimeSnapshotCoordinatorResult blocked = fixture.coordinator.poll();
  expect(blocked.outcome == rc::RuntimeSnapshotCoordinatorOutcome::BLOCKED,
         "hard completion failure was not retryable");
  expect(blocked.request_id == 47, "hard completion BLOCKED lost request ID");
  expect(fixture.publication.publish_calls == 1,
         "hard completion failure did not follow publication");
  expect(fixture.acquirer.leases_released == 0,
         "hard completion failure released retained state");

  const rc::RuntimeSnapshotCoordinatorResult published = fixture.coordinator.poll();
  expect(published.published(), "hard completion retry did not finish");
  expect(fixture.control.reserve_calls == 1,
         "hard completion retry repeated reservation");
  expect(fixture.control.complete_calls == 2,
         "hard completion was not retried exactly once");
  expect(fixture.acquirer.acquire_calls == 1,
         "hard completion retry reacquired the cut");
  expect(fixture.publication.prepare_calls == 1,
         "hard completion retry reprepared the snapshot");
  expect(fixture.publication.publish_calls == 1,
         "hard completion retry republished the snapshot");
  expect(fixture.acquirer.leases_released == 1,
         "hard completion success did not release retained state");
}

void test_soft_to_hard_refresh_reserves_before_next_phase() {
  Fixture fixture;
  fixture.control.requests.push_back(
      request(52, rc::RuntimeSnapshotRequestReason::SOFT_LIMIT));
  fixture.control.hard_on_refresh_call = 2;
  fixture.initialize();

  const rc::RuntimeSnapshotCoordinatorResult result = fixture.coordinator.poll();
  expect(result.published(), "refreshed SOFT-to-HARD request did not publish");
  expect(result.request_id == 52,
         "refreshed SOFT-to-HARD request changed authority");
  expect(fixture.control.reserve_calls == 1,
         "refreshed HARD request did not reserve before preparation");
  expect(fixture.control.complete_calls == 1,
         "refreshed HARD request did not complete atomically");
  expect(fixture.events ==
             std::vector<std::string>{
                 "take",          "refresh:52",    "acquire:52",
                 "refresh:52",    "reserve:52",    "prepare:cut-1",
                 "refresh:52",    "publish:cut-1", "complete:52",
                 "release:1"},
         "SOFT-to-HARD refresh was not applied before the next phase");
}

void test_post_applied_soft_to_hard_upgrade_reserves_without_republish() {
  Fixture fixture;
  fixture.control.requests.push_back(
      request(53, rc::RuntimeSnapshotRequestReason::SOFT_LIMIT));
  fixture.control.hard_before_complete = true;
  fixture.control.complete_outcomes = {
      rc::RuntimeSnapshotRequestOutcome::BLOCKED,
      rc::RuntimeSnapshotRequestOutcome::COMPLETED};
  fixture.initialize();

  const rc::RuntimeSnapshotCoordinatorResult blocked = fixture.coordinator.poll();
  expect(blocked.outcome == rc::RuntimeSnapshotCoordinatorOutcome::BLOCKED,
         "late SOFT-to-HARD upgrade did not block for hard reservation");
  expect(fixture.control.reserve_calls == 0,
         "stale SOFT copy fabricated an early hard reservation");
  expect(fixture.control.complete_calls == 1,
         "soft publication skipped unconditional completion");
  expect(fixture.acquirer.leases_released == 0,
         "late HARD completion released the fixed source while blocked");
  expect(fixture.acquirer.acquire_calls == 1,
         "late HARD completion reacquired before retry");
  expect(fixture.publication.prepare_calls == 1,
         "late HARD completion reprepared before retry");
  expect(fixture.publication.publish_calls == 1,
         "late HARD completion republished before retry");

  const rc::RuntimeSnapshotCoordinatorResult result = fixture.coordinator.poll();
  expect(result.published(), "late SOFT-to-HARD upgrade did not complete");
  expect(fixture.control.reserve_calls == 1,
         "late HARD request was not reserved exactly once on retry");
  expect(fixture.control.complete_calls == 2,
         "late HARD completion was not retried exactly once");
  expect(fixture.control.completed_reasons ==
             std::vector<rc::RuntimeSnapshotRequestReason>{
                 rc::RuntimeSnapshotRequestReason::HARD_LIMIT,
                 rc::RuntimeSnapshotRequestReason::HARD_LIMIT},
         "atomic completion did not observe the late HARD request");
  expect(fixture.acquirer.acquire_calls == 1,
         "late HARD completion retry reacquired the fixed cut");
  expect(fixture.publication.prepare_calls == 1,
         "late HARD completion retry reprepared the snapshot");
  expect(fixture.publication.publish_calls == 1,
         "late HARD completion retry republished the SNAPSHOT");
  expect(fixture.acquirer.leases_released == 1,
         "late SOFT-to-HARD completion leaked its source lease");
  expect(fixture.events ==
             std::vector<std::string>{
                 "take",          "refresh:53",   "acquire:53",
                 "refresh:53",    "prepare:cut-1", "refresh:53",
                 "publish:cut-1", "complete:53",  "refresh:53",
                 "reserve:53",    "complete:53",  "release:1"},
         "late HARD retry changed the post-publication lifecycle");
}

void test_completion_refix_after_applied_publication_is_terminal() {
  Fixture fixture;
  fixture.control.requests.push_back(
      request(54, rc::RuntimeSnapshotRequestReason::SOFT_LIMIT));
  fixture.control.hard_before_complete = true;
  fixture.control.complete_outcomes = {
      rc::RuntimeSnapshotRequestOutcome::REFIX_REQUIRED};
  fixture.initialize();

  const rc::RuntimeSnapshotCoordinatorResult result = fixture.coordinator.poll();
  expect(result.outcome ==
             rc::RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
         "post-publication REFIX was not terminal");
  expect(result.request_id == 54,
         "post-publication REFIX lost request authority");
  expect(fixture.acquirer.leases_released == 1,
         "terminal post-publication REFIX leaked its source lease");
  expect(fixture.control.reserve_calls == 0,
         "late upgrade fabricated a pre-publication hard reservation");
  expect(fixture.control.complete_calls == 1,
         "post-publication REFIX repeated completion");
  expect(fixture.acquirer.acquire_calls == 1,
         "post-publication REFIX reacquired a cut");
  expect(fixture.publication.prepare_calls == 1,
         "post-publication REFIX reprepared a snapshot");
  expect(fixture.publication.publish_calls == 1,
         "post-publication REFIX published a second SNAPSHOT");
  expect(fixture.control.terminal_outcomes ==
             std::vector<rc::RuntimeSnapshotRequestOutcome>{
                 rc::RuntimeSnapshotRequestOutcome::PERMANENT_ERROR},
         "post-publication REFIX did not fence the server request");
}

void test_refresh_refix_after_applied_publication_is_terminal() {
  Fixture fixture;
  fixture.control.requests.push_back(
      request(57, rc::RuntimeSnapshotRequestReason::SOFT_LIMIT));
  fixture.control.refresh_outcomes = {
      rc::RuntimeSnapshotRequestOutcome::ACTIVE,
      rc::RuntimeSnapshotRequestOutcome::ACTIVE,
      rc::RuntimeSnapshotRequestOutcome::ACTIVE,
      rc::RuntimeSnapshotRequestOutcome::REFIX_REQUIRED};
  fixture.control.complete_outcomes = {
      rc::RuntimeSnapshotRequestOutcome::BLOCKED};
  fixture.initialize();

  const rc::RuntimeSnapshotCoordinatorResult blocked = fixture.coordinator.poll();
  expect(blocked.outcome == rc::RuntimeSnapshotCoordinatorOutcome::BLOCKED,
         "post-publication refresh REFIX fixture did not retain completion");
  expect(fixture.acquirer.leases_released == 0,
         "completion BLOCKED released the source before refresh REFIX");

  const rc::RuntimeSnapshotCoordinatorResult terminal = fixture.coordinator.poll();
  expect(terminal.outcome ==
             rc::RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
         "post-publication refresh REFIX was not terminal");
  expect(terminal.request_id == 57,
         "post-publication refresh REFIX lost request authority");
  expect(fixture.acquirer.acquire_calls == 1,
         "post-publication refresh REFIX reacquired a cut");
  expect(fixture.publication.prepare_calls == 1,
         "post-publication refresh REFIX reprepared a snapshot");
  expect(fixture.publication.publish_calls == 1,
         "post-publication refresh REFIX published a second SNAPSHOT");
  expect(fixture.control.complete_calls == 1,
         "post-publication refresh REFIX retried completion");
  expect(fixture.acquirer.leases_released == 1,
         "post-publication refresh REFIX leaked the source lease");
  expect(fixture.events.back() == "release:1" &&
             fixture.events[fixture.events.size() - 2] ==
                 "terminal-permanent:57",
         "post-publication refresh REFIX released before terminal marking");
}

void test_invalid_acquisition_authority_is_terminal_and_releases() {
  Fixture fixture;
  fixture.control.requests.push_back(
      request(48, rc::RuntimeSnapshotRequestReason::SOFT_LIMIT));
  fixture.acquirer.returned_authority_id = 49;
  fixture.initialize();

  const rc::RuntimeSnapshotCoordinatorResult result = fixture.coordinator.poll();
  expect(result.outcome ==
             rc::RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
         "mismatched acquisition authority was not terminal");
  expect(result.request_id == 48,
         "authority mismatch result lost the owning request ID");
  expect(fixture.publication.prepare_calls == 0,
         "mismatched authority reached preparation");
  expect(fixture.acquirer.leases_released == 1,
         "mismatched acquisition leaked its source lease");
  expect(fixture.events ==
             std::vector<std::string>{"take", "refresh:48", "acquire:48",
                                      "terminal-permanent:48", "release:1"},
         "authority failure released its lease before terminal marking");

  fixture.coordinator.poll();
  expect(fixture.acquirer.acquire_calls == 1,
         "terminal authority mismatch was retried");
}

void test_missing_source_lease_is_terminal() {
  Fixture fixture;
  fixture.control.requests.push_back(
      request(49, rc::RuntimeSnapshotRequestReason::SOFT_LIMIT));
  fixture.acquirer.omit_source_lease = true;
  fixture.initialize();

  const rc::RuntimeSnapshotCoordinatorResult result = fixture.coordinator.poll();
  expect(result.outcome ==
             rc::RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
         "missing source lease was not terminal");
  expect(fixture.publication.prepare_calls == 0,
         "missing source lease reached preparation");
}

void test_fenced_publication_is_terminal_and_releases() {
  Fixture fixture;
  fixture.control.requests.push_back(
      request(50, rc::RuntimeSnapshotRequestReason::SOFT_LIMIT));
  fixture.publication.publish_outcomes = {rc::PublishOutcome::FENCED};
  fixture.initialize();

  const rc::RuntimeSnapshotCoordinatorResult fenced = fixture.coordinator.poll();
  expect(fenced.outcome == rc::RuntimeSnapshotCoordinatorOutcome::FENCED,
         "FENCED publication was not terminal");
  expect(fenced.state == rc::RuntimeSnapshotCoordinatorState::FENCED,
         "FENCED publication set the wrong state");
  expect(fenced.request_id == 50, "FENCED publication lost request ID");
  expect(fixture.acquirer.leases_released == 1,
         "FENCED publication leaked its source lease");
  expect(fixture.events ==
             std::vector<std::string>{
                 "take",          "refresh:50",       "acquire:50",
                 "refresh:50",    "prepare:cut-1",    "refresh:50",
                 "publish:cut-1", "terminal-fenced:50", "release:1"},
         "FENCED publication released its lease before server fencing");

  fixture.coordinator.poll();
  expect(fixture.publication.publish_calls == 1,
         "FENCED publication was retried");
}

void test_shutdown_wait_and_active_lease_release() {
  {
    Fixture fixture;
    fixture.control.wait_returns_shutdown = true;
    fixture.initialize();
    const rc::RuntimeSnapshotCoordinatorResult result = fixture.coordinator.wait();
    expect(result.outcome == rc::RuntimeSnapshotCoordinatorOutcome::SHUTDOWN,
           "blocking wait cancellation was not SHUTDOWN");
    expect(result.state == rc::RuntimeSnapshotCoordinatorState::SHUTDOWN,
           "blocking wait cancellation set the wrong state");
  }

  {
    Fixture fixture;
    fixture.control.requests.push_back(
        request(51, rc::RuntimeSnapshotRequestReason::SOFT_LIMIT));
    fixture.publication.prepare_outcomes = {rc::PublishOutcome::BLOCKED};
    fixture.initialize();
    expect(fixture.coordinator.poll().outcome ==
               rc::RuntimeSnapshotCoordinatorOutcome::BLOCKED,
           "shutdown fixture did not retain a fixed acquisition");
    expect(fixture.acquirer.leases_released == 0,
           "shutdown fixture released its lease before shutdown");

    fixture.control.refresh_outcomes = {
        rc::RuntimeSnapshotRequestOutcome::ACTIVE,
        rc::RuntimeSnapshotRequestOutcome::ACTIVE,
        rc::RuntimeSnapshotRequestOutcome::SHUTDOWN};
    const rc::RuntimeSnapshotCoordinatorResult shutdown =
        fixture.coordinator.poll();
    expect(shutdown.outcome == rc::RuntimeSnapshotCoordinatorOutcome::SHUTDOWN,
           "active request did not observe hooks shutdown");
    expect(shutdown.request_id == 51,
           "active shutdown result lost request authority");
    expect(fixture.acquirer.leases_released == 1,
           "shutdown did not release the active source lease");
    expect(fixture.coordinator.poll().outcome ==
               rc::RuntimeSnapshotCoordinatorOutcome::SHUTDOWN,
           "poll after explicit shutdown did not remain SHUTDOWN");
    expect(fixture.acquirer.leases_released == 1,
           "poll after hooks shutdown released the source lease twice");
  }

  {
    Fixture fixture;
    fixture.control.requests.push_back(
        request(55, rc::RuntimeSnapshotRequestReason::SOFT_LIMIT));
    fixture.acquirer.outcomes = {
        rc::RuntimeSnapshotAcquisitionOutcome::SHUTDOWN};
    fixture.initialize();
    const rc::RuntimeSnapshotCoordinatorResult shutdown =
        fixture.coordinator.poll();
    expect(shutdown.outcome == rc::RuntimeSnapshotCoordinatorOutcome::SHUTDOWN,
           "source-fixation shutdown was not propagated");
    expect(shutdown.request_id == 55,
           "source-fixation shutdown lost request authority");
    expect(fixture.control.terminal_request_ids.empty(),
           "orderly source shutdown marked the request terminal");
  }

  {
    Fixture fixture;
    fixture.control.requests.push_back(
        request(56, rc::RuntimeSnapshotRequestReason::SOFT_LIMIT));
    fixture.publication.prepare_outcomes = {rc::PublishOutcome::BLOCKED};
    fixture.initialize();
    expect(fixture.coordinator.poll().outcome ==
               rc::RuntimeSnapshotCoordinatorOutcome::BLOCKED,
           "explicit shutdown fixture did not retain a source lease");
    fixture.coordinator.shutdown();
    expect(fixture.acquirer.leases_released == 1,
           "explicit coordinator shutdown leaked its source lease");
    fixture.coordinator.shutdown();
    expect(fixture.coordinator.poll().outcome ==
               rc::RuntimeSnapshotCoordinatorOutcome::SHUTDOWN,
           "repeated coordinator shutdown did not remain SHUTDOWN");
    expect(fixture.acquirer.leases_released == 1,
           "repeated coordinator shutdown released the source lease twice");
    expect(fixture.coordinator.state() ==
               rc::RuntimeSnapshotCoordinatorState::SHUTDOWN,
           "explicit coordinator shutdown set the wrong state");
  }
}

}  // namespace

int main() {
  test_initialize_then_idle();
  test_soft_happy_path_preserves_request_id();
  test_hard_happy_path_orders_gate_around_publication();
  test_prepare_blocked_retains_same_acquisition();
  test_publish_blocked_retains_prepared_snapshot();
  test_prepare_refix_releases_and_reacquires();
  test_publish_refix_keeps_hard_reservation_and_reacquires();
  test_hard_completion_blocked_does_not_republish();
  test_soft_to_hard_refresh_reserves_before_next_phase();
  test_post_applied_soft_to_hard_upgrade_reserves_without_republish();
  test_completion_refix_after_applied_publication_is_terminal();
  test_refresh_refix_after_applied_publication_is_terminal();
  test_invalid_acquisition_authority_is_terminal_and_releases();
  test_missing_source_lease_is_terminal();
  test_fenced_publication_is_terminal_and_releases();
  test_shutdown_wait_and_active_lease_release();
  std::cout << "runtime snapshot coordinator tests passed\n";
  return EXIT_SUCCESS;
}
