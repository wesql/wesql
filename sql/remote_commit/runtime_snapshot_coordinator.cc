/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/runtime_snapshot_coordinator.h"

#include <utility>

namespace wesql::remote_commit {
#ifndef WESQL_RUNTIME_SNAPSHOT_COORDINATOR_TEST_ONLY
namespace {

PublishResult publication_error(std::string detail) {
  return {PublishOutcome::PERMANENT_ERROR, std::move(detail), std::nullopt};
}

}  // namespace

bool ServerRuntimeSnapshotControl::take(RuntimeSnapshotRequest *request) {
  return take_runtime_snapshot_request(request);
}

bool ServerRuntimeSnapshotControl::wait(RuntimeSnapshotRequest *request) {
  return wait_for_runtime_snapshot_request(request);
}

RuntimeSnapshotRequestResult ServerRuntimeSnapshotControl::refresh(
    uint64_t request_id) {
  return refresh_runtime_snapshot_request(request_id);
}

bool ServerRuntimeSnapshotControl::reserve_hard_gate(uint64_t request_id,
                                                     std::string *error) {
  return reserve_runtime_snapshot_hard_gate(request_id, error);
}

RuntimeSnapshotRequestResult ServerRuntimeSnapshotControl::complete(
    uint64_t request_id) {
  return complete_runtime_snapshot_request(request_id);
}

void ServerRuntimeSnapshotControl::mark_terminal(
    uint64_t request_id, RuntimeSnapshotRequestOutcome outcome,
    std::string_view detail) {
  mark_runtime_snapshot_terminal(request_id, outcome, detail);
}

PublishResult ServerRuntimeSnapshotPublicationDriver::prepare(
    const RuntimeSnapshotAcquisition &acquisition,
    PreparedSnapshotPublication *prepared) {
  if (snapshot_publisher_ == nullptr || prepared == nullptr ||
      acquisition.request_id == 0 ||
      acquisition.authority.request_id != acquisition.request_id ||
      acquisition.authority.metadata_io == nullptr ||
      acquisition.authority.metadata_store == nullptr ||
      acquisition.authority.head_publisher == nullptr) {
    return publication_error(
        "runtime snapshot preparation authority is incomplete");
  }

  const PublisherState &state = acquisition.authority.publisher_state;
  SnapshotPrepareAuthority authority;
  authority.stream = acquisition.authority.stream;
  authority.epoch_object = state.epoch_object;
  authority.epoch = state.epoch;
  authority.head_object = state.head_object;
  authority.head = state.head;
  return snapshot_publisher_->prepare(acquisition.cut, authority, prepared);
}

PublishResult ServerRuntimeSnapshotPublicationDriver::publish(
    uint64_t request_id, const PreparedSnapshotPublication &prepared,
    SnapshotPublication *publication) {
  if (snapshot_publisher_ == nullptr)
    return publication_error("runtime snapshot publisher is null");
  return publish_prepared_runtime_snapshot(
      request_id, snapshot_publisher_, prepared, publication);
}
#endif

RuntimeSnapshotCoordinatorResult RuntimeSnapshotCoordinator::current(
    RuntimeSnapshotCoordinatorOutcome outcome, std::string detail) const {
  return {outcome, state_,
          active_request_.has_value() ? active_request_->request_id : 0,
          std::move(detail)};
}

RuntimeSnapshotCoordinatorResult RuntimeSnapshotCoordinator::fail(
    RuntimeSnapshotCoordinatorOutcome outcome,
    RuntimeSnapshotCoordinatorState state, std::string detail) {
  state_ = state;
  return current(outcome, std::move(detail));
}

RuntimeSnapshotCoordinatorResult RuntimeSnapshotCoordinator::terminal(
    RuntimeSnapshotCoordinatorOutcome outcome,
    RuntimeSnapshotCoordinatorState state,
    RuntimeSnapshotRequestOutcome request_outcome, std::string detail) {
  const uint64_t request_id =
      active_request_.has_value() ? active_request_->request_id : 0;
  if (request_id != 0)
    control_->mark_terminal(request_id, request_outcome, detail);
  discard_fixed_cut();
  state_ = state;
  return {outcome, state_, request_id, std::move(detail)};
}

RuntimeSnapshotCoordinatorResult RuntimeSnapshotCoordinator::stop_active(
    std::string detail) {
  const uint64_t request_id =
      active_request_.has_value() ? active_request_->request_id : 0;
  discard_fixed_cut();
  active_request_.reset();
  hard_gate_reserved_ = false;
  state_ = RuntimeSnapshotCoordinatorState::SHUTDOWN;
  return {RuntimeSnapshotCoordinatorOutcome::SHUTDOWN, state_, request_id,
          std::move(detail)};
}

RuntimeSnapshotCoordinatorResult RuntimeSnapshotCoordinator::initialize() {
  if (state_ == RuntimeSnapshotCoordinatorState::READY)
    return current(RuntimeSnapshotCoordinatorOutcome::READY);
  if (state_ != RuntimeSnapshotCoordinatorState::UNINITIALIZED)
    return fail(RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
                RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
                "runtime snapshot coordinator cannot be reinitialized");
  if (control_ == nullptr || acquirer_ == nullptr || publication_ == nullptr)
    return fail(RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
                RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
                "runtime snapshot coordinator dependency is null");
  state_ = RuntimeSnapshotCoordinatorState::READY;
  return current(RuntimeSnapshotCoordinatorOutcome::READY);
}

RuntimeSnapshotCoordinatorResult RuntimeSnapshotCoordinator::poll() {
  return next(false);
}

RuntimeSnapshotCoordinatorResult RuntimeSnapshotCoordinator::wait() {
  return next(true);
}

void RuntimeSnapshotCoordinator::shutdown() {
  discard_fixed_cut();
  active_request_.reset();
  hard_gate_reserved_ = false;
  state_ = RuntimeSnapshotCoordinatorState::SHUTDOWN;
}

RuntimeSnapshotCoordinatorResult RuntimeSnapshotCoordinator::next(
    bool block_for_request) {
  if (state_ == RuntimeSnapshotCoordinatorState::UNINITIALIZED)
    return current(RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
                   "runtime snapshot coordinator is not initialized");
  if (state_ == RuntimeSnapshotCoordinatorState::SHUTDOWN)
    return current(RuntimeSnapshotCoordinatorOutcome::SHUTDOWN);
  if (state_ == RuntimeSnapshotCoordinatorState::FENCED)
    return current(RuntimeSnapshotCoordinatorOutcome::FENCED);
  if (state_ == RuntimeSnapshotCoordinatorState::PERMANENT_ERROR)
    return current(RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR);

  if (!active_request_.has_value()) {
    RuntimeSnapshotRequest request;
    if (block_for_request) {
      if (control_->wait(&request)) {
        state_ = RuntimeSnapshotCoordinatorState::SHUTDOWN;
        return current(RuntimeSnapshotCoordinatorOutcome::SHUTDOWN);
      }
    } else if (!control_->take(&request)) {
      state_ = RuntimeSnapshotCoordinatorState::READY;
      return current(RuntimeSnapshotCoordinatorOutcome::IDLE);
    }
    if (request.request_id == 0)
      return fail(RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
                  RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
                  "runtime snapshot request ID is zero");
    active_request_ = std::move(request);
    discard_fixed_cut();
    hard_gate_reserved_ = false;
  }
  return advance();
}

RuntimeSnapshotCoordinatorResult RuntimeSnapshotCoordinator::advance() {
  if (const auto refresh_result = refresh_active_request();
      refresh_result.has_value())
    return *refresh_result;
  if (const auto reserve_result = reserve_hard_gate_if_needed();
      reserve_result.has_value())
    return *reserve_result;

  const uint64_t request_id = active_request_->request_id;
  if (!acquisition_.has_value()) {
    state_ = RuntimeSnapshotCoordinatorState::ACQUIRING;
    RuntimeSnapshotAcquisitionResult acquired =
        acquirer_->acquire(*active_request_);
    if (!acquired.fixed()) {
      switch (acquired.outcome) {
        case RuntimeSnapshotAcquisitionOutcome::BLOCKED:
          return fail(RuntimeSnapshotCoordinatorOutcome::BLOCKED,
                      RuntimeSnapshotCoordinatorState::BLOCKED,
                      std::move(acquired.detail));
        case RuntimeSnapshotAcquisitionOutcome::REFIX_REQUIRED:
          discard_fixed_cut();
          return fail(RuntimeSnapshotCoordinatorOutcome::REFIX_REQUIRED,
                      RuntimeSnapshotCoordinatorState::REFIX_REQUIRED,
                      std::move(acquired.detail));
        case RuntimeSnapshotAcquisitionOutcome::FENCED:
          return terminal(RuntimeSnapshotCoordinatorOutcome::FENCED,
                          RuntimeSnapshotCoordinatorState::FENCED,
                          RuntimeSnapshotRequestOutcome::FENCED,
                          std::move(acquired.detail));
        case RuntimeSnapshotAcquisitionOutcome::SHUTDOWN:
          return stop_active(std::move(acquired.detail));
        case RuntimeSnapshotAcquisitionOutcome::FIXED:
        case RuntimeSnapshotAcquisitionOutcome::PERMANENT_ERROR:
          return terminal(
              RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
              RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
              RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
              acquired.detail.empty()
                  ? "runtime snapshot acquisition is incomplete"
                  : std::move(acquired.detail));
      }
    }
    if (acquired.acquisition->request_id != request_id ||
        acquired.acquisition->authority.request_id != request_id) {
      return terminal(
          RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
          RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
          RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
          "runtime snapshot acquisition changed request authority");
    }
    if (acquired.acquisition->source_lease == nullptr) {
      return terminal(
          RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
          RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
          RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
          "runtime snapshot acquisition does not own its source lease");
    }
    acquisition_ = std::move(*acquired.acquisition);
    if (const auto refresh_result = refresh_active_request();
        refresh_result.has_value())
      return *refresh_result;
    if (const auto reserve_result = reserve_hard_gate_if_needed();
        reserve_result.has_value())
      return *reserve_result;
  }

  if (!prepared_.has_value()) {
    state_ = RuntimeSnapshotCoordinatorState::PREPARING;
    PreparedSnapshotPublication prepared;
    const PublishResult result = publication_->prepare(*acquisition_, &prepared);
    if (!result.applied()) return handle_publish_failure(result);
    prepared_ = std::move(prepared);
    if (const auto refresh_result = refresh_active_request();
        refresh_result.has_value())
      return *refresh_result;
    if (const auto reserve_result = reserve_hard_gate_if_needed();
        reserve_result.has_value())
      return *reserve_result;
  }

  if (!applied_publication_.has_value()) {
    state_ = RuntimeSnapshotCoordinatorState::PUBLISHING;
    SnapshotPublication publication;
    const PublishResult result =
        publication_->publish(request_id, *prepared_, &publication);
    if (!result.applied()) return handle_publish_failure(result);
    applied_publication_ = std::move(publication);
  }

  state_ = RuntimeSnapshotCoordinatorState::COMPLETING_REQUEST;
  RuntimeSnapshotRequestResult completion = control_->complete(request_id);
  switch (completion.outcome) {
    case RuntimeSnapshotRequestOutcome::COMPLETED:
      break;
    case RuntimeSnapshotRequestOutcome::BLOCKED:
      return fail(RuntimeSnapshotCoordinatorOutcome::BLOCKED,
                  RuntimeSnapshotCoordinatorState::BLOCKED,
                  std::move(completion.detail));
    case RuntimeSnapshotRequestOutcome::REFIX_REQUIRED:
      return terminal(
          RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
          RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
          RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
          completion.detail.empty()
              ? "runtime snapshot completion requested REFIX after publication"
              : std::move(completion.detail));
    case RuntimeSnapshotRequestOutcome::FENCED:
      return terminal(RuntimeSnapshotCoordinatorOutcome::FENCED,
                      RuntimeSnapshotCoordinatorState::FENCED,
                      RuntimeSnapshotRequestOutcome::FENCED,
                      std::move(completion.detail));
    case RuntimeSnapshotRequestOutcome::PERMANENT_ERROR:
      return terminal(RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
                      RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
                      RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
                      std::move(completion.detail));
    case RuntimeSnapshotRequestOutcome::SHUTDOWN:
      return stop_active(std::move(completion.detail));
    case RuntimeSnapshotRequestOutcome::ACTIVE:
      return terminal(
          RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
          RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
          RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
          "runtime snapshot completion left the request active");
  }

  SnapshotPublication completed = std::move(*applied_publication_);
  finish_request(std::move(completed));
  return {RuntimeSnapshotCoordinatorOutcome::PUBLISHED, state_, request_id, {}};
}

std::optional<RuntimeSnapshotCoordinatorResult>
RuntimeSnapshotCoordinator::refresh_active_request() {
  const uint64_t request_id = active_request_->request_id;
  RuntimeSnapshotRequestResult refreshed = control_->refresh(request_id);
  switch (refreshed.outcome) {
    case RuntimeSnapshotRequestOutcome::ACTIVE:
      if (!refreshed.request.has_value() ||
          refreshed.request->request_id != request_id) {
        return terminal(
            RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
            RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
            RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
            "runtime snapshot refresh changed request authority");
      }
      if (active_request_->reason == RuntimeSnapshotRequestReason::HARD_LIMIT &&
          refreshed.request->reason !=
              RuntimeSnapshotRequestReason::HARD_LIMIT) {
        return terminal(
            RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
            RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
            RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
            "runtime snapshot request reason regressed from HARD to SOFT");
      }
      active_request_ = std::move(*refreshed.request);
      return std::nullopt;
    case RuntimeSnapshotRequestOutcome::BLOCKED:
      return fail(RuntimeSnapshotCoordinatorOutcome::BLOCKED,
                  RuntimeSnapshotCoordinatorState::BLOCKED,
                  std::move(refreshed.detail));
    case RuntimeSnapshotRequestOutcome::REFIX_REQUIRED:
      if (applied_publication_.has_value()) {
        return terminal(
            RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
            RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
            RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
            refreshed.detail.empty()
                ? "runtime snapshot refresh requested REFIX after publication"
                : std::move(refreshed.detail));
      }
      discard_fixed_cut();
      return fail(RuntimeSnapshotCoordinatorOutcome::REFIX_REQUIRED,
                  RuntimeSnapshotCoordinatorState::REFIX_REQUIRED,
                  std::move(refreshed.detail));
    case RuntimeSnapshotRequestOutcome::FENCED:
      return terminal(RuntimeSnapshotCoordinatorOutcome::FENCED,
                      RuntimeSnapshotCoordinatorState::FENCED,
                      RuntimeSnapshotRequestOutcome::FENCED,
                      std::move(refreshed.detail));
    case RuntimeSnapshotRequestOutcome::PERMANENT_ERROR:
      return terminal(RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
                      RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
                      RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
                      std::move(refreshed.detail));
    case RuntimeSnapshotRequestOutcome::SHUTDOWN:
      return stop_active(std::move(refreshed.detail));
    case RuntimeSnapshotRequestOutcome::COMPLETED:
      return terminal(
          RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
          RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
          RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
          "runtime snapshot request completed before coordinator publication");
  }
  return terminal(RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
                  RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
                  RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
                  "runtime snapshot refresh returned an invalid outcome");
}

std::optional<RuntimeSnapshotCoordinatorResult>
RuntimeSnapshotCoordinator::reserve_hard_gate_if_needed() {
  if (active_request_->reason != RuntimeSnapshotRequestReason::HARD_LIMIT ||
      hard_gate_reserved_)
    return std::nullopt;

  state_ = RuntimeSnapshotCoordinatorState::RESERVING_HARD_GATE;
  std::string error;
  if (control_->reserve_hard_gate(active_request_->request_id, &error)) {
    if (const auto classified = refresh_active_request();
        classified.has_value())
      return classified;
    return terminal(
        RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
        RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
        RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
        error.empty() ? "cannot reserve runtime snapshot hard gate"
                      : std::move(error));
  }
  hard_gate_reserved_ = true;
  return std::nullopt;
}

RuntimeSnapshotCoordinatorResult
RuntimeSnapshotCoordinator::handle_publish_failure(const PublishResult &result) {
  switch (result.outcome) {
    case PublishOutcome::BLOCKED:
      return fail(RuntimeSnapshotCoordinatorOutcome::BLOCKED,
                  RuntimeSnapshotCoordinatorState::BLOCKED, result.detail);
    case PublishOutcome::REFIX_REQUIRED:
      discard_fixed_cut();
      return fail(RuntimeSnapshotCoordinatorOutcome::REFIX_REQUIRED,
                  RuntimeSnapshotCoordinatorState::REFIX_REQUIRED,
                  result.detail);
    case PublishOutcome::FENCED:
      return terminal(RuntimeSnapshotCoordinatorOutcome::FENCED,
                      RuntimeSnapshotCoordinatorState::FENCED,
                      RuntimeSnapshotRequestOutcome::FENCED, result.detail);
    case PublishOutcome::APPLIED:
      break;
    case PublishOutcome::ABSENT:
    case PublishOutcome::PERMANENT_ERROR:
      return terminal(RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
                      RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
                      RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
                      result.detail);
  }
  return terminal(
      RuntimeSnapshotCoordinatorOutcome::PERMANENT_ERROR,
      RuntimeSnapshotCoordinatorState::PERMANENT_ERROR,
      RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
      "runtime snapshot publication returned an invalid outcome");
}

void RuntimeSnapshotCoordinator::discard_fixed_cut() {
  applied_publication_.reset();
  prepared_.reset();
  acquisition_.reset();
}

void RuntimeSnapshotCoordinator::finish_request(
    SnapshotPublication publication) {
  last_publication_ = std::move(publication);
  discard_fixed_cut();
  active_request_.reset();
  hard_gate_reserved_ = false;
  state_ = RuntimeSnapshotCoordinatorState::READY;
}

}  // namespace wesql::remote_commit
