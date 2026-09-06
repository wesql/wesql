/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/server_hooks.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <condition_variable>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

#include "my_rnd.h"
#include "my_sys.h"
#include "scope_guard.h"
#include "mysql/binlog/event/trx_boundary_parser.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/my_loglevel.h"
#include "mysqld_error.h"
#include "objstore.h"
#include "sql/binlog.h"
#include "sql/binlog_reader.h"
#include "sql/current_thd.h"
#include "sql/dd/dd.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/dictionary.h"
#include "sql/dd/dd_schema.h"
#include "sql/dd/dd_table.h"
#include "sql/dd/impl/bootstrap/bootstrap_ctx.h"
#include "sql/dd/impl/sdi.h"
#include "sql/dd/types/schema.h"
#include "sql/dd/types/table.h"
#include "sql/item.h"
#include "sql/log_event.h"
#include "sql/mysqld.h"
#include "sql/plugin_table.h"
#include "sql/remote_commit/evidence.h"
#include "sql/remote_commit/publisher.h"
#include "sql/remote_commit/recovery.h"
#include "sql/remote_commit/segment_sealer.h"
#include "sql/remote_commit/snapshot_publisher.h"
#include "sql/remote_commit/sql_command_policy.h"
#include "sql/remote_commit/startup_dictionary.h"
#include "sql/rpl_gtid.h"
#include "sql/rpl_log_encryption.h"
#include "sql/set_var.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_db.h"
#include "sql/sql_parse.h"
#include "sql/sql_table.h"
#include "sql/transaction_info.h"

extern ulong srv_flush_log_at_trx_commit;

#ifdef WITH_SMARTENGINE
namespace smartengine {
extern uint32_t se_flush_log_at_trx_commit;
}
#endif

namespace wesql::remote_commit {

class CloneCutBinlogPin {
 public:
  CloneCutBinlogPin(std::string file, uint64_t pos)
      : file_(std::move(file)), pos_(pos) {}
  ~CloneCutBinlogPin();

  CloneCutBinlogPin(const CloneCutBinlogPin &) = delete;
  CloneCutBinlogPin &operator=(const CloneCutBinlogPin &) = delete;

  bool install(bool test_mode, const std::filesystem::path &test_source,
               std::string *error);
  bool active() const { return test_only_ || registered_; }
  bool matches(const CloneCutState &state) const {
    return active() && file_ == state.file && pos_ == state.pos;
  }
  const std::filesystem::path &source_path() const { return source_path_; }

 private:
  std::string file_;
  uint64_t pos_{0};
  std::filesystem::path source_path_;
  std::unique_ptr<Log_info> log_info_;
  bool registered_{false};
  bool test_only_{false};
};

namespace {

namespace fs = std::filesystem;

thread_local THD *g_pfs_restore_thd{nullptr};
thread_local const Plugin_table *g_pfs_restore_definition{nullptr};

struct AuthorizationRecord {
  CommitAuthorization authorization;
  CommitBinding binding;
  bool required{false};
};

enum class AdmissionKind : uint8_t {
  NORMAL,
  RECOVERY,
};

constexpr uint64_t kRuntimeSnapshotSoftManifestCount = 80000;
constexpr RecoveryWindow kNormalAdmissionReservation{
    1, static_cast<uint64_t>(kDeltaManifestMaxBytes),
    static_cast<uint64_t>(kMaxSegmentsPerManifest)};
constexpr RecoveryWindow kRecoveryAdmissionReservation{};

struct AdmissionRecord {
  AdmissionKind kind{AdmissionKind::NORMAL};
  RecoveryWindow reservation;
};

struct RuntimeSnapshotRequestState {
  uint64_t request_id{0};
  RuntimeSnapshotRequestReason reason{RuntimeSnapshotRequestReason::SOFT_LIMIT};
  RecoveryWindow prospective_window;
  uint64_t start_generation{0};
  bool notification_pending{false};
  bool hard_gate_reserved{false};
  std::optional<uint64_t> published_generation;
  std::optional<Head> published_head;
  std::optional<RuntimeSnapshotRequestOutcome> terminal_outcome;
  std::string terminal_detail;
};

enum class StartupPhase : uint8_t {
  UNINITIALIZED,
  HEAD_PROBED,
  EPOCH_ACQUIRED,
  INSTALLED_REEXEC_PRE_RECOVERY,
  ROOT_VERIFIED,
  ACTIVE,
  SHUTTING_DOWN,
};

enum class CloneCutBarrierPhase : uint8_t {
  NONE,
  CLOSING,
  ACTIVE,
};

struct RuntimeState {
  std::mutex state_mutex;
  bool initialized{false};
  bool running{false};
  bool bootstrap_preflight{false};
  StartupPhase startup_phase{StartupPhase::UNINITIALIZED};
  StartupRoute startup_route{StartupRoute::DISABLED};
  bool startup_epoch_adopted{false};
  bool recovery_snapshot_gtids_restored{false};
  StartupEpochAdoptionRole startup_epoch_adoption_role{
      StartupEpochAdoptionRole::TAKEOVER_RECOVERY};
  std::optional<StartupEpochProof> adopted_startup_proof;
  uint64_t probed_head_generation{0};
  std::string last_error;
  Cursor local_flushed_cursor;
  Cursor public_committed_cursor;
  StatusSnapshot status;

  std::unique_ptr<ObjectStoreConditionalIo> io;
  ConditionalIo *metadata_io{nullptr};
  std::unique_ptr<ProtocolStore> store;
  std::unique_ptr<HeadPublisher> publisher;
  std::unique_ptr<SegmentSealer> sealer;
  StreamIdentity stream;
  std::string cluster_object_prefix;
  objstore::ObjectStore *object_store{nullptr};
  bool object_store_owned{false};
  bool object_store_provider_initialized{false};
  std::string object_store_provider;
  std::string bucket;
  std::optional<Head> installed_head;
  std::string installed_head_body;
  std::string installed_head_etag;
  std::optional<LocalInstallMarker> installed_marker;

  std::unordered_map<THD *, AuthorizationRecord> authorizations;

  std::mutex admission_mutex;
  std::condition_variable admission_cond;
  bool shutdown_draining{false};
  bool shutting_down{false};
  bool admission_open{false};
  bool admission_reopen_in_progress{false};
  std::unordered_map<THD *, AdmissionRecord> admissions;
  RecoveryWindow admission_reservations;
  bool published_window_valid{false};
  uint64_t published_window_generation{0};
  RecoveryWindow published_window;
  std::optional<RuntimeSnapshotRequestState> snapshot_request;
  uint64_t next_snapshot_request_id{1};
  uint64_t completed_snapshot_request_id{0};
  bool snapshot_terminal_failure{false};

  // clone_cut_mutex serializes begin/verify/end. The phase and active state are
  // additionally protected by admission_mutex because they own its CLOSED
  // transition.
  std::mutex clone_cut_mutex;
  CloneCutBarrierPhase clone_cut_phase{CloneCutBarrierPhase::NONE};
  uint64_t clone_cut_token{0};
  uint64_t next_clone_cut_token{1};
  uint64_t clone_cut_pins_releasing{0};
  std::optional<CloneCutState> active_clone_cut;
#ifdef WESQL_TEST
  std::optional<CloneCutState> clone_cut_source_for_test;
  std::optional<Cursor> clone_cut_public_cursor_for_test;
  std::optional<fs::path> clone_cut_binlog_source_path_for_test;
  bool pause_clone_cut_pin_release_for_test{false};
  bool clone_cut_pin_release_waiting_for_test{false};
#endif
};

RuntimeState g_runtime;

struct TicketQueue {
  std::mutex mutex;
  std::condition_variable cond;
  uint64_t next{0};
  uint64_t serving{0};
};

TicketQueue g_order;

struct SnapshotOrderState {
  std::mutex mutex;
  std::optional<uint64_t> request_id;
  std::optional<uint64_t> ticket;
};

SnapshotOrderState g_snapshot_order;

bool cursor_less(const Cursor &left, const Cursor &right);
void complete_commit_admission_reopen();
[[noreturn]] void fail_stop_holding_admission(const char *reason);

std::string configured_binlog_basename() {
  if (opt_bin_logname != nullptr && opt_bin_logname[0] != '\0')
    return fs::path(opt_bin_logname).filename().string();
  if (log_bin_supplied) return std::string(glob_hostname) + "-bin";
  return "binlog";
}

StartupPolicy configured_startup_policy(
    const objstore::ConditionalObjectStoreCapabilities &capabilities) {
  StartupPolicy policy;
  policy.provider = opt_objstore_provider != nullptr ? opt_objstore_provider : "";
  policy.repository_id =
      opt_repo_objstore_id != nullptr ? opt_repo_objstore_id : "";
  policy.branch_id =
      opt_branch_objstore_id != nullptr ? opt_branch_objstore_id : "";
  policy.cluster_object_prefix =
      policy.repository_id + "/" + policy.branch_id;
  policy.binlog_basename = configured_binlog_basename();
  policy.conditional_io = {
      capabilities.exact_get_with_etag,
      capabilities.exact_get_to_file,
      capabilities.create_only,
      capabilities.compare_and_swap,
      capabilities.create_from_file,
      capabilities.distinct_conflict_status,
  };

  policy.log_bin = opt_bin_log;
  policy.binlog_format_row =
      global_system_variables.binlog_format == BINLOG_FORMAT_ROW;
  policy.binlog_row_image_full =
      global_system_variables.binlog_row_image == BINLOG_ROW_IMAGE_FULL;
  policy.binlog_transaction_compression =
      global_system_variables.binlog_trx_compression;
  policy.binlog_checksum_crc32 =
      binlog_checksum_options ==
      mysql::binlog::event::BINLOG_CHECKSUM_ALG_CRC32;
  policy.binlog_row_value_options_empty =
      global_system_variables.binlog_row_value_options == 0;
  policy.binlog_encryption = rpl_encryption.get_enabled_var();
  policy.binlog_order_commits = opt_binlog_order_commits;
  policy.binlog_error_action_abort_server =
      binlog_error_action == ABORT_SERVER;
  // Options are parsed here, but gtid_server_init() has not populated the
  // runtime cache or created global_tsid_lock yet.
  policy.gtid_mode_on = Gtid_mode::sysvar_mode == Gtid_mode::ON;
  policy.enforce_gtid_consistency =
      _gtid_consistency_mode == GTID_CONSISTENCY_MODE_ON;
  policy.anonymous_gtid_forbidden = policy.gtid_mode_on;
  policy.max_binlog_cache_size = max_binlog_cache_size;
  policy.max_binlog_stmt_cache_size = max_binlog_stmt_cache_size;
  policy.max_segment_bytes = maximum_segment_bytes();

  policy.innodb_flush_log_at_trx_commit =
      static_cast<uint32_t>(srv_flush_log_at_trx_commit);
#ifdef WITH_SMARTENGINE
  policy.smartengine_enabled = true;
  policy.smartengine_flush_log_at_trx_commit =
      smartengine::se_flush_log_at_trx_commit;
  // These are enforced structurally in remote mode and by read-only defaults.
  policy.smartengine_write_disable_wal = false;
  policy.smartengine_persistent_cache_size = 0;
  policy.smartengine_commit_in_the_middle = false;
  policy.smartengine_immutable_extents = true;
#endif

  policy.heuristic_recovery_off = tc_heuristic_recover == 0;
  policy.snapshot_archive = opt_consistent_snapshot_archive;
  policy.snapshot_archive_on_objectstore =
      opt_consistent_snapshot_persistent_on_objstore;

  // Persistent repositories and prepared-XID inventories are checked after
  // their subsystems are loaded and before write admission opens.
  policy.external_replication_present = opt_binlog_archive_replica;
  policy.group_replication_present = false;
  policy.internal_prepared_present = false;
  policy.external_xa_present = false;
  policy.legacy_source_present =
      opt_recovery_from_objstore || opt_initialize_from_source_objectstore;
  return policy;
}

bool reject_startup_policy(const StartupPolicy &policy, bool bootstrap_preflight,
                           std::string *error) {
  const std::vector<PolicyViolation> violations =
      validate_startup_policy(policy, bootstrap_preflight);
  if (violations.empty()) return false;
  const PolicyViolation &first = violations.front();
  *error = "remote commit startup policy violation: ";
  error->append(policy_error_name(first.code));
  if (!first.detail.empty()) error->append(": ").append(first.detail);
  return true;
}

bool fail_with(std::string *error, std::string message) {
  if (error != nullptr) *error = std::move(message);
  return false;
}

bool parse_binlog_name(std::string_view file, std::string *basename,
                       uint64_t *sequence, size_t *width) {
  const size_t dot = file.rfind('.');
  if (dot == std::string_view::npos || dot == 0 || dot + 1 == file.size())
    return false;
  uint64_t parsed = 0;
  const auto converted = std::from_chars(file.data() + dot + 1,
                                         file.data() + file.size(), parsed);
  if (converted.ec != std::errc() || converted.ptr != file.data() + file.size())
    return false;
  if (basename != nullptr) basename->assign(file.substr(0, dot));
  if (sequence != nullptr) *sequence = parsed;
  if (width != nullptr) *width = file.size() - dot - 1;
  return true;
}

int compare_cursor(const Cursor &left, const Cursor &right) {
  std::string left_base;
  std::string right_base;
  uint64_t left_sequence = 0;
  uint64_t right_sequence = 0;
  if (!parse_binlog_name(left.file, &left_base, &left_sequence, nullptr) ||
      !parse_binlog_name(right.file, &right_base, &right_sequence, nullptr) ||
      left_base != right_base)
    return left.file < right.file ? -1 : left.file == right.file ? 0 : 1;
  if (left_sequence != right_sequence)
    return left_sequence < right_sequence ? -1 : 1;
  if (left.pos != right.pos) return left.pos < right.pos ? -1 : 1;
  return 0;
}

bool cursor_less(const Cursor &left, const Cursor &right) {
  return compare_cursor(left, right) < 0;
}

size_t decimal_digit_count(uint64_t value) {
  size_t digits = 1;
  while (value >= 10) {
    value /= 10;
    ++digits;
  }
  return digits;
}

std::string numbered_binlog_name(std::string_view basename,
                                 uint64_t sequence) {
  std::array<char, 32> number{};
  const auto converted = std::to_chars(number.data(), number.data() + number.size(),
                                       sequence);
  const size_t digits = static_cast<size_t>(converted.ptr - number.data());
  const size_t width = std::max<size_t>(6, decimal_digit_count(sequence));
  std::string result(basename);
  result.push_back('.');
  if (digits < width) result.append(width - digits, '0');
  result.append(number.data(), digits);
  return result;
}

std::string random_writer_id() {
  std::array<unsigned char, 16> bytes{};
  if (my_rand_buffer(bytes.data(), bytes.size()) != 0) return {};
  static constexpr char kHex[] = "0123456789abcdef";
  std::string value;
  value.reserve(bytes.size() * 2);
  for (const unsigned char byte : bytes) {
    value.push_back(kHex[byte >> 4]);
    value.push_back(kHex[byte & 0x0f]);
  }
  return value;
}

void update_status_locked(LifecycleState lifecycle) {
  g_runtime.status = {};
  g_runtime.status.state = lifecycle;
  if (lifecycle == LifecycleState::OFF) return;
  g_runtime.status.stream_sha256 = g_runtime.stream.stream_sha256;
  if (g_runtime.publisher == nullptr) return;
  const PublisherState &publisher_state = g_runtime.publisher->state();
  if (publisher_state.epoch.has_value())
    g_runtime.status.writer =
        StatusWriter{publisher_state.epoch->epoch,
                     publisher_state.epoch->writer_id};
  if (publisher_state.head.has_value() &&
      publisher_state.head_object.has_value()) {
    std::string etag_sha;
    std::string body_sha;
    std::string error;
    if (sha256_hex(publisher_state.head_object->etag, &etag_sha, &error) &&
        sha256_hex(publisher_state.head_object->body, &body_sha, &error)) {
      g_runtime.status.head = StatusHead{publisher_state.head->generation,
                                         std::move(etag_sha),
                                         std::move(body_sha)};
      g_runtime.status.durable_cursor = publisher_state.head->durable_cursor;
    }
  }
}

bool recovery_window_is_valid(const RecoveryWindow &window) {
  return window.manifest_count != 0 &&
         window.manifest_count <= kRecoveryManifestCountMax &&
         window.manifest_bytes != 0 &&
         window.manifest_bytes <= kRecoveryManifestBytesMax &&
         window.segment_count <= kRecoverySegmentCountMax;
}

bool add_recovery_window(const RecoveryWindow &base,
                         const RecoveryWindow &increment,
                         RecoveryWindow *result) {
  if (result == nullptr ||
      base.manifest_count > kRecoveryManifestCountMax ||
      base.manifest_bytes > kRecoveryManifestBytesMax ||
      base.segment_count > kRecoverySegmentCountMax ||
      increment.manifest_count >
          kRecoveryManifestCountMax - base.manifest_count ||
      increment.manifest_bytes >
          kRecoveryManifestBytesMax - base.manifest_bytes ||
      increment.segment_count >
          kRecoverySegmentCountMax - base.segment_count)
    return false;
  *result = {base.manifest_count + increment.manifest_count,
             base.manifest_bytes + increment.manifest_bytes,
             base.segment_count + increment.segment_count};
  return true;
}

void add_reservation_locked(const RecoveryWindow &reservation) {
  RecoveryWindow next;
  if (!add_recovery_window(g_runtime.admission_reservations, reservation,
                           &next))
    fail_stop_holding_admission("remote admission reservation overflow");
  g_runtime.admission_reservations = next;
}

void remove_reservation_locked(const RecoveryWindow &reservation) {
  if (reservation.manifest_count >
          g_runtime.admission_reservations.manifest_count ||
      reservation.manifest_bytes >
          g_runtime.admission_reservations.manifest_bytes ||
      reservation.segment_count >
          g_runtime.admission_reservations.segment_count)
    fail_stop_holding_admission("remote admission reservation underflow");
  g_runtime.admission_reservations.manifest_count -=
      reservation.manifest_count;
  g_runtime.admission_reservations.manifest_bytes -=
      reservation.manifest_bytes;
  g_runtime.admission_reservations.segment_count -=
      reservation.segment_count;
}

bool prospective_window_locked(const RecoveryWindow &reservation,
                               RecoveryWindow *prospective) {
  RecoveryWindow with_existing;
  return g_runtime.published_window_valid &&
         add_recovery_window(g_runtime.published_window,
                             g_runtime.admission_reservations,
                             &with_existing) &&
         add_recovery_window(with_existing, reservation, prospective);
}

bool one_normal_reservation_fits_locked() {
  RecoveryWindow prospective;
  return prospective_window_locked(kNormalAdmissionReservation, &prospective);
}

RuntimeSnapshotRequest snapshot_request_locked() {
  const RuntimeSnapshotRequestState &request = *g_runtime.snapshot_request;
  return RuntimeSnapshotRequest{
      request.request_id, request.reason, g_runtime.published_window,
      request.prospective_window};
}

RuntimeSnapshotRequestResult snapshot_request_result(
    RuntimeSnapshotRequestOutcome outcome, std::string detail = {},
    std::optional<RuntimeSnapshotRequest> request = std::nullopt) {
  return {outcome, std::move(request), std::move(detail)};
}

bool live_snapshot_request_keeps_admission_closed_locked() {
  return g_runtime.snapshot_terminal_failure ||
         (g_runtime.snapshot_request.has_value() &&
          (g_runtime.snapshot_request->reason ==
               RuntimeSnapshotRequestReason::HARD_LIMIT ||
           g_runtime.snapshot_request->terminal_outcome.has_value()));
}

// A terminal coordinator result cannot make progress by waiting for a later
// admission reopen. Waiters must wake and return their existing unknown/error
// result instead of sleeping behind a permanently closed gate.
bool terminal_snapshot_failure_locked() {
  return g_runtime.snapshot_terminal_failure ||
         (g_runtime.snapshot_request.has_value() &&
          g_runtime.snapshot_request->terminal_outcome.has_value());
}

void refresh_request_prospective_locked() {
  if (!g_runtime.snapshot_request.has_value() ||
      !g_runtime.published_window_valid ||
      g_runtime.snapshot_request->terminal_outcome.has_value())
    return;
  RecoveryWindow prospective;
  if (!add_recovery_window(g_runtime.published_window,
                           g_runtime.admission_reservations, &prospective)) {
    prospective = {kRecoveryManifestCountMax, kRecoveryManifestBytesMax,
                   kRecoverySegmentCountMax};
  }
  g_runtime.snapshot_request->prospective_window = prospective;
}

void ensure_snapshot_request_locked(RuntimeSnapshotRequestReason reason,
                                    const RecoveryWindow &prospective) {
  if (!g_runtime.snapshot_request.has_value()) {
    if (g_runtime.next_snapshot_request_id == 0 ||
        g_runtime.next_snapshot_request_id ==
            std::numeric_limits<uint64_t>::max())
      fail_stop_holding_admission("runtime snapshot request ID space exhausted");
    RuntimeSnapshotRequestState request;
    request.request_id = g_runtime.next_snapshot_request_id++;
    request.reason = reason;
    request.prospective_window = prospective;
    request.start_generation = g_runtime.published_window_generation;
    request.notification_pending = true;
    g_runtime.snapshot_request = std::move(request);
  } else {
    RuntimeSnapshotRequestState &request = *g_runtime.snapshot_request;
    if (request.terminal_outcome.has_value())
      fail_stop_holding_admission(
          "terminal runtime snapshot request was reused");
    request.prospective_window = prospective;
    if (reason == RuntimeSnapshotRequestReason::HARD_LIMIT &&
        request.reason != RuntimeSnapshotRequestReason::HARD_LIMIT) {
      request.reason = RuntimeSnapshotRequestReason::HARD_LIMIT;
      request.notification_pending = true;
    }
  }
  if (reason == RuntimeSnapshotRequestReason::HARD_LIMIT)
    g_runtime.admission_open = false;
  g_runtime.admission_cond.notify_all();
}

void maybe_request_next_snapshot_locked() {
  if (!g_runtime.published_window_valid) return;
  RecoveryWindow prospective;
  if (!add_recovery_window(g_runtime.published_window,
                           g_runtime.admission_reservations, &prospective)) {
    ensure_snapshot_request_locked(RuntimeSnapshotRequestReason::HARD_LIMIT,
                                   g_runtime.published_window);
    return;
  }
  if (prospective.manifest_count >= kRuntimeSnapshotSoftManifestCount)
    ensure_snapshot_request_locked(RuntimeSnapshotRequestReason::SOFT_LIMIT,
                                   prospective);
}

void reset_admission_accounting_locked() {
  g_runtime.admissions.clear();
  g_runtime.admission_reservations = {};
  g_runtime.published_window_valid = false;
  g_runtime.published_window_generation = 0;
  g_runtime.published_window = {};
  g_runtime.snapshot_request.reset();
  g_runtime.next_snapshot_request_id = 1;
  g_runtime.completed_snapshot_request_id = 0;
  g_runtime.snapshot_terminal_failure = false;
}

void refresh_published_window_after_order_release(
    uint64_t generation, const RecoveryWindow &window,
    uint64_t snapshot_request_id, const Head *snapshot_head = nullptr) {
  std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
  if (generation == 0 || !recovery_window_is_valid(window))
    fail_stop_holding_admission("published recovery window is invalid");
  if (!g_runtime.published_window_valid ||
      generation > g_runtime.published_window_generation) {
    g_runtime.published_window_valid = true;
    g_runtime.published_window_generation = generation;
    g_runtime.published_window = window;
  } else if (generation == g_runtime.published_window_generation &&
             g_runtime.published_window != window) {
    fail_stop_holding_admission(
        "one HEAD generation produced different recovery windows");
  }

  if (snapshot_request_id != 0) {
    if (!g_runtime.snapshot_request.has_value() ||
        g_runtime.snapshot_request->request_id != snapshot_request_id)
      fail_stop_holding_admission(
          "ordered snapshot publication lost request ownership");
    if (snapshot_head == nullptr || snapshot_head->generation != generation ||
        snapshot_head->recovery_window != window ||
        snapshot_head->writer.id.empty() || snapshot_head->writer.epoch == 0)
      fail_stop_holding_admission(
          "ordered snapshot publication lost its exact HEAD identity");
    g_runtime.snapshot_request->published_generation = generation;
    g_runtime.snapshot_request->published_head = *snapshot_head;
    refresh_request_prospective_locked();
  } else {
    refresh_request_prospective_locked();
    maybe_request_next_snapshot_locked();
  }
  g_runtime.admission_cond.notify_all();
}

bool admission_closed_and_drained_locked() {
  return !g_runtime.shutdown_draining && !g_runtime.shutting_down &&
         !g_runtime.admission_open &&
         g_runtime.admissions.empty() &&
         g_runtime.clone_cut_phase == CloneCutBarrierPhase::NONE;
}

bool exact_adopted_epoch_locked() {
  if (!g_runtime.adopted_startup_proof.has_value() ||
      g_runtime.publisher == nullptr)
    return false;
  const StartupEpochProof &proof = *g_runtime.adopted_startup_proof;
  const PublisherState &state = g_runtime.publisher->state();
  return state.epoch.has_value() && state.epoch_object.has_value() &&
         *state.epoch == proof.value &&
         state.epoch_object->body == proof.body &&
         state.epoch_object->etag == proof.etag;
}

bool exact_adopted_head_locked(bool expect_absent) {
  if (!g_runtime.adopted_startup_proof.has_value() ||
      g_runtime.publisher == nullptr)
    return false;
  const StartupEpochProof &proof = *g_runtime.adopted_startup_proof;
  const PublisherState &state = g_runtime.publisher->state();
  if (expect_absent) {
    return proof.head_body.empty() && proof.head_etag.empty() &&
           proof.head_generation == 0 && !state.head.has_value() &&
           !state.head_object.has_value();
  }
  return !proof.head_body.empty() && !proof.head_etag.empty() &&
         proof.head_generation != 0 && state.head.has_value() &&
         state.head_object.has_value() &&
         state.head->generation == proof.head_generation &&
         state.head_object->body == proof.head_body &&
         state.head_object->etag == proof.head_etag;
}

bool bootstrap_preflight_worker_authorized_locked() {
  const PublisherState *state =
      g_runtime.publisher == nullptr ? nullptr : &g_runtime.publisher->state();
  return g_runtime.initialized && !g_runtime.running &&
         g_runtime.startup_route == StartupRoute::BOOTSTRAP &&
         !g_runtime.startup_epoch_adopted &&
         g_runtime.startup_phase == StartupPhase::HEAD_PROBED &&
         g_runtime.status.state == LifecycleState::INITIALIZING &&
         admission_closed_and_drained_locked() &&
         state != nullptr && !state->head.has_value() &&
         !state->head_object.has_value() &&
         state->lifecycle == LifecycleState::INITIALIZING;
}

bool takeover_recovery_worker_authorized_locked() {
  const PublisherState *state =
      g_runtime.publisher == nullptr ? nullptr : &g_runtime.publisher->state();
  return g_runtime.initialized && !g_runtime.running &&
         g_runtime.startup_route == StartupRoute::TAKEOVER &&
         g_runtime.startup_epoch_adopted &&
         g_runtime.startup_epoch_adoption_role ==
             StartupEpochAdoptionRole::TAKEOVER_RECOVERY &&
         g_runtime.startup_phase == StartupPhase::EPOCH_ACQUIRED &&
         g_runtime.status.state == LifecycleState::RECOVERING &&
         admission_closed_and_drained_locked() && state != nullptr &&
         state->lifecycle == LifecycleState::RECOVERING &&
         exact_adopted_epoch_locked() && exact_adopted_head_locked(false);
}

bool bootstrap_snapshot_worker_authorized_locked() {
  const PublisherState *state =
      g_runtime.publisher == nullptr ? nullptr : &g_runtime.publisher->state();
  return g_runtime.initialized && !g_runtime.running &&
         g_runtime.startup_route == StartupRoute::BOOTSTRAP &&
         g_runtime.startup_epoch_adopted &&
         g_runtime.startup_epoch_adoption_role ==
             StartupEpochAdoptionRole::BOOTSTRAP_SNAPSHOT &&
         g_runtime.startup_phase == StartupPhase::EPOCH_ACQUIRED &&
         g_runtime.status.state == LifecycleState::INITIALIZING &&
         admission_closed_and_drained_locked() && state != nullptr &&
         state->lifecycle == LifecycleState::INITIALIZING &&
         exact_adopted_epoch_locked() && exact_adopted_head_locked(true);
}

bool installed_reexec_pre_recovery_authorized_locked() {
  const PublisherState *state =
      g_runtime.publisher == nullptr ? nullptr : &g_runtime.publisher->state();
  return g_runtime.initialized && !g_runtime.running &&
         g_runtime.startup_route == StartupRoute::TAKEOVER &&
         g_runtime.startup_epoch_adopted &&
         g_runtime.startup_epoch_adoption_role ==
             StartupEpochAdoptionRole::INSTALLED_ROOT &&
         g_runtime.startup_phase ==
             StartupPhase::INSTALLED_REEXEC_PRE_RECOVERY &&
         g_runtime.status.state == LifecycleState::RECOVERING &&
         admission_closed_and_drained_locked() && state != nullptr &&
         state->lifecycle == LifecycleState::RECOVERING &&
         exact_adopted_epoch_locked() && exact_adopted_head_locked(false) &&
         g_runtime.installed_head.has_value() &&
         g_runtime.installed_marker.has_value() &&
         *state->head == *g_runtime.installed_head &&
         state->head_object->body == g_runtime.installed_head_body &&
         state->head_object->etag == g_runtime.installed_head_etag;
}

bool installed_marker_binds_head(const LocalInstallMarker &marker,
                                 const Head &head, std::string_view head_body,
                                 std::string *error) {
  std::string canonical_marker;
  if (!serialize_local_install_marker(marker, &canonical_marker, error))
    return false;
  std::string head_sha256;
  if (!sha256_hex(head_body, &head_sha256, error)) return false;
  if (marker.stream_id != g_runtime.stream.stream_id ||
      marker.installed_head.generation != head.generation ||
      marker.installed_head.body_sha256 != head_sha256 ||
      marker.installed_head.snapshot_id != head.snapshot.id ||
      marker.installed_head.snapshot_manifest_sha256 !=
          head.snapshot.manifest_sha256 ||
      marker.installed_head.snapshot_cursor != head.snapshot.cursor) {
    if (error != nullptr)
      *error = "installed marker does not bind the exact HEAD and snapshot";
    return false;
  }
  return true;
}

[[noreturn]] void fatal_publish(std::string_view operation,
                                const PublishResult &result) {
  std::string reason(operation);
  reason.append(" failed");
  if (!result.detail.empty()) {
    reason.append(": ");
    reason.append(result.detail);
  }
  fail_stop(reason.c_str());
}

[[noreturn]] void fail_stop_holding_admission(const char *reason) {
  const char *message =
      reason != nullptr ? reason : "remote commit invariant failure";
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    g_runtime.last_error = message;
    g_runtime.running = false;
    update_status_locked(LifecycleState::FENCED);
  }
  g_runtime.admission_open = false;
  g_runtime.admission_cond.notify_all();
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message);
  std::abort();
}

bool append_gtid(const Gtid_log_event &event, std::vector<std::string> *gtids,
                 std::string *error) {
  if (event.get_type() != ASSIGNED_GTID)
    return fail_with(error, "native segment contains an anonymous GTID");
  std::array<char, mysql::gtid::tsid_max_length + 1> tsid{};
  const int tsid_length = event.get_tsid().to_string(tsid.data());
  if (tsid_length <= 0 || static_cast<size_t>(tsid_length) >= tsid.size())
    return fail_with(error, "cannot render segment GTID TSID");
  std::array<char, 32> gno{};
  const auto converted = std::to_chars(gno.data(), gno.data() + gno.size(),
                                       event.get_gno());
  if (converted.ec != std::errc())
    return fail_with(error, "cannot render segment GTID GNO");
  std::string value(tsid.data(), static_cast<size_t>(tsid_length));
  value.push_back(':');
  value.append(gno.data(), static_cast<size_t>(converted.ptr - gno.data()));
  gtids->push_back(std::move(value));
  return true;
}

bool scan_native_range(const NativeBinlogRange &range,
                       std::string_view expected_next_file,
                       NativeRangeMetadata *metadata, std::string *error) {
  if (metadata == nullptr) return fail_with(error, "null range metadata");
  metadata->transaction_count = 0;
  metadata->gtid_set.clear();
  metadata->xids.clear();
  metadata->native_sha256.clear();
  if (range.source.start_pos >= range.source.end_pos ||
      range.source.end_pos - range.source.start_pos > maximum_segment_bytes())
    return fail_with(error, "native range exceeds the segment limit");
  std::string validated_bytes;
  validated_bytes.reserve(range.source.end_pos - range.source.start_pos);
  if (range.source.start_pos == 0)
    validated_bytes.assign(BINLOG_MAGIC, BIN_LOG_HEADER_SIZE);

  Binlog_file_reader reader(true);
  const my_off_t scan_start =
      static_cast<my_off_t>(range.source.start_pos == 0
                                ? BIN_LOG_HEADER_SIZE
                                : range.source.start_pos);
  if (reader.open(range.local_path.c_str(), scan_start))
    return fail_with(error, std::string("cannot open native range: ") +
                                reader.get_error_str());

  std::vector<std::string> gtids;
  mysql::binlog::event::Transaction_boundary_parser boundary_parser(
      mysql::binlog::event::Transaction_boundary_parser::
          TRX_BOUNDARY_PARSER_RECEIVER);
  mysql::binlog::event::Log_event_type last_event_type =
      mysql::binlog::event::UNKNOWN_EVENT;
  std::string rotate_target;
  while (reader.position() < range.source.end_pos) {
    const my_off_t event_start = reader.position();
    std::unique_ptr<Log_event> event(reader.read_event_object());
    if (event == nullptr)
      return fail_with(error, std::string("cannot decode native range: ") +
                                  reader.get_error_str());
    if (reader.position() > range.source.end_pos)
      return fail_with(error, "native range ends in a partial event");
    if (event->temp_buf == nullptr || reader.position() <= event_start)
      return fail_with(error, "native event lacks its validated wire bytes");
    validated_bytes.append(event->temp_buf, reader.position() - event_start);
    const auto [info_error, event_info] =
        extract_log_event_basic_info(event.get());
    if (info_error || boundary_parser.feed_event(event_info, false))
      return fail_with(error,
                       "native range contains invalid transaction boundaries");
    const auto type = event->get_type_code();
    last_event_type = type;
    if (mysql::binlog::event::Log_event_type_helper::is_assigned_gtid_event(
            type)) {
      const auto *gtid = dynamic_cast<const Gtid_log_event *>(event.get());
      if (gtid == nullptr || !append_gtid(*gtid, &gtids, error)) return false;
    } else if (type == mysql::binlog::event::ANONYMOUS_GTID_LOG_EVENT) {
      return fail_with(error, "native segment contains an anonymous GTID");
    } else if (type == mysql::binlog::event::XID_EVENT) {
      const auto *xid = dynamic_cast<const Xid_log_event *>(event.get());
      if (xid == nullptr)
        return fail_with(error, "cannot decode native XID event");
      metadata->xids.push_back(xid->xid);
    } else if (type == mysql::binlog::event::ROTATE_EVENT) {
      const auto *rotate = dynamic_cast<const Rotate_log_event *>(event.get());
      if (rotate == nullptr || rotate->new_log_ident == nullptr ||
          rotate->ident_len == 0)
        return fail_with(error, "cannot decode native rotate event");
      rotate_target.assign(rotate->new_log_ident, rotate->ident_len);
    }
  }
  if (reader.position() != range.source.end_pos)
    return fail_with(error,
                     "native range does not end at an event boundary");
  if (!boundary_parser.is_not_inside_transaction() ||
      boundary_parser.is_error())
    return fail_with(error, "native range ends inside a transaction");
  if (!expected_next_file.empty() &&
      (last_event_type != mysql::binlog::event::ROTATE_EVENT ||
       rotate_target != expected_next_file))
    return fail_with(error,
                     "native binlog files are not joined by the expected "
                     "rotate event");
  if (gtids.empty() &&
      (expected_next_file.empty() || !metadata->xids.empty()))
    return fail_with(error,
                     "native range is empty or not at a transaction boundary");
  for (size_t index = 0; index < gtids.size(); ++index) {
    if (index != 0) metadata->gtid_set.push_back(',');
    metadata->gtid_set.append(gtids[index]);
  }
  metadata->transaction_count = gtids.size();
  return sha256_hex(validated_bytes, &metadata->native_sha256, error);
}

bool build_native_ranges(MYSQL_BIN_LOG *binlog, const Cursor &start,
                         const Cursor &end,
                         std::vector<NativeBinlogRange> *ranges,
                         std::vector<uint64_t> *all_xids,
                         std::string *all_gtids, uint64_t *transaction_count,
                         std::string *error) {
  if (binlog == nullptr || ranges == nullptr || all_xids == nullptr ||
      all_gtids == nullptr || transaction_count == nullptr)
    return fail_with(error, "null native range input");
  if (!cursor_less(start, end))
    return fail_with(error, "group endpoint does not advance durable cursor");

  std::string start_base;
  std::string end_base;
  uint64_t start_sequence = 0;
  uint64_t end_sequence = 0;
  if (!parse_binlog_name(start.file, &start_base, &start_sequence, nullptr) ||
      !parse_binlog_name(end.file, &end_base, &end_sequence, nullptr) ||
      start_base != end_base || start_sequence > end_sequence)
    return fail_with(error, "group cursor uses a non-canonical binlog chain");
  if (start.file != numbered_binlog_name(start_base, start_sequence) ||
      end.file != numbered_binlog_name(end_base, end_sequence))
    return fail_with(error, "group cursor uses a non-canonical binlog name");

  const fs::path active_path(binlog->get_log_fname());
  if (active_path.filename().string() != end.file)
    return fail_with(error, "group endpoint is not in the active binlog file");
  const fs::path parent = active_path.parent_path();

  ranges->clear();
  all_xids->clear();
  all_gtids->clear();
  *transaction_count = 0;
  for (uint64_t sequence = start_sequence;; ++sequence) {
    NativeBinlogRange range;
    range.source.file = numbered_binlog_name(start_base, sequence);
    range.local_path = (parent / range.source.file).string();
    range.source.start_pos = sequence == start_sequence ? start.pos : 0;
    std::error_code file_error;
    const uint64_t file_size = fs::file_size(range.local_path, file_error);
    if (file_error)
      return fail_with(error, "cannot stat native binlog range source");
    range.source.end_pos = sequence == end_sequence ? end.pos : file_size;
    if (range.source.end_pos > file_size ||
        range.source.start_pos >= range.source.end_pos)
      return fail_with(error, "native binlog range is outside its source file");
    const std::string next_file =
        sequence == end_sequence
            ? std::string()
            : numbered_binlog_name(start_base, sequence + 1);
    if (!scan_native_range(range, next_file, &range.metadata, error))
      return false;
    if (!range.metadata.gtid_set.empty()) {
      if (!all_gtids->empty()) all_gtids->push_back(',');
      all_gtids->append(range.metadata.gtid_set);
    }
    all_xids->insert(all_xids->end(), range.metadata.xids.begin(),
                     range.metadata.xids.end());
    if (range.metadata.transaction_count >
        kJsonSafeIntegerMax - *transaction_count)
      return fail_with(error, "group transaction count overflow");
    *transaction_count += range.metadata.transaction_count;
    ranges->push_back(std::move(range));
    if (sequence == end_sequence) break;
    if (sequence == std::numeric_limits<uint64_t>::max())
      return fail_with(error, "binlog sequence overflow");
  }
  if (*transaction_count == 0)
    return fail_with(error, "remote commit group contains no transactions");
  return true;
}

Cursor group_endpoint(THD *queue) {
  Cursor endpoint;
  for (THD *head = queue; head != nullptr; head = head->next_to_commit) {
    if (head->commit_error != THD::CE_NONE)
      fail_stop("commit group contains a pre-decision error");
    const char *file = nullptr;
    my_off_t pos = 0;
    head->get_trans_fixed_pos(&file, &pos);
    if (file == nullptr || pos <= 0)
      fail_stop("commit group member has no binlog endpoint");
    const Cursor current{file, static_cast<uint64_t>(pos)};
    if (endpoint.file.empty() || cursor_less(endpoint, current))
      endpoint = current;
  }
  if (endpoint.file.empty()) fail_stop("remote commit group is empty");
  return endpoint;
}

Writer local_writer(const HeadPublisher &publisher) {
  const auto &epoch = publisher.state().epoch;
  if (!epoch.has_value()) fail_stop("remote writer has no acquired epoch");
  return {epoch->writer_id, epoch->epoch};
}

HeadParent exact_parent(const HeadPublisher &publisher) {
  const PublisherState &state = publisher.state();
  if (!state.head.has_value() || !state.head_object.has_value())
    fail_stop("remote LOG transition has no prior HEAD");
  std::string body_sha;
  std::string error;
  if (!sha256_hex(state.head_object->body, &body_sha, &error))
    fail_stop("cannot hash prior remote HEAD");
  return {state.head->generation, state.head_object->etag, std::move(body_sha)};
}

bool clone_cut_error(std::string *error, std::string message) {
  if (error != nullptr) *error = std::move(message);
  return true;
}

std::unique_ptr<CloneCutBinlogPin> pin_clone_cut_binlog(
    const CloneCutState &state, bool test_mode, std::string *error) {
  auto pin = std::make_unique<CloneCutBinlogPin>(state.file, state.pos);
  fs::path test_source;
#ifdef WESQL_TEST
  if (test_mode && g_runtime.clone_cut_binlog_source_path_for_test.has_value())
    test_source = *g_runtime.clone_cut_binlog_source_path_for_test;
#endif
  if (!pin->install(test_mode, test_source, error)) return nullptr;
  return pin;
}

bool is_lowercase_sha256(std::string_view value) {
  if (value.size() != 64) return false;
  return std::all_of(value.begin(), value.end(), [](const char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
  });
}

bool validate_clone_cut_state(const CloneCutState &state, std::string *error) {
  if (state.request_id == 0)
    return fail_with(error, "clone cut has an invalid snapshot request ID");
  std::string basename;
  uint64_t sequence = 0;
  if (!parse_binlog_name(state.file, &basename, &sequence, nullptr) ||
      state.file != numbered_binlog_name(basename, sequence) || state.pos == 0)
    return fail_with(error, "clone cut has an invalid public cursor");
  if (state.head_generation == 0 ||
      state.head_generation > kJsonSafeIntegerMax ||
      !is_lowercase_sha256(state.head_body_sha256))
    return fail_with(error, "clone cut has an invalid exact HEAD identity");

  GtidSetDigest digest;
  std::string digest_error;
  if (!gtid_digest(state.canonical_gtid, &digest, &digest_error) ||
      digest.canonical != state.canonical_gtid ||
      digest.sha256 != state.gtid_sha256 ||
      !is_lowercase_sha256(state.gtid_sha256)) {
    std::string message = "clone cut has an invalid canonical GTID digest";
    if (!digest_error.empty()) message.append(": ").append(digest_error);
    return fail_with(error, std::move(message));
  }
  return true;
}

bool capture_executed_gtid(GtidSetDigest *digest, std::string *error) {
  if (digest == nullptr || global_tsid_lock == nullptr || gtid_state == nullptr)
    return fail_with(error, "executed GTID state is unavailable at clone cut");

  char *text = nullptr;
  global_tsid_lock->wrlock();
  const long length =
      gtid_state->get_executed_gtids()->to_string(&text, false, nullptr);
  global_tsid_lock->unlock();
  if (length < 0 || text == nullptr)
    return fail_with(error, "cannot allocate executed GTID snapshot");

  const bool valid =
      gtid_digest(std::string_view(text, static_cast<size_t>(length)), digest,
                  error);
  my_free(text);
  return valid;
}

bool capture_exact_remote_head(uint64_t request_id, CloneCutState *state,
                               std::string *error) {
  if (state == nullptr) return fail_with(error, "null clone cut HEAD result");
  if (request_id == 0)
    return fail_with(error, "clone cut has an invalid snapshot request ID");

  std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
  if (!g_runtime.initialized || !g_runtime.running ||
      g_runtime.startup_phase != StartupPhase::ACTIVE ||
      g_runtime.publisher == nullptr)
    return fail_with(error, "remote runtime is not ACTIVE at clone cut");

  const PublisherState &before = g_runtime.publisher->state();
  if (!before.head.has_value() || !before.head_object.has_value() ||
      !before.epoch.has_value() || !before.epoch_object.has_value())
    return fail_with(error, "remote runtime lacks exact HEAD/epoch state");
  const Head expected_head = *before.head;
  const PublishedBytes expected_head_object = *before.head_object;
  const WriterEpoch expected_epoch = *before.epoch;
  const PublishedBytes expected_epoch_object = *before.epoch_object;

  const PublishResult verified =
      g_runtime.publisher->verify_decision(expected_head);
  const PublisherState &after = g_runtime.publisher->state();
  if (!verified.applied()) {
    update_status_locked(after.lifecycle);
    std::string message = "clone cut HEAD/epoch verification failed";
    if (!verified.detail.empty()) message.append(": ").append(verified.detail);
    return fail_with(error, std::move(message));
  }
  if (!after.head.has_value() || !after.head_object.has_value() ||
      !after.epoch.has_value() || !after.epoch_object.has_value() ||
      *after.head != expected_head ||
      after.head_object->body != expected_head_object.body ||
      after.head_object->etag != expected_head_object.etag ||
      *after.epoch != expected_epoch ||
      after.epoch_object->body != expected_epoch_object.body ||
      after.epoch_object->etag != expected_epoch_object.etag)
    return fail_with(error,
                     "clone cut HEAD/epoch exact object identity changed");
  if (g_runtime.public_committed_cursor != expected_head.durable_cursor)
    return fail_with(
        error,
        "clone cut HEAD cursor is not the engine-committed public cursor");

  CloneCutState captured;
  captured.request_id = request_id;
  captured.file = expected_head.durable_cursor.file;
  captured.pos = expected_head.durable_cursor.pos;
  captured.head_generation = expected_head.generation;
  if (!sha256_hex(expected_head_object.body, &captured.head_body_sha256,
                  error))
    return false;
  *state = std::move(captured);
  return true;
}

bool capture_current_clone_cut(uint64_t request_id, CloneCutState *state,
                               std::string *error) {
  if (state == nullptr) return fail_with(error, "null clone cut state result");
  if (request_id == 0)
    return fail_with(error, "clone cut has an invalid snapshot request ID");
#ifdef WESQL_TEST
  if (g_runtime.clone_cut_source_for_test.has_value()) {
    *state = *g_runtime.clone_cut_source_for_test;
    if (state->request_id != request_id)
      return fail_with(error, "clone cut snapshot request ID changed");
    if (!g_runtime.clone_cut_public_cursor_for_test.has_value() ||
        *g_runtime.clone_cut_public_cursor_for_test !=
            Cursor{state->file, state->pos})
      return fail_with(
          error,
          "clone cut test HEAD cursor is not the committed public cursor");
    return validate_clone_cut_state(*state, error);
  }
#endif

  CloneCutState before_gtid;
  CloneCutState after_gtid;
  GtidSetDigest gtid;
  if (!capture_exact_remote_head(request_id, &before_gtid, error) ||
      !capture_executed_gtid(&gtid, error) ||
      !capture_exact_remote_head(request_id, &after_gtid, error))
    return false;
  if (before_gtid.file != after_gtid.file ||
      before_gtid.pos != after_gtid.pos ||
      before_gtid.head_generation != after_gtid.head_generation ||
      before_gtid.head_body_sha256 != after_gtid.head_body_sha256)
    return fail_with(error, "remote HEAD changed while snapshotting clone GTID");

  after_gtid.canonical_gtid = std::move(gtid.canonical);
  after_gtid.gtid_sha256 = std::move(gtid.sha256);
  if (!validate_clone_cut_state(after_gtid, error)) return false;
  *state = std::move(after_gtid);
  return true;
}

bool clone_cut_test_mode() {
#ifdef WESQL_TEST
  return g_runtime.clone_cut_source_for_test.has_value();
#else
  return false;
#endif
}

void start_clone_cut_pin_release() {
  std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
  if (g_runtime.clone_cut_pins_releasing ==
      std::numeric_limits<uint64_t>::max())
    fail_stop_holding_admission(
        "clone cut binlog pin release counter is exhausted");
  ++g_runtime.clone_cut_pins_releasing;
}

void finish_clone_cut_pin_release() {
  {
    std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
    if (g_runtime.clone_cut_pins_releasing == 0)
      fail_stop_holding_admission(
          "clone cut binlog pin release counter underflow");
    --g_runtime.clone_cut_pins_releasing;
  }
  g_runtime.admission_cond.notify_all();
}

void wait_before_clone_cut_pin_release_for_test() {
#ifdef WESQL_TEST
  std::unique_lock<std::mutex> guard(g_runtime.admission_mutex);
  if (!g_runtime.pause_clone_cut_pin_release_for_test) return;
  g_runtime.clone_cut_pin_release_waiting_for_test = true;
  g_runtime.admission_cond.notify_all();
  g_runtime.admission_cond.wait(guard, [] {
    return !g_runtime.pause_clone_cut_pin_release_for_test;
  });
  g_runtime.clone_cut_pin_release_waiting_for_test = false;
#endif
}

void rollback_clone_cut_reservation() {
  bool reopen = false;
  {
    std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
    if (g_runtime.clone_cut_phase == CloneCutBarrierPhase::CLOSING) {
      g_runtime.clone_cut_phase = CloneCutBarrierPhase::NONE;
      g_runtime.clone_cut_token = 0;
      g_runtime.active_clone_cut.reset();
      if (!g_runtime.shutdown_draining && !g_runtime.shutting_down) {
        if (live_snapshot_request_keeps_admission_closed_locked()) {
          g_runtime.admission_open = false;
        } else if (clone_cut_test_mode()) {
          g_runtime.admission_open = true;
        } else {
          if (g_runtime.admission_reopen_in_progress)
            fail_stop_holding_admission(
                "clone cut rollback overlapped an admission reopen");
          g_runtime.admission_reopen_in_progress = true;
          reopen = true;
        }
      }
    }
  }
  g_runtime.admission_cond.notify_all();
  if (reopen) complete_commit_admission_reopen();
}

bool authorization_is_required(THD *thd) {
  Transaction_ctx *transaction = thd->get_transaction();
  const bool all = transaction->m_flags.real_commit;
  const auto scope = all ? Transaction_ctx::SESSION : Transaction_ctx::STMT;
  const bool real = all || !transaction->is_active(Transaction_ctx::SESSION);
  return transaction->m_flags.commit_low && real &&
         transaction->rw_ha_count(scope) > 0;
}

bool recovery_runtime_matches_binding_locked(const CommitBinding &binding,
                                             std::string *error) {
  if (!g_runtime.initialized || g_runtime.publisher == nullptr ||
      g_runtime.startup_route != StartupRoute::TAKEOVER ||
      !g_runtime.startup_epoch_adopted ||
      g_runtime.startup_epoch_adoption_role !=
          StartupEpochAdoptionRole::TAKEOVER_RECOVERY ||
      g_runtime.startup_phase != StartupPhase::EPOCH_ACQUIRED ||
      g_runtime.status.state != LifecycleState::RECOVERING ||
      g_runtime.running) {
    return fail_with(
        error,
        "recovery authorization requires takeover EPOCH_ACQUIRED + RECOVERING");
  }

  std::string endpoint_base;
  std::string head_base;
  uint64_t endpoint_sequence = 0;
  uint64_t head_sequence = 0;
  const PublisherState &state = g_runtime.publisher->state();
  if (binding.stream_id != g_runtime.stream.stream_id ||
      binding.head_generation == 0 ||
      binding.head_generation > kJsonSafeIntegerMax ||
      !is_lowercase_sha256(binding.head_body_sha256) ||
      !is_lowercase_sha256(binding.gtid_sha256) ||
      !is_lowercase_sha256(binding.xid_sha256) || binding.endpoint_pos == 0 ||
      !parse_binlog_name(binding.endpoint_file, &endpoint_base,
                         &endpoint_sequence, nullptr) ||
      binding.endpoint_file !=
          numbered_binlog_name(endpoint_base, endpoint_sequence) ||
      !state.head.has_value() || !state.head_object.has_value() ||
      !state.epoch.has_value() || !state.epoch_object.has_value() ||
      state.head->generation != binding.head_generation ||
      g_runtime.probed_head_generation != binding.head_generation ||
      !parse_binlog_name(state.head->durable_cursor.file, &head_base,
                         &head_sequence, nullptr) ||
      state.head->durable_cursor.file !=
          numbered_binlog_name(head_base, head_sequence) ||
      endpoint_base != head_base || endpoint_sequence > head_sequence ||
      compare_cursor(Cursor{binding.endpoint_file, binding.endpoint_pos},
                     state.head->durable_cursor) > 0) {
    return fail_with(error,
                     "recovery authorization binding is outside the exact "
                     "candidate HEAD");
  }

  std::string body_sha256;
  std::string digest_error;
  if (!sha256_hex(state.head_object->body, &body_sha256, &digest_error)) {
    std::string message = "cannot hash the recovery candidate HEAD";
    if (!digest_error.empty()) message.append(": ").append(digest_error);
    return fail_with(error, std::move(message));
  }
  if (body_sha256 != binding.head_body_sha256)
    return fail_with(error,
                     "recovery authorization candidate HEAD bytes changed");
  return true;
}

void install_authorizations(THD *queue, const CommitBinding &binding) {
  bool duplicate = false;
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    for (THD *head = queue; head != nullptr; head = head->next_to_commit) {
      if (g_runtime.authorizations.contains(head)) {
        duplicate = true;
        break;
      }
    }
    if (!duplicate) {
      for (THD *head = queue; head != nullptr; head = head->next_to_commit) {
        const bool required = authorization_is_required(head);
        CommitAuthorization authorization =
            required ? CommitAuthorization::group(binding)
                     : CommitAuthorization::read_only();
        g_runtime.authorizations.emplace(
            head,
            AuthorizationRecord{std::move(authorization), binding, required});
      }
    }
  }
  if (duplicate)
    fail_stop("remote commit authorization already exists for THD");
}

uint64_t acquire_ticket() {
  std::unique_lock<std::mutex> lock(g_order.mutex);
  if (g_order.next == std::numeric_limits<uint64_t>::max()) {
    lock.unlock();
    fail_stop("remote commit order ticket space is exhausted");
  }
  const uint64_t ticket = g_order.next++;
  g_order.cond.wait(lock, [ticket] { return g_order.serving == ticket; });
  return ticket;
}

void release_ticket(uint64_t ticket) {
  bool invalid = false;
  {
    std::lock_guard<std::mutex> guard(g_order.mutex);
    if (g_order.serving != ticket ||
        g_order.serving == std::numeric_limits<uint64_t>::max()) {
      invalid = true;
    } else {
      ++g_order.serving;
      g_order.cond.notify_all();
    }
  }
  if (invalid) fail_stop("remote order ticket released out of order");
}

uint64_t hold_snapshot_order_ticket_locked(uint64_t request_id) {
  if (request_id == 0)
    fail_stop("runtime snapshot requested an invalid order owner");
  if (g_snapshot_order.request_id.has_value() !=
      g_snapshot_order.ticket.has_value())
    fail_stop("runtime snapshot order ownership is incomplete");
  if (g_snapshot_order.ticket.has_value()) {
    if (*g_snapshot_order.request_id != request_id)
      fail_stop("another runtime snapshot owns the order ticket");
    return *g_snapshot_order.ticket;
  }
  const uint64_t ticket = acquire_ticket();
  g_snapshot_order.request_id = request_id;
  g_snapshot_order.ticket = ticket;
  return ticket;
}

bool release_snapshot_order_ticket_locked(uint64_t request_id) {
  if (g_snapshot_order.request_id.has_value() !=
      g_snapshot_order.ticket.has_value())
    fail_stop("runtime snapshot order ownership is incomplete");
  if (!g_snapshot_order.ticket.has_value()) return false;
  if (request_id != 0 && *g_snapshot_order.request_id != request_id) return false;
  const uint64_t ticket = *g_snapshot_order.ticket;
  g_snapshot_order.request_id.reset();
  g_snapshot_order.ticket.reset();
  release_ticket(ticket);
  return true;
}

bool release_snapshot_order_ticket(uint64_t request_id) {
  std::lock_guard<std::mutex> guard(g_snapshot_order.mutex);
  return release_snapshot_order_ticket_locked(request_id);
}

}  // namespace

struct OrderToken {
  uint64_t ticket{0};
  Head head;
  TransitionManifest manifest;
  AckReadyEvent ack;
  CommitBinding binding;
  Cursor pre_ack_public_cursor;
  std::vector<THD *> members;
  bool engine_checked{false};
  bool ownership_verified{false};
  bool ack_emitted{false};
};

namespace {

bool queue_matches_token(THD *queue, const OrderToken &token) {
  size_t index = 0;
  for (THD *member = queue; member != nullptr;
       member = member->next_to_commit) {
    if (index >= token.members.size() || token.members[index] != member)
      return false;
    ++index;
  }
  return index == token.members.size() && index != 0;
}

const char *authorization_integrity_failure(const OrderToken &token,
                                            bool require_consumed) {
  for (THD *member : token.members) {
    const auto found = g_runtime.authorizations.find(member);
    if (found == g_runtime.authorizations.end())
      return "commit group member lost remote authorization";
    const AuthorizationRecord &record = found->second;
    if (!(record.binding == token.binding))
      return "commit group authorization binding changed";
    if (record.required) {
      if (record.authorization.kind() != AuthorizationKind::GROUP)
        return "commit group authorization kind changed";
      if (require_consumed && !record.authorization.consumed())
        return "engine commit bypassed remote authorization consumption";
    } else if (record.authorization.kind() != AuthorizationKind::READ_ONLY ||
               record.authorization.consumed()) {
      return "read-only commit authorization changed";
    }
  }
  return nullptr;
}

bool reserve_ack_public_cursor_locked(
    const Head &head, const AckReadyEvent &ack, const CommitBinding &binding,
    const Cursor &pre_ack_public_cursor, std::string *error) {
  if (!g_runtime.initialized || g_runtime.publisher == nullptr ||
      g_runtime.startup_phase != StartupPhase::ACTIVE || !g_runtime.running)
    return fail_with(error,
                     "remote ACK publication occurred outside ACTIVE state");

  const PublisherState &state = g_runtime.publisher->state();
  if (!state.head.has_value() || !state.head_object.has_value() ||
      !state.epoch.has_value() || !state.epoch_object.has_value())
    return fail_with(error, "remote ACK publication lost exact HEAD or epoch");

  const Writer current_writer{state.epoch->writer_id, state.epoch->epoch};
  if (*state.head != head || head.writer != current_writer ||
      ack.writer != current_writer ||
      ack.stream_id != g_runtime.stream.stream_id ||
      ack.endpoint != head.durable_cursor ||
      ack.head_generation != head.generation || ack.manifest != head.manifest ||
      binding.stream_id != ack.stream_id ||
      binding.head_generation != ack.head_generation ||
      binding.endpoint_file != ack.endpoint.file ||
      binding.endpoint_pos != ack.endpoint.pos ||
      binding.gtid_sha256 != ack.gtid_set_sha256 ||
      binding.xid_sha256 != ack.xid_sha256)
    return fail_with(error, "remote ACK token does not bind current HEAD/epoch");

  std::string head_body_sha256;
  std::string head_etag_sha256;
  if (!sha256_hex(state.head_object->body, &head_body_sha256, error) ||
      !sha256_hex(state.head_object->etag, &head_etag_sha256, error))
    return false;
  if (ack.head_body_sha256 != head_body_sha256 ||
      ack.head_etag_sha256 != head_etag_sha256 ||
      binding.head_body_sha256 != head_body_sha256)
    return fail_with(error, "remote ACK token lost exact HEAD object identity");
  if (g_runtime.public_committed_cursor != pre_ack_public_cursor)
    return fail_with(error, "remote ACK public cursor changed before publication");

  g_runtime.public_committed_cursor = ack.endpoint;
  update_status_locked(LifecycleState::RUNNING);
  return true;
}

}  // namespace

CloneCutBinlogPin::~CloneCutBinlogPin() {
  if (registered_ && log_info_ != nullptr)
    mysql_bin_log.unregister_log_info(log_info_.get());
}

bool CloneCutBinlogPin::install(bool test_mode, const fs::path &test_source,
                                std::string *error) {
  if (active() || file_.empty() || pos_ == 0) {
    if (error != nullptr) *error = "clone cut binlog pin is invalid";
    return false;
  }
#ifdef WESQL_TEST
  if (test_mode) {
    if (!test_source.empty()) {
      std::error_code path_error;
      const fs::path resolved = fs::canonical(test_source, path_error);
      const uintmax_t size = path_error ? 0 : fs::file_size(resolved, path_error);
      if (path_error || !fs::is_regular_file(resolved, path_error) ||
          path_error || pos_ > size) {
        if (error != nullptr)
          *error = "clone cut test binlog source is invalid";
        return false;
      }
      source_path_ = resolved;
    }
    test_only_ = true;
    return true;
  }
#else
  (void)test_mode;
#endif

  auto log_info = std::make_unique<Log_info>();
  mysql_bin_log.lock_index();
  const int find_error =
      mysql_bin_log.find_log_pos(log_info.get(), file_.c_str(), false);
  if (find_error != 0 ||
      fs::path(log_info->log_file_name).filename().string() != file_) {
    mysql_bin_log.unlock_index();
    if (error != nullptr)
      *error = "clone cut public cursor file is absent from the binlog index";
    return false;
  }

  std::error_code path_error;
  const fs::path resolved = fs::canonical(log_info->log_file_name, path_error);
  const uintmax_t file_size = path_error ? 0 : fs::file_size(resolved, path_error);
  if (path_error || !fs::is_regular_file(resolved, path_error) || path_error ||
      resolved.filename() != file_ || pos_ > file_size) {
    mysql_bin_log.unlock_index();
    if (error != nullptr)
      *error = "clone cut public cursor is outside its indexed binlog file";
    return false;
  }

  log_info->pos = static_cast<my_off_t>(pos_);
  log_info->thread_id = current_thd == nullptr ? 0 : current_thd->thread_id();
  mysql_bin_log.register_log_info(log_info.get());
  mysql_bin_log.unlock_index();

  log_info_ = std::move(log_info);
  source_path_ = resolved;
  registered_ = true;
  return true;
}

CloneCutBarrierLease::CloneCutBarrierLease() = default;

CloneCutBarrierLease::~CloneCutBarrierLease() {
  end_clone_cut_barrier(this);
}

CloneCutBarrierLease::CloneCutBarrierLease(
    CloneCutBarrierLease &&other) noexcept
    : token_(std::exchange(other.token_, 0)),
      request_id_(std::exchange(other.request_id_, 0)),
      binlog_pin_(std::move(other.binlog_pin_)) {}

CloneCutBarrierLease &CloneCutBarrierLease::operator=(
    CloneCutBarrierLease &&other) noexcept {
  if (this != &other) {
    end_clone_cut_barrier(this);
    token_ = std::exchange(other.token_, 0);
    request_id_ = std::exchange(other.request_id_, 0);
    binlog_pin_ = std::move(other.binlog_pin_);
  }
  return *this;
}

bool enabled() {
  return opt_binlog_archive_remote_commit;
}

uint64_t maximum_segment_bytes() {
  return opt_binlog_archive_remote_commit_max_segment_bytes;
}

bool is_fenced() {
  std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
  return g_runtime.status.state == LifecycleState::FENCED;
}

bool immutable_extent_runtime(ImmutableExtentRuntime *runtime) {
  if (runtime == nullptr || !enabled()) return false;
  std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
  if (!g_runtime.initialized || g_runtime.object_store == nullptr ||
      g_runtime.publisher == nullptr ||
      g_runtime.status.state == LifecycleState::FENCED ||
      !g_runtime.publisher->state().epoch.has_value())
    return false;
  *runtime = ImmutableExtentRuntime{
      g_runtime.cluster_object_prefix,
      g_runtime.stream.stream_sha256,
      g_runtime.publisher->state().epoch->epoch,
      g_runtime.object_store,
      g_runtime.bucket,
  };
  return true;
}

bool startup_io_runtime(StartupIoRuntime *runtime) {
  if (runtime == nullptr || !enabled()) return false;
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  if (!g_runtime.initialized || g_runtime.running || g_runtime.shutting_down ||
      g_runtime.admission_open || !g_runtime.admissions.empty() ||
      g_runtime.clone_cut_phase != CloneCutBarrierPhase::NONE ||
      (g_runtime.startup_phase != StartupPhase::HEAD_PROBED &&
       g_runtime.startup_phase != StartupPhase::EPOCH_ACQUIRED &&
       g_runtime.startup_phase !=
           StartupPhase::INSTALLED_REEXEC_PRE_RECOVERY &&
       g_runtime.startup_phase != StartupPhase::ROOT_VERIFIED) ||
      g_runtime.object_store == nullptr || g_runtime.bucket.empty() ||
      g_runtime.stream.stream_id.empty() || g_runtime.io == nullptr ||
      g_runtime.publisher == nullptr)
    return false;
  *runtime = StartupIoRuntime{g_runtime.stream, g_runtime.object_store,
                              g_runtime.io.get(), g_runtime.publisher.get(),
                              g_runtime.bucket};
  return true;
}

bool runtime_snapshot_authority(uint64_t request_id,
                                RuntimeSnapshotAuthority *authority,
                                std::string *error) {
  if (error != nullptr) error->clear();
  if (authority == nullptr)
    return clone_cut_error(error, "null runtime snapshot authority output");
  *authority = {};
  if (!enabled())
    return clone_cut_error(error,
                           "runtime snapshot authority requires remote mode");
  if (request_id == 0)
    return clone_cut_error(
        error, "runtime snapshot authority has an invalid request ID");

  std::lock_guard<std::mutex> clone_guard(g_runtime.clone_cut_mutex);
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  const PublisherState *publisher_state =
      g_runtime.publisher == nullptr ? nullptr : &g_runtime.publisher->state();
  if (g_runtime.shutdown_draining || g_runtime.shutting_down ||
      g_runtime.admission_open || g_runtime.admission_reopen_in_progress ||
      !g_runtime.admissions.empty() ||
      g_runtime.clone_cut_phase != CloneCutBarrierPhase::ACTIVE ||
      g_runtime.clone_cut_token == 0 ||
      !g_runtime.active_clone_cut.has_value() ||
      g_runtime.active_clone_cut->request_id != request_id) {
    return clone_cut_error(
        error,
        "runtime snapshot authority requires the matching CLOSED clone cut");
  }
  if (!g_runtime.initialized || !g_runtime.running ||
      g_runtime.startup_phase != StartupPhase::ACTIVE ||
      g_runtime.status.state != LifecycleState::RUNNING ||
      publisher_state == nullptr ||
      publisher_state->lifecycle != LifecycleState::RUNNING ||
      !publisher_state->epoch.has_value() ||
      !publisher_state->epoch_object.has_value() ||
      !publisher_state->head.has_value() ||
      !publisher_state->head_object.has_value() ||
      g_runtime.metadata_io == nullptr || g_runtime.store == nullptr) {
    return clone_cut_error(error,
                           "runtime snapshot authority requires RUNNING state");
  }
  const Writer writer{publisher_state->epoch->writer_id,
                      publisher_state->epoch->epoch};
  const Cursor cut_cursor{g_runtime.active_clone_cut->file,
                          g_runtime.active_clone_cut->pos};
  if (publisher_state->head->writer != writer ||
      g_runtime.public_committed_cursor !=
          publisher_state->head->durable_cursor ||
      cut_cursor != g_runtime.public_committed_cursor ||
      !g_runtime.published_window_valid ||
      g_runtime.published_window_generation !=
          publisher_state->head->generation ||
      g_runtime.published_window != publisher_state->head->recovery_window) {
    return clone_cut_error(
        error,
        "runtime snapshot authority is not public-cursor or window exact");
  }

  *authority = RuntimeSnapshotAuthority{
      request_id, g_runtime.stream, *publisher_state, writer,
      g_runtime.metadata_io, g_runtime.store.get(), g_runtime.publisher.get()};
  return false;
}

PublishResult publish_prepared_runtime_snapshot(
    uint64_t request_id, SnapshotPublisher *snapshot_publisher,
    const PreparedSnapshotPublication &prepared,
    SnapshotPublication *publication) {
  if (!enabled() || request_id == 0 || snapshot_publisher == nullptr ||
      publication == nullptr) {
    return {PublishOutcome::PERMANENT_ERROR,
            "ordered runtime snapshot publication has invalid input"};
  }
  std::unique_lock<std::mutex> snapshot_order_guard(g_snapshot_order.mutex);
  {
    std::lock_guard<std::mutex> admission_guard(g_runtime.admission_mutex);
    // Preserve a request-specific terminal outcome below; the global fence is
    // the stale-authority case that must stop publication unconditionally.
    if (g_runtime.snapshot_terminal_failure) {
      release_snapshot_order_ticket_locked(request_id);
      return {PublishOutcome::FENCED,
              "runtime snapshot subsystem is terminal"};
    }
    if (!g_runtime.snapshot_request.has_value() ||
        g_runtime.snapshot_request->request_id != request_id) {
      release_snapshot_order_ticket_locked(request_id);
      return {PublishOutcome::REFIX_REQUIRED,
              "runtime snapshot publication request is stale"};
    }
    if (g_runtime.snapshot_request->terminal_outcome.has_value()) {
      const RuntimeSnapshotRequestOutcome terminal =
          *g_runtime.snapshot_request->terminal_outcome;
      const std::string detail = g_runtime.snapshot_request->terminal_detail;
      release_snapshot_order_ticket_locked(request_id);
      return {terminal == RuntimeSnapshotRequestOutcome::FENCED
                  ? PublishOutcome::FENCED
                  : PublishOutcome::PERMANENT_ERROR,
              detail};
    }
    if (g_runtime.shutdown_draining || g_runtime.shutting_down) {
      release_snapshot_order_ticket_locked(request_id);
      return {PublishOutcome::BLOCKED,
              "server shutdown interrupted runtime snapshot publication"};
    }
  }

  const bool retrying_with_owned_ticket =
      g_snapshot_order.request_id.has_value() &&
      *g_snapshot_order.request_id == request_id;
  (void)hold_snapshot_order_ticket_locked(request_id);
  bool runtime_exact = false;
  {
    std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
    const PublisherState *state =
        g_runtime.publisher == nullptr ? nullptr : &g_runtime.publisher->state();
    runtime_exact =
        g_runtime.initialized && g_runtime.running &&
        g_runtime.startup_phase == StartupPhase::ACTIVE && state != nullptr &&
        (state->lifecycle == LifecycleState::RUNNING ||
         (retrying_with_owned_ticket &&
          state->lifecycle == LifecycleState::BLOCKED)) &&
        state->epoch.has_value() &&
        state->head.has_value() && state->head_object.has_value() &&
        prepared.writer ==
            Writer{state->epoch->writer_id, state->epoch->epoch} &&
        g_runtime.public_committed_cursor == state->head->durable_cursor;
  }
  if (!runtime_exact) {
    release_snapshot_order_ticket_locked(request_id);
    return {PublishOutcome::REFIX_REQUIRED,
            "runtime snapshot publication lost RUNNING cursor authority"};
  }

  const PublishResult published =
      snapshot_publisher->publish_prepared(prepared, publication);
  if (!published.applied()) {
    if (published.outcome != PublishOutcome::BLOCKED)
      release_snapshot_order_ticket_locked(request_id);
    return published;
  }

  bool publication_exact = false;
  {
    std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
    const PublisherState &state = g_runtime.publisher->state();
    publication_exact = state.lifecycle == LifecycleState::RUNNING &&
                        state.head.has_value() &&
                        *state.head == publication->head &&
                        g_runtime.public_committed_cursor ==
                            publication->head.durable_cursor;
    if (publication_exact) update_status_locked(LifecycleState::RUNNING);
  }
  const uint64_t generation = publication->head.generation;
  const RecoveryWindow window = publication->head.recovery_window;
  if (!release_snapshot_order_ticket_locked(request_id))
    fail_stop("ordered SNAPSHOT lost its request-owned ticket");
  if (!publication_exact)
    fail_stop("ordered SNAPSHOT did not advance the server HEAD exactly");
  snapshot_order_guard.unlock();
  refresh_published_window_after_order_release(generation, window, request_id,
                                               &publication->head);
  return published;
}

bool initialize() {
  return initialize(false);
}

bool initialize(bool bootstrap_preflight) {
  std::unique_lock<std::mutex> state_lock(g_runtime.state_mutex);
  if (!enabled()) {
    g_runtime.startup_route = StartupRoute::DISABLED;
    update_status_locked(LifecycleState::OFF);
    return false;
  }
  if (g_runtime.initialized ||
      g_runtime.startup_phase != StartupPhase::UNINITIALIZED) {
    g_runtime.last_error = "remote commit runtime was initialized twice";
    return true;
  }
  g_runtime.last_error.clear();
  if (!opt_serverless || !opt_binlog_archive) {
    g_runtime.last_error =
        "remote commit requires serverless binlog archive mode";
    return true;
  }
  if (!sql_command_classification_complete()) {
    g_runtime.last_error =
        "remote commit SQL command classification is incomplete";
    return true;
  }
  if (opt_objstore_provider == nullptr || opt_objstore_region == nullptr ||
      opt_repo_objstore_id == nullptr || opt_branch_objstore_id == nullptr ||
      opt_objstore_bucket == nullptr) {
    g_runtime.last_error = "remote commit object-store identity is incomplete";
    return true;
  }

  const std::string cluster_prefix =
      std::string(opt_repo_objstore_id) + "/" + opt_branch_objstore_id;
  std::string error;
  if (!build_stream_identity(opt_repo_objstore_id, opt_branch_objstore_id,
                             cluster_prefix, &g_runtime.stream, &error)) {
    g_runtime.last_error = std::move(error);
    return true;
  }

  const std::string provider(opt_objstore_provider);
  const std::string_view endpoint =
      opt_objstore_endpoint != nullptr ? std::string_view(opt_objstore_endpoint)
                                       : std::string_view();
  objstore::init_objstore_provider(provider);
  objstore::ObjectStore *object_store = objstore::create_object_store(
      provider, opt_objstore_region,
      opt_objstore_endpoint != nullptr ? &endpoint : nullptr,
      opt_objstore_use_https, error);
  if (object_store == nullptr) {
    objstore::cleanup_objstore_provider(provider);
    g_runtime.last_error = "cannot create remote commit object-store client";
    if (!error.empty()) g_runtime.last_error.append(": ").append(error);
    return true;
  }
  g_runtime.object_store = object_store;
  g_runtime.object_store_owned = true;
  g_runtime.object_store_provider_initialized = true;
  g_runtime.object_store_provider = provider;
  const objstore::ConditionalObjectStoreCapabilities capabilities =
      object_store->conditional_capabilities();
  if (reject_startup_policy(configured_startup_policy(capabilities),
                            bootstrap_preflight,
                            &g_runtime.last_error)) {
    return true;
  }

  g_runtime.io = std::make_unique<ObjectStoreConditionalIo>(
      object_store, std::string(opt_objstore_bucket));
  g_runtime.metadata_io = g_runtime.io.get();
  g_runtime.store = std::make_unique<ProtocolStore>(g_runtime.io.get());
  g_runtime.publisher =
      std::make_unique<HeadPublisher>(g_runtime.io.get(), g_runtime.stream);
  g_runtime.sealer = std::make_unique<SegmentSealer>(
      g_runtime.store.get(), g_runtime.stream, maximum_segment_bytes());

  PublishResult probe = g_runtime.publisher->probe();
  if (!probe.applied()) {
    g_runtime.last_error = "cannot probe remote commit namespace: " + probe.detail;
    update_status_locked(g_runtime.publisher->state().lifecycle);
    return true;
  }

  g_runtime.initialized = true;
  g_runtime.running = false;
  g_runtime.bootstrap_preflight = bootstrap_preflight;
  g_runtime.startup_epoch_adopted = false;
  g_runtime.recovery_snapshot_gtids_restored = false;
  g_runtime.startup_epoch_adoption_role =
      StartupEpochAdoptionRole::TAKEOVER_RECOVERY;
  g_runtime.adopted_startup_proof.reset();
  g_runtime.startup_phase = StartupPhase::HEAD_PROBED;
  g_runtime.startup_route = g_runtime.publisher->state().head.has_value()
                                ? StartupRoute::TAKEOVER
                                : StartupRoute::BOOTSTRAP;
  g_runtime.probed_head_generation =
      g_runtime.publisher->state().head.has_value()
          ? g_runtime.publisher->state().head->generation
          : 0;
  g_runtime.installed_head.reset();
  g_runtime.installed_head_body.clear();
  g_runtime.installed_head_etag.clear();
  g_runtime.installed_marker.reset();
  g_runtime.cluster_object_prefix = cluster_prefix;
  g_runtime.bucket = opt_objstore_bucket;
  const std::optional<Head> probed_head = g_runtime.publisher->state().head;
  update_status_locked(g_runtime.publisher->state().head.has_value()
                           ? LifecycleState::RECOVERING
                           : LifecycleState::INITIALIZING);
  state_lock.unlock();
  {
    std::lock_guard<std::mutex> admission_guard(g_runtime.admission_mutex);
    g_runtime.shutting_down = false;
    g_runtime.admission_open = false;
    g_runtime.admission_reopen_in_progress = false;
    reset_admission_accounting_locked();
    if (probed_head.has_value()) {
      g_runtime.published_window_valid = true;
      g_runtime.published_window_generation = probed_head->generation;
      g_runtime.published_window = probed_head->recovery_window;
    }
    g_runtime.clone_cut_phase = CloneCutBarrierPhase::NONE;
    g_runtime.clone_cut_token = 0;
    g_runtime.active_clone_cut.reset();
  }
  return false;
}

bool startup_probe(StartupProbe *probe) {
  if (probe == nullptr) return true;
  *probe = {};
  if (!enabled()) return false;
  std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
  if (!g_runtime.initialized || g_runtime.publisher == nullptr ||
      g_runtime.startup_phase == StartupPhase::UNINITIALIZED ||
      g_runtime.startup_phase == StartupPhase::SHUTTING_DOWN) {
    g_runtime.last_error = "remote commit HEAD probe is not available";
    return true;
  }
  const PublisherState &state = g_runtime.publisher->state();
  probe->route = g_runtime.startup_route;
  if (state.epoch.has_value()) {
    probe->epoch_present = true;
    probe->epoch = state.epoch->epoch;
  }
  if (g_runtime.startup_route == StartupRoute::TAKEOVER) {
    if (!state.head.has_value() || !state.head_object.has_value()) {
      g_runtime.last_error = "takeover route lost its probed HEAD";
      return true;
    }
    probe->head_body = state.head_object->body;
    probe->head_etag = state.head_object->etag;
    probe->head_generation = state.head->generation;
    probe->durable_file = state.head->durable_cursor.file;
    probe->durable_pos = state.head->durable_cursor.pos;
  }
  return false;
}

bool acquire_startup_epoch(StartupEpochProof *proof) {
  if (proof == nullptr) return true;
  *proof = {};
  if (!enabled()) return false;
  std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
  if (!g_runtime.initialized || g_runtime.publisher == nullptr ||
      g_runtime.startup_phase != StartupPhase::HEAD_PROBED) {
    g_runtime.last_error =
        "writer epoch acquisition did not follow the HEAD-first probe";
    return true;
  }
  const std::string writer_id = random_writer_id();
  if (writer_id.empty()) {
    g_runtime.last_error = "cannot generate remote writer id";
    return true;
  }
  PublishResult epoch = g_runtime.publisher->acquire_epoch(writer_id);
  if (!epoch.applied()) {
    g_runtime.last_error = "cannot acquire remote writer epoch: " + epoch.detail;
    update_status_locked(g_runtime.publisher->state().lifecycle);
    return true;
  }
  g_runtime.startup_phase = StartupPhase::EPOCH_ACQUIRED;
  g_runtime.last_error.clear();
  update_status_locked(g_runtime.startup_route == StartupRoute::TAKEOVER
                           ? LifecycleState::RECOVERING
                           : LifecycleState::INITIALIZING);
  const PublisherState &state = g_runtime.publisher->state();
  if (!state.epoch.has_value() || !state.epoch_object.has_value()) {
    g_runtime.last_error =
        "epoch acquisition did not retain its exact object proof";
    return true;
  }
  WriterEpoch parsed;
  std::string error;
  if (!parse_writer_epoch(state.epoch_object->body, g_runtime.stream, &parsed,
                          &error) ||
      parsed != *state.epoch) {
    g_runtime.last_error = "acquired epoch object is not canonical";
    if (!error.empty()) g_runtime.last_error.append(": ").append(error);
    return true;
  }
  proof->body = state.epoch_object->body;
  proof->etag = state.epoch_object->etag;
  proof->value = std::move(parsed);
  if (state.head.has_value() && state.head_object.has_value()) {
    proof->head_body = state.head_object->body;
    proof->head_etag = state.head_object->etag;
    proof->head_generation = state.head->generation;
  }
  g_runtime.startup_epoch_adopted = false;
  g_runtime.startup_epoch_adoption_role =
      StartupEpochAdoptionRole::TAKEOVER_RECOVERY;
  g_runtime.adopted_startup_proof.reset();
  return false;
}

bool adopt_startup_epoch(const StartupEpochProof &proof,
                         StartupEpochAdoptionRole role) {
  if (!enabled()) return false;
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  if (!g_runtime.initialized || g_runtime.publisher == nullptr ||
      g_runtime.startup_phase != StartupPhase::HEAD_PROBED ||
      g_runtime.running || g_runtime.shutting_down ||
      g_runtime.admission_open || !g_runtime.admissions.empty() ||
      g_runtime.clone_cut_phase != CloneCutBarrierPhase::NONE) {
    g_runtime.last_error =
        "epoch adoption requires a HEAD-first probe with CLOSED admission";
    return true;
  }

  const bool bootstrap_snapshot =
      role == StartupEpochAdoptionRole::BOOTSTRAP_SNAPSHOT;
  const bool takeover_role =
      role == StartupEpochAdoptionRole::TAKEOVER_RECOVERY ||
      role == StartupEpochAdoptionRole::INSTALLED_ROOT;
  if ((bootstrap_snapshot &&
       (g_runtime.startup_route != StartupRoute::BOOTSTRAP ||
        g_runtime.status.state != LifecycleState::INITIALIZING)) ||
      (takeover_role &&
       (g_runtime.startup_route != StartupRoute::TAKEOVER ||
        g_runtime.status.state != LifecycleState::RECOVERING))) {
    g_runtime.last_error = "epoch adoption role does not match the HEAD route";
    return true;
  }
  if (proof.body.empty() || proof.etag.empty() || proof.value.epoch == 0 ||
      (bootstrap_snapshot &&
       (!proof.head_body.empty() || !proof.head_etag.empty() ||
        proof.head_generation != 0 || g_runtime.probed_head_generation != 0)) ||
      (takeover_role &&
       (proof.head_body.empty() || proof.head_etag.empty() ||
        proof.head_generation == 0 ||
        proof.head_generation != g_runtime.probed_head_generation))) {
    g_runtime.last_error = "same-binary epoch adoption proof is incomplete";
    return true;
  }

  WriterEpoch expected_epoch;
  std::string error;
  if (!parse_writer_epoch(proof.body, g_runtime.stream, &expected_epoch,
                          &error) ||
      expected_epoch != proof.value) {
    g_runtime.last_error = "same-binary epoch adoption proof is not canonical";
    if (!error.empty()) g_runtime.last_error.append(": ").append(error);
    return true;
  }

  PublishResult adopted;
  if (bootstrap_snapshot) {
    adopted = g_runtime.publisher->adopt_epoch_without_head(
        PublishedBytes{proof.body, proof.etag}, proof.value);
  } else {
    Head expected_head;
    if (!parse_head(proof.head_body, g_runtime.stream, &expected_head, &error) ||
        expected_head.generation != proof.head_generation ||
        (role == StartupEpochAdoptionRole::INSTALLED_ROOT &&
         expected_head.writer !=
             Writer{proof.value.writer_id, proof.value.epoch})) {
      g_runtime.last_error =
          "same-binary HEAD adoption proof is not canonical for its role";
      if (!error.empty()) g_runtime.last_error.append(": ").append(error);
      return true;
    }
    adopted = g_runtime.publisher->adopt_epoch(
        PublishedBytes{proof.body, proof.etag}, proof.value,
        PublishedBytes{proof.head_body, proof.head_etag}, expected_head);
  }
  if (!adopted.applied()) {
    g_runtime.last_error = "cannot adopt exact startup epoch";
    if (!adopted.detail.empty())
      g_runtime.last_error.append(": ").append(adopted.detail);
    update_status_locked(g_runtime.publisher->state().lifecycle);
    return true;
  }

  g_runtime.startup_epoch_adopted = true;
  g_runtime.startup_epoch_adoption_role = role;
  g_runtime.adopted_startup_proof = proof;
  g_runtime.startup_phase = StartupPhase::EPOCH_ACQUIRED;
  g_runtime.last_error.clear();
  update_status_locked(bootstrap_snapshot ? LifecycleState::INITIALIZING
                                          : LifecycleState::RECOVERING);
  return false;
}

bool activate_installed_root(const InstalledRootActivationProof &proof) {
  if (!enabled()) return false;
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  if (!g_runtime.initialized || g_runtime.publisher == nullptr ||
      g_runtime.startup_phase != StartupPhase::EPOCH_ACQUIRED ||
      !g_runtime.startup_epoch_adopted ||
      g_runtime.startup_epoch_adoption_role !=
          StartupEpochAdoptionRole::INSTALLED_ROOT ||
      g_runtime.startup_route != StartupRoute::TAKEOVER ||
      g_runtime.running || !admission_closed_and_drained_locked()) {
    g_runtime.last_error =
        "installed root pre-recovery activation requires an exact adopted "
        "epoch with CLOSED admission";
    return true;
  }
  if (!proof.marker_matches || !proof.root_identity_matches) {
    g_runtime.last_error =
        "installed root marker or pre-recovery root identity differs";
    return true;
  }
  if (proof.head_body.empty() || proof.head_etag.empty() ||
      proof.recovered_file.empty() || proof.recovered_pos == 0) {
    g_runtime.last_error =
        "installed root pre-recovery proof has empty exact identifiers";
    return true;
  }

  std::string error;
  Head installed;
  if (!parse_head(proof.head_body, g_runtime.stream, &installed, &error)) {
    g_runtime.last_error = "installed root contains an invalid HEAD: " + error;
    return true;
  }
  if (installed.durable_cursor.file != proof.recovered_file ||
      installed.durable_cursor.pos != proof.recovered_pos) {
    g_runtime.last_error =
        "installed root cursor does not equal the installed HEAD";
    return true;
  }
  if (!g_runtime.adopted_startup_proof.has_value() ||
      proof.head_body != g_runtime.adopted_startup_proof->head_body ||
      proof.head_etag != g_runtime.adopted_startup_proof->head_etag ||
      installed.generation != g_runtime.probed_head_generation ||
      installed.generation !=
          g_runtime.adopted_startup_proof->head_generation) {
    g_runtime.last_error =
        "installed HEAD differs from the adopted restart proof";
    return true;
  }
  if (!installed_marker_binds_head(proof.marker, installed, proof.head_body,
                                   &error)) {
    g_runtime.last_error = "installed root marker verification failed";
    if (!error.empty()) g_runtime.last_error.append(": ").append(error);
    return true;
  }

  // The installed root is still in pre-recovery mode. Verify the exact
  // adopted objects without promoting the publisher to RUNNING; activation
  // remains read-only until post-engine verification completes.
  PublishResult verified =
      g_runtime.publisher->verify_adopted_head_read_only(installed);
  const PublisherState &state = g_runtime.publisher->state();
  if (!verified.applied() || !state.head_object.has_value() ||
      state.head_object->body != proof.head_body ||
      state.head_object->etag != proof.head_etag ||
      !exact_adopted_epoch_locked() || !exact_adopted_head_locked(false)) {
    if (verified.applied())
      g_runtime.publisher->fence(
          "installed root exact HEAD/epoch proof changed during activation");
    g_runtime.last_error =
        "installed root pre-recovery HEAD/epoch verification failed";
    if (!verified.detail.empty())
      g_runtime.last_error.append(": ").append(verified.detail);
    update_status_locked(g_runtime.publisher->state().lifecycle);
    return true;
  }

  g_runtime.installed_head = installed;
  g_runtime.installed_head_body = proof.head_body;
  g_runtime.installed_head_etag = proof.head_etag;
  g_runtime.installed_marker = proof.marker;
  g_runtime.local_flushed_cursor = installed.durable_cursor;
  g_runtime.public_committed_cursor = installed.durable_cursor;
  g_runtime.published_window_valid = true;
  g_runtime.published_window_generation = installed.generation;
  g_runtime.published_window = installed.recovery_window;
  g_runtime.startup_phase = StartupPhase::INSTALLED_REEXEC_PRE_RECOVERY;
  g_runtime.running = false;
  g_runtime.last_error.clear();
  update_status_locked(LifecycleState::RECOVERING);
  return false;
}

bool verify_installed_root_post_engine(const InstalledRootProof &proof) {
  if (!enabled()) return false;
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  if (!installed_reexec_pre_recovery_authorized_locked()) {
    g_runtime.last_error =
        "post-engine verification requires one installed-root pre-recovery "
        "activation with CLOSED admission";
    return true;
  }
  if (!proof.marker_matches || !proof.root_identity_matches ||
      !proof.snapshot_matches || !proof.server_uuid_matches ||
      !proof.configuration_matches || !proof.gtid_matches ||
      !proof.dd_matches || !proof.repository_empty ||
      !proof.extent_live_set_matches || !proof.internal_prepared_empty ||
      !proof.external_xa_empty) {
    g_runtime.last_error = "installed root post-engine proof is incomplete";
    return true;
  }
  if (!g_runtime.installed_head.has_value() ||
      proof.head_body != g_runtime.installed_head_body ||
      proof.head_etag != g_runtime.installed_head_etag ||
      proof.recovered_file !=
          g_runtime.installed_head->durable_cursor.file ||
      proof.recovered_pos != g_runtime.installed_head->durable_cursor.pos) {
    g_runtime.last_error =
        "installed root post-engine proof differs from pre-recovery activation";
    return true;
  }

  GtidSetDigest recovered_gtid;
  std::string error;
  if (!gtid_digest(proof.canonical_gtid, &recovered_gtid, &error) ||
      recovered_gtid.canonical != proof.canonical_gtid ||
      recovered_gtid.sha256 != proof.gtid_sha256) {
    g_runtime.last_error =
        "installed root proof has an invalid canonical GTID digest";
    if (!error.empty()) g_runtime.last_error.append(": ").append(error);
    return true;
  }

  Head installed;
  if (!parse_head(proof.head_body, g_runtime.stream, &installed, &error) ||
      installed != *g_runtime.installed_head) {
    g_runtime.last_error =
        "installed root post-engine HEAD is invalid or changed";
    if (!error.empty()) g_runtime.last_error.append(": ").append(error);
    return true;
  }

  PublishResult verified =
      g_runtime.publisher->activate_adopted_epoch(installed);
  const PublisherState &state = g_runtime.publisher->state();
  if (!verified.applied() || !state.head_object.has_value() ||
      state.head_object->body != proof.head_body ||
      state.head_object->etag != proof.head_etag ||
      !exact_adopted_epoch_locked() || !exact_adopted_head_locked(false)) {
    if (verified.applied())
      g_runtime.publisher->fence(
          "installed root exact HEAD/epoch proof changed after engine open");
    g_runtime.last_error =
        "installed root post-engine HEAD/epoch verification failed";
    if (!verified.detail.empty())
      g_runtime.last_error.append(": ").append(verified.detail);
    update_status_locked(g_runtime.publisher->state().lifecycle);
    return true;
  }

  g_runtime.startup_phase = StartupPhase::ROOT_VERIFIED;
  g_runtime.running = false;
  g_runtime.last_error.clear();
  update_status_locked(LifecycleState::RECOVERING);
  return false;
}

std::string startup_error() {
  std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
  return g_runtime.last_error;
}

bool may_run_startup_recovery_worker() {
  if (!enabled()) return false;
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  return takeover_recovery_worker_authorized_locked();
}

bool may_run_startup_bootstrap_worker() {
  if (!enabled()) return false;
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  return bootstrap_preflight_worker_authorized_locked();
}

bool may_initialize_empty_root() {
  if (!enabled() || !opt_initialize) return false;
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  return g_runtime.bootstrap_preflight &&
         bootstrap_preflight_worker_authorized_locked();
}

bool may_initialize_system_tables(const THD *thd) {
  if (thd == nullptr) return false;
  if (opt_initialize) {
    return (thd->system_thread == SYSTEM_THREAD_DD_INITIALIZE ||
            thd->system_thread == SYSTEM_THREAD_SERVER_INITIALIZE) &&
           may_initialize_empty_root();
  }
  // MySQL also uses DD_INITIALIZE for compiled dictionary restart work.
  // The first snapshot will cover this work in the unpublished empty root.
  return thd->system_thread == SYSTEM_THREAD_DD_INITIALIZE &&
         may_run_startup_bootstrap_snapshot_worker();
}

bool may_rebuild_startup_dictionary_cache(const THD *thd) {
  if (!enabled() || opt_initialize || thd == nullptr ||
      thd->system_thread != SYSTEM_THREAD_DD_INITIALIZE ||
      thd->lex == nullptr)
    return false;
  const auto &context = dd::bootstrap::DD_bootstrap_ctx::instance();
  if (!context.is_restart()) return false;
  if (thd->lex->sql_command == SQLCOM_FLUSH) {
    if (context.get_stage() != dd::bootstrap::Stage::SYNCED ||
        thd->lex->type != REFRESH_TABLES ||
        thd->lex->query_tables != nullptr || thd->lex->no_write_to_binlog)
      return false;
  } else if (context.get_stage() != dd::bootstrap::Stage::FETCHED_PROPERTIES) {
    return false;
  } else if (thd->lex->sql_command == SQLCOM_CREATE_TABLE) {
    const Table_ref *table = thd->lex->query_tables;
    const dd::Dictionary *dictionary = dd::get_dictionary();
    if (thd->lex->create_info == nullptr ||
        (thd->lex->create_info->options & HA_LEX_CREATE_TMP_TABLE) != 0 ||
        dictionary == nullptr || table == nullptr ||
        table->next_global != nullptr || table->db == nullptr ||
        table->table_name == nullptr ||
        !dictionary->is_dd_table_name(table->db, table->table_name))
      return false;
  } else if (thd->lex->sql_command == SQLCOM_SET_OPTION) {
    if (thd->lex->query_tables != nullptr || thd->lex->var_list.elements != 1)
      return false;
    const auto *assignment =
        dynamic_cast<const set_var *>(thd->lex->var_list.head());
    if (assignment == nullptr ||
        (assignment->type != OPT_DEFAULT && assignment->type != OPT_SESSION) ||
        std::string_view(assignment->m_var_tracker.get_var_name()) !=
            "foreign_key_checks" ||
        assignment->value == nullptr ||
        assignment->value->type() != Item::INT_ITEM)
      return false;
    const longlong value = assignment->value->val_int();
    if (value != 0 && value != 1) return false;
  } else {
    return false;
  }

  // mysql_create_table_no_lock uses no_ha_table for these registered names;
  // Storage_adapter::store only core_store()s before CREATED_TABLES.
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  return takeover_recovery_worker_authorized_locked() ||
         installed_reexec_pre_recovery_authorized_locked();
}

static bool may_validate_restored_dictionary_stage(
    const THD *thd, dd::bootstrap::Stage stage,
    enum_thread_type thread_type = SYSTEM_THREAD_DD_INITIALIZE) {
  if (!enabled() || opt_initialize || thd == nullptr ||
      thd->system_thread != thread_type)
    return false;
  const auto &context = dd::bootstrap::DD_bootstrap_ctx::instance();
  if (context.get_stage() != stage || !context.is_restart())
    return false;
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  return takeover_recovery_worker_authorized_locked() ||
         installed_reexec_pre_recovery_authorized_locked();
}

bool may_validate_startup_dictionary_contents(const THD *thd) {
  return may_validate_restored_dictionary_stage(thd,
                                               dd::bootstrap::Stage::SYNCED);
}

bool may_validate_startup_resource_groups(const THD *thd) {
  return may_validate_restored_dictionary_stage(thd,
                                               dd::bootstrap::Stage::FINISHED);
}

Scoped_startup_pfs_restore::Scoped_startup_pfs_restore(
    THD *thd, const Plugin_table *definition) {
  if (!may_validate_restored_dictionary_stage(
          thd, dd::bootstrap::Stage::FINISHED, SYSTEM_THREAD_BACKGROUND))
    return;
  if (g_pfs_restore_thd != nullptr || definition == nullptr ||
      definition->get_schema_name() == nullptr ||
      definition->get_name() == nullptr ||
      definition->get_table_definition() == nullptr ||
      definition->get_table_options() == nullptr ||
      std::string_view(definition->get_schema_name()) != "performance_schema" ||
      std::string_view(definition->get_name()) != "innodb_redo_log_files")
    fail_stop("invalid compiled startup PFS restore scope");
  m_thd = thd;
  g_pfs_restore_thd = thd;
  g_pfs_restore_definition = definition;
}

Scoped_startup_pfs_restore::~Scoped_startup_pfs_restore() {
  if (m_thd != nullptr) {
    g_pfs_restore_thd = nullptr;
    g_pfs_restore_definition = nullptr;
  }
}

bool startup_pfs_restore_active(const THD *thd) {
  return thd != nullptr && thd == g_pfs_restore_thd;
}

bool validate_startup_pfs_table(THD *thd, const Plugin_table *definition) {
  const auto reject = [](const char *reason) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, reason);
    return true;
  };
  if (!startup_pfs_restore_active(thd) || definition == nullptr ||
      definition->get_name() == nullptr ||
      definition->get_schema_name() == nullptr ||
      definition->get_table_definition() == nullptr ||
      definition->get_table_options() == nullptr ||
      g_pfs_restore_definition == nullptr ||
      !may_validate_restored_dictionary_stage(
          thd, dd::bootstrap::Stage::FINISHED, SYSTEM_THREAD_BACKGROUND))
    return reject("startup PFS validation is unauthorized");
  const Plugin_table &compiled = *g_pfs_restore_definition;
  if (definition->get_ddl() != compiled.get_ddl() ||
      compiled.get_table_options() == nullptr ||
      std::string_view(compiled.get_table_options()) !=
          "engine = 'performance_schema'" ||
      compiled.get_tablespace_name() != nullptr)
    return reject("startup PFS registration differs from compiled definition");

  dd::Schema_MDL_locker schema_lock(thd);
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  const dd::Schema *schema = nullptr;
  const dd::Table *stored = nullptr;
  if (schema_lock.ensure_locked(compiled.get_schema_name()) ||
      dd::acquire_exclusive_table_mdl(thd, compiled.get_schema_name(),
                                      compiled.get_name(), false, nullptr) ||
      thd->dd_client()->acquire(compiled.get_schema_name(), &schema) ||
      thd->dd_client()->acquire(compiled.get_schema_name(), compiled.get_name(),
                                 &stored) ||
      schema == nullptr || stored == nullptr || !stored->triggers().empty())
    return reject("restored startup PFS table cannot be read or has triggers");

  // Parse and prepare the compiled DDL, but never execute or persist it.
  LEX lex;
  LEX *saved_lex = thd->lex;
  auto *saved_partition = thd->work_part_info;
  thd->lex = &lex;
  lex_start(thd);
  auto restore_lex = create_scope_guard([&] {
    lex_end(&lex);
    thd->lex = saved_lex;
    thd->work_part_info = saved_partition;
  });
  const auto ddl = compiled.get_ddl();
  Parser_state parser;
  if (parser.init(thd, ddl.data(), ddl.size()) ||
      parse_sql(thd, &parser, nullptr) ||
      lex.sql_command != SQLCOM_CREATE_TABLE || lex.create_info == nullptr ||
      lex.alter_info == nullptr || thd->work_part_info != nullptr)
    return reject("cannot parse compiled startup PFS definition");
  auto *info = lex.create_info;
  if (info->db_type == nullptr ||
      get_default_db_collation(*schema, &info->default_table_charset))
    return reject("cannot resolve compiled startup PFS engine or collation");
  std::unique_ptr<handler, Destroy_only<handler>> file(
      get_new_handler(nullptr, false, thd->mem_root, info->db_type));
  KEY *keys = nullptr;
  uint key_count = 0;
  FOREIGN_KEY *foreign_keys = nullptr;
  uint foreign_key_count = 0;
  if (!file || mysql_prepare_create_table(
                   thd, compiled.get_schema_name(), compiled.get_name(), info,
                   lex.alter_info, file.get(), false, &keys, &key_count,
                   &foreign_keys, &foreign_key_count, nullptr, 0, nullptr, 0,
                   0, false))
    return reject("cannot prepare compiled startup PFS definition");
  auto expected = dd::create_dd_user_table(
      thd, *schema, compiled.get_name(), info, lex.alter_info->create_list,
      keys, key_count, lex.alter_info->keys_onoff, foreign_keys,
      foreign_key_count, &lex.alter_info->check_constraint_spec_list, file.get());
  if (!expected) return reject("cannot describe compiled startup PFS table");
  if (!startup_pfs_table_definition_matches(thd, stored, expected.get(),
                                           schema->name()))
    return reject("restored startup PFS definition differs from compiled server");
  if (!may_validate_restored_dictionary_stage(
          thd, dd::bootstrap::Stage::FINISHED, SYSTEM_THREAD_BACKGROUND))
    return reject("startup PFS validation authority changed");
  return false;
}

bool validate_startup_dictionary_contents(THD *thd, std::string *error) {
  if (!may_validate_startup_dictionary_contents(thd)) {
    if (error != nullptr) *error = "startup dictionary validation is unauthorized";
    return true;
  }
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  std::vector<const dd::Charset *> charsets;
  std::vector<const dd::Collation *> collations;
  if (thd->dd_client()->fetch_global_components(&charsets) ||
      thd->dd_client()->fetch_global_components(&collations)) {
    if (error != nullptr) *error = "cannot read snapshot dictionary character sets";
    return true;
  }
  if (!startup_character_sets_match(charsets, collations, all_charsets)) {
    if (error != nullptr)
      *error = "snapshot dictionary character sets differ from compiled server";
    return true;
  }
  if (!may_validate_startup_dictionary_contents(thd)) {
    if (error != nullptr) *error = "startup dictionary validation authority changed";
    return true;
  }
  return false;
}

Scoped_startup_pfs_initialization::Scoped_startup_pfs_initialization(THD *thd) {
  if (thd != nullptr && thd->system_thread == SYSTEM_THREAD_BACKGROUND &&
      !opt_initialize && may_run_startup_bootstrap_snapshot_worker()) {
    m_thd = thd;
    m_thd->system_thread = SYSTEM_THREAD_DD_INITIALIZE;
  }
}

Scoped_startup_pfs_initialization::~Scoped_startup_pfs_initialization() {
  if (m_thd != nullptr) m_thd->system_thread = SYSTEM_THREAD_BACKGROUND;
}

bool may_run_startup_bootstrap_snapshot_worker() {
  if (!enabled()) return false;
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  return bootstrap_snapshot_worker_authorized_locked();
}

bool may_collect_bootstrap_root_evidence(const THD *thd, bool installed) {
  if (!enabled() || opt_initialize || thd == nullptr ||
      thd->system_thread != SYSTEM_THREAD_BACKGROUND)
    return false;
  const auto &context = dd::bootstrap::DD_bootstrap_ctx::instance();
  if (context.get_stage() != dd::bootstrap::Stage::FINISHED ||
      !context.is_restart())
    return false;
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  return installed ? installed_reexec_pre_recovery_authorized_locked()
                   : bootstrap_snapshot_worker_authorized_locked();
}

bool startup_existing_binlog_boundary(std::string *file, uint64_t *pos) {
  if (file == nullptr || pos == nullptr) return false;
  file->clear();
  *pos = 0;
  if (!enabled()) return false;
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  if (!takeover_recovery_worker_authorized_locked() &&
      !installed_reexec_pre_recovery_authorized_locked())
    return false;
  const PublisherState &state = g_runtime.publisher->state();
  if (!state.head.has_value()) return false;
  *file = state.head->durable_cursor.file;
  *pos = state.head->durable_cursor.pos;
  return true;
}

bool may_read_installed_terminal_binlog_gtids() {
  if (!enabled()) return false;
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  return installed_reexec_pre_recovery_authorized_locked();
}

bool restore_recovery_snapshot_gtids(const RecoveryPlan &candidate,
                                     std::string *error) {
  if (!enabled() || opt_initialize || global_tsid_lock == nullptr ||
      global_tsid_map == nullptr || gtid_state == nullptr)
    return fail_with(error, "recovery snapshot GTID state is unavailable");
  THD *const thd = current_thd;
  if (thd != nullptr &&
      (thd->system_thread != SYSTEM_THREAD_BACKGROUND ||
       !thd->owned_gtid_is_empty() ||
       thd->get_transaction()->is_active(Transaction_ctx::STMT) ||
       thd->get_transaction()->is_active(Transaction_ctx::SESSION)))
    return fail_with(error, "recovery snapshot GTID restore has an active THD");

  global_tsid_lock->wrlock();
  auto unlock_gtids = create_scope_guard([] { global_tsid_lock->unlock(); });
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  if (!takeover_recovery_worker_authorized_locked() ||
      g_runtime.recovery_snapshot_gtids_restored ||
      !g_runtime.authorizations.empty() ||
      !gtid_state->get_owned_gtids()->is_empty() ||
      gtid_state->get_anonymous_ownership_count() != 0)
    return fail_with(error, "recovery snapshot GTID restore is not authorized");

  const PublisherState &state = g_runtime.publisher->state();
  if (candidate.head_object.body != state.head_object->body ||
      candidate.head_object.etag != state.head_object->etag ||
      candidate.head != *state.head)
    return fail_with(error, "recovery snapshot GTID HEAD differs from authority");
  const SnapshotRef &reference = state.head->snapshot;
  std::string snapshot_sha;
  SnapshotManifest snapshot;
  if (candidate.snapshot_object.body.size() != reference.manifest_size ||
      !sha256_hex(candidate.snapshot_object.body, &snapshot_sha, error) ||
      snapshot_sha != reference.manifest_sha256 ||
      !parse_snapshot_manifest(candidate.snapshot_object.body, g_runtime.stream,
                               reference.manifest_key, &snapshot, error) ||
      snapshot != candidate.snapshot || snapshot.snapshot_id != reference.id ||
      snapshot.cursor != reference.cursor)
    return fail_with(error, "recovery snapshot GTID manifest differs from HEAD");

  Gtid_set baseline(global_tsid_map, global_tsid_lock);
  if (baseline.add_gtid_text(snapshot.gtid_executed.canonical.c_str()) !=
          RETURN_STATUS_OK ||
      gtid_state->ensure_sidno() != RETURN_STATUS_OK)
    return fail_with(error, "cannot allocate recovery snapshot GTID baseline");

  // Stock startup can read pending tails from earlier materialized files.
  // These are not applied yet. Reset only worker memory; Gtid_state::clear
  // would also mutate the derived mysql.gtid_executed table.
  g_runtime.recovery_snapshot_gtids_restored = true;
  auto *executed = const_cast<Gtid_set *>(gtid_state->get_executed_gtids());
  executed->clear();
  const_cast<Gtid_set *>(gtid_state->get_lost_gtids())->clear();
  const_cast<Gtid_set *>(gtid_state->get_gtids_only_in_table())->clear();
  auto *previous =
      const_cast<Gtid_set *>(gtid_state->get_previous_gtids_logged());
  previous->clear();
  if (executed->add_gtid_set(&baseline) != RETURN_STATUS_OK ||
      previous->add_gtid_set(&baseline) != RETURN_STATUS_OK)
    return fail_with(error, "cannot restore recovery snapshot GTID baseline");
  return true;
}

bool may_bypass_stock_binlog_recovery() {
  if (!enabled()) return false;
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  return installed_reexec_pre_recovery_authorized_locked() ||
         bootstrap_snapshot_worker_authorized_locked() ||
         takeover_recovery_worker_authorized_locked();
}

void shutdown() {
  {
    std::unique_lock<std::mutex> guard(g_runtime.admission_mutex);
    g_runtime.shutdown_draining = true;
    g_runtime.admission_open = false;
    g_runtime.admission_cond.notify_all();
    guard.unlock();
    release_snapshot_order_ticket(0);
    guard.lock();
    g_runtime.admission_cond.wait(guard, [] {
      return g_runtime.admissions.empty() &&
             g_runtime.clone_cut_phase == CloneCutBarrierPhase::NONE &&
             g_runtime.clone_cut_pins_releasing == 0 &&
             !g_runtime.admission_reopen_in_progress;
    });
    g_runtime.shutting_down = true;
    g_runtime.admission_cond.notify_all();
  }
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    g_runtime.running = false;
    if (g_runtime.startup_phase != StartupPhase::UNINITIALIZED)
      g_runtime.startup_phase = StartupPhase::SHUTTING_DOWN;
  }
}

void deinitialize() {
  objstore::ObjectStore *object_store = nullptr;
  bool object_store_owned = false;
  bool provider_initialized = false;
  std::string provider;
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    g_runtime.sealer.reset();
    g_runtime.publisher.reset();
    g_runtime.store.reset();
    g_runtime.metadata_io = nullptr;
    g_runtime.io.reset();
    object_store = g_runtime.object_store;
    object_store_owned = g_runtime.object_store_owned;
    provider_initialized = g_runtime.object_store_provider_initialized;
    provider = std::move(g_runtime.object_store_provider);
    g_runtime.object_store = nullptr;
    g_runtime.object_store_owned = false;
    g_runtime.object_store_provider_initialized = false;
    g_runtime.initialized = false;
    g_runtime.bootstrap_preflight = false;
    g_runtime.running = false;
    g_runtime.startup_phase = StartupPhase::UNINITIALIZED;
    g_runtime.startup_route = StartupRoute::DISABLED;
    g_runtime.startup_epoch_adopted = false;
    g_runtime.recovery_snapshot_gtids_restored = false;
    g_runtime.startup_epoch_adoption_role =
        StartupEpochAdoptionRole::TAKEOVER_RECOVERY;
    g_runtime.adopted_startup_proof.reset();
    g_runtime.probed_head_generation = 0;
    g_runtime.cluster_object_prefix.clear();
    g_runtime.bucket.clear();
    g_runtime.stream = {};
    g_runtime.installed_head.reset();
    g_runtime.installed_head_body.clear();
    g_runtime.installed_head_etag.clear();
    g_runtime.installed_marker.reset();
    g_runtime.local_flushed_cursor = {};
    g_runtime.public_committed_cursor = {};
    g_runtime.authorizations.clear();
    g_runtime.status = {};
    g_runtime.last_error.clear();
  }
  {
    std::lock_guard<std::mutex> clone_guard(g_runtime.clone_cut_mutex);
    std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
    g_runtime.shutdown_draining = false;
    g_runtime.shutting_down = false;
    g_runtime.admission_open = false;
    g_runtime.admission_reopen_in_progress = false;
    reset_admission_accounting_locked();
    if (g_runtime.clone_cut_pins_releasing != 0)
      fail_stop_holding_admission(
          "remote deinitialize raced a clone cut binlog pin release");
    g_runtime.clone_cut_phase = CloneCutBarrierPhase::NONE;
    g_runtime.clone_cut_token = 0;
    g_runtime.active_clone_cut.reset();
#ifdef WESQL_TEST
    g_runtime.clone_cut_source_for_test.reset();
    g_runtime.clone_cut_public_cursor_for_test.reset();
#endif
  }
  if (object_store_owned && object_store != nullptr)
    objstore::destroy_object_store(object_store);
  if (provider_initialized) objstore::cleanup_objstore_provider(provider);
}

bool read_public_binlog_cursor(Cursor *cursor) {
  if (cursor == nullptr || !enabled()) return false;
  std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
  if (g_runtime.public_committed_cursor.file.empty() ||
      g_runtime.public_committed_cursor.pos < BIN_LOG_HEADER_SIZE)
    return false;
  *cursor = g_runtime.public_committed_cursor;
  return true;
}

std::string status_json() {
  std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
  std::string json;
  std::string error;
  if (!format_status_json(g_runtime.status, &json, &error)) return {};
  return json;
}

[[noreturn]] void fail_stop(const char *reason) {
  const char *message = reason != nullptr ? reason : "remote commit invariant failure";
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    g_runtime.last_error = message;
    g_runtime.running = false;
    update_status_locked(LifecycleState::FENCED);
  }
  {
    std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
    g_runtime.admission_open = false;
    g_runtime.admission_cond.notify_all();
  }
  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, message);
  std::abort();
}

[[noreturn]] void fence(const char *reason) { fail_stop(reason); }

void note_local_flush(const char *file, uint64_t pos) {
  if (!enabled()) return;
  if (file == nullptr || *file == '\0' || pos == 0)
    fail_stop("invalid private local binlog cursor");
  const Cursor next{file, pos};
  bool backwards = false;
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    backwards = !g_runtime.local_flushed_cursor.file.empty() &&
                cursor_less(next, g_runtime.local_flushed_cursor);
    if (!backwards) g_runtime.local_flushed_cursor = next;
  }
  if (backwards) fail_stop("private local binlog cursor moved backwards");
}

OrderToken *decide_group(MYSQL_BIN_LOG *binlog, THD *final_queue) {
  if (!enabled()) return nullptr;
  std::unique_ptr<OrderToken> token = std::make_unique<OrderToken>();
  token->ticket = acquire_ticket();

  bool runtime_ready = false;
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    runtime_ready = g_runtime.initialized && g_runtime.running &&
                    g_runtime.publisher != nullptr &&
                    g_runtime.sealer != nullptr;
  }
  if (!runtime_ready) fail_stop("remote commit runtime is not RUNNING");

  const Cursor endpoint = group_endpoint(final_queue);
  const PublisherState &prior_state = g_runtime.publisher->state();
  if (!prior_state.head.has_value())
    fail_stop("remote LOG group has no bootstrap HEAD");
  bool cursor_covers_endpoint = false;
  bool public_cursor_matches_prior_head = false;
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    cursor_covers_endpoint =
        compare_cursor(g_runtime.local_flushed_cursor, endpoint) >= 0;
    token->pre_ack_public_cursor = g_runtime.public_committed_cursor;
    public_cursor_matches_prior_head =
        token->pre_ack_public_cursor == prior_state.head->durable_cursor;
  }
  if (!cursor_covers_endpoint)
    fail_stop("private local cursor does not cover commit group endpoint");
  if (!public_cursor_matches_prior_head)
    fail_stop("public cursor does not equal the prior durable HEAD cursor");

  std::vector<NativeBinlogRange> ranges;
  std::vector<uint64_t> group_xids;
  std::string group_gtids;
  uint64_t transaction_count = 0;
  std::string error;
  if (!build_native_ranges(binlog, prior_state.head->durable_cursor, endpoint,
                           &ranges, &group_xids, &group_gtids,
                           &transaction_count, &error))
    fail_stop(error.c_str());

  const Writer writer = local_writer(*g_runtime.publisher);
  uint64_t first_sequence = 1;
  if (prior_state.head->segment_tip.kind == SegmentTipKind::SEGMENT) {
    if (!prior_state.head->segment_tip.sequence.has_value() ||
        *prior_state.head->segment_tip.sequence >= kJsonSafeIntegerMax)
      fail_stop("remote segment sequence is exhausted");
    first_sequence = *prior_state.head->segment_tip.sequence + 1;
  }
  SealedSegments sealed;
  production_fault_point("remote_commit_crash_before_segment_put");
  production_fault_point("remote_commit_writer_fencing");
  PublishResult seal = g_runtime.sealer->seal(
      writer, first_sequence, prior_state.head->segment_tip, ranges, &sealed);
  if (!seal.applied()) fatal_publish("native segment seal", seal);
  production_fault_point(
      "remote_commit_crash_after_segment_put_before_head_cas");

  const HeadParent parent = exact_parent(*g_runtime.publisher);
  TransitionManifest manifest;
  manifest.kind = ManifestKind::LOG_TRANSITION;
  if (prior_state.head->generation >= kJsonSafeIntegerMax)
    fail_stop("remote HEAD generation is exhausted");
  manifest.generation = prior_state.head->generation + 1;
  manifest.writer = writer;
  manifest.head_parent = parent;
  manifest.previous = ManifestRef{prior_state.head->generation,
                                  prior_state.head->manifest.key,
                                  prior_state.head->manifest.size,
                                  prior_state.head->manifest.sha256};
  manifest.recovery_window = prior_state.head->recovery_window;
  if (manifest.recovery_window.manifest_count == kJsonSafeIntegerMax ||
      sealed.segments.size() >
          kJsonSafeIntegerMax - manifest.recovery_window.segment_count)
    fail_stop("remote recovery window is exhausted");
  ++manifest.recovery_window.manifest_count;
  manifest.recovery_window.segment_count += sealed.segments.size();
  manifest.segment_tip = sealed.tip;
  manifest.snapshot = prior_state.head->snapshot;
  manifest.base_cursor = prior_state.head->base_cursor;
  manifest.durable_cursor = sealed.durable_cursor;
  manifest.segments = sealed.segments;

  std::string manifest_body;
  if (!stabilize_transition_manifest(
          g_runtime.stream, prior_state.head->recovery_window.manifest_bytes,
          &manifest, &manifest_body, &error))
    fail_stop(error.c_str());
  std::string manifest_sha;
  std::string manifest_key;
  if (!sha256_hex(manifest_body, &manifest_sha, &error) ||
      !transition_manifest_key(g_runtime.stream, writer, manifest.generation,
                               manifest_sha, &manifest_key, &error))
    fail_stop(error.c_str());

  Head intended;
  intended.generation = manifest.generation;
  intended.writer = writer;
  intended.parent = parent;
  intended.manifest =
      ObjectRef{manifest_key, manifest_body.size(), manifest_sha};
  intended.recovery_window = manifest.recovery_window;
  intended.segment_tip = manifest.segment_tip;
  intended.base_cursor = manifest.base_cursor;
  intended.durable_cursor = manifest.durable_cursor;
  intended.snapshot = manifest.snapshot;

  PublishResult publish = g_runtime.publisher->publish(manifest, intended);
  if (!publish.applied()) fatal_publish("LOG manifest/HEAD publication", publish);
  production_fault_point("remote_commit_pause_after_head_before_visibility");

  bool post_publish_visibility_exact = false;
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    const PublisherState &state = g_runtime.publisher->state();
    if (g_runtime.public_committed_cursor == token->pre_ack_public_cursor &&
        state.head.has_value() && *state.head == intended) {
      update_status_locked(LifecycleState::RUNNING);
      post_publish_visibility_exact =
          g_runtime.status.head.has_value() &&
          g_runtime.status.head->generation == intended.generation &&
          g_runtime.status.durable_cursor.has_value() &&
          *g_runtime.status.durable_cursor == intended.durable_cursor &&
          g_runtime.public_committed_cursor == token->pre_ack_public_cursor;
    }
  }
  if (!post_publish_visibility_exact)
    fail_stop("post-HEAD status or pre-ACK public cursor is inconsistent");

  GtidSetDigest group_gtid_digest;
  XidDigest group_xid_digest;
  if (!gtid_digest(group_gtids, &group_gtid_digest, &error) ||
      !xid_digest(group_xids, &group_xid_digest, &error))
    fail_stop(error.c_str());
  std::string head_body_sha;
  std::string head_etag_sha;
  std::string segment_refs_sha;
  const PublisherState &published_state = g_runtime.publisher->state();
  if (!published_state.head_object.has_value() ||
      !sha256_hex(published_state.head_object->body, &head_body_sha, &error) ||
      !sha256_hex(published_state.head_object->etag, &head_etag_sha, &error) ||
      !segment_refs_digest(sealed.segments, &segment_refs_sha, &error))
    fail_stop(error.c_str());

  CommitBinding binding{g_runtime.stream.stream_id,
                        intended.generation,
                        head_body_sha,
                        endpoint.file,
                        endpoint.pos,
                        group_gtid_digest.sha256,
                        group_xid_digest.sha256};
  install_authorizations(final_queue, binding);
  token->binding = binding;

  token->head = intended;
  std::unordered_set<THD *> unique_members;
  token->manifest = manifest;
  for (THD *member = final_queue; member != nullptr;
       member = member->next_to_commit) {
    if (!unique_members.insert(member).second)
      fail_stop("remote commit group contains a duplicate THD");
    token->members.push_back(member);
  }
  token->ack = AckReadyEvent{g_runtime.stream.stream_id,
                             writer,
                             endpoint,
                             transaction_count,
                             group_gtid_digest.sha256,
                             group_xid_digest.sha256,
                             intended.generation,
                             head_etag_sha,
                             head_body_sha,
                             intended.manifest,
                             sealed.segments.size(),
                             segment_refs_sha,
                             sealed.segments.back()};
  return token.release();
}

void check_engine_commits(THD *commit_queue, OrderToken *token) {
  if (!enabled()) return;
  if (token == nullptr || token->engine_checked ||
      !queue_matches_token(commit_queue, *token))
    fail_stop("engine commit queue does not match its remote order token");
  const char *failure = nullptr;
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    for (THD *head = commit_queue; head != nullptr;
         head = head->next_to_commit) {
      if (head->commit_error != THD::CE_NONE) {
        failure = "engine commit failed after remote HEAD decision";
        break;
      }
    }
    if (failure == nullptr)
      failure = authorization_integrity_failure(*token, true);
  }
  if (failure != nullptr) fail_stop(failure);
  token->engine_checked = true;
}

void verify_before_ack(THD *final_queue, OrderToken *token) {
  if (!enabled()) return;
  if (token == nullptr || !token->engine_checked ||
      token->ownership_verified ||
      !queue_matches_token(final_queue, *token))
    fail_stop("invalid final remote ownership verification state");
  const char *failure = nullptr;
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    failure = authorization_integrity_failure(*token, true);
    if (failure == nullptr &&
        (g_runtime.startup_phase != StartupPhase::ACTIVE ||
         !g_runtime.running))
      failure = "remote final verification occurred outside ACTIVE state";
  }
  if (failure != nullptr) fail_stop(failure);
  PublishResult verify = g_runtime.publisher->verify_decision(token->head);
  if (!verify.applied()) fatal_publish("final HEAD/epoch verification", verify);
  token->ownership_verified = true;
}

void emit_ack_and_publish_cursor(MYSQL_BIN_LOG *binlog, THD *final_queue,
                                 OrderToken *token) {
  if (!enabled()) return;
  if (binlog == nullptr || token == nullptr || !token->engine_checked ||
      !token->ownership_verified || token->ack_emitted ||
      !queue_matches_token(final_queue, *token))
    fail_stop("invalid remote ACK publication state");
  std::string line;
  std::string error;
  if (!format_ack_ready_log_line(token->ack, &line, &error))
    fail_stop(error.c_str());

  bool cursor_reserved = false;
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    const char *authorization_failure =
        authorization_integrity_failure(*token, true);
    if (authorization_failure != nullptr)
      error = authorization_failure;
    else
      cursor_reserved = reserve_ack_public_cursor_locked(
          token->head, token->ack, token->binding,
          token->pre_ack_public_cursor, &error);
  }
  if (!cursor_reserved) fail_stop(error.c_str());

  LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, line.c_str());
  binlog->update_binlog_end_pos(token->ack.endpoint.file.c_str(),
                               token->ack.endpoint.pos);
  token->ack_emitted = true;
}

void release_order_token(OrderToken *token) {
  if (token == nullptr) return;
  if (!token->engine_checked || !token->ownership_verified ||
      !token->ack_emitted)
    fail_stop("remote order token released before ACK publication");
  const char *failure = nullptr;
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    failure = authorization_integrity_failure(*token, true);
    if (failure == nullptr) {
      for (THD *member : token->members)
        g_runtime.authorizations.erase(member);
    }
  }
  if (failure != nullptr) fail_stop(failure);
  const uint64_t ticket = token->ticket;
  const uint64_t generation = token->head.generation;
  const RecoveryWindow published_window = token->head.recovery_window;
  delete token;
  release_ticket(ticket);
  refresh_published_window_after_order_release(generation, published_window, 0);
}

void check_commit_authorization(THD *thd, bool all) {
  if (!enabled()) return;
  if (thd == nullptr) fail_stop("null THD at remote commit guard");
  if (may_initialize_system_tables(thd)) return;
  Transaction_ctx *transaction = thd->get_transaction();
  const auto scope = all ? Transaction_ctx::SESSION : Transaction_ctx::STMT;
  const bool real = all || !transaction->is_active(Transaction_ctx::SESSION);
  if (!real || transaction->rw_ha_count(scope) == 0) return;
  bool authorized = false;
  std::string error;
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    const auto found = g_runtime.authorizations.find(thd);
    if (found != g_runtime.authorizations.end() && found->second.required &&
        !found->second.authorization.consumed()) {
      const AuthorizationKind kind = found->second.authorization.kind();
      authorized =
          kind == AuthorizationKind::GROUP ||
          (kind == AuthorizationKind::RECOVERY &&
           recovery_runtime_matches_binding_locked(found->second.binding,
                                                   &error));
    }
  }
  if (!authorized) {
    if (error.empty())
      error = "durable engine commit lacks a live remote authorization";
    fail_stop(error.c_str());
  }
}

void consume_commit_authorization(THD *thd, bool, bool is_real_trans,
                                  bool has_read_write_engine) {
  if (!enabled() || !is_real_trans || !has_read_write_engine) return;
  if (may_initialize_system_tables(thd)) return;
  std::string error;
  bool authorized = false;
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    const auto found = g_runtime.authorizations.find(thd);
    if (found != g_runtime.authorizations.end() && found->second.required) {
      const AuthorizationKind kind = found->second.authorization.kind();
      if (kind == AuthorizationKind::GROUP) {
        authorized = found->second.authorization.consume(
            AuthorizationKind::GROUP, &found->second.binding, &error);
      } else if (kind == AuthorizationKind::RECOVERY &&
                 recovery_runtime_matches_binding_locked(found->second.binding,
                                                         &error)) {
        authorized = found->second.authorization.consume(
            AuthorizationKind::RECOVERY, &found->second.binding, &error);
      }
    }
  }
  if (!authorized) {
    if (error.empty())
      error = "ha_commit_low reached an unauthorized durable engine commit";
    fail_stop(error.c_str());
  }
}

bool install_recovery_commit_authorization(THD *thd,
                                           const CommitBinding &binding,
                                           std::string *error) {
  if (error != nullptr) error->clear();
  if (!enabled()) {
    if (error != nullptr)
      *error = "recovery authorization requires remote commit mode";
    return true;
  }
  if (thd == nullptr) {
    if (error != nullptr) *error = "null THD at recovery authorization";
    return true;
  }

  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  if (terminal_snapshot_failure_locked() || g_runtime.shutdown_draining ||
      g_runtime.shutting_down ||
      g_runtime.admission_open || !g_runtime.admissions.empty() ||
      g_runtime.clone_cut_phase != CloneCutBarrierPhase::NONE) {
    if (error != nullptr)
      *error = "recovery authorization requires CLOSED, drained admission";
    return true;
  }
  if (!recovery_runtime_matches_binding_locked(binding, error)) return true;
  if (g_runtime.authorizations.contains(thd)) {
    if (error != nullptr)
      *error = "recovery THD already has a remote authorization";
    return true;
  }
  g_runtime.authorizations.emplace(
      thd, AuthorizationRecord{CommitAuthorization::recovery(binding), binding,
                               true});
  return false;
}

bool finish_recovery_commit_authorization(THD *thd, bool require_consumed,
                                          std::string *error) {
  if (error != nullptr) error->clear();
  if (thd == nullptr) {
    if (error != nullptr) *error = "null THD at recovery authorization finish";
    return true;
  }

  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  const auto found = g_runtime.authorizations.find(thd);
  if (found == g_runtime.authorizations.end()) {
    if (error != nullptr) *error = "recovery THD lost its authorization";
    return true;
  }
  if (found->second.authorization.kind() != AuthorizationKind::RECOVERY) {
    if (error != nullptr)
      *error = "recovery THD carries a non-recovery authorization";
    return true;
  }

  std::string binding_error;
  const bool binding_matches = recovery_runtime_matches_binding_locked(
      found->second.binding, &binding_error);
  const bool consumed = found->second.authorization.consumed();
  g_runtime.authorizations.erase(found);

  if (g_runtime.admissions.contains(thd)) {
    if (error != nullptr)
      *error = "recovery THD remained in commit admission after apply";
    return true;
  }
  if (!binding_matches) {
    if (error != nullptr)
      *error = binding_error.empty()
                   ? "recovery authorization binding changed during apply"
                   : std::move(binding_error);
    return true;
  }
  if (consumed != require_consumed) {
    if (error != nullptr)
      *error = require_consumed
                   ? "durable recovery transaction bypassed authorization"
                   : "read-only recovery transaction consumed authorization";
    return true;
  }
  return false;
}

void discard_recovery_commit_authorization(THD *thd) {
  if (thd == nullptr) return;
  std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
  const auto found = g_runtime.authorizations.find(thd);
  if (found != g_runtime.authorizations.end() &&
      found->second.authorization.kind() == AuthorizationKind::RECOVERY)
    g_runtime.authorizations.erase(found);
}

bool may_complete_recovery_gtid(const THD *thd) {
  if (!enabled() || thd == nullptr || thd != current_thd ||
      !thd->slave_thread || thd->system_thread != SYSTEM_THREAD_SLAVE_SQL ||
      thd->variables.sql_log_bin ||
      (thd->variables.option_bits & OPTION_BIN_LOG) != 0 ||
      thd->owned_gtid.sidno <= 0 ||
      thd->variables.gtid_next.type != ASSIGNED_GTID ||
      !thd->variables.gtid_next.gtid.equals(thd->owned_gtid) ||
      thd->is_error())
    return false;

  std::array<char, Gtid::MAX_TEXT_LENGTH + 1> text{};
  const int length = thd->owned_gtid.to_string(thd->owned_tsid, text.data());
  GtidSetDigest digest;
  std::string error;
  if (length <= 0 || static_cast<size_t>(length) >= text.size() ||
      !gtid_digest(std::string_view(text.data(), length), &digest, &error))
    return false;

  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  if (terminal_snapshot_failure_locked() || g_runtime.shutdown_draining ||
      g_runtime.shutting_down || g_runtime.admission_open ||
      !g_runtime.admissions.empty() ||
      g_runtime.clone_cut_phase != CloneCutBarrierPhase::NONE)
    return false;
  const auto found = g_runtime.authorizations.find(const_cast<THD *>(thd));
  return found != g_runtime.authorizations.end() && found->second.required &&
         found->second.authorization.kind() == AuthorizationKind::RECOVERY &&
         found->second.binding.gtid_sha256 == digest.sha256 &&
         recovery_runtime_matches_binding_locked(found->second.binding, &error);
}

bool begin_commit_admission(THD *thd, bool potentially_durable) {
  if (!enabled() || !potentially_durable) return false;
  if (thd == nullptr) fail_stop("null THD at remote commit admission");
  if (may_initialize_system_tables(thd)) return false;
  std::unique_lock<std::mutex> lock(g_runtime.admission_mutex);
  if (terminal_snapshot_failure_locked() || g_runtime.shutdown_draining ||
      g_runtime.shutting_down)
    return true;
  if (!g_runtime.admission_open && !g_runtime.shutting_down) {
    bool recovery_phase = false;
    bool recovery_authorized = false;
    std::string recovery_error;
    {
      std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
      recovery_phase =
          g_runtime.initialized &&
          g_runtime.startup_route == StartupRoute::TAKEOVER &&
          g_runtime.startup_phase == StartupPhase::EPOCH_ACQUIRED &&
          g_runtime.status.state == LifecycleState::RECOVERING &&
          !g_runtime.running;
      const auto found = g_runtime.authorizations.find(thd);
      if (recovery_phase && found != g_runtime.authorizations.end() &&
          found->second.required &&
          found->second.authorization.kind() == AuthorizationKind::RECOVERY &&
          !found->second.authorization.consumed()) {
        recovery_authorized = recovery_runtime_matches_binding_locked(
            found->second.binding, &recovery_error);
      }
    }
    if (recovery_phase) {
      if (!recovery_authorized) {
        lock.unlock();
        if (recovery_error.empty())
          recovery_error =
              "commit during startup recovery lacks a live authorization";
        fail_stop(recovery_error.c_str());
      }
      if (!g_runtime.admissions.empty() ||
          !g_runtime.admissions
               .emplace(thd, AdmissionRecord{AdmissionKind::RECOVERY,
                                             kRecoveryAdmissionReservation})
               .second) {
        lock.unlock();
        fail_stop("recovery THD entered commit admission out of sequence");
      }
      return false;
    }
  }
  for (;;) {
    g_runtime.admission_cond.wait(lock, [] {
      return g_runtime.admission_open || g_runtime.shutdown_draining ||
             g_runtime.shutting_down || terminal_snapshot_failure_locked();
    });
    if (terminal_snapshot_failure_locked() || g_runtime.shutdown_draining ||
        g_runtime.shutting_down)
      return true;
    if (g_runtime.admissions.contains(thd)) {
      lock.unlock();
      fail_stop("THD entered remote commit admission twice");
    }
    if (!g_runtime.published_window_valid) {
      lock.unlock();
      fail_stop("OPEN admission has no exact published recovery window");
    }

    RecoveryWindow prospective;
    if (!prospective_window_locked(kNormalAdmissionReservation,
                                   &prospective)) {
      RecoveryWindow current;
      if (!add_recovery_window(g_runtime.published_window,
                               g_runtime.admission_reservations, &current))
        current = g_runtime.published_window;
      const RecoveryWindow blocked{
          current.manifest_count + kNormalAdmissionReservation.manifest_count,
          current.manifest_bytes + kNormalAdmissionReservation.manifest_bytes,
          current.segment_count + kNormalAdmissionReservation.segment_count};
      ensure_snapshot_request_locked(RuntimeSnapshotRequestReason::HARD_LIMIT,
                                     blocked);
      continue;
    }

    add_reservation_locked(kNormalAdmissionReservation);
    if (!g_runtime.admissions
             .emplace(thd, AdmissionRecord{AdmissionKind::NORMAL,
                                           kNormalAdmissionReservation})
             .second) {
      remove_reservation_locked(kNormalAdmissionReservation);
      lock.unlock();
      fail_stop("THD entered remote commit admission twice");
    }
    if (prospective.manifest_count >= kRuntimeSnapshotSoftManifestCount)
      ensure_snapshot_request_locked(RuntimeSnapshotRequestReason::SOFT_LIMIT,
                                     prospective);
    return false;
  }
}

void end_commit_admission(THD *thd, bool) {
  if (!enabled() || thd == nullptr) return;
  std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
  const auto found = g_runtime.admissions.find(thd);
  if (found == g_runtime.admissions.end()) return;
  remove_reservation_locked(found->second.reservation);
  g_runtime.admissions.erase(found);
  refresh_request_prospective_locked();
  g_runtime.admission_cond.notify_all();
}

bool close_commit_admission_and_wait() {
  if (!enabled()) return false;
  std::unique_lock<std::mutex> lock(g_runtime.admission_mutex);
  if (g_runtime.clone_cut_phase != CloneCutBarrierPhase::NONE) return true;
  g_runtime.admission_open = false;
  g_runtime.admission_cond.wait(lock, [] {
    return g_runtime.admissions.empty() || g_runtime.shutdown_draining ||
           g_runtime.shutting_down;
  });
  return g_runtime.shutdown_draining || g_runtime.shutting_down;
}

bool take_runtime_snapshot_request(RuntimeSnapshotRequest *request) {
  if (request == nullptr) return false;
  *request = {};
  if (!enabled()) return false;
  std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
  if (terminal_snapshot_failure_locked() ||
      !g_runtime.snapshot_request.has_value() ||
      !g_runtime.snapshot_request->notification_pending)
    return false;
  *request = snapshot_request_locked();
  g_runtime.snapshot_request->notification_pending = false;
  return true;
}

bool wait_for_runtime_snapshot_request(RuntimeSnapshotRequest *request) {
  if (request == nullptr || !enabled()) return true;
  *request = {};
  std::unique_lock<std::mutex> lock(g_runtime.admission_mutex);
  g_runtime.admission_cond.wait(lock, [] {
    return (g_runtime.snapshot_request.has_value() &&
            g_runtime.snapshot_request->notification_pending) ||
           g_runtime.shutdown_draining || g_runtime.shutting_down ||
           terminal_snapshot_failure_locked();
  });
  if (terminal_snapshot_failure_locked() || g_runtime.shutdown_draining ||
      g_runtime.shutting_down)
    return true;
  *request = snapshot_request_locked();
  g_runtime.snapshot_request->notification_pending = false;
  return false;
}

RuntimeSnapshotRequestResult refresh_runtime_snapshot_request(
    uint64_t request_id) {
  if (!enabled() || request_id == 0) {
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
        "runtime snapshot refresh has invalid request authority");
  }

  std::unique_lock<std::mutex> lock(g_runtime.admission_mutex);
  if (g_runtime.shutdown_draining || g_runtime.shutting_down) {
    lock.unlock();
    release_snapshot_order_ticket(request_id);
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::SHUTDOWN,
        "server shutdown canceled the runtime snapshot request");
  }
  if (g_runtime.snapshot_terminal_failure) {
    lock.unlock();
    release_snapshot_order_ticket(request_id);
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::FENCED,
        "runtime snapshot subsystem is terminal");
  }
  if (g_runtime.completed_snapshot_request_id == request_id) {
    return snapshot_request_result(RuntimeSnapshotRequestOutcome::COMPLETED);
  }
  if (!g_runtime.snapshot_request.has_value() ||
      g_runtime.snapshot_request->request_id != request_id) {
    lock.unlock();
    release_snapshot_order_ticket(request_id);
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::REFIX_REQUIRED,
        "runtime snapshot request is stale");
  }
  const RuntimeSnapshotRequestState &state = *g_runtime.snapshot_request;
  if (state.terminal_outcome.has_value()) {
    return snapshot_request_result(*state.terminal_outcome,
                                   state.terminal_detail);
  }
  return snapshot_request_result(RuntimeSnapshotRequestOutcome::ACTIVE, {},
                                 snapshot_request_locked());
}

bool reserve_runtime_snapshot_hard_gate(uint64_t request_id,
                                        std::string *error) {
  if (error != nullptr) error->clear();
  if (!enabled())
    return clone_cut_error(error,
                           "runtime snapshot hard gate requires remote mode");
  if (request_id == 0)
    return clone_cut_error(error,
                           "runtime snapshot hard gate has an invalid request ID");

  std::unique_lock<std::mutex> lock(g_runtime.admission_mutex);
  if (terminal_snapshot_failure_locked() ||
      !g_runtime.snapshot_request.has_value() ||
      g_runtime.snapshot_request->request_id != request_id ||
      g_runtime.snapshot_request->reason !=
          RuntimeSnapshotRequestReason::HARD_LIMIT ||
      g_runtime.snapshot_request->terminal_outcome.has_value()) {
    return clone_cut_error(error,
                           "runtime snapshot hard gate request is stale");
  }
  if (g_runtime.shutdown_draining || g_runtime.shutting_down)
    return clone_cut_error(error,
                           "server shutdown prevents a runtime snapshot hard gate");

  RuntimeSnapshotRequestState &request = *g_runtime.snapshot_request;
  request.hard_gate_reserved = true;
  g_runtime.admission_open = false;
  g_runtime.admission_cond.notify_all();
  g_runtime.admission_cond.wait(lock, [&] {
    return g_runtime.admissions.empty() || g_runtime.shutdown_draining ||
           g_runtime.shutting_down || !g_runtime.snapshot_request.has_value() ||
           g_runtime.snapshot_request->request_id != request_id ||
           terminal_snapshot_failure_locked();
  });
  if (!g_runtime.snapshot_request.has_value() ||
      g_runtime.snapshot_request->request_id != request_id)
    return clone_cut_error(error,
                           "runtime snapshot hard gate lost request ownership");
  if (terminal_snapshot_failure_locked() || g_runtime.shutdown_draining ||
      g_runtime.shutting_down)
    return clone_cut_error(error,
                           "server shutdown interrupted runtime snapshot drain");
  return false;
}

namespace {

bool cancel_reopen_or_fail_stop(const char *reason) {
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  if (!g_runtime.admission_reopen_in_progress)
    fail_stop_holding_admission(
        "commit admission reopen lost its reservation");
  if (live_snapshot_request_keeps_admission_closed_locked()) {
    g_runtime.admission_open = false;
    g_runtime.admission_reopen_in_progress = false;
    admission_lock.unlock();
    g_runtime.admission_cond.notify_all();
    return true;
  }
  if (!g_runtime.shutdown_draining && !g_runtime.shutting_down)
    fail_stop_holding_admission(reason);

  g_runtime.admission_reopen_in_progress = false;
  admission_lock.unlock();
  g_runtime.admission_cond.notify_all();
  return true;
}

void complete_commit_admission_reopen() {
  Head intended;
  std::string expected_etag;
  const char *invalid_state = nullptr;
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    if (!g_runtime.initialized || g_runtime.publisher == nullptr) {
      invalid_state =
          "cannot open admission before remote startup initialization";
    } else if (g_runtime.startup_phase == StartupPhase::ROOT_VERIFIED) {
      if (!g_runtime.installed_head.has_value() ||
          g_runtime.installed_head_etag.empty()) {
        invalid_state = "verified startup root lost its exact HEAD proof";
      } else {
        intended = *g_runtime.installed_head;
        expected_etag = g_runtime.installed_head_etag;
      }
    } else if (g_runtime.startup_phase == StartupPhase::ACTIVE) {
      const PublisherState &state = g_runtime.publisher->state();
      if (!state.head.has_value() || !state.head_object.has_value()) {
        invalid_state = "active remote runtime lost its exact HEAD state";
      } else {
        intended = *state.head;
        expected_etag = state.head_object->etag;
      }
    } else {
      invalid_state = "commit admission requires a verified installed root";
    }
  }
  if (invalid_state != nullptr &&
      cancel_reopen_or_fail_stop(invalid_state))
    return;

  PublishResult verified;
  {
    std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
    verified = g_runtime.publisher->verify_decision(intended);
  }
  if (!verified.applied()) {
    std::string reason("admission HEAD/epoch verification failed");
    if (!verified.detail.empty())
      reason.append(": ").append(verified.detail);
    if (cancel_reopen_or_fail_stop(reason.c_str())) return;
  }

  bool exact_state = false;
  bool cursor_and_window_exact = false;
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  if (!g_runtime.admission_reopen_in_progress)
    fail_stop_holding_admission(
        "commit admission reopen lost its reservation");
  if (g_runtime.shutdown_draining || g_runtime.shutting_down) {
    g_runtime.admission_reopen_in_progress = false;
    admission_lock.unlock();
    g_runtime.admission_cond.notify_all();
    return;
  }
  if (live_snapshot_request_keeps_admission_closed_locked()) {
    g_runtime.admission_open = false;
    g_runtime.admission_reopen_in_progress = false;
    admission_lock.unlock();
    g_runtime.admission_cond.notify_all();
    return;
  }
  if (g_runtime.clone_cut_phase != CloneCutBarrierPhase::NONE)
    fail_stop_holding_admission(
        "clone cut became active while reopening commit admission");
  {
    std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
    const PublisherState &state = g_runtime.publisher->state();
    exact_state = state.lifecycle == LifecycleState::RUNNING &&
                  state.head.has_value() && state.head_object.has_value() &&
                  *state.head == intended &&
                  state.head_object->etag == expected_etag &&
                  exact_adopted_epoch_locked() &&
                  (g_runtime.startup_phase != StartupPhase::ROOT_VERIFIED ||
                   exact_adopted_head_locked(false));
    cursor_and_window_exact =
        exact_state &&
        g_runtime.public_committed_cursor == state.head->durable_cursor &&
        g_runtime.published_window_valid &&
        g_runtime.published_window_generation == state.head->generation &&
        g_runtime.published_window == state.head->recovery_window;
    if (exact_state && cursor_and_window_exact) {
      g_runtime.startup_phase = StartupPhase::ACTIVE;
      g_runtime.running = true;
      update_status_locked(LifecycleState::RUNNING);
      if (one_normal_reservation_fits_locked())
        g_runtime.admission_open = true;
    }
  }
  if (!exact_state)
    fail_stop_holding_admission(
        "admission HEAD ETag differs from the installed proof");
  if (!cursor_and_window_exact)
    fail_stop_holding_admission(
        "admission public cursor or recovery window is not exact");
  if (!g_runtime.admission_open) {
    RecoveryWindow current;
    if (!add_recovery_window(g_runtime.published_window,
                             g_runtime.admission_reservations, &current))
      current = g_runtime.published_window;
    const RecoveryWindow blocked{
        current.manifest_count + kNormalAdmissionReservation.manifest_count,
        current.manifest_bytes + kNormalAdmissionReservation.manifest_bytes,
        current.segment_count + kNormalAdmissionReservation.segment_count};
    ensure_snapshot_request_locked(RuntimeSnapshotRequestReason::HARD_LIMIT,
                                   blocked);
  }
  g_runtime.admission_reopen_in_progress = false;
  admission_lock.unlock();
  g_runtime.admission_cond.notify_all();
}

RuntimeSnapshotRequestResult classify_snapshot_verification(
    const PublishResult &result, std::string_view context) {
  std::string detail(context);
  if (!result.detail.empty()) detail.append(": ").append(result.detail);
  switch (result.outcome) {
    case PublishOutcome::BLOCKED:
      return snapshot_request_result(RuntimeSnapshotRequestOutcome::BLOCKED,
                                     std::move(detail));
    case PublishOutcome::REFIX_REQUIRED:
      return snapshot_request_result(
          RuntimeSnapshotRequestOutcome::PERMANENT_ERROR, std::move(detail));
    case PublishOutcome::FENCED:
    case PublishOutcome::ABSENT:
      return snapshot_request_result(RuntimeSnapshotRequestOutcome::FENCED,
                                     std::move(detail));
    case PublishOutcome::PERMANENT_ERROR:
      return snapshot_request_result(
          RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
          std::move(detail));
    case PublishOutcome::APPLIED:
      return snapshot_request_result(RuntimeSnapshotRequestOutcome::ACTIVE);
  }
  return snapshot_request_result(
      RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
      "unknown runtime snapshot verification outcome");
}

bool transition_matches_head(const TransitionManifest &manifest,
                             const Head &head) {
  return manifest.generation == head.generation &&
         manifest.writer == head.writer &&
         manifest.head_parent == head.parent &&
         manifest.recovery_window == head.recovery_window &&
         manifest.segment_tip == head.segment_tip &&
         manifest.snapshot == head.snapshot &&
         manifest.base_cursor == head.base_cursor &&
         manifest.durable_cursor == head.durable_cursor;
}

RuntimeSnapshotRequestResult verify_published_snapshot_lineage(
    const Head &published_head) {
  Head current_head;
  StreamIdentity stream;
  ProtocolStore *store = nullptr;
  HeadPublisher *publisher = nullptr;
  {
    std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
    const PublisherState *state =
        g_runtime.publisher == nullptr ? nullptr : &g_runtime.publisher->state();
    if (!g_runtime.initialized || !g_runtime.running ||
        g_runtime.startup_phase != StartupPhase::ACTIVE || state == nullptr ||
        (state->lifecycle != LifecycleState::RUNNING &&
         state->lifecycle != LifecycleState::BLOCKED) ||
        !state->head.has_value() || !state->head_object.has_value() ||
        g_runtime.store == nullptr) {
      return snapshot_request_result(
          RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
          "runtime snapshot completion requires an active exact publisher");
    }
    current_head = *state->head;
    stream = g_runtime.stream;
    store = g_runtime.store.get();
    publisher = g_runtime.publisher.get();
  }

  PublishResult exact;
  {
    std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
    exact = publisher->verify_decision(current_head);
    update_status_locked(publisher->state().lifecycle);
  }
  if (!exact.applied())
    return classify_snapshot_verification(
        exact, "cannot exact-verify runtime snapshot completion HEAD/epoch");

  if (current_head.generation < published_head.generation ||
      current_head.writer != published_head.writer) {
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::FENCED,
        "runtime snapshot publication is outside the current writer lineage");
  }
  if (current_head.snapshot != published_head.snapshot ||
      current_head.base_cursor != published_head.base_cursor) {
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::FENCED,
        "a later snapshot superseded the request-owned publication");
  }
  if (current_head.generation == published_head.generation &&
      current_head != published_head) {
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::FENCED,
        "request-owned SNAPSHOT generation has divergent HEAD fields");
  }
  const uint64_t distance =
      current_head.generation - published_head.generation;
  if (distance >= kRecoveryManifestCountMax) {
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::FENCED,
        "request-owned SNAPSHOT is outside the retained manifest window");
  }

  ManifestRef next{current_head.generation, current_head.manifest.key,
                   current_head.manifest.size,
                   current_head.manifest.sha256};
  for (uint64_t traversed = 0; traversed <= distance; ++traversed) {
    const PublishResult read = store->read(next.key, kDeltaManifestMaxBytes);
    if (!read.applied() || !read.object.has_value()) {
      PublishResult classified = read;
      if (read.outcome == PublishOutcome::ABSENT) {
        classified.outcome = PublishOutcome::FENCED;
        classified.detail = "lineage manifest is absent";
      }
      return classify_snapshot_verification(
          classified, "cannot exact-read runtime snapshot lineage");
    }

    std::string body_sha;
    std::string error;
    TransitionManifest manifest;
    if (read.object->body.size() != next.size ||
        !sha256_hex(read.object->body, &body_sha, &error) ||
        body_sha != next.sha256 ||
        !parse_transition_manifest(read.object->body, stream, next.key,
                                   &manifest, &error) ||
        manifest.generation != next.generation) {
      std::string detail = "runtime snapshot lineage manifest is corrupt";
      if (!error.empty()) detail.append(": ").append(error);
      return snapshot_request_result(RuntimeSnapshotRequestOutcome::FENCED,
                                     std::move(detail));
    }
    if (traversed == 0 && !transition_matches_head(manifest, current_head)) {
      return snapshot_request_result(
          RuntimeSnapshotRequestOutcome::FENCED,
          "current HEAD does not match its exact lineage manifest");
    }
    if (manifest.writer != published_head.writer) {
      return snapshot_request_result(
          RuntimeSnapshotRequestOutcome::FENCED,
          "runtime snapshot lineage changed writer identity");
    }

    if (next.generation == published_head.generation) {
      if (next.key != published_head.manifest.key ||
          next.size != published_head.manifest.size ||
          next.sha256 != published_head.manifest.sha256 ||
          manifest.kind != ManifestKind::SNAPSHOT ||
          !transition_matches_head(manifest, published_head)) {
        return snapshot_request_result(
            RuntimeSnapshotRequestOutcome::FENCED,
            "request-owned SNAPSHOT is not the exact lineage ancestor");
      }
      return snapshot_request_result(RuntimeSnapshotRequestOutcome::ACTIVE);
    }
    if (manifest.kind != ManifestKind::LOG_TRANSITION ||
        manifest.snapshot != published_head.snapshot ||
        manifest.base_cursor != published_head.base_cursor ||
        !manifest.previous.has_value() ||
        manifest.previous->generation + 1 != manifest.generation) {
      return snapshot_request_result(
          RuntimeSnapshotRequestOutcome::FENCED,
          "request-owned SNAPSHOT has a non-LOG descendant transition");
    }
    next = *manifest.previous;
  }
  return snapshot_request_result(
      RuntimeSnapshotRequestOutcome::FENCED,
      "request-owned SNAPSHOT was not found in the current lineage");
}

}  // namespace

void open_commit_admission() {
  if (!enabled()) return;
  {
    std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
    if (g_runtime.shutdown_draining || g_runtime.shutting_down ||
        terminal_snapshot_failure_locked())
      return;
    maybe_request_next_snapshot_locked();
    if (live_snapshot_request_keeps_admission_closed_locked()) {
      g_runtime.admission_open = false;
      return;
    }
    if (g_runtime.clone_cut_phase != CloneCutBarrierPhase::NONE)
      fail_stop_holding_admission(
          "cannot reopen commit admission during an active clone cut");
    if (g_runtime.admission_reopen_in_progress)
      fail_stop_holding_admission(
          "commit admission reopen is already in progress");
    g_runtime.admission_reopen_in_progress = true;
  }
  complete_commit_admission_reopen();
}

RuntimeSnapshotRequestResult complete_runtime_snapshot_request(
    uint64_t request_id) {
  if (!enabled() || request_id == 0) {
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
        "runtime snapshot completion has invalid request authority");
  }

  std::unique_lock<std::mutex> clone_lock(g_runtime.clone_cut_mutex);
  std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
  if (g_runtime.shutdown_draining || g_runtime.shutting_down) {
    admission_lock.unlock();
    clone_lock.unlock();
    release_snapshot_order_ticket(request_id);
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::SHUTDOWN,
        "server shutdown canceled runtime snapshot completion");
  }
  if (g_runtime.completed_snapshot_request_id == request_id) {
    return snapshot_request_result(RuntimeSnapshotRequestOutcome::COMPLETED);
  }
  if (!g_runtime.snapshot_request.has_value() ||
      g_runtime.snapshot_request->request_id != request_id) {
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::FENCED,
        "runtime snapshot completion request is stale");
  }
  RuntimeSnapshotRequestState &request = *g_runtime.snapshot_request;
  if (request.terminal_outcome.has_value()) {
    return snapshot_request_result(*request.terminal_outcome,
                                   request.terminal_detail);
  }
  if (g_runtime.snapshot_terminal_failure) {
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::FENCED,
        "runtime snapshot subsystem is terminal");
  }
  if (!request.published_generation.has_value() ||
      !request.published_head.has_value() ||
      *request.published_generation <= request.start_generation ||
      request.published_head->generation != *request.published_generation) {
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::BLOCKED,
        "runtime snapshot request has no ordered SNAPSHOT publication");
  }

  const auto clone_phase_matches_request = [&] {
    return g_runtime.clone_cut_phase == CloneCutBarrierPhase::ACTIVE &&
           g_runtime.active_clone_cut.has_value() &&
           g_runtime.active_clone_cut->request_id == request_id;
  };
  if (g_runtime.clone_cut_phase != CloneCutBarrierPhase::NONE &&
      !clone_phase_matches_request()) {
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
        "runtime snapshot completion lost its clone-cut ownership");
  }

  bool reopen = false;
  if (request.reason == RuntimeSnapshotRequestReason::SOFT_LIMIT) {
    if (g_runtime.admission_reopen_in_progress) {
      return snapshot_request_result(
          RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
          "runtime snapshot completion overlapped an admission reopen");
    }
    g_runtime.completed_snapshot_request_id = request_id;
    g_runtime.snapshot_request.reset();
    maybe_request_next_snapshot_locked();
    if (g_runtime.clone_cut_phase == CloneCutBarrierPhase::NONE &&
        !g_runtime.admission_open) {
      g_runtime.admission_reopen_in_progress = true;
      reopen = true;
    }
    admission_lock.unlock();
    clone_lock.unlock();
    g_runtime.admission_cond.notify_all();
    if (reopen) complete_commit_admission_reopen();
    return snapshot_request_result(RuntimeSnapshotRequestOutcome::COMPLETED);
  }

  if (!request.hard_gate_reserved || g_runtime.admission_open ||
      !g_runtime.admissions.empty() ||
      g_runtime.admission_reopen_in_progress) {
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::BLOCKED,
        "runtime snapshot hard gate is not CLOSED and drained");
  }
  const Head published_head = *request.published_head;
  admission_lock.unlock();

  RuntimeSnapshotRequestResult lineage =
      verify_published_snapshot_lineage(published_head);
  if (lineage.outcome != RuntimeSnapshotRequestOutcome::ACTIVE) return lineage;

  admission_lock.lock();
  if (g_runtime.shutdown_draining || g_runtime.shutting_down) {
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::SHUTDOWN,
        "server shutdown interrupted runtime snapshot completion");
  }
  if (!g_runtime.snapshot_request.has_value() ||
      g_runtime.snapshot_request->request_id != request_id ||
      g_runtime.snapshot_request->reason !=
          RuntimeSnapshotRequestReason::HARD_LIMIT ||
      !g_runtime.snapshot_request->hard_gate_reserved ||
      g_runtime.snapshot_request->terminal_outcome.has_value() ||
      g_runtime.snapshot_request->published_head != published_head ||
      g_runtime.admission_open || !g_runtime.admissions.empty() ||
      g_runtime.admission_reopen_in_progress) {
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
        "runtime snapshot hard completion changed during verification");
  }
  if (g_runtime.clone_cut_phase != CloneCutBarrierPhase::NONE &&
      !clone_phase_matches_request()) {
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
        "runtime snapshot hard completion lost its clone-cut lease");
  }

  bool exact_publication = false;
  {
    std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
    const PublisherState *state =
        g_runtime.publisher == nullptr ? nullptr : &g_runtime.publisher->state();
    exact_publication =
        g_runtime.initialized && g_runtime.running &&
        g_runtime.startup_phase == StartupPhase::ACTIVE && state != nullptr &&
        state->lifecycle == LifecycleState::RUNNING && state->head.has_value() &&
        state->head_object.has_value() &&
        g_runtime.public_committed_cursor == state->head->durable_cursor &&
        g_runtime.published_window_valid &&
        g_runtime.published_window_generation == state->head->generation &&
        g_runtime.published_window == state->head->recovery_window;
  }
  if (!exact_publication) {
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::FENCED,
        "runtime snapshot completion is not public-cursor/window exact");
  }
  if (!one_normal_reservation_fits_locked()) {
    return snapshot_request_result(
        RuntimeSnapshotRequestOutcome::FENCED,
        "runtime snapshot completion has no room for one normal LOG");
  }

  g_runtime.completed_snapshot_request_id = request_id;
  g_runtime.snapshot_request.reset();
  maybe_request_next_snapshot_locked();
  if (g_runtime.clone_cut_phase == CloneCutBarrierPhase::NONE) {
    g_runtime.admission_reopen_in_progress = true;
    reopen = true;
  }
  admission_lock.unlock();
  clone_lock.unlock();
  g_runtime.admission_cond.notify_all();
  if (reopen) complete_commit_admission_reopen();
  return snapshot_request_result(RuntimeSnapshotRequestOutcome::COMPLETED);
}

void mark_runtime_snapshot_terminal(uint64_t request_id,
                                    RuntimeSnapshotRequestOutcome outcome,
                                    std::string_view detail) {
  if (!enabled() || request_id == 0) return;
  if (outcome != RuntimeSnapshotRequestOutcome::FENCED &&
      outcome != RuntimeSnapshotRequestOutcome::PERMANENT_ERROR) {
    outcome = RuntimeSnapshotRequestOutcome::PERMANENT_ERROR;
  }
  std::string terminal_detail =
      detail.empty() ? "runtime snapshot coordinator stopped terminally"
                     : std::string(detail);

  // Publish the terminal admission state and release any stale FIFO ticket
  // before taking state_mutex. An opener can hold state_mutex while paused in
  // remote I/O; retaining snapshot_order.mutex here would deadlock ticket
  // observers and prevent that opener from being released.
  {
    std::unique_lock<std::mutex> snapshot_order_guard(g_snapshot_order.mutex);
    {
      std::lock_guard<std::mutex> admission_guard(g_runtime.admission_mutex);
      g_runtime.snapshot_terminal_failure = true;
      if (g_runtime.snapshot_request.has_value() &&
          g_runtime.snapshot_request->request_id == request_id) {
        RuntimeSnapshotRequestState &request = *g_runtime.snapshot_request;
        request.terminal_outcome = outcome;
        request.terminal_detail = terminal_detail;
        request.notification_pending = false;
      }
      g_runtime.admission_open = false;
    }
    release_snapshot_order_ticket_locked(0);
  }
  {
    std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
    g_runtime.running = false;
    g_runtime.last_error = terminal_detail;
    if (g_runtime.publisher != nullptr) {
      g_runtime.publisher->fence(terminal_detail);
      update_status_locked(g_runtime.publisher->state().lifecycle);
    }
  }
  g_runtime.admission_cond.notify_all();
}

bool begin_clone_cut_barrier(uint64_t request_id, CloneCutState *state,
                             CloneCutBarrierLease *lease,
                             std::string *error) {
  if (error != nullptr) error->clear();
  if (request_id == 0)
    return clone_cut_error(error,
                           "clone cut has an invalid snapshot request ID");
  if (state == nullptr || lease == nullptr)
    return clone_cut_error(error, "null clone cut barrier output");
  *state = {};
  if (!enabled())
    return clone_cut_error(error, "clone cut barrier requires remote mode");

  std::lock_guard<std::mutex> clone_guard(g_runtime.clone_cut_mutex);
  if (lease->token_ != 0 || lease->request_id_ != 0 ||
      lease->binlog_pin_ != nullptr)
    return clone_cut_error(error,
                           "clone cut output lease is already active");
  const bool test_mode = clone_cut_test_mode();

  uint64_t token = 0;
  {
    std::unique_lock<std::mutex> admission_lock(g_runtime.admission_mutex);
    if (g_runtime.clone_cut_phase != CloneCutBarrierPhase::NONE)
      return clone_cut_error(error, "another clone cut barrier is active");
    if (g_runtime.clone_cut_pins_releasing != 0)
      return clone_cut_error(
          error, "a previous clone cut binlog pin is still releasing");
    if (g_runtime.shutdown_draining || g_runtime.shutting_down)
      return clone_cut_error(error,
                             "server shutdown prevents a clone cut barrier");
    if (g_runtime.snapshot_terminal_failure)
      return clone_cut_error(error,
                             "runtime snapshot subsystem is terminal");
    const bool request_matches =
        g_runtime.snapshot_request.has_value() &&
        g_runtime.snapshot_request->request_id == request_id;
    const bool hard_gate_owned =
        request_matches &&
        g_runtime.snapshot_request->reason ==
            RuntimeSnapshotRequestReason::HARD_LIMIT &&
        g_runtime.snapshot_request->hard_gate_reserved &&
        !g_runtime.snapshot_request->terminal_outcome.has_value();
    if (!test_mode) {
      if (!request_matches)
        return clone_cut_error(
            error, "clone cut snapshot request is stale or was not notified");
      if (g_runtime.snapshot_request->terminal_outcome.has_value())
        return clone_cut_error(
            error, "clone cut snapshot request is terminal");
    }
    if (!g_runtime.admission_open && !hard_gate_owned)
      return clone_cut_error(
          error, "clone cut cannot take ownership of CLOSED admission");
    if (g_runtime.next_clone_cut_token == 0 ||
        g_runtime.next_clone_cut_token ==
            std::numeric_limits<uint64_t>::max())
      return clone_cut_error(error, "clone cut barrier token space exhausted");

    token = g_runtime.next_clone_cut_token++;
    g_runtime.clone_cut_phase = CloneCutBarrierPhase::CLOSING;
    g_runtime.clone_cut_token = token;
    g_runtime.active_clone_cut.reset();
    g_runtime.admission_open = false;
    g_runtime.admission_cond.wait(admission_lock, [] {
      return g_runtime.admissions.empty() || g_runtime.shutdown_draining ||
             g_runtime.shutting_down;
    });
    if (g_runtime.shutdown_draining || g_runtime.shutting_down) {
      g_runtime.clone_cut_phase = CloneCutBarrierPhase::NONE;
      g_runtime.clone_cut_token = 0;
      g_runtime.active_clone_cut.reset();
      admission_lock.unlock();
      g_runtime.admission_cond.notify_all();
      return clone_cut_error(error,
                             "server shutdown interrupted clone cut drain");
    }
  }

  CloneCutState captured;
  if (!capture_current_clone_cut(request_id, &captured, error)) {
    rollback_clone_cut_reservation();
    return true;
  }
  std::unique_ptr<CloneCutBinlogPin> binlog_pin =
      pin_clone_cut_binlog(captured, test_mode, error);
  if (binlog_pin == nullptr) {
    rollback_clone_cut_reservation();
    return true;
  }

  bool installed = false;
  {
    std::lock_guard<std::mutex> admission_guard(g_runtime.admission_mutex);
    installed = !g_runtime.shutdown_draining && !g_runtime.shutting_down &&
                !g_runtime.admission_open &&
                g_runtime.admissions.empty() &&
                g_runtime.clone_cut_phase == CloneCutBarrierPhase::CLOSING &&
                g_runtime.clone_cut_token == token;
    if (installed) {
      g_runtime.active_clone_cut = captured;
      g_runtime.clone_cut_phase = CloneCutBarrierPhase::ACTIVE;
    }
  }
  if (!installed) {
    start_clone_cut_pin_release();
    rollback_clone_cut_reservation();
    wait_before_clone_cut_pin_release_for_test();
    binlog_pin.reset();
    finish_clone_cut_pin_release();
    return clone_cut_error(error,
                           "clone cut lost exclusive CLOSED admission");
  }

  *state = std::move(captured);
  lease->token_ = token;
  lease->request_id_ = request_id;
  lease->binlog_pin_ = std::move(binlog_pin);
  return false;
}

bool verify_clone_cut_barrier_locked(const CloneCutState &state,
                                     const CloneCutBarrierLease &lease,
                                     std::string *error) {
  if (state.request_id == 0 || lease.token_ == 0 ||
      lease.request_id_ != state.request_id || lease.binlog_pin_ == nullptr ||
      !lease.binlog_pin_->matches(state))
    return clone_cut_error(error, "clone cut barrier lease is inactive");
  {
    std::lock_guard<std::mutex> admission_guard(g_runtime.admission_mutex);
    if (g_runtime.shutting_down || g_runtime.admission_open ||
        !g_runtime.admissions.empty() ||
        g_runtime.clone_cut_phase != CloneCutBarrierPhase::ACTIVE ||
        g_runtime.clone_cut_token != lease.token_ ||
        !g_runtime.active_clone_cut.has_value() ||
        *g_runtime.active_clone_cut != state)
      return clone_cut_error(
          error, "clone cut lease does not own unchanged CLOSED admission");
  }

  CloneCutState current;
  if (!capture_current_clone_cut(state.request_id, &current, error)) return true;
  if (current != state)
    return clone_cut_error(error,
                           "clone cut cursor, GTID, or HEAD identity changed");

  {
    std::lock_guard<std::mutex> admission_guard(g_runtime.admission_mutex);
    if (g_runtime.shutting_down || g_runtime.admission_open ||
        !g_runtime.admissions.empty() ||
        g_runtime.clone_cut_phase != CloneCutBarrierPhase::ACTIVE ||
        g_runtime.clone_cut_token != lease.token_ ||
        !g_runtime.active_clone_cut.has_value() ||
        *g_runtime.active_clone_cut != state)
      return clone_cut_error(
          error, "clone cut lost CLOSED admission during verification");
  }
  return false;
}

namespace {

std::string filesystem_operation_error(std::string_view operation,
                                       int error_number) {
  std::string detail(operation);
  detail.append(": ").append(std::strerror(error_number));
  return detail;
}

}  // namespace

bool verify_clone_cut_barrier(const CloneCutState &state,
                              const CloneCutBarrierLease &lease,
                              std::string *error) {
  if (error != nullptr) error->clear();
  if (!enabled())
    return clone_cut_error(error, "clone cut barrier requires remote mode");

  std::lock_guard<std::mutex> clone_guard(g_runtime.clone_cut_mutex);
  return verify_clone_cut_barrier_locked(state, lease, error);
}

bool materialize_clone_cut_binlog_seed(
    const CloneCutState &state, const CloneCutBarrierLease &lease,
    const fs::path &destination, std::string *error) {
  if (error != nullptr) error->clear();
  if (!enabled())
    return clone_cut_error(error,
                           "binlog seed materialization requires remote mode");
  if (destination.empty())
    return clone_cut_error(error, "binlog seed destination is empty");

  std::lock_guard<std::mutex> clone_guard(g_runtime.clone_cut_mutex);
  if (verify_clone_cut_barrier_locked(state, lease, error)) return true;
  const fs::path source = lease.binlog_pin_->source_path();
  if (source.empty())
    return clone_cut_error(
        error, "clone cut binlog pin has no materializable source path");

  int source_flags = O_RDONLY | O_CLOEXEC;
#ifdef O_NOFOLLOW
  source_flags |= O_NOFOLLOW;
#endif
  const int source_fd = ::open(source.c_str(), source_flags);
  if (source_fd < 0) {
    return clone_cut_error(
        error, filesystem_operation_error("cannot open pinned binlog source",
                                          errno));
  }

  bool destination_created = false;
  int destination_fd = -1;
  auto cleanup = [&] {
    if (destination_fd >= 0) {
      const int saved_errno = errno;
      (void)::close(destination_fd);
      errno = saved_errno;
      destination_fd = -1;
    }
    const int saved_errno = errno;
    (void)::close(source_fd);
    errno = saved_errno;
    if (destination_created) {
      std::error_code ignored;
      fs::remove(destination, ignored);
    }
  };

  struct stat source_stat {};
  if (::fstat(source_fd, &source_stat) != 0 ||
      !S_ISREG(source_stat.st_mode) || source_stat.st_size < 0 ||
      static_cast<uint64_t>(source_stat.st_size) < state.pos) {
    const int saved_errno = errno;
    cleanup();
    return clone_cut_error(
        error, saved_errno == 0
                   ? "pinned binlog source is shorter than the clone cut"
                   : filesystem_operation_error(
                         "cannot validate pinned binlog source", saved_errno));
  }

  int destination_flags = O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC;
#ifdef O_NOFOLLOW
  destination_flags |= O_NOFOLLOW;
#endif
  destination_fd = ::open(destination.c_str(), destination_flags, 0600);
  if (destination_fd < 0) {
    const int saved_errno = errno;
    cleanup();
    return clone_cut_error(
        error, filesystem_operation_error(
                   "cannot create exclusive binlog seed destination",
                   saved_errno));
  }
  destination_created = true;

  std::array<char, 1024 * 1024> buffer{};
  uint64_t offset = 0;
  while (offset < state.pos) {
    const size_t requested = static_cast<size_t>(
        std::min<uint64_t>(buffer.size(), state.pos - offset));
    ssize_t received = -1;
    do {
      received = ::pread(source_fd, buffer.data(), requested,
                         static_cast<off_t>(offset));
    } while (received < 0 && errno == EINTR);
    if (received <= 0) {
      const int saved_errno = received == 0 ? 0 : errno;
      cleanup();
      return clone_cut_error(
          error, saved_errno == 0
                     ? "pinned binlog source became short during copy"
                     : filesystem_operation_error(
                           "cannot read pinned binlog source", saved_errno));
    }
    size_t written = 0;
    while (written < static_cast<size_t>(received)) {
      ssize_t count = -1;
      do {
        count = ::write(destination_fd, buffer.data() + written,
                        static_cast<size_t>(received) - written);
      } while (count < 0 && errno == EINTR);
      if (count <= 0) {
        const int saved_errno = count == 0 ? EIO : errno;
        cleanup();
        return clone_cut_error(
            error, filesystem_operation_error("cannot write binlog seed",
                                              saved_errno));
      }
      written += static_cast<size_t>(count);
    }
    offset += static_cast<uint64_t>(received);
  }

  if (::fsync(destination_fd) != 0) {
    const int saved_errno = errno;
    cleanup();
    return clone_cut_error(
        error,
        filesystem_operation_error("cannot fsync binlog seed", saved_errno));
  }
  if (::close(destination_fd) != 0) {
    const int saved_errno = errno;
    destination_fd = -1;
    cleanup();
    return clone_cut_error(
        error,
        filesystem_operation_error("cannot close binlog seed", saved_errno));
  }
  destination_fd = -1;
  destination_created = false;
  if (::close(source_fd) != 0) {
    const int saved_errno = errno;
    std::error_code ignored;
    fs::remove(destination, ignored);
    return clone_cut_error(
        error, filesystem_operation_error("cannot close pinned binlog source",
                                          saved_errno));
  }
  return false;
}

void end_clone_cut_barrier(CloneCutBarrierLease *lease) {
  if (lease == nullptr) return;
  if (lease->token_ == 0) {
    if (lease->request_id_ != 0 || lease->binlog_pin_ != nullptr)
      fail_stop("inactive clone cut barrier retained request authority");
    return;
  }
  if (lease->binlog_pin_ == nullptr || !lease->binlog_pin_->active())
    fail_stop("active clone cut barrier lost its binlog pin");

  start_clone_cut_pin_release();
  std::unique_ptr<CloneCutBinlogPin> binlog_pin =
      std::move(lease->binlog_pin_);

  bool reopen = false;
  {
    std::lock_guard<std::mutex> clone_guard(g_runtime.clone_cut_mutex);
    const uint64_t token = std::exchange(lease->token_, 0);
    const uint64_t request_id = std::exchange(lease->request_id_, 0);
    if (request_id == 0)
      fail_stop("active clone cut barrier lost its snapshot request ID");
    const bool test_mode = clone_cut_test_mode();
    {
      std::lock_guard<std::mutex> admission_guard(g_runtime.admission_mutex);
      if (g_runtime.clone_cut_phase == CloneCutBarrierPhase::ACTIVE &&
          g_runtime.clone_cut_token == token &&
          g_runtime.active_clone_cut.has_value() &&
          g_runtime.active_clone_cut->request_id == request_id) {
        if (g_runtime.admission_open || !g_runtime.admissions.empty() ||
            !g_runtime.active_clone_cut.has_value())
          fail_stop_holding_admission(
              "clone cut lease lost exclusive CLOSED admission before release");
        g_runtime.clone_cut_phase = CloneCutBarrierPhase::NONE;
        g_runtime.clone_cut_token = 0;
        g_runtime.active_clone_cut.reset();
        if (!g_runtime.shutdown_draining && !g_runtime.shutting_down) {
          if (live_snapshot_request_keeps_admission_closed_locked()) {
            g_runtime.admission_open = false;
          } else if (test_mode) {
            g_runtime.admission_open = true;
          } else {
            if (g_runtime.admission_reopen_in_progress)
              fail_stop_holding_admission(
                  "clone cut release overlapped an admission reopen");
            g_runtime.admission_reopen_in_progress = true;
            reopen = true;
          }
        }
      }
    }
  }
  g_runtime.admission_cond.notify_all();
  if (reopen) complete_commit_admission_reopen();
  wait_before_clone_cut_pin_release_for_test();
  binlog_pin.reset();
  finish_clone_cut_pin_release();
}

#ifdef WESQL_TEST
StartupPolicy configured_startup_policy_for_test() {
  return configured_startup_policy({});
}

bool initialize_startup_lifecycle_for_test(ConditionalIo *conditional_io,
                                           const StreamIdentity &stream,
                                           bool bootstrap_preflight) {
  if (conditional_io == nullptr || stream.stream_id.empty()) return true;
  std::lock_guard<std::mutex> admission_guard(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  if (g_runtime.initialized ||
      g_runtime.startup_phase != StartupPhase::UNINITIALIZED) {
    g_runtime.last_error = "startup lifecycle test runtime was initialized twice";
    return true;
  }

  g_runtime.stream = stream;
  g_runtime.metadata_io = conditional_io;
  g_runtime.store = std::make_unique<ProtocolStore>(conditional_io);
  g_runtime.publisher =
      std::make_unique<HeadPublisher>(conditional_io, g_runtime.stream);
  PublishResult probe = g_runtime.publisher->probe();
  if (!probe.applied()) {
    g_runtime.last_error = "startup lifecycle test probe failed";
    if (!probe.detail.empty())
      g_runtime.last_error.append(": ").append(probe.detail);
    update_status_locked(g_runtime.publisher->state().lifecycle);
    return true;
  }

  g_runtime.initialized = true;
  g_runtime.running = false;
  g_runtime.bootstrap_preflight = bootstrap_preflight;
  g_runtime.startup_phase = StartupPhase::HEAD_PROBED;
  g_runtime.recovery_snapshot_gtids_restored = false;
  g_runtime.startup_route = g_runtime.publisher->state().head.has_value()
                                ? StartupRoute::TAKEOVER
                                : StartupRoute::BOOTSTRAP;
  g_runtime.startup_epoch_adopted = false;
  g_runtime.startup_epoch_adoption_role =
      StartupEpochAdoptionRole::TAKEOVER_RECOVERY;
  g_runtime.adopted_startup_proof.reset();
  g_runtime.probed_head_generation =
      g_runtime.publisher->state().head.has_value()
          ? g_runtime.publisher->state().head->generation
          : 0;
  g_runtime.installed_head.reset();
  g_runtime.installed_head_body.clear();
  g_runtime.installed_head_etag.clear();
  g_runtime.installed_marker.reset();
  g_runtime.local_flushed_cursor = {};
  g_runtime.public_committed_cursor = {};
  g_runtime.shutdown_draining = false;
  g_runtime.shutting_down = false;
  g_runtime.admission_open = false;
  g_runtime.admission_reopen_in_progress = false;
  reset_admission_accounting_locked();
  if (g_runtime.publisher->state().head.has_value()) {
    g_runtime.published_window_valid = true;
    g_runtime.published_window_generation =
        g_runtime.publisher->state().head->generation;
    g_runtime.published_window =
        g_runtime.publisher->state().head->recovery_window;
  }
  g_runtime.clone_cut_phase = CloneCutBarrierPhase::NONE;
  g_runtime.clone_cut_token = 0;
  g_runtime.active_clone_cut.reset();
  g_runtime.last_error.clear();
  update_status_locked(g_runtime.startup_route == StartupRoute::TAKEOVER
                           ? LifecycleState::RECOVERING
                           : LifecycleState::INITIALIZING);
  return false;
}

void reset_startup_lifecycle_for_test() {
  release_snapshot_order_ticket(0);
  std::lock_guard<std::mutex> admission_guard(g_runtime.admission_mutex);
  std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
  g_runtime.publisher.reset();
  g_runtime.store.reset();
  g_runtime.metadata_io = nullptr;
  g_runtime.initialized = false;
  g_runtime.bootstrap_preflight = false;
  g_runtime.running = false;
  g_runtime.startup_phase = StartupPhase::UNINITIALIZED;
  g_runtime.startup_route = StartupRoute::DISABLED;
  g_runtime.startup_epoch_adopted = false;
  g_runtime.recovery_snapshot_gtids_restored = false;
  g_runtime.startup_epoch_adoption_role =
      StartupEpochAdoptionRole::TAKEOVER_RECOVERY;
  g_runtime.adopted_startup_proof.reset();
  g_runtime.probed_head_generation = 0;
  g_runtime.stream = {};
  g_runtime.installed_head.reset();
  g_runtime.installed_head_body.clear();
  g_runtime.installed_head_etag.clear();
  g_runtime.installed_marker.reset();
  g_runtime.local_flushed_cursor = {};
  g_runtime.public_committed_cursor = {};
  g_runtime.authorizations.clear();
  g_runtime.status = {};
  g_runtime.last_error.clear();
  g_runtime.shutdown_draining = false;
  g_runtime.shutting_down = false;
  g_runtime.admission_open = false;
  g_runtime.admission_reopen_in_progress = false;
  reset_admission_accounting_locked();
  g_runtime.clone_cut_phase = CloneCutBarrierPhase::NONE;
  g_runtime.clone_cut_token = 0;
  g_runtime.active_clone_cut.reset();
}

void reset_commit_admission_for_test(bool open) {
  release_snapshot_order_ticket(0);
  std::lock_guard<std::mutex> clone_guard(g_runtime.clone_cut_mutex);
  std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
  g_runtime.shutdown_draining = false;
  g_runtime.shutting_down = false;
  g_runtime.admission_open = open;
  g_runtime.admission_reopen_in_progress = false;
  reset_admission_accounting_locked();
  g_runtime.published_window_valid = true;
  g_runtime.published_window_generation = 1;
  g_runtime.published_window = {1, 1, 0};
  g_runtime.clone_cut_phase = CloneCutBarrierPhase::NONE;
  g_runtime.clone_cut_token = 0;
  g_runtime.active_clone_cut.reset();
  g_runtime.clone_cut_binlog_source_path_for_test.reset();
  g_runtime.pause_clone_cut_pin_release_for_test = false;
  g_runtime.clone_cut_pin_release_waiting_for_test = false;
  g_runtime.admission_cond.notify_all();
}

std::size_t commit_admission_count_for_test() {
  std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
  return g_runtime.admissions.size();
}

void set_runtime_snapshot_window_for_test(uint64_t generation,
                                          const RecoveryWindow &window) {
  std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
  if (generation == 0 || !recovery_window_is_valid(window) ||
      !g_runtime.admissions.empty())
    fail_stop_holding_admission(
        "invalid focused-test runtime snapshot window");
  g_runtime.published_window_valid = true;
  g_runtime.published_window_generation = generation;
  g_runtime.published_window = window;
  g_runtime.admission_reservations = {};
  g_runtime.snapshot_request.reset();
  g_runtime.next_snapshot_request_id = 1;
  g_runtime.completed_snapshot_request_id = 0;
}

RecoveryWindow runtime_snapshot_reservations_for_test() {
  std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
  return g_runtime.admission_reservations;
}

bool runtime_snapshot_hard_gate_for_test() {
  std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
  return !g_runtime.admission_open && g_runtime.snapshot_request.has_value() &&
         g_runtime.snapshot_request->reason ==
             RuntimeSnapshotRequestReason::HARD_LIMIT;
}

bool note_runtime_snapshot_publication_for_test(uint64_t request_id,
                                                const Head &head,
                                                std::string *error) {
  if (error != nullptr) error->clear();
  bool exact = false;
  {
    std::lock_guard<std::mutex> state_guard(g_runtime.state_mutex);
    const PublisherState *state =
        g_runtime.publisher == nullptr ? nullptr : &g_runtime.publisher->state();
    exact = state != nullptr && state->head.has_value() &&
            *state->head == head &&
            g_runtime.public_committed_cursor == head.durable_cursor;
  }
  if (!exact)
    return clone_cut_error(
        error, "focused snapshot publication is not public-cursor exact");
  {
    std::lock_guard<std::mutex> admission_guard(g_runtime.admission_mutex);
    if (!g_runtime.snapshot_request.has_value() ||
        g_runtime.snapshot_request->request_id != request_id)
      return clone_cut_error(error,
                             "focused snapshot publication request is stale");
  }
  refresh_published_window_after_order_release(
      head.generation, head.recovery_window, request_id, &head);
  return false;
}

void set_clone_cut_source_for_test(const CloneCutState *source) {
  std::lock_guard<std::mutex> guard(g_runtime.clone_cut_mutex);
  if (source == nullptr) {
    g_runtime.clone_cut_source_for_test.reset();
    g_runtime.clone_cut_public_cursor_for_test.reset();
    g_runtime.clone_cut_binlog_source_path_for_test.reset();
  } else {
    g_runtime.clone_cut_source_for_test = *source;
    g_runtime.clone_cut_public_cursor_for_test =
        Cursor{source->file, source->pos};
  }
}

void set_clone_cut_binlog_source_path_for_test(const fs::path *source) {
  std::lock_guard<std::mutex> guard(g_runtime.clone_cut_mutex);
  if (source == nullptr)
    g_runtime.clone_cut_binlog_source_path_for_test.reset();
  else
    g_runtime.clone_cut_binlog_source_path_for_test = *source;
}

void set_clone_cut_public_cursor_for_test(const char *file, uint64_t pos) {
  std::lock_guard<std::mutex> guard(g_runtime.clone_cut_mutex);
  if (file == nullptr)
    g_runtime.clone_cut_public_cursor_for_test.reset();
  else
    g_runtime.clone_cut_public_cursor_for_test = Cursor{file, pos};
}

bool clone_cut_barrier_active_for_test() {
  std::lock_guard<std::mutex> clone_guard(g_runtime.clone_cut_mutex);
  std::lock_guard<std::mutex> admission_guard(g_runtime.admission_mutex);
  return g_runtime.clone_cut_phase == CloneCutBarrierPhase::ACTIVE &&
         g_runtime.clone_cut_token != 0 &&
         g_runtime.active_clone_cut.has_value();
}

bool commit_admission_open_for_test() {
  std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
  return g_runtime.admission_open;
}

bool commit_admission_draining_for_test() {
  std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
  return g_runtime.shutdown_draining;
}

bool commit_admission_reopening_for_test() {
  std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
  return g_runtime.admission_reopen_in_progress;
}

bool runtime_snapshot_terminal_for_test() {
  std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
  return g_runtime.snapshot_terminal_failure;
}

void pause_clone_cut_pin_release_for_test(bool pause) {
  {
    std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
    g_runtime.pause_clone_cut_pin_release_for_test = pause;
  }
  g_runtime.admission_cond.notify_all();
}

bool clone_cut_pin_release_waiting_for_test() {
  std::lock_guard<std::mutex> guard(g_runtime.admission_mutex);
  return g_runtime.clone_cut_pin_release_waiting_for_test;
}

bool reserve_ack_public_cursor_for_test(
    const Head &head, const AckReadyEvent &ack, const CommitBinding &binding,
    const Cursor &pre_ack_public_cursor, std::string *error) {
  if (error != nullptr) error->clear();
  std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
  return !reserve_ack_public_cursor_locked(head, ack, binding,
                                           pre_ack_public_cursor, error);
}

bool verify_ack_head_for_test(const Head &head, std::string *error) {
  if (error != nullptr) error->clear();
  HeadPublisher *publisher = nullptr;
  {
    std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
    publisher = g_runtime.publisher.get();
  }
  if (publisher == nullptr)
    return fail_with(error, "ACK test publisher is not initialized");
  PublishResult verified = publisher->verify_decision(head);
  if (verified.applied()) return false;
  if (error != nullptr) {
    *error = "ACK test HEAD verification failed";
    if (!verified.detail.empty()) error->append(": ").append(verified.detail);
  }
  return true;
}

Cursor public_committed_cursor_for_test() {
  std::lock_guard<std::mutex> guard(g_runtime.state_mutex);
  return g_runtime.public_committed_cursor;
}

uint64_t hold_runtime_snapshot_order_ticket_for_test(uint64_t request_id) {
  std::lock_guard<std::mutex> guard(g_snapshot_order.mutex);
  return hold_snapshot_order_ticket_locked(request_id);
}

bool runtime_snapshot_order_ticket_held_for_test(uint64_t request_id) {
  std::lock_guard<std::mutex> guard(g_snapshot_order.mutex);
  return g_snapshot_order.request_id.has_value() &&
         g_snapshot_order.ticket.has_value() &&
         *g_snapshot_order.request_id == request_id;
}

void release_runtime_snapshot_order_ticket_for_test(uint64_t request_id) {
  std::lock_guard<std::mutex> guard(g_snapshot_order.mutex);
  (void)release_snapshot_order_ticket_locked(request_id);
}

uint64_t acquire_order_ticket_for_test() { return acquire_ticket(); }

void release_order_ticket_for_test(uint64_t ticket) { release_ticket(ticket); }
#endif

}  // namespace wesql::remote_commit
