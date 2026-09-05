/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_SERVER_HOOKS_INCLUDED
#define SQL_REMOTE_COMMIT_SERVER_HOOKS_INCLUDED

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "sql/remote_commit/local_install.h"
#include "sql/remote_commit/publisher.h"
#include "sql/remote_commit/protocol_codec.h"

class MYSQL_BIN_LOG;
class THD;
namespace objstore {
class ObjectStore;
}

namespace wesql::remote_commit {

struct OrderToken;
struct AckReadyEvent;
struct CommitBinding;
struct PreparedSnapshotPublication;
struct SnapshotPublication;
class SnapshotPublisher;
class CloneCutBinlogPin;

bool enabled();
bool is_fenced();

enum class StartupRoute : uint8_t {
  DISABLED,
  BOOTSTRAP,
  TAKEOVER,
};

enum class StartupEpochAdoptionRole : uint8_t {
  TAKEOVER_RECOVERY,
  BOOTSTRAP_SNAPSHOT,
  INSTALLED_ROOT,
};

// Exact result of the HEAD-first, mutation-free namespace probe. An absent
// HEAD always selects BOOTSTRAP, including the epoch-without-HEAD crash window.
struct StartupProbe {
  StartupRoute route{StartupRoute::DISABLED};
  bool epoch_present{false};
  uint64_t epoch{0};
  std::string head_body;
  std::string head_etag;
  uint64_t head_generation{0};
  std::string durable_file;
  uint64_t durable_pos{0};
};

// Exact create/CAS result returned by the epoch acquisition hook. The startup
// coordinator must carry these bytes, ETag, and parsed value unchanged through
// snapshot publication and final activation.
struct StartupEpochProof {
  std::string body;
  std::string etag;
  WriterEpoch value;
  // The adapter binds the final published HEAD here before persisting the
  // proof across re-exec. For takeover acquisition these initially name the
  // probed candidate; for bootstrap acquisition they are initially empty.
  std::string head_body;
  std::string head_etag;
  uint64_t head_generation{0};
};

// Bounded evidence available before stock TC/binlog recovery opens the
// atomically installed root. marker_matches means the marker read from that
// root exactly equals the restart proof marker; root_identity_matches means
// mysqld is configured to open that exact root. Live engine evidence is
// deliberately absent from this phase.
struct InstalledRootActivationProof {
  std::string head_body;
  std::string head_etag;
  LocalInstallMarker marker;
  std::string recovered_file;
  uint64_t recovered_pos{0};
  bool marker_matches{false};
  bool root_identity_matches{false};
};

// Full evidence supplied only after the installed root's engines, GTID state,
// data dictionary, and repositories are open. The hook rechecks the exact
// pre-recovery HEAD and writer epoch before admitting writes.
struct InstalledRootProof {
  std::string head_body;
  std::string head_etag;
  std::string recovered_file;
  uint64_t recovered_pos{0};
  std::string canonical_gtid;
  std::string gtid_sha256;
  bool marker_matches{false};
  bool root_identity_matches{false};
  bool snapshot_matches{false};
  bool server_uuid_matches{false};
  bool configuration_matches{false};
  bool gtid_matches{false};
  bool dd_matches{false};
  bool repository_empty{false};
  bool extent_live_set_matches{false};
  bool internal_prepared_empty{false};
  bool external_xa_empty{false};
};

struct ImmutableExtentRuntime {
  std::string cluster_object_prefix;
  std::string stream_sha256;
  uint64_t writer_epoch{0};
  objstore::ObjectStore *object_store{nullptr};
  std::string bucket;
};

struct StartupIoRuntime {
  StreamIdentity stream;
  objstore::ObjectStore *object_store{nullptr};
  ConditionalIo *conditional_io{nullptr};
  HeadPublisher *publisher{nullptr};
  std::string bucket;
};

enum class RuntimeSnapshotRequestReason : uint8_t {
  SOFT_LIMIT,
  HARD_LIMIT,
};

enum class RuntimeSnapshotRequestOutcome : uint8_t {
  ACTIVE,
  COMPLETED,
  BLOCKED,
  REFIX_REQUIRED,
  FENCED,
  PERMANENT_ERROR,
  SHUTDOWN,
};

// One notification remains owned by request_id from the first proactive
// threshold crossing through its ordered SNAPSHOT publication. A later hard
// limit upgrades the same live request instead of creating a competing cut.
struct RuntimeSnapshotRequest {
  uint64_t request_id{0};
  RuntimeSnapshotRequestReason reason{RuntimeSnapshotRequestReason::SOFT_LIMIT};
  RecoveryWindow published_window;
  RecoveryWindow prospective_window;
};

struct RuntimeSnapshotRequestResult {
  RuntimeSnapshotRequestOutcome outcome{
      RuntimeSnapshotRequestOutcome::PERMANENT_ERROR};
  std::optional<RuntimeSnapshotRequest> request;
  std::string detail;
};

// Immutable cut-time authority copied while the matching clone barrier owns
// CLOSED, drained admission. Every pointer is server-owned and remains valid
// only while the remote runtime remains initialized.
struct RuntimeSnapshotAuthority {
  uint64_t request_id{0};
  StreamIdentity stream;
  PublisherState publisher_state;
  Writer writer;
  ConditionalIo *metadata_io{nullptr};
  ProtocolStore *metadata_store{nullptr};
  HeadPublisher *head_publisher{nullptr};
};

// Immutable server state bound to one runtime snapshot request and one
// physical-clone REDO_COPY cut. Both digests are lowercase 64-character
// SHA-256 hex strings.
struct CloneCutState {
  uint64_t request_id{0};
  std::string file;
  uint64_t pos{0};
  std::string canonical_gtid;
  std::string gtid_sha256;
  uint64_t head_generation{0};
  std::string head_body_sha256;

  bool operator==(const CloneCutState &) const = default;
};

class CloneCutBarrierLease {
 public:
  CloneCutBarrierLease();
  ~CloneCutBarrierLease();
  CloneCutBarrierLease(CloneCutBarrierLease &&other) noexcept;
  CloneCutBarrierLease &operator=(CloneCutBarrierLease &&other) noexcept;
  CloneCutBarrierLease(const CloneCutBarrierLease &) = delete;
  CloneCutBarrierLease &operator=(const CloneCutBarrierLease &) = delete;

  bool active() const { return token_ != 0; }

 private:
  friend bool begin_clone_cut_barrier(uint64_t, CloneCutState *,
                                      CloneCutBarrierLease *, std::string *);
  friend bool verify_clone_cut_barrier(const CloneCutState &,
                                       const CloneCutBarrierLease &,
                                       std::string *);
  friend bool verify_clone_cut_barrier_locked(
      const CloneCutState &, const CloneCutBarrierLease &, std::string *);
  friend void end_clone_cut_barrier(CloneCutBarrierLease *);
  friend bool materialize_clone_cut_binlog_seed(
      const CloneCutState &, const CloneCutBarrierLease &,
      const std::filesystem::path &, std::string *);

  uint64_t token_{0};
  uint64_t request_id_{0};
  std::unique_ptr<CloneCutBinlogPin> binlog_pin_;
};

// Returns one coherent, read-only snapshot of the identity used to derive and
// create SmartEngine immutable v2 extent objects. The ObjectStore remains
// server-owned and must not be deleted by the caller.
bool immutable_extent_runtime(ImmutableExtentRuntime *runtime);

// Returns the already initialized startup dependencies without constructing a
// second provider or client. Every pointer remains server-owned and is invalid
// after deinitialize().
bool startup_io_runtime(StartupIoRuntime *runtime);

// Initializes the fixed stream identity, conditional client, and HEAD-first
// startup probe. MySQL-style return: false is success, true is error.
bool initialize();
bool initialize(bool bootstrap_preflight);
void shutdown();
void deinitialize();

// The coordinator must inspect the probe before acquiring an epoch so target
// and EMPTY_SOURCE validation happens on the correct route. These functions
// use MySQL-style return values: false is success, true is error.
bool startup_probe(StartupProbe *probe);
bool acquire_startup_epoch(StartupEpochProof *proof);
bool adopt_startup_epoch(const StartupEpochProof &proof,
                         StartupEpochAdoptionRole role);
// Re-exec pre-recovery transition. This exact-verifies the adopted HEAD,
// writer epoch, installed marker, and target-root identity. It keeps admission
// closed, keeps the adopted publisher read-only, and authorizes only the one
// stock recovery bypass needed to open the installed root.
bool activate_installed_root(const InstalledRootActivationProof &proof);

// Re-exec post-engine transition. Every full installed-root authority must be
// present, and HEAD/epoch must still equal the pre-recovery activation. Success
// promotes the adopted publisher and permits a later open_commit_admission()
// call; it does not open admission.
bool verify_installed_root_post_engine(const InstalledRootProof &proof);
std::string startup_error();

// Allows only EMPTY_SOURCE initialization of a known fresh bootstrap root.
// It never authorizes native replay or write admission.
bool may_run_startup_bootstrap_worker();

// Only compiled-in DD/system-table statements in the isolated initialize
// child may mutate its unpublished empty root without a remote LOG decision.
bool may_initialize_empty_root();
bool may_initialize_system_tables(const THD *thd);

// True only after a fresh bootstrap snapshot/export worker adopted the exact
// parent epoch while HEAD was still absent. It never grants native replay,
// remote publication, or write admission.
bool may_run_startup_bootstrap_snapshot_worker();

// True only for the pre-activation takeover worker: the parent's exact epoch
// and candidate HEAD were adopted read-only, lifecycle is RECOVERING, and
// commit admission remains CLOSED and drained. This is the only earlier phase
// in which a server adapter may open the reconstructed root while explicitly
// bypassing stock Binlog_recovery and installing replay authorizations.
bool may_run_startup_recovery_worker();

// Returns the exact existing terminal binlog boundary only while an authorized
// takeover worker or installed re-exec is opening its reconstructed root.
// Bootstrap routes and all ordinary runtime phases return false.
bool startup_existing_binlog_boundary(std::string *file, uint64_t *pos);

// True only for one exact CLOSED-admission startup role: installed re-exec
// after pre-recovery activation, bootstrap snapshot worker, or takeover
// recovery worker. Normal startup, bootstrap preflight, and post-engine ACTIVE
// operation are never authorized.
bool may_bypass_stock_binlog_recovery();

// Returns the atomically formatted machine status. An empty result means the
// formatter detected an invariant violation and the caller must fail closed.
std::string status_json();

// The value used by both the native range sealer and pre-prepare cache gate.
uint64_t maximum_segment_bytes();

[[noreturn]] void fail_stop(const char *reason);
[[noreturn]] void fence(const char *reason);

// The private cursor wakes only the native range sealer. It is not visible to
// dumpers, status, clients, or legacy archive durability consumers.
void note_local_flush(const char *file, uint64_t pos);

// Performs final_queue-wide validation, seals native ranges, publishes the
// immutable manifest and HEAD, checks epoch ownership, and installs one-shot
// authorization for every durable member. The returned FIFO token remains
// owned by the group leader through engine commit and signal_done().
OrderToken *decide_group(MYSQL_BIN_LOG *binlog, THD *final_queue);

// Both are fatal invariant checks after HEAD has decided the group.
void check_engine_commits(THD *commit_queue, OrderToken *token);
void verify_before_ack(THD *final_queue, OrderToken *token);

// Logs REMOTE_COMMIT_ACK_READY and publishes the external binlog cursor. It
// must run immediately before signal_done().
void emit_ack_and_publish_cursor(MYSQL_BIN_LOG *binlog, THD *final_queue,
                                 OrderToken *token);

void release_order_token(OrderToken *token);

// Early and final engine-commit guards. The final call is made immediately
// before the first handlerton commit callback and consumes authorization only
// for a real read-write transaction.
void check_commit_authorization(THD *thd, bool all);
void consume_commit_authorization(THD *thd, bool all, bool is_real_trans,
                                  bool has_read_write_engine);

// A recovery authorization exists only around one already validated native
// transaction. Installation requires EPOCH_ACQUIRED + RECOVERING, exact
// candidate HEAD identity, and CLOSED/drained admission. finish removes the
// record on every path and, for a durable transaction, requires that the
// ha_commit_low guard consumed it exactly once. MySQL-style return values:
// false is success, true is error.
bool install_recovery_commit_authorization(THD *thd,
                                           const CommitBinding &binding,
                                           std::string *error);
bool finish_recovery_commit_authorization(THD *thd,
                                          bool require_consumed,
                                          std::string *error);
void discard_recovery_commit_authorization(THD *thd);

// Snapshot admission uses the same state transition for every potentially
// durable transaction. begin returns true with no admission when graceful
// shutdown has closed the gate, so the caller can roll back normally. An
// admitted transaction remains counted through its complete commit return;
// shutdown keeps the remote runtime ACTIVE until the set drains.
bool begin_commit_admission(THD *thd, bool potentially_durable);
void end_commit_admission(THD *thd, bool decided);
bool close_commit_admission_and_wait();
void open_commit_admission();

// Consumes the current notification once. The blocking form returns true only
// when shutdown cancels the wait; false means request was populated. Refresh
// returns the latest reason for the same live request so a SOFT request can be
// promoted to HARD without losing its fixed-cut ownership.
bool take_runtime_snapshot_request(RuntimeSnapshotRequest *request);
bool wait_for_runtime_snapshot_request(RuntimeSnapshotRequest *request);
RuntimeSnapshotRequestResult refresh_runtime_snapshot_request(
    uint64_t request_id);

// A hard reservation atomically owns CLOSED admission and drains every normal
// admission before the matching clone cut begins. It is idempotent for the
// matching request. Typed completion linearizes the request's final reason,
// validates the exact published SNAPSHOT lineage for a HARD request, and
// reopens only after the source lease releases the matching clone barrier.
bool reserve_runtime_snapshot_hard_gate(uint64_t request_id,
                                        std::string *error);
RuntimeSnapshotRequestResult complete_runtime_snapshot_request(
    uint64_t request_id);
void mark_runtime_snapshot_terminal(uint64_t request_id,
                                    RuntimeSnapshotRequestOutcome outcome,
                                    std::string_view detail);

// Captures immutable publisher authority only for the matching active clone
// request while admission is CLOSED/drained and the runtime is exactly
// RUNNING at the public durable cursor.
bool runtime_snapshot_authority(uint64_t request_id,
                                RuntimeSnapshotAuthority *authority,
                                std::string *error);

// Serializes the final prepared SNAPSHOT transition with every LOG transition
// in the FIFO remote order domain. The exact published recovery window is
// installed into admission only after the order ticket has been released.
PublishResult publish_prepared_runtime_snapshot(
    uint64_t request_id, SnapshotPublisher *snapshot_publisher,
    const PreparedSnapshotPublication &prepared,
    SnapshotPublication *publication);

// A clone cut lease has exclusive ownership of CLOSED admission. begin closes
// and drains admission before it exact-verifies HEAD/epoch and snapshots the
// engine-committed public cursor plus executed GTIDs. Before begin returns it
// also registers a purge-safe file reference for that exact cursor under the
// binlog index lock. verify requires the same lease, pin, and state to remain
// unchanged. A successful lease is moved into the clone handle/coordinator;
// destruction or explicit end consumes it and reopens admission exactly once.
// request_id must be nonzero and is retained in both the state and lease;
// moved-from, duplicate, and stale request authorities are harmless.
// MySQL-style return: false is success, true is error.
bool begin_clone_cut_barrier(uint64_t request_id, CloneCutState *state,
                             CloneCutBarrierLease *lease, std::string *error);
bool verify_clone_cut_barrier(const CloneCutState &state,
                              const CloneCutBarrierLease &lease,
                              std::string *error);
void end_clone_cut_barrier(CloneCutBarrierLease *lease);

// Copies exactly [0, state.pos) from the purge-pinned binlog into a newly
// created destination. The matching lease remains validated and protected by
// clone_cut_mutex for the whole copy. Existing paths and symlinks are rejected;
// failures remove only a partial file created by this call.
bool materialize_clone_cut_binlog_seed(
    const CloneCutState &state, const CloneCutBarrierLease &lease,
    const std::filesystem::path &destination, std::string *error);

#ifdef WESQL_TEST
struct StartupPolicy;
StartupPolicy configured_startup_policy_for_test();
// Creates only the server-hook runtime around a fake conditional store and
// performs the real mutation-free publisher probe. Tests then drive the public
// adoption/activation APIs and their exact remote readbacks. The ConditionalIo
// remains caller-owned through deinitialize().
bool initialize_startup_lifecycle_for_test(ConditionalIo *conditional_io,
                                           const StreamIdentity &stream,
                                           bool bootstrap_preflight = false);
void reset_startup_lifecycle_for_test();
// Resets only the admission primitive for focused commit-path tests. It does
// not manufacture a startup phase, installed HEAD proof, or RUNNING state.
void reset_commit_admission_for_test(bool open);
std::size_t commit_admission_count_for_test();
void set_runtime_snapshot_window_for_test(uint64_t generation,
                                          const RecoveryWindow &window);
RecoveryWindow runtime_snapshot_reservations_for_test();
bool runtime_snapshot_hard_gate_for_test();
bool note_runtime_snapshot_publication_for_test(uint64_t request_id,
                                                const Head &head,
                                                std::string *error);
// A non-null source bypasses object-store/GTID startup only for focused barrier
// lifecycle tests. Passing null clears the seam.
void set_clone_cut_source_for_test(const CloneCutState *source);
void set_clone_cut_binlog_source_path_for_test(
    const std::filesystem::path *source);
void set_clone_cut_public_cursor_for_test(const char *file, uint64_t pos);
bool clone_cut_barrier_active_for_test();
bool commit_admission_open_for_test();
bool commit_admission_draining_for_test();
bool commit_admission_reopening_for_test();
bool runtime_snapshot_terminal_for_test();
void pause_clone_cut_pin_release_for_test(bool pause);
bool clone_cut_pin_release_waiting_for_test();
// Exercises the same final exact-state reservation used immediately before
// ACK/external-cursor publication. MySQL-style return: false is success.
bool reserve_ack_public_cursor_for_test(const Head &head,
                                        const AckReadyEvent &ack,
                                        const CommitBinding &binding,
                                        const Cursor &pre_ack_public_cursor,
                                        std::string *error);
bool verify_ack_head_for_test(const Head &head, std::string *error);
Cursor public_committed_cursor_for_test();
// Focused FIFO probes. Repeated hold calls for one request reuse the same
// serving ticket; a normal ticket waits behind it until explicit release.
uint64_t hold_runtime_snapshot_order_ticket_for_test(uint64_t request_id);
bool runtime_snapshot_order_ticket_held_for_test(uint64_t request_id);
void release_runtime_snapshot_order_ticket_for_test(uint64_t request_id);
uint64_t acquire_order_ticket_for_test();
void release_order_ticket_for_test(uint64_t ticket);
#endif

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_SERVER_HOOKS_INCLUDED
