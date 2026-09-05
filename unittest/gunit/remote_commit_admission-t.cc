/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "mysqld_error.h"
#include "scope_guard.h"
#include "sql/handler.h"
#include "sql/mysqld.h"
#include "sql/partition_info.h"
#include "sql/remote_commit/policy.h"
#include "sql/remote_commit/protocol_codec.h"
#include "sql/remote_commit/evidence.h"
#include "sql/remote_commit/publisher.h"
#include "sql/remote_commit/segment_sealer.h"
#include "sql/remote_commit/server_hooks.h"
#include "sql/remote_commit/sql_admission.h"
#include "sql/rpl_gtid.h"
#include "sql/sql_lex.h"
#include "sql/tc_log.h"
#include "sql/xa.h"
#include "unittest/gunit/handler-t.h"
#include "unittest/gunit/test_utils.h"

namespace remote_commit_admission_unittest {
namespace {

using namespace std::chrono_literals;
using my_testing::Server_initializer;
namespace fs = std::filesystem;

TEST(RemoteCommitStartupPolicy, ReadsParsedGtidBeforeRuntimeInitialization) {
  const auto saved_mode = Gtid_mode::sysvar_mode;
  const auto saved_consistency = _gtid_consistency_mode;
  const auto saved_runtime = global_gtid_mode.get();
  auto *const saved_lock = global_tsid_lock;
  auto restore = create_scope_guard([&] {
    Gtid_mode::sysvar_mode = saved_mode;
    _gtid_consistency_mode = saved_consistency;
    global_gtid_mode.set(saved_runtime);
    global_tsid_lock = saved_lock;
  });
  global_tsid_lock = nullptr;
  global_gtid_mode.set(Gtid_mode::OFF);
  Gtid_mode::sysvar_mode = Gtid_mode::ON;
  _gtid_consistency_mode = GTID_CONSISTENCY_MODE_ON;

  const auto policy =
      wesql::remote_commit::configured_startup_policy_for_test();
  EXPECT_TRUE(policy.gtid_mode_on);
  EXPECT_TRUE(policy.enforce_gtid_consistency);
  EXPECT_TRUE(policy.anonymous_gtid_forbidden);
  EXPECT_EQ(Gtid_mode::OFF, global_gtid_mode.get());
  EXPECT_EQ(nullptr, global_tsid_lock);

  for (const auto mode : {Gtid_mode::OFF, Gtid_mode::OFF_PERMISSIVE,
                          Gtid_mode::ON_PERMISSIVE}) {
    Gtid_mode::sysvar_mode = mode;
    const auto rejected =
        wesql::remote_commit::configured_startup_policy_for_test();
    EXPECT_FALSE(rejected.gtid_mode_on);
    EXPECT_FALSE(rejected.anonymous_gtid_forbidden);
  }
  Gtid_mode::sysvar_mode = Gtid_mode::ON;
  for (const auto mode : {GTID_CONSISTENCY_MODE_OFF,
                          GTID_CONSISTENCY_MODE_WARN}) {
    _gtid_consistency_mode = mode;
    EXPECT_FALSE(wesql::remote_commit::configured_startup_policy_for_test()
                     .enforce_gtid_consistency);
  }
}

class Scoped_temp_directory {
 public:
  Scoped_temp_directory() {
    std::string pattern =
        (fs::temp_directory_path() / "wesql-admission-XXXXXX").string();
    char *created = ::mkdtemp(pattern.data());
    if (created != nullptr) path_ = created;
  }

  ~Scoped_temp_directory() {
    std::error_code ignored;
    fs::remove_all(path_, ignored);
  }

  const fs::path &path() const { return path_; }

 private:
  fs::path path_;
};

void write_test_file(const fs::path &path, std::string_view bytes) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  ASSERT_TRUE(static_cast<bool>(output));
}

std::string read_test_file(const fs::path &path) {
  std::ifstream input(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(input),
          std::istreambuf_iterator<char>()};
}

TEST(RemoteCommitSegmentSealer, ConcurrentAppendKeepsExactValidatedRange) {
  namespace rc = wesql::remote_commit;
  class Store final : public rc::ConditionalIo {
   public:
    objstore::ExactObjectResult get(std::string_view, uint64_t) override {
      return objstore::ExactObjectResult::found(body, "\"segment\"");
    }
    objstore::ConditionalPutResult put(
        std::string_view, std::string_view bytes,
        const objstore::ConditionalPutCondition &) override {
      body = bytes;
      return objstore::ConditionalPutResult::applied("\"segment\"");
    }
    std::string body;
  };

  for (const std::string change : {"append", "truncate", "replace", "corrupt"}) {
    SCOPED_TRACE(change);
    Scoped_temp_directory directory;
    ASSERT_FALSE(directory.path().empty());
    const fs::path path = directory.path() / "binlog.000001";
    std::string body(32, '\0');
    body[4] = 2;
    body[9] = static_cast<char>(body.size());
    write_test_file(path, std::string(4, 'x') + body);
    rc::NativeBinlogRange range;
    range.local_path = path.string();
    range.source = {"binlog.000001", 4, 4 + body.size()};
    range.metadata.transaction_count = 1;
    range.metadata.gtid_set = "11111111-1111-1111-1111-111111111111:1";
    range.metadata.xids = {1};
    std::string error;
    ASSERT_TRUE(rc::sha256_hex(body, &range.metadata.native_sha256, &error));
    rc::StreamIdentity stream;
    ASSERT_TRUE(rc::build_stream_identity("repo", "main", "repo/main",
                                          &stream, &error));
    const rc::Writer writer{"11111111111111111111111111111111", 1};
    rc::SegmentTip tip;
    tip.kind = rc::SegmentTipKind::SNAPSHOT_ROOT;
    tip.snapshot_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    tip.cursor = rc::Cursor{range.source.file, range.source.start_pos};
    Store io;
    rc::ProtocolStore store(&io);
    rc::SegmentSealer sealer(&store, stream, 1024);
    if (change == "corrupt") {
      std::string changed = body;
      changed.back() = 'x';
      write_test_file(path, std::string(4, 'x') + changed);
    } else {
      sealer.set_after_read_for_test([&] {
        if (change == "append") {
          std::ofstream append(path, std::ios::binary | std::ios::app);
          append << "next group";
          ASSERT_TRUE(static_cast<bool>(append));
        } else if (change == "truncate") {
          fs::resize_file(path, 4);
        } else {
          const fs::path replacement = directory.path() / "replacement";
          write_test_file(replacement, std::string(4, 'x') + body);
          fs::rename(replacement, path);
        }
      });
    }
    rc::SealedSegments sealed;
    const auto result = sealer.seal(writer, 1, tip, {range}, &sealed);
    if (change == "append") {
      ASSERT_TRUE(result.applied()) << result.detail;
      ASSERT_EQ(1U, sealed.segments.size());
      EXPECT_EQ(body, io.body);
      EXPECT_EQ(range.metadata.native_sha256, sealed.segments[0].sha256);
      EXPECT_EQ(range.source.end_pos, sealed.durable_cursor.pos);
    } else {
      EXPECT_EQ(rc::PublishOutcome::PERMANENT_ERROR, result.outcome);
      EXPECT_TRUE(io.body.empty());
    }
  }
}

int fake_engine_prepare(handlerton *, THD *, bool) { return 0; }

class Admission_probe_tc_log final : public TC_LOG {
 public:
  enum class Probe_point { PREPARE, COMMIT };

  explicit Admission_probe_tc_log(Probe_point probe_point,
                                  int prepare_result = 0,
                                  enum_result commit_result = RESULT_SUCCESS)
      : m_probe_point(probe_point),
        m_prepare_result(prepare_result),
        m_commit_result(commit_result) {}

  ~Admission_probe_tc_log() override {
    if (m_closer.joinable()) m_closer.join();
  }

  int open(const char *) override { return 0; }
  void close() override {}

  enum_result commit(THD *thd, bool) override {
    ++commit_calls;
    if (m_probe_point == Probe_point::COMMIT) start_close_probe(thd);
    return m_commit_result;
  }

  int rollback(THD *, bool) override {
    ++rollback_calls;
    return 0;
  }

  int prepare(THD *thd, bool) override {
    ++prepare_calls;
    if (m_probe_point == Probe_point::PREPARE) start_close_probe(thd);
    return m_prepare_result;
  }

  bool finish_close_probe(THD *thd) {
    bool completed_without_help = false;
    {
      std::unique_lock<std::mutex> lock(m_mutex);
      completed_without_help =
          m_condition.wait_for(lock, 1s, [&] { return m_close_finished; });
    }
    if (!completed_without_help) {
      wesql::remote_commit::end_commit_admission(thd, false);
      std::unique_lock<std::mutex> lock(m_mutex);
      (void)m_condition.wait_for(lock, 1s, [&] { return m_close_finished; });
    }
    if (m_closer.joinable()) m_closer.join();
    return completed_without_help;
  }

  bool probe_started() const {
    std::lock_guard<std::mutex> guard(m_mutex);
    return m_close_started;
  }

  std::size_t observed_admission_count() const {
    std::lock_guard<std::mutex> guard(m_mutex);
    return m_observed_admission_count;
  }

  bool close_was_blocked() const {
    std::lock_guard<std::mutex> guard(m_mutex);
    return m_close_blocked;
  }

  bool close_finished() const {
    std::lock_guard<std::mutex> guard(m_mutex);
    return m_close_finished;
  }

  bool close_result() const {
    std::lock_guard<std::mutex> guard(m_mutex);
    return m_close_result;
  }

  int prepare_calls{0};
  int commit_calls{0};
  int rollback_calls{0};

 private:
  void start_close_probe(THD *) {
    ASSERT_FALSE(m_closer.joinable());
    {
      std::lock_guard<std::mutex> guard(m_mutex);
      m_observed_admission_count =
          wesql::remote_commit::commit_admission_count_for_test();
    }
    m_closer = std::thread([this] {
      {
        std::lock_guard<std::mutex> guard(m_mutex);
        m_close_started = true;
      }
      m_condition.notify_all();

      const bool close_result =
          wesql::remote_commit::close_commit_admission_and_wait();
      {
        std::lock_guard<std::mutex> guard(m_mutex);
        m_close_result = close_result;
        m_close_finished = true;
      }
      m_condition.notify_all();
    });

    std::unique_lock<std::mutex> lock(m_mutex);
    ASSERT_TRUE(
        m_condition.wait_for(lock, 1s, [&] { return m_close_started; }));
    m_close_blocked =
        !m_condition.wait_for(lock, 50ms, [&] { return m_close_finished; });
  }

  const Probe_point m_probe_point;
  const int m_prepare_result;
  const enum_result m_commit_result;
  mutable std::mutex m_mutex;
  std::condition_variable m_condition;
  std::thread m_closer;
  bool m_close_started{false};
  bool m_close_blocked{false};
  bool m_close_finished{false};
  bool m_close_result{false};
  std::size_t m_observed_admission_count{0};
};

class RemoteCommitAdmissionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    m_initializer.SetUp();
    m_saved_remote_commit = opt_binlog_archive_remote_commit;
    m_saved_tc_log = tc_log;
    opt_binlog_archive_remote_commit = true;
    wesql::remote_commit::reset_commit_admission_for_test(true);
    m_clone_cut.request_id = 9001;
    m_clone_cut.file = "binlog.000001";
    m_clone_cut.pos = 512;
    m_clone_cut.canonical_gtid =
        "24bc785e-9f5c-11ee-a6f6-0242ac120002:1-7";
    wesql::remote_commit::GtidSetDigest digest;
    std::string error;
    ASSERT_TRUE(wesql::remote_commit::gtid_digest(
        m_clone_cut.canonical_gtid, &digest, &error))
        << error;
    m_clone_cut.canonical_gtid = std::move(digest.canonical);
    m_clone_cut.gtid_sha256 = std::move(digest.sha256);
    m_clone_cut.head_generation = 9;
    m_clone_cut.head_body_sha256 = std::string(64, 'a');
    wesql::remote_commit::set_clone_cut_source_for_test(&m_clone_cut);
  }

  void TearDown() override {
    wesql::remote_commit::reset_commit_admission_for_test(false);
    wesql::remote_commit::set_clone_cut_source_for_test(nullptr);
    tc_log = m_saved_tc_log;
    opt_binlog_archive_remote_commit = m_saved_remote_commit;
    m_initializer.TearDown();
  }

  THD *thd() { return m_initializer.thd(); }

  const wesql::remote_commit::CloneCutState &clone_cut() const {
    return m_clone_cut;
  }

  void register_rw_engine(Fake_handlerton *hton, uint slot) {
    hton->slot = slot;
    hton->db_type = DB_TYPE_BINLOG;
    hton->prepare = fake_engine_prepare;
    trans_register_ha(thd(), false, hton, nullptr);
    thd()->get_ha_data(slot)->ha_info[0].set_trx_read_write();
  }

  void expect_completed_probe(Admission_probe_tc_log *log) {
    EXPECT_TRUE(log->probe_started());
    EXPECT_EQ(1U, log->observed_admission_count());
    EXPECT_TRUE(log->close_was_blocked());
    EXPECT_EQ(0U,
              wesql::remote_commit::commit_admission_count_for_test());
    EXPECT_TRUE(log->finish_close_probe(thd()));
    EXPECT_TRUE(log->close_finished());
    EXPECT_FALSE(log->close_result());
  }

  void expect_predispatch_rejected(enum_sql_command command,
                                   HA_CREATE_INFO *create_info = nullptr,
                                   partition_info *part_info = nullptr) {
    LEX *const lex = thd()->lex;
    lex->sql_command = command;
    lex->create_info = create_info;
    lex->part_info = part_info;

    Server_initializer::set_expected_error(ER_NOT_SUPPORTED_YET);
    EXPECT_TRUE(wesql::remote_commit::enforce_sql_command_admission(thd()));
    Server_initializer::set_expected_error(0);
    EXPECT_FALSE(thd()->is_error());

    thd()->clear_error();
    lex->create_info = nullptr;
    lex->part_info = nullptr;
  }

  void expect_predispatch_allowed(enum_sql_command command,
                                  HA_CREATE_INFO *create_info = nullptr,
                                  partition_info *part_info = nullptr) {
    LEX *const lex = thd()->lex;
    lex->sql_command = command;
    lex->create_info = create_info;
    lex->part_info = part_info;

    EXPECT_FALSE(wesql::remote_commit::enforce_sql_command_admission(thd()));
    EXPECT_FALSE(thd()->is_error());

    lex->create_info = nullptr;
    lex->part_info = nullptr;
  }

 private:
  Server_initializer m_initializer;
  wesql::remote_commit::CloneCutState m_clone_cut;
  bool m_saved_remote_commit{false};
  TC_LOG *m_saved_tc_log{nullptr};
};

TEST_F(RemoteCommitAdmissionTest, StartsBeforeTwoPhasePrepare) {
  Fake_handlerton first;
  Fake_handlerton second;
  register_rw_engine(&first, 0);
  register_rw_engine(&second, 1);
  Admission_probe_tc_log log(
      Admission_probe_tc_log::Probe_point::PREPARE);
  tc_log = &log;

  EXPECT_EQ(0, ha_commit_trans(thd(), false, true));
  EXPECT_EQ(1, log.prepare_calls);
  EXPECT_EQ(1, log.commit_calls);
  EXPECT_EQ(0, log.rollback_calls);
  expect_completed_probe(&log);
}

TEST_F(RemoteCommitAdmissionTest, PrepareFailureReleasesAdmission) {
  Fake_handlerton first;
  Fake_handlerton second;
  register_rw_engine(&first, 0);
  register_rw_engine(&second, 1);
  Admission_probe_tc_log log(Admission_probe_tc_log::Probe_point::PREPARE,
                             1);
  tc_log = &log;

  EXPECT_EQ(1, ha_commit_trans(thd(), false, true));
  EXPECT_EQ(1, log.prepare_calls);
  EXPECT_EQ(0, log.commit_calls);
  EXPECT_EQ(1, log.rollback_calls);
  expect_completed_probe(&log);
}

TEST_F(RemoteCommitAdmissionTest, OnePhaseCommitIsAlsoAdmitted) {
  Fake_handlerton engine;
  register_rw_engine(&engine, 0);
  Admission_probe_tc_log log(Admission_probe_tc_log::Probe_point::COMMIT);
  tc_log = &log;

  EXPECT_EQ(0, ha_commit_trans(thd(), false, true));
  EXPECT_EQ(0, log.prepare_calls);
  EXPECT_EQ(1, log.commit_calls);
  EXPECT_EQ(0, log.rollback_calls);
  expect_completed_probe(&log);
}

TEST_F(RemoteCommitAdmissionTest,
       GracefulShutdownDrainsAdmittedAndRejectsNewCommitters) {
  ASSERT_FALSE(wesql::remote_commit::begin_commit_admission(thd(), true));

  std::thread shutdown([] { wesql::remote_commit::shutdown(); });
  auto cleanup = create_scope_guard([&] {
    wesql::remote_commit::end_commit_admission(thd(), false);
    if (shutdown.joinable()) shutdown.join();
  });
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (!wesql::remote_commit::commit_admission_draining_for_test() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  const bool draining =
      wesql::remote_commit::commit_admission_draining_for_test();
  if (draining) {
    EXPECT_FALSE(wesql::remote_commit::commit_admission_open_for_test());
    EXPECT_EQ(1U, wesql::remote_commit::commit_admission_count_for_test());

    THD waiting(false);
    EXPECT_TRUE(
        wesql::remote_commit::begin_commit_admission(&waiting, true));
    EXPECT_EQ(1U, wesql::remote_commit::commit_admission_count_for_test());
  }

  wesql::remote_commit::end_commit_admission(thd(), true);
  shutdown.join();
  cleanup.release();
  ASSERT_TRUE(draining);
  EXPECT_EQ(0U, wesql::remote_commit::commit_admission_count_for_test());
  EXPECT_FALSE(wesql::remote_commit::commit_admission_open_for_test());
}

TEST_F(RemoteCommitAdmissionTest, CloneCutDrainsAndTransfersClosedLease) {
  ASSERT_FALSE(wesql::remote_commit::begin_commit_admission(thd(), true));

  std::mutex mutex;
  std::condition_variable condition;
  bool started = false;
  bool finished = false;
  bool begin_error = true;
  std::string error;
  wesql::remote_commit::CloneCutState captured;
  wesql::remote_commit::CloneCutBarrierLease lease;
  std::thread beginner([&] {
    {
      std::lock_guard<std::mutex> guard(mutex);
      started = true;
    }
    condition.notify_all();
    begin_error = wesql::remote_commit::begin_clone_cut_barrier(
        clone_cut().request_id, &captured, &lease, &error);
    {
      std::lock_guard<std::mutex> guard(mutex);
      finished = true;
    }
    condition.notify_all();
  });
  auto cleanup = create_scope_guard([&] {
    wesql::remote_commit::end_commit_admission(thd(), false);
    if (beginner.joinable()) beginner.join();
  });

  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(condition.wait_for(lock, 1s, [&] { return started; }));
    EXPECT_FALSE(condition.wait_for(lock, 50ms, [&] { return finished; }));
  }
  EXPECT_FALSE(wesql::remote_commit::commit_admission_open_for_test());

  wesql::remote_commit::end_commit_admission(thd(), false);
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(condition.wait_for(lock, 1s, [&] { return finished; }));
  }
  beginner.join();
  cleanup.release();

  ASSERT_FALSE(begin_error) << error;
  EXPECT_TRUE(captured == clone_cut());
  EXPECT_TRUE(lease.active());
  EXPECT_TRUE(wesql::remote_commit::clone_cut_barrier_active_for_test());
  EXPECT_FALSE(wesql::remote_commit::commit_admission_open_for_test());
  EXPECT_FALSE(wesql::remote_commit::verify_clone_cut_barrier(captured, lease,
                                                               &error))
      << error;

  wesql::remote_commit::CloneCutBarrierLease transferred(std::move(lease));
  EXPECT_FALSE(lease.active());
  ASSERT_TRUE(transferred.active());
  EXPECT_FALSE(wesql::remote_commit::verify_clone_cut_barrier(
      captured, transferred, &error))
      << error;
  wesql::remote_commit::end_clone_cut_barrier(&transferred);
  EXPECT_FALSE(transferred.active());
  EXPECT_FALSE(wesql::remote_commit::clone_cut_barrier_active_for_test());
  EXPECT_TRUE(wesql::remote_commit::commit_admission_open_for_test());

  wesql::remote_commit::end_clone_cut_barrier(&transferred);
  EXPECT_TRUE(wesql::remote_commit::commit_admission_open_for_test());
}

TEST_F(RemoteCommitAdmissionTest, CloneCutRejectsNestedAndStaleLeaseRelease) {
  std::string error;
  wesql::remote_commit::CloneCutState first_state;
  wesql::remote_commit::CloneCutBarrierLease first;
  ASSERT_FALSE(wesql::remote_commit::begin_clone_cut_barrier(
      clone_cut().request_id, &first_state, &first, &error))
      << error;

  wesql::remote_commit::CloneCutState nested_state;
  wesql::remote_commit::CloneCutBarrierLease nested;
  EXPECT_TRUE(wesql::remote_commit::begin_clone_cut_barrier(
      clone_cut().request_id + 1, &nested_state, &nested, &error));
  EXPECT_FALSE(nested.active());
  EXPECT_TRUE(first.active());
  EXPECT_FALSE(wesql::remote_commit::commit_admission_open_for_test());

  wesql::remote_commit::reset_commit_admission_for_test(true);
  auto second_source = clone_cut();
  ++second_source.request_id;
  wesql::remote_commit::set_clone_cut_source_for_test(&second_source);
  wesql::remote_commit::CloneCutState second_state;
  wesql::remote_commit::CloneCutBarrierLease second;
  ASSERT_FALSE(wesql::remote_commit::begin_clone_cut_barrier(
      second_source.request_id, &second_state, &second, &error))
      << error;
  wesql::remote_commit::end_clone_cut_barrier(&first);
  EXPECT_FALSE(first.active());
  EXPECT_TRUE(second.active());
  EXPECT_TRUE(wesql::remote_commit::clone_cut_barrier_active_for_test());
  EXPECT_FALSE(wesql::remote_commit::commit_admission_open_for_test());
  EXPECT_FALSE(wesql::remote_commit::verify_clone_cut_barrier(
      second_state, second, &error))
      << error;

  wesql::remote_commit::end_clone_cut_barrier(&second);
  EXPECT_TRUE(wesql::remote_commit::commit_admission_open_for_test());
}

TEST_F(RemoteCommitAdmissionTest,
       ShutdownWaitsForCloneCutBinlogPinRelease) {
  std::string error;
  wesql::remote_commit::CloneCutState captured;
  wesql::remote_commit::CloneCutBarrierLease lease;
  ASSERT_FALSE(wesql::remote_commit::begin_clone_cut_barrier(
      clone_cut().request_id, &captured, &lease, &error))
      << error;

  wesql::remote_commit::pause_clone_cut_pin_release_for_test(true);
  std::atomic<bool> release_finished{false};
  std::thread release([&] {
    wesql::remote_commit::end_clone_cut_barrier(&lease);
    release_finished.store(true);
  });
  auto cleanup = create_scope_guard([&] {
    wesql::remote_commit::pause_clone_cut_pin_release_for_test(false);
    if (release.joinable()) release.join();
  });

  const auto release_deadline = std::chrono::steady_clock::now() + 1s;
  while (!wesql::remote_commit::clone_cut_pin_release_waiting_for_test() &&
         std::chrono::steady_clock::now() < release_deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(
      wesql::remote_commit::clone_cut_pin_release_waiting_for_test());
  ASSERT_FALSE(release_finished.load());

  std::atomic<bool> shutdown_finished{false};
  std::thread shutdown([&] {
    wesql::remote_commit::shutdown();
    shutdown_finished.store(true);
  });
  auto shutdown_cleanup = create_scope_guard([&] {
    wesql::remote_commit::pause_clone_cut_pin_release_for_test(false);
    if (shutdown.joinable()) shutdown.join();
  });
  const auto shutdown_deadline = std::chrono::steady_clock::now() + 1s;
  while (!wesql::remote_commit::commit_admission_draining_for_test() &&
         std::chrono::steady_clock::now() < shutdown_deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(wesql::remote_commit::commit_admission_draining_for_test());
  EXPECT_FALSE(shutdown_finished.load());

  wesql::remote_commit::pause_clone_cut_pin_release_for_test(false);
  release.join();
  shutdown.join();
  cleanup.release();
  shutdown_cleanup.release();

  EXPECT_TRUE(release_finished.load());
  EXPECT_TRUE(shutdown_finished.load());
  EXPECT_FALSE(wesql::remote_commit::commit_admission_open_for_test());
}

TEST_F(RemoteCommitAdmissionTest, CloneCutMutationAndBeginFailureDoNotLeak) {
  std::string error;
  wesql::remote_commit::CloneCutState captured;
  wesql::remote_commit::CloneCutBarrierLease lease;
  ASSERT_FALSE(wesql::remote_commit::begin_clone_cut_barrier(
      clone_cut().request_id, &captured, &lease, &error))
      << error;

  wesql::remote_commit::CloneCutState changed = clone_cut();
  ++changed.pos;
  wesql::remote_commit::set_clone_cut_source_for_test(&changed);
  EXPECT_TRUE(wesql::remote_commit::verify_clone_cut_barrier(captured, lease,
                                                              &error));
  EXPECT_TRUE(lease.active());
  EXPECT_TRUE(wesql::remote_commit::clone_cut_barrier_active_for_test());
  EXPECT_FALSE(wesql::remote_commit::commit_admission_open_for_test());
  wesql::remote_commit::end_clone_cut_barrier(&lease);
  EXPECT_TRUE(wesql::remote_commit::commit_admission_open_for_test());

  changed = clone_cut();
  changed.head_body_sha256.resize(63);
  wesql::remote_commit::set_clone_cut_source_for_test(&changed);
  wesql::remote_commit::CloneCutBarrierLease invalid;
  EXPECT_TRUE(wesql::remote_commit::begin_clone_cut_barrier(
      clone_cut().request_id, &captured, &invalid, &error));
  EXPECT_FALSE(invalid.active());
  EXPECT_FALSE(wesql::remote_commit::clone_cut_barrier_active_for_test());
  EXPECT_TRUE(wesql::remote_commit::commit_admission_open_for_test());

  wesql::remote_commit::set_clone_cut_source_for_test(&clone_cut());
  EXPECT_TRUE(wesql::remote_commit::begin_clone_cut_barrier(
      0, &captured, &invalid, &error));
  EXPECT_FALSE(invalid.active());
  EXPECT_TRUE(wesql::remote_commit::commit_admission_open_for_test());

  EXPECT_TRUE(wesql::remote_commit::begin_clone_cut_barrier(
      clone_cut().request_id + 1, &captured, &invalid, &error));
  EXPECT_FALSE(invalid.active());
  EXPECT_NE(error.find("request ID"), std::string::npos);
  EXPECT_FALSE(wesql::remote_commit::clone_cut_barrier_active_for_test());
  EXPECT_TRUE(wesql::remote_commit::commit_admission_open_for_test());

  wesql::remote_commit::set_clone_cut_source_for_test(&clone_cut());
  wesql::remote_commit::set_clone_cut_public_cursor_for_test(
      clone_cut().file.c_str(), clone_cut().pos - 1);
  wesql::remote_commit::CloneCutBarrierLease cursor_mismatch;
  EXPECT_TRUE(wesql::remote_commit::begin_clone_cut_barrier(
      clone_cut().request_id, &captured, &cursor_mismatch, &error));
  EXPECT_FALSE(cursor_mismatch.active());
  EXPECT_FALSE(wesql::remote_commit::clone_cut_barrier_active_for_test());
  EXPECT_TRUE(wesql::remote_commit::commit_admission_open_for_test());

  wesql::remote_commit::set_clone_cut_source_for_test(&clone_cut());
  {
    wesql::remote_commit::CloneCutBarrierLease raii;
    ASSERT_FALSE(wesql::remote_commit::begin_clone_cut_barrier(
        clone_cut().request_id, &captured, &raii, &error))
        << error;
    EXPECT_FALSE(wesql::remote_commit::commit_admission_open_for_test());
  }
  EXPECT_FALSE(wesql::remote_commit::clone_cut_barrier_active_for_test());
  EXPECT_TRUE(wesql::remote_commit::commit_admission_open_for_test());
}

TEST_F(RemoteCommitAdmissionTest,
       LateHardRequestKeepsCloneReleaseAndRollbackClosed) {
  namespace rc = wesql::remote_commit;
  rc::set_runtime_snapshot_window_for_test(9, {99999, 1, 0});

  THD first(false);
  THD waiting(false);
  ASSERT_FALSE(rc::begin_commit_admission(&first, true));
  bool first_admitted = true;
  std::atomic<bool> waiting_started{false};
  std::atomic<bool> waiting_returned{false};
  std::thread waiter;
  auto cleanup = create_scope_guard([&] {
    if (first_admitted) rc::end_commit_admission(&first, false);
    rc::reset_commit_admission_for_test(true);
    if (waiter.joinable()) waiter.join();
  });

  rc::RuntimeSnapshotRequest request;
  ASSERT_TRUE(rc::take_runtime_snapshot_request(&request));
  ASSERT_EQ(1U, request.request_id);
  ASSERT_EQ(rc::RuntimeSnapshotRequestReason::SOFT_LIMIT, request.reason);

  waiter = std::thread([&] {
    waiting_started.store(true);
    const bool rejected = rc::begin_commit_admission(&waiting, true);
    waiting_returned.store(true);
    if (!rejected) rc::end_commit_admission(&waiting, false);
  });
  const auto deadline = std::chrono::steady_clock::now() + 1s;
  while (!rc::runtime_snapshot_hard_gate_for_test() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(waiting_started.load());
  ASSERT_TRUE(rc::runtime_snapshot_hard_gate_for_test());
  EXPECT_FALSE(waiting_returned.load());
  EXPECT_FALSE(rc::commit_admission_open_for_test());

  const rc::RuntimeSnapshotRequestResult refreshed =
      rc::refresh_runtime_snapshot_request(request.request_id);
  ASSERT_EQ(rc::RuntimeSnapshotRequestOutcome::ACTIVE, refreshed.outcome);
  ASSERT_TRUE(refreshed.request.has_value());
  EXPECT_EQ(request.request_id, refreshed.request->request_id);
  EXPECT_EQ(rc::RuntimeSnapshotRequestReason::HARD_LIMIT,
            refreshed.request->reason);

  rc::end_commit_admission(&first, false);
  first_admitted = false;
  std::string error;
  ASSERT_FALSE(
      rc::reserve_runtime_snapshot_hard_gate(request.request_id, &error))
      << error;

  rc::CloneCutState source = clone_cut();
  source.request_id = request.request_id;
  rc::set_clone_cut_source_for_test(&source);
  rc::CloneCutState captured;
  rc::CloneCutBarrierLease lease;
  ASSERT_FALSE(rc::begin_clone_cut_barrier(request.request_id, &captured,
                                           &lease, &error))
      << error;
  rc::end_clone_cut_barrier(&lease);
  EXPECT_FALSE(rc::commit_admission_open_for_test());

  source.head_body_sha256.resize(63);
  rc::set_clone_cut_source_for_test(&source);
  rc::CloneCutBarrierLease invalid;
  EXPECT_TRUE(rc::begin_clone_cut_barrier(request.request_id, &captured,
                                          &invalid, &error));
  EXPECT_FALSE(invalid.active());
  EXPECT_FALSE(rc::clone_cut_barrier_active_for_test());
  EXPECT_FALSE(rc::commit_admission_open_for_test());

  rc::reset_commit_admission_for_test(true);
  waiter.join();
  cleanup.release();
  EXPECT_TRUE(waiting_returned.load());
}

TEST_F(RemoteCommitAdmissionTest,
       StaleCloneLeaseCannotReopenANewerHardRequest) {
  namespace rc = wesql::remote_commit;
  std::string error;
  rc::CloneCutState captured;
  rc::CloneCutBarrierLease stale;
  ASSERT_FALSE(rc::begin_clone_cut_barrier(clone_cut().request_id, &captured,
                                           &stale, &error))
      << error;

  rc::reset_commit_admission_for_test(true);
  rc::set_runtime_snapshot_window_for_test(10, {100000, 1, 0});
  THD waiting(false);
  std::atomic<bool> waiting_returned{false};
  std::thread waiter([&] {
    const bool rejected = rc::begin_commit_admission(&waiting, true);
    waiting_returned.store(true);
    if (!rejected) rc::end_commit_admission(&waiting, false);
  });
  auto cleanup = create_scope_guard([&] {
    rc::reset_commit_admission_for_test(true);
    if (waiter.joinable()) waiter.join();
  });
  const auto deadline = std::chrono::steady_clock::now() + 1s;
  while (!rc::runtime_snapshot_hard_gate_for_test() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(rc::runtime_snapshot_hard_gate_for_test());
  EXPECT_FALSE(waiting_returned.load());

  rc::end_clone_cut_barrier(&stale);
  EXPECT_FALSE(stale.active());
  EXPECT_TRUE(rc::runtime_snapshot_hard_gate_for_test());
  EXPECT_FALSE(rc::commit_admission_open_for_test());

  rc::reset_commit_admission_for_test(true);
  waiter.join();
  cleanup.release();
  EXPECT_TRUE(waiting_returned.load());
}

TEST_F(RemoteCommitAdmissionTest,
       RuntimeSnapshotOrderTicketIsRetainedAndReused) {
  namespace rc = wesql::remote_commit;
  constexpr uint64_t request_id = 73;
  const uint64_t retained =
      rc::hold_runtime_snapshot_order_ticket_for_test(request_id);
  EXPECT_TRUE(rc::runtime_snapshot_order_ticket_held_for_test(request_id));
  EXPECT_EQ(retained,
            rc::hold_runtime_snapshot_order_ticket_for_test(request_id));

  std::atomic<bool> acquired{false};
  uint64_t normal_ticket = 0;
  std::thread normal([&] {
    normal_ticket = rc::acquire_order_ticket_for_test();
    acquired.store(true);
  });
  auto cleanup = create_scope_guard([&] {
    rc::release_runtime_snapshot_order_ticket_for_test(request_id);
    if (normal.joinable()) normal.join();
    if (acquired.load()) rc::release_order_ticket_for_test(normal_ticket);
  });

  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(acquired.load());
  EXPECT_EQ(retained,
            rc::hold_runtime_snapshot_order_ticket_for_test(request_id));
  rc::release_runtime_snapshot_order_ticket_for_test(request_id);
  normal.join();
  ASSERT_TRUE(acquired.load());
  rc::release_order_ticket_for_test(normal_ticket);
  cleanup.release();
}

TEST_F(RemoteCommitAdmissionTest,
       CloneCutBinlogSeedIsExactExclusiveAndFailureClean) {
  namespace rc = wesql::remote_commit;
  Scoped_temp_directory temporary;
  ASSERT_FALSE(temporary.path().empty());
  const fs::path source_path = temporary.path() / "binlog.source";
  std::string source(1024, '\0');
  for (size_t index = 0; index < source.size(); ++index) {
    source[index] = static_cast<char>(index % 251);
  }
  write_test_file(source_path, source);
  rc::set_clone_cut_binlog_source_path_for_test(&source_path);

  std::string error;
  rc::CloneCutState captured;
  rc::CloneCutBarrierLease lease;
  ASSERT_FALSE(rc::begin_clone_cut_barrier(clone_cut().request_id, &captured,
                                           &lease, &error))
      << error;
  const fs::path seed_path = temporary.path() / "binlog.seed";
  ASSERT_FALSE(
      rc::materialize_clone_cut_binlog_seed(captured, lease, seed_path, &error))
      << error;
  EXPECT_EQ(source.substr(0, clone_cut().pos), read_test_file(seed_path));

  const fs::path existing_path = temporary.path() / "existing.seed";
  write_test_file(existing_path, "keep");
  EXPECT_TRUE(rc::materialize_clone_cut_binlog_seed(
      captured, lease, existing_path, &error));
  EXPECT_EQ("keep", read_test_file(existing_path));

  rc::end_clone_cut_barrier(&lease);
  const fs::path inactive_path = temporary.path() / "inactive.seed";
  EXPECT_TRUE(rc::materialize_clone_cut_binlog_seed(
      captured, lease, inactive_path, &error));
  EXPECT_FALSE(fs::exists(inactive_path));

  // Acquire the lease while the pinned source is complete. Truncating after
  // acquisition exercises materialization's failure cleanup while retaining
  // the install-time regular-file/size guard.
  rc::CloneCutBarrierLease short_lease;
  ASSERT_FALSE(rc::begin_clone_cut_barrier(clone_cut().request_id, &captured,
                                           &short_lease, &error))
      << error;
  write_test_file(source_path, source.substr(0, 128));
  const fs::path short_path = temporary.path() / "short.seed";
  EXPECT_TRUE(rc::materialize_clone_cut_binlog_seed(
      captured, short_lease, short_path, &error));
  EXPECT_FALSE(fs::exists(short_path));
  rc::end_clone_cut_barrier(&short_lease);
}

TEST_F(RemoteCommitAdmissionTest,
       PredispatchRejectsCreateAndAlterExternalDirectoryFlags) {
  for (const enum_sql_command command :
       {SQLCOM_CREATE_TABLE, SQLCOM_ALTER_TABLE}) {
    for (const uint64_t used_field :
         {uint64_t{HA_CREATE_USED_DATADIR},
          uint64_t{HA_CREATE_USED_INDEXDIR}}) {
      SCOPED_TRACE(::testing::Message()
                   << "command=" << static_cast<int>(command)
                   << " used_field=" << used_field);
      HA_CREATE_INFO create_info;
      create_info.used_fields = used_field;
      expect_predispatch_rejected(command, &create_info);
    }
  }
}

TEST_F(RemoteCommitAdmissionTest,
       PredispatchRejectsCreateAndAlterExternalDirectoryPointerFallback) {
  for (const enum_sql_command command :
       {SQLCOM_CREATE_TABLE, SQLCOM_ALTER_TABLE}) {
    for (const bool data_directory : {true, false}) {
      SCOPED_TRACE(::testing::Message()
                   << "command=" << static_cast<int>(command)
                   << " data_directory=" << data_directory);
      HA_CREATE_INFO create_info;
      if (data_directory)
        create_info.data_file_name = "/external/table-data";
      else
        create_info.index_file_name = "/external/table-index";
      ASSERT_EQ(0U, create_info.used_fields);
      expect_predispatch_rejected(command, &create_info);
    }
  }
}

TEST_F(RemoteCommitAdmissionTest,
       PredispatchRejectsPartitionAndSubpartitionExternalPaths) {
  for (const enum_sql_command command :
       {SQLCOM_CREATE_TABLE, SQLCOM_ALTER_TABLE}) {
    for (const bool subpartition : {false, true}) {
      for (const bool data_directory : {true, false}) {
        SCOPED_TRACE(::testing::Message()
                     << "command=" << static_cast<int>(command)
                     << " subpartition=" << subpartition
                     << " data_directory=" << data_directory);
        partition_info part_info;
        partition_element partition;
        partition_element child;
        partition_element *const external =
            subpartition ? &child : &partition;
        if (data_directory)
          external->data_file_name = "/external/partition-data";
        else
          external->index_file_name = "/external/partition-index";
        if (subpartition)
          ASSERT_FALSE(partition.subpartitions.push_back(&child));
        ASSERT_FALSE(part_info.partitions.push_back(&partition));
        expect_predispatch_rejected(command, nullptr, &part_info);
      }
    }
  }
}

TEST_F(RemoteCommitAdmissionTest, PredispatchRejectsAlterTablespace) {
  expect_predispatch_rejected(SQLCOM_ALTER_TABLESPACE);
}

TEST_F(RemoteCommitAdmissionTest,
       PredispatchAllowsOrdinaryRootLocalCreateAndAlter) {
  HA_CREATE_INFO create_info;
  partition_info part_info;
  expect_predispatch_allowed(SQLCOM_CREATE_TABLE, &create_info, &part_info);
  expect_predispatch_allowed(SQLCOM_ALTER_TABLE, &create_info, &part_info);
}

}  // namespace
}  // namespace remote_commit_admission_unittest

namespace remote_commit_server_hooks_lifecycle_unittest {
namespace {

namespace rc = wesql::remote_commit;
using namespace std::chrono_literals;

objstore::Status status(objstore::Errors code, const char *message) {
  return objstore::Status(code, 0, message);
}

class FakeConditionalIo final : public rc::ConditionalIo {
 public:
  struct StoredObject {
    std::string body;
    std::string etag;
  };

  objstore::ExactObjectResult get(std::string_view key,
                                  uint64_t max_bytes) override {
    std::unique_lock<std::mutex> lock(mutex);
    reads.emplace_back(key);
    if (pause_armed && key == paused_key) {
      pause_armed = false;
      read_paused = true;
      condition.notify_all();
      condition.wait(lock, [&] { return read_released; });
    }
    const auto found = objects.find(std::string(key));
    if (found == objects.end()) {
      return objstore::ExactObjectResult::not_found(
          status(objstore::SE_NO_SUCH_KEY, "absent"));
    }
    if (found->second.body.size() > max_bytes) {
      return objstore::ExactObjectResult::permanent_error(
          status(objstore::SE_UNEXPECTED, "bounded exact GET exceeded"));
    }
    return objstore::ExactObjectResult::found(found->second.body,
                                               found->second.etag);
  }

  objstore::ConditionalPutResult put(
      std::string_view key, std::string_view body,
      const objstore::ConditionalPutCondition &condition) override {
    if (!allow_epoch_cas) return objstore::ConditionalPutResult::unsupported();
    std::lock_guard<std::mutex> guard(mutex);
    const auto found = objects.find(std::string(key));
    if (condition.mode() != objstore::ConditionalPutMode::MATCH_ETAG ||
        found == objects.end() || found->second.etag != condition.etag()) {
      return objstore::ConditionalPutResult::precondition_failed_412(
          status(objstore::SE_UNEXPECTED, "epoch condition changed"));
    }
    found->second = {std::string(body), "\"acquired-epoch\""};
    return objstore::ConditionalPutResult::applied(found->second.etag);
  }

  bool allow_epoch_cas{false};

  void set(std::string key, std::string body, std::string etag) {
    std::lock_guard<std::mutex> guard(mutex);
    objects.insert_or_assign(
        std::move(key), StoredObject{std::move(body), std::move(etag)});
  }

  void erase(std::string_view key) {
    std::lock_guard<std::mutex> guard(mutex);
    objects.erase(std::string(key));
  }

  void pause_next_get(std::string key) {
    std::lock_guard<std::mutex> guard(mutex);
    paused_key = std::move(key);
    pause_armed = true;
    read_paused = false;
    read_released = false;
  }

  bool wait_for_paused(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex);
    return condition.wait_for(lock, timeout, [&] { return read_paused; });
  }

  void resume_get() {
    {
      std::lock_guard<std::mutex> guard(mutex);
      read_released = true;
    }
    condition.notify_all();
  }

 private:
  std::mutex mutex;
  std::condition_variable condition;
  std::string paused_key;
  bool pause_armed{false};
  bool read_paused{false};
  bool read_released{false};
  std::unordered_map<std::string, StoredObject> objects;
  std::vector<std::string> reads;
};

class RemoteCommitServerHooksLifecycleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_remote_commit = opt_binlog_archive_remote_commit;
    opt_binlog_archive_remote_commit = true;
    std::string error;
    ASSERT_TRUE(rc::build_stream_identity("repo-hooks", "branch-hooks",
                                          "cluster/hooks", &stream, &error))
        << error;

    epoch = {7, "77777777777777777777777777777777", 6};
    ASSERT_TRUE(rc::serialize_writer_epoch(stream, epoch, &epoch_body, &error))
        << error;

    cursor = {"binlog.000007", 700};
    snapshot.id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    snapshot.cursor = cursor;
    snapshot.manifest_size = 128;
    snapshot.manifest_sha256 = std::string(64, 'b');
    ASSERT_TRUE(rc::snapshot_manifest_key(
        stream, snapshot.id, snapshot.manifest_sha256,
        &snapshot.manifest_key, &error))
        << error;

    head.state = "READY";
    head.generation = 1;
    head.writer = {epoch.writer_id, epoch.epoch};
    head.recovery_window = {1, 256, 0};
    head.snapshot = snapshot;
    head.base_cursor = cursor;
    head.durable_cursor = cursor;
    head.segment_tip.kind = rc::SegmentTipKind::SNAPSHOT_ROOT;
    head.segment_tip.snapshot_id = snapshot.id;
    head.segment_tip.cursor = cursor;
    head.manifest.size = 256;
    head.manifest.sha256 = std::string(64, 'c');
    ASSERT_TRUE(rc::transition_manifest_key(
        stream, head.writer, head.generation, head.manifest.sha256,
        &head.manifest.key, &error))
        << error;
    ASSERT_TRUE(rc::serialize_head(stream, head, &head_body, &error)) << error;

    marker.stream_id = stream.stream_id;
    marker.server_uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    marker.installed_head.generation = head.generation;
    ASSERT_TRUE(rc::sha256_hex(head_body,
                               &marker.installed_head.body_sha256, &error))
        << error;
    marker.installed_head.snapshot_id = snapshot.id;
    marker.installed_head.snapshot_manifest_sha256 =
        snapshot.manifest_sha256;
    marker.installed_head.snapshot_cursor = snapshot.cursor;
    marker.config_digest = std::string(64, 'd');
    marker.binary_fingerprint = std::string(64, 'e');
    std::string marker_body;
    ASSERT_TRUE(rc::serialize_local_install_marker(marker, &marker_body, &error))
        << error;

    epoch_proof.body = epoch_body;
    epoch_proof.etag = epoch_etag;
    epoch_proof.value = epoch;
    epoch_proof.head_body = head_body;
    epoch_proof.head_etag = head_etag;
    epoch_proof.head_generation = head.generation;

    activation.head_body = head_body;
    activation.head_etag = head_etag;
    activation.marker = marker;
    activation.recovered_file = cursor.file;
    activation.recovered_pos = cursor.pos;
    activation.marker_matches = true;
    activation.root_identity_matches = true;

    full_proof.head_body = head_body;
    full_proof.head_etag = head_etag;
    full_proof.recovered_file = cursor.file;
    full_proof.recovered_pos = cursor.pos;
    const std::string canonical_gtid =
        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-7";
    rc::GtidSetDigest digest;
    ASSERT_TRUE(rc::gtid_digest(canonical_gtid, &digest, &error)) << error;
    full_proof.canonical_gtid = std::move(digest.canonical);
    full_proof.gtid_sha256 = std::move(digest.sha256);
    full_proof.marker_matches = true;
    full_proof.root_identity_matches = true;
    full_proof.snapshot_matches = true;
    full_proof.server_uuid_matches = true;
    full_proof.configuration_matches = true;
    full_proof.gtid_matches = true;
    full_proof.dd_matches = true;
    full_proof.repository_empty = true;
    full_proof.extent_live_set_matches = true;
    full_proof.internal_prepared_empty = true;
    full_proof.external_xa_empty = true;
  }

  void TearDown() override {
    rc::reset_startup_lifecycle_for_test();
    opt_binlog_archive_remote_commit = saved_remote_commit;
  }

  void publish_namespace(bool with_head) {
    io.set(stream.remote_prefix + "/WRITER_EPOCH", epoch_body, epoch_etag);
    if (with_head)
      io.set(stream.remote_prefix + "/HEAD", head_body, head_etag);
    else
      io.erase(stream.remote_prefix + "/HEAD");
  }

  void initialize(bool with_head) {
    publish_namespace(with_head);
    ASSERT_FALSE(rc::initialize_startup_lifecycle_for_test(&io, stream))
        << rc::startup_error();
  }

  void restore_head_recovery_window(uint64_t manifest_count) {
    head.recovery_window.manifest_count = manifest_count;
    std::string error;
    ASSERT_TRUE(rc::serialize_head(stream, head, &head_body, &error)) << error;
    ASSERT_TRUE(rc::sha256_hex(head_body,
                               &marker.installed_head.body_sha256, &error))
        << error;
    activation.head_body = head_body;
    activation.marker.installed_head.body_sha256 =
        marker.installed_head.body_sha256;
    epoch_proof.head_body = head_body;
    full_proof.head_body = head_body;
  }

  void adopt(rc::StartupEpochAdoptionRole role) {
    ASSERT_FALSE(rc::adopt_startup_epoch(epoch_proof, role))
        << rc::startup_error();
  }

  bool saved_remote_commit{false};
  FakeConditionalIo io;
  rc::StreamIdentity stream;
  rc::WriterEpoch epoch;
  std::string epoch_body;
  const std::string epoch_etag{"\"epoch-7\""};
  rc::Cursor cursor;
  rc::SnapshotRef snapshot;
  rc::Head head;
  std::string head_body;
  const std::string head_etag{"\"head-1\""};
  rc::LocalInstallMarker marker;
  rc::StartupEpochProof epoch_proof;
  rc::InstalledRootActivationProof activation;
  rc::InstalledRootProof full_proof;
};

TEST_F(RemoteCommitServerHooksLifecycleTest,
       NormalAndBootstrapPreflightNeverBypassRecovery) {
  initialize(true);
  EXPECT_FALSE(rc::may_bypass_stock_binlog_recovery());
  EXPECT_FALSE(rc::may_run_startup_recovery_worker());
  std::string file = "sentinel";
  uint64_t pos = 99;
  EXPECT_FALSE(rc::startup_existing_binlog_boundary(&file, &pos));
  EXPECT_TRUE(file.empty());
  EXPECT_EQ(0U, pos);
  EXPECT_TRUE(rc::verify_installed_root_post_engine(full_proof));
  EXPECT_TRUE(rc::activate_installed_root(activation));

  rc::reset_startup_lifecycle_for_test();
  initialize(false);
  EXPECT_TRUE(rc::may_run_startup_bootstrap_worker());
  EXPECT_FALSE(rc::may_bypass_stock_binlog_recovery());
  EXPECT_FALSE(rc::startup_existing_binlog_boundary(&file, &pos));
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       OnlyCompiledInitializationInPreflightMayMutateUnpublishedRoot) {
  const bool saved_initialize = opt_initialize;
  auto restore = create_scope_guard([&] { opt_initialize = saved_initialize; });
  opt_initialize = true;
  THD thd(false);
  thd.system_thread = SYSTEM_THREAD_DD_INITIALIZE;

  initialize(false);
  EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
  rc::reset_startup_lifecycle_for_test();
  ASSERT_FALSE(rc::initialize_startup_lifecycle_for_test(&io, stream, true));
  EXPECT_FALSE(rc::may_initialize_system_tables(nullptr));
  for (const auto kind : {SYSTEM_THREAD_DD_INITIALIZE,
                          SYSTEM_THREAD_SERVER_INITIALIZE}) {
    thd.system_thread = kind;
    ASSERT_TRUE(rc::may_initialize_system_tables(&thd));
    thd.lex->sql_command = SQLCOM_CREATE_TABLE;
    thd.lex->no_write_to_binlog = true;
    EXPECT_FALSE(rc::enforce_sql_command_admission(&thd));
    EXPECT_FALSE(rc::begin_commit_admission(&thd, true));
    rc::check_commit_authorization(&thd, true);
    rc::consume_commit_authorization(&thd, true, true, true);
    rc::end_commit_admission(&thd, false);
    EXPECT_FALSE(rc::commit_admission_open_for_test());
  }
  for (const auto kind : {NON_SYSTEM_THREAD, SYSTEM_THREAD_DD_RESTART,
                          SYSTEM_THREAD_INIT_FILE,
                          SYSTEM_THREAD_SERVER_UPGRADE}) {
    thd.system_thread = kind;
    EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
  }
  thd.system_thread = SYSTEM_THREAD_DD_INITIALIZE;
  opt_initialize = false;
  EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
  opt_initialize = true;
  rc::reset_startup_lifecycle_for_test();
  publish_namespace(true);
  ASSERT_FALSE(rc::initialize_startup_lifecycle_for_test(&io, stream, true));
  EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       EmptyRootInitializationSkipsOnlyDummyRecoveryAndRevokesOnStateChange) {
  const bool saved_initialize = opt_initialize;
  auto restore = create_scope_guard([&] { opt_initialize = saved_initialize; });
  opt_initialize = true;
  THD thd(false);
  thd.system_thread = SYSTEM_THREAD_DD_INITIALIZE;
  TC_LOG_DUMMY dummy;
  publish_namespace(false);
  ASSERT_FALSE(rc::initialize_startup_lifecycle_for_test(&io, stream, true));
  ASSERT_TRUE(rc::may_initialize_empty_root());
  EXPECT_EQ(0, dummy.open(nullptr));
  EXPECT_FALSE(rc::may_bypass_stock_binlog_recovery());

  rc::reset_commit_admission_for_test(true);
  EXPECT_FALSE(rc::may_initialize_empty_root());
  EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
  rc::reset_commit_admission_for_test(false);
  ASSERT_TRUE(rc::may_initialize_empty_root());

  rc::StartupEpochProof acquired;
  io.allow_epoch_cas = true;
  ASSERT_FALSE(rc::acquire_startup_epoch(&acquired)) << rc::startup_error();
  EXPECT_FALSE(rc::may_initialize_empty_root());
  EXPECT_FALSE(rc::may_initialize_system_tables(&thd));

  rc::reset_startup_lifecycle_for_test();
  publish_namespace(false);
  ASSERT_FALSE(rc::initialize_startup_lifecycle_for_test(&io, stream, true));
  epoch_proof.head_body.clear();
  epoch_proof.head_etag.clear();
  epoch_proof.head_generation = 0;
  adopt(rc::StartupEpochAdoptionRole::BOOTSTRAP_SNAPSHOT);
  EXPECT_FALSE(rc::may_initialize_empty_root());
  EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       EmptyXaRecoveryQueueIsNoopButPreparedEntriesRemainRejected) {
  ASSERT_FALSE(Recovered_xa_transactions::init());
  auto destroy = create_scope_guard([] { Recovered_xa_transactions::destroy(); });
  auto &recovered = Recovered_xa_transactions::instance();
  EXPECT_FALSE(recovered.recover_prepared_xa_transactions());
  XA_recover_txn transaction{};
  transaction.id.set(41, "external", 8, "", 0);
  ASSERT_FALSE(recovered.add_prepared_xa_transaction(&transaction));
  EXPECT_TRUE(recovered.recover_prepared_xa_transactions());
  EXPECT_TRUE(recovered.recover_prepared_xa_transactions());
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       WorkerRolesAreExactAndBootstrapHasNoExistingBoundary) {
  initialize(false);
  epoch_proof.head_body.clear();
  epoch_proof.head_etag.clear();
  epoch_proof.head_generation = 0;
  adopt(rc::StartupEpochAdoptionRole::BOOTSTRAP_SNAPSHOT);
  EXPECT_TRUE(rc::may_run_startup_bootstrap_snapshot_worker());
  EXPECT_FALSE(rc::may_run_startup_recovery_worker());
  EXPECT_TRUE(rc::may_bypass_stock_binlog_recovery());
  std::string file;
  uint64_t pos = 0;
  EXPECT_FALSE(rc::startup_existing_binlog_boundary(&file, &pos));

  rc::reset_startup_lifecycle_for_test();
  initialize(true);
  epoch_proof.head_body = head_body;
  epoch_proof.head_etag = head_etag;
  epoch_proof.head_generation = head.generation;
  adopt(rc::StartupEpochAdoptionRole::TAKEOVER_RECOVERY);
  EXPECT_FALSE(rc::may_run_startup_bootstrap_snapshot_worker());
  EXPECT_TRUE(rc::may_run_startup_recovery_worker());
  EXPECT_TRUE(rc::may_bypass_stock_binlog_recovery());
  ASSERT_TRUE(rc::startup_existing_binlog_boundary(&file, &pos));
  EXPECT_EQ(cursor.file, file);
  EXPECT_EQ(cursor.pos, pos);
  EXPECT_TRUE(rc::activate_installed_root(activation));
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       InstalledReexecRequiresOrderedTwoPhaseVerification) {
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  EXPECT_FALSE(rc::may_bypass_stock_binlog_recovery());
  EXPECT_TRUE(rc::verify_installed_root_post_engine(full_proof));

  ASSERT_FALSE(rc::activate_installed_root(activation)) << rc::startup_error();
  EXPECT_TRUE(rc::may_bypass_stock_binlog_recovery());
  EXPECT_FALSE(rc::commit_admission_open_for_test());
  std::string file;
  uint64_t pos = 0;
  ASSERT_TRUE(rc::startup_existing_binlog_boundary(&file, &pos));
  EXPECT_EQ(cursor.file, file);
  EXPECT_EQ(cursor.pos, pos);
  EXPECT_TRUE(rc::activate_installed_root(activation));

  rc::InstalledRootProof incomplete = full_proof;
  incomplete.dd_matches = false;
  EXPECT_TRUE(rc::verify_installed_root_post_engine(incomplete));
  EXPECT_TRUE(rc::may_bypass_stock_binlog_recovery());

  ASSERT_FALSE(rc::verify_installed_root_post_engine(full_proof))
      << rc::startup_error();
  EXPECT_FALSE(rc::may_bypass_stock_binlog_recovery());
  EXPECT_FALSE(rc::startup_existing_binlog_boundary(&file, &pos));
  EXPECT_FALSE(rc::commit_admission_open_for_test());
  EXPECT_TRUE(rc::verify_installed_root_post_engine(full_proof));

  rc::open_commit_admission();
  EXPECT_TRUE(rc::commit_admission_open_for_test());
  EXPECT_FALSE(rc::may_bypass_stock_binlog_recovery());
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       RestoredThresholdSchedulesSnapshotBeforeAdmissionOpens) {
  constexpr uint64_t kSoftManifestThreshold = 80000;
  restore_head_recovery_window(kSoftManifestThreshold);
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  ASSERT_FALSE(rc::activate_installed_root(activation)) << rc::startup_error();
  ASSERT_FALSE(rc::verify_installed_root_post_engine(full_proof))
      << rc::startup_error();

  rc::open_commit_admission();
  ASSERT_TRUE(rc::commit_admission_open_for_test());

  rc::RuntimeSnapshotRequest request;
  ASSERT_TRUE(rc::take_runtime_snapshot_request(&request));
  EXPECT_EQ(rc::RuntimeSnapshotRequestReason::SOFT_LIMIT, request.reason);
  EXPECT_EQ(head.recovery_window, request.published_window);
  EXPECT_EQ(head.recovery_window, request.prospective_window);
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       AckCursorReservationPrecedesExternalPublication) {
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  ASSERT_FALSE(rc::activate_installed_root(activation)) << rc::startup_error();
  ASSERT_FALSE(rc::verify_installed_root_post_engine(full_proof))
      << rc::startup_error();
  rc::open_commit_admission();

  rc::Head next = head;
  ++next.generation;
  next.durable_cursor.pos += 100;
  std::string prior_head_sha256;
  std::string error;
  ASSERT_TRUE(rc::sha256_hex(head_body, &prior_head_sha256, &error)) << error;
  next.parent = rc::HeadParent{head.generation, head_etag, prior_head_sha256};
  next.manifest.sha256 = std::string(64, 'f');
  ASSERT_TRUE(rc::transition_manifest_key(stream, next.writer, next.generation,
                                          next.manifest.sha256,
                                          &next.manifest.key, &error))
      << error;

  std::string next_head_body;
  ASSERT_TRUE(rc::serialize_head(stream, next, &next_head_body, &error)) << error;
  const std::string next_head_etag{"\"head-2\""};
  io.set(stream.remote_prefix + "/HEAD", next_head_body, next_head_etag);
  ASSERT_FALSE(rc::verify_ack_head_for_test(next, &error)) << error;

  rc::AckReadyEvent ack;
  ack.stream_id = stream.stream_id;
  ack.writer = next.writer;
  ack.endpoint = next.durable_cursor;
  ack.transaction_count = 1;
  ack.gtid_set_sha256 = full_proof.gtid_sha256;
  ack.xid_sha256 = std::string(64, '1');
  ack.head_generation = next.generation;
  ASSERT_TRUE(rc::sha256_hex(next_head_body, &ack.head_body_sha256, &error))
      << error;
  ASSERT_TRUE(rc::sha256_hex(next_head_etag, &ack.head_etag_sha256, &error))
      << error;
  ack.manifest = next.manifest;
  const rc::CommitBinding binding{
      stream.stream_id, next.generation, ack.head_body_sha256,
      ack.endpoint.file, ack.endpoint.pos, ack.gtid_set_sha256, ack.xid_sha256};

  rc::AckReadyEvent mismatched = ack;
  mismatched.head_etag_sha256 = std::string(64, '0');
  EXPECT_TRUE(rc::reserve_ack_public_cursor_for_test(
      next, mismatched, binding, cursor, &error));
  EXPECT_EQ("remote ACK token lost exact HEAD object identity", error);
  EXPECT_EQ(cursor, rc::public_committed_cursor_for_test());

  ASSERT_FALSE(rc::reserve_ack_public_cursor_for_test(next, ack, binding, cursor,
                                                      &error))
      << error;
  EXPECT_EQ(next.durable_cursor, rc::public_committed_cursor_for_test());
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       StalePermanentTerminalWinsPausedAdmissionReopen) {
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  ASSERT_FALSE(rc::activate_installed_root(activation)) << rc::startup_error();
  ASSERT_FALSE(rc::verify_installed_root_post_engine(full_proof))
      << rc::startup_error();
  rc::open_commit_admission();
  ASSERT_TRUE(rc::commit_admission_open_for_test());
  ASSERT_FALSE(rc::close_commit_admission_and_wait());

  constexpr uint64_t stale_request_id = 987;
  (void)rc::hold_runtime_snapshot_order_ticket_for_test(stale_request_id);
  io.pause_next_get(stream.remote_prefix + "/HEAD");
  std::thread opener([&] { rc::open_commit_admission(); });
  std::thread terminal;
  auto cleanup = create_scope_guard([&] {
    io.resume_get();
    if (opener.joinable()) opener.join();
    if (terminal.joinable()) terminal.join();
  });
  ASSERT_TRUE(io.wait_for_paused(std::chrono::seconds(1)));

  terminal = std::thread([&] {
    rc::mark_runtime_snapshot_terminal(
        stale_request_id, rc::RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
        "injected terminal coordinator failure");
  });
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (!rc::runtime_snapshot_terminal_for_test() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(rc::runtime_snapshot_terminal_for_test());
  EXPECT_FALSE(
      rc::runtime_snapshot_order_ticket_held_for_test(stale_request_id));

  io.resume_get();
  opener.join();
  terminal.join();
  cleanup.release();

  EXPECT_TRUE(rc::is_fenced());
  EXPECT_FALSE(rc::commit_admission_open_for_test());
  EXPECT_FALSE(rc::commit_admission_reopening_for_test());
  rc::open_commit_admission();
  EXPECT_TRUE(rc::is_fenced());
  EXPECT_FALSE(rc::commit_admission_open_for_test());
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       TerminalFenceWakesSnapshotWaiterAndRejectsNewAdmission) {
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  ASSERT_FALSE(rc::activate_installed_root(activation)) << rc::startup_error();
  ASSERT_FALSE(rc::verify_installed_root_post_engine(full_proof))
      << rc::startup_error();
  rc::open_commit_admission();
  ASSERT_TRUE(rc::commit_admission_open_for_test());

  std::atomic<bool> waiter_started{false};
  std::atomic<bool> waiter_stopped{false};
  rc::RuntimeSnapshotRequest request;
  std::thread waiter([&] {
    waiter_started.store(true);
    waiter_stopped.store(rc::wait_for_runtime_snapshot_request(&request));
  });
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (!waiter_started.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(waiter_started.load());

  rc::mark_runtime_snapshot_terminal(
      0xdead, rc::RuntimeSnapshotRequestOutcome::PERMANENT_ERROR,
      "terminal waiter wake test");
  waiter.join();
  EXPECT_TRUE(waiter_stopped.load());

  THD rejected(false);
  EXPECT_TRUE(rc::begin_commit_admission(&rejected, true));
  const rc::RuntimeSnapshotRequestResult refreshed =
      rc::refresh_runtime_snapshot_request(0xdead);
  EXPECT_EQ(rc::RuntimeSnapshotRequestOutcome::FENCED, refreshed.outcome);
  EXPECT_FALSE(rc::commit_admission_open_for_test());
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       ShutdownCancelsAdmissionReopenWithoutFailStop) {
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  ASSERT_FALSE(rc::activate_installed_root(activation)) << rc::startup_error();
  ASSERT_FALSE(rc::verify_installed_root_post_engine(full_proof))
      << rc::startup_error();

  io.pause_next_get(stream.remote_prefix + "/HEAD");
  std::atomic<bool> opener_finished{false};
  std::atomic<bool> shutdown_finished{false};
  std::thread opener;
  std::thread shutdown;
  auto cleanup = create_scope_guard([&] {
    io.resume_get();
    if (opener.joinable()) opener.join();
    if (shutdown.joinable()) shutdown.join();
  });

  opener = std::thread([&] {
    rc::open_commit_admission();
    opener_finished.store(true);
  });
  const bool paused = io.wait_for_paused(std::chrono::seconds(1));
  if (!paused) {
    io.resume_get();
    opener.join();
    cleanup.release();
    ASSERT_TRUE(paused);
  }

  shutdown = std::thread([&] {
    rc::shutdown();
    shutdown_finished.store(true);
  });
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (!rc::commit_admission_draining_for_test() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  const bool draining = rc::commit_admission_draining_for_test();
  if (draining) {
    EXPECT_TRUE(rc::commit_admission_reopening_for_test());
    EXPECT_FALSE(rc::commit_admission_open_for_test());
    EXPECT_FALSE(opener_finished.load());
    EXPECT_FALSE(shutdown_finished.load());
  }

  io.resume_get();
  opener.join();
  shutdown.join();
  cleanup.release();

  ASSERT_TRUE(draining);
  EXPECT_TRUE(opener_finished.load());
  EXPECT_TRUE(shutdown_finished.load());
  EXPECT_FALSE(rc::commit_admission_reopening_for_test());
  EXPECT_FALSE(rc::commit_admission_open_for_test());
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       ShutdownDrainRejectsNewRecoveryAuthorization) {
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::TAKEOVER_RECOVERY);

  std::string head_sha256;
  std::string error;
  ASSERT_TRUE(rc::sha256_hex(head_body, &head_sha256, &error)) << error;
  const rc::CommitBinding binding{
      stream.stream_id, head.generation, head_sha256, cursor.file, cursor.pos,
      full_proof.gtid_sha256, std::string(64, 'f')};
  THD admitted(false);
  THD rejected(false);
  ASSERT_FALSE(
      rc::install_recovery_commit_authorization(&admitted, binding, &error))
      << error;
  ASSERT_FALSE(rc::begin_commit_admission(&admitted, true));

  std::thread shutdown([] { rc::shutdown(); });
  auto cleanup = create_scope_guard([&] {
    rc::end_commit_admission(&admitted, false);
    if (shutdown.joinable()) shutdown.join();
    rc::discard_recovery_commit_authorization(&admitted);
    rc::discard_recovery_commit_authorization(&rejected);
  });
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (!rc::commit_admission_draining_for_test() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  const bool draining = rc::commit_admission_draining_for_test();
  bool rejected_during_drain = false;
  if (draining) {
    rejected_during_drain =
        rc::install_recovery_commit_authorization(&rejected, binding, &error);
  }

  rc::end_commit_admission(&admitted, false);
  shutdown.join();
  rc::discard_recovery_commit_authorization(&admitted);
  rc::discard_recovery_commit_authorization(&rejected);
  cleanup.release();

  ASSERT_TRUE(draining);
  EXPECT_TRUE(rejected_during_drain);
  EXPECT_EQ("recovery authorization requires CLOSED, drained admission", error);
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       MarkerAndPostEngineEpochChangesFailClosed) {
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  rc::InstalledRootActivationProof changed_marker = activation;
  changed_marker.marker.installed_head.snapshot_id =
      "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
  EXPECT_TRUE(rc::activate_installed_root(changed_marker));
  EXPECT_FALSE(rc::may_bypass_stock_binlog_recovery());

  ASSERT_FALSE(rc::activate_installed_root(activation)) << rc::startup_error();
  io.set(stream.remote_prefix + "/WRITER_EPOCH", epoch_body,
         "\"epoch-same-body-new-etag\"");
  EXPECT_TRUE(rc::verify_installed_root_post_engine(full_proof));
  EXPECT_FALSE(rc::may_bypass_stock_binlog_recovery());
  std::string file;
  uint64_t pos = 0;
  EXPECT_FALSE(rc::startup_existing_binlog_boundary(&file, &pos));
}

}  // namespace
}  // namespace remote_commit_server_hooks_lifecycle_unittest
