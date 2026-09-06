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
#include "sql/binlog.h"
#include "sql/dd/impl/bootstrap/bootstrap_ctx.h"
#include "sql/dd/impl/system_registry.h"
#include "sql/dd/impl/types/charset_impl.h"
#include "sql/dd/impl/types/collation_impl.h"
#include "sql/dd/impl/types/resource_group_impl.h"
#include "sql/dd/impl/types/table_impl.h"
#include "sql/dd/impl/types/tablespace_impl.h"
#include "sql/handler.h"
#include "sql/item.h"
#include "sql/log_event.h"
#include "sql/mysqld.h"
#include "sql/partition_info.h"
#include "sql/plugin_table.h"
#include "sql/remote_commit/policy.h"
#include "sql/remote_commit/protocol_codec.h"
#include "sql/remote_commit/evidence.h"
#include "sql/remote_commit/publisher.h"
#include "sql/remote_commit/runtime_snapshot_service.h"
#include "sql/remote_commit/segment_sealer.h"
#include "sql/remote_commit/server_hooks.h"
#include "sql/remote_commit/sql_admission.h"
#include "sql/remote_commit/startup_dictionary.h"
#include "sql/rpl_gtid.h"
#include "sql/rpl_gtid_persist.h"
#include "sql/set_var.h"
#include "sql/sql_lex.h"
#include "sql/sql_parse.h"
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

TEST(RemoteCommitStartupBinlog, ExistingCursorAppendsWithoutOverwritingPrefix) {
  Scoped_temp_directory directory;
  ASSERT_FALSE(directory.path().empty());
  const auto path = directory.path() / "binlog.000001";
  const auto base = directory.path() / "binlog";
  const auto index = directory.path() / "binlog.index";
  std::string original(BINLOG_MAGIC, BIN_LOG_HEADER_SIZE);
  original.append(154, '\0');
  original.back() = 'x';
  write_test_file(path, original);
  const auto original_index = path.string() + "\n";
  write_test_file(index, original_index);

  const bool saved_remote = opt_binlog_archive_remote_commit;
  opt_binlog_archive_remote_commit = true;
  auto restore = create_scope_guard([&] {
    opt_binlog_archive_remote_commit = saved_remote;
  });
  uint sync_period = 1;
  MYSQL_BIN_LOG binlog(&sync_period);
  binlog.set_psi_keys(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                      0, 0, 0, 0, 0);
  binlog.init_pthread_objects();
  auto cleanup = create_scope_guard([&] { binlog.cleanup(); });
  ASSERT_FALSE(binlog.open_index_file(index.c_str(), base.c_str(), true));
  mysql_mutex_lock(binlog.get_log_lock());
  auto unlock = create_scope_guard([&] {
    mysql_mutex_unlock(binlog.get_log_lock());
  });
  EXPECT_TRUE(binlog.open_remote_existing_binlog(
      base.c_str(), "binlog.000002", original.size(), 1024 * 1024));
  EXPECT_TRUE(binlog.open_remote_existing_binlog(
      base.c_str(), "binlog.000001", original.size() - 1, 1024 * 1024));
  EXPECT_TRUE(binlog.open_remote_existing_binlog(
      base.c_str(), "binlog.000001", original.size() + 1, 1024 * 1024));
  EXPECT_EQ(original, read_test_file(path));
  ASSERT_FALSE(binlog.open_remote_existing_binlog(
      base.c_str(), "binlog.000001", original.size(), 1024 * 1024));
  Log_info position;
  ASSERT_EQ(0, binlog.get_current_log(&position, false));
  EXPECT_EQ(original.size(), position.pos);
  EXPECT_EQ(original.size(), fs::file_size(path));
  original[BIN_LOG_HEADER_SIZE + FLAGS_OFFSET] = LOG_EVENT_BINLOG_IN_USE_F;
  EXPECT_EQ(original, read_test_file(path));
  EXPECT_EQ(original_index, read_test_file(index));
  Format_description_log_event event;
  ASSERT_FALSE(binlog.write_event_to_binlog_and_sync(&event));
  ASSERT_EQ(0, binlog.get_current_log(&position, false));
  const auto appended = read_test_file(path);
  ASSERT_GT(appended.size(), original.size());
  EXPECT_EQ(original, appended.substr(0, original.size()));
  EXPECT_EQ(appended.size(), position.pos);
  EXPECT_EQ(appended.size(), binlog.get_binlog_end_pos());
  EXPECT_EQ(original_index, read_test_file(index));
}

TEST(RemoteCommitGtidCache, SetPersistenceAndCompressionDoNotOpenTransactions) {
  const bool saved_remote = opt_binlog_archive_remote_commit;
  opt_binlog_archive_remote_commit = true;
  auto restore = create_scope_guard([&] {
    opt_binlog_archive_remote_commit = saved_remote;
  });
  Tsid_map map(nullptr);
  Gtid_set gtids(&map, nullptr);
  const char *text = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-17";
  ASSERT_EQ(RETURN_STATUS_OK, gtids.add_gtid_text(text));
  Gtid_set expected(&map, nullptr);
  ASSERT_EQ(RETURN_STATUS_OK, expected.add_gtid_text(text));
  Gtid_table_persistor persistor;
  ASSERT_EQ(nullptr, current_thd);
  for (const bool compress : {false, true}) {
    EXPECT_EQ(0, persistor.save(&gtids, compress));
    EXPECT_TRUE(gtids.equals(&expected));
    EXPECT_EQ(nullptr, current_thd);
  }
  EXPECT_EQ(0, persistor.compress(nullptr));
  EXPECT_EQ(nullptr, current_thd);
}

TEST(RemoteCommitRuntimeSnapshotService, CreatesOnlyFreshPrivateDirectory) {
  namespace rc = wesql::remote_commit;
  Scoped_temp_directory directory;
  ASSERT_FALSE(directory.path().empty());
  const auto root = directory.path() / ".snapshot-service";
  std::string error;
  const auto create = [&](const fs::path &path) {
    return rc::create_runtime_snapshot_service_root_for_test(path, &error);
  };
  ASSERT_FALSE(create(root)) << error;
  EXPECT_TRUE(fs::is_directory(root));
  EXPECT_EQ(fs::perms::owner_all, fs::status(root).permissions() & fs::perms::all);
  write_test_file(root / "sentinel", "preserve");
  EXPECT_TRUE(create(root));
  EXPECT_EQ("preserve", read_test_file(root / "sentinel"));
  const auto file = directory.path() / "file";
  write_test_file(file, "preserve");
  EXPECT_TRUE(create(file));
  EXPECT_TRUE(create(file / "child"));
  EXPECT_EQ("preserve", read_test_file(file));
  const auto link = directory.path() / "link";
  fs::create_directory_symlink(root, link);
  EXPECT_TRUE(create(link));
  EXPECT_TRUE(create(link / "child"));
  EXPECT_FALSE(fs::exists(root / "child"));
  const auto dangling = directory.path() / "dangling";
  fs::create_directory_symlink(directory.path() / "absent", dangling);
  EXPECT_TRUE(create(dangling));
  EXPECT_TRUE(fs::is_symlink(dangling));
  EXPECT_TRUE(create(directory.path() / "missing-parent" / "child"));
  EXPECT_TRUE(create({}));
  EXPECT_TRUE(create("relative-root"));
  EXPECT_TRUE(create(directory.path() / "unused" / ".." / "child"));
  EXPECT_FALSE(fs::exists(directory.path() / "child"));
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
       BootstrapSnapshotAllowsOnlyCompiledDictionaryWork) {
  const bool saved_initialize = opt_initialize;
  auto restore = create_scope_guard([&] { opt_initialize = saved_initialize; });
  opt_initialize = false;
  THD thd(false);
  thd.system_thread = SYSTEM_THREAD_DD_INITIALIZE;
  initialize(false);
  EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
  epoch_proof.head_body.clear();
  epoch_proof.head_etag.clear();
  epoch_proof.head_generation = 0;
  adopt(rc::StartupEpochAdoptionRole::BOOTSTRAP_SNAPSHOT);
  ASSERT_TRUE(rc::may_initialize_system_tables(&thd));
  EXPECT_FALSE(rc::may_initialize_empty_root());
  thd.lex->sql_command = SQLCOM_CREATE_TABLE;
  thd.lex->no_write_to_binlog = true;
  EXPECT_FALSE(rc::enforce_sql_command_admission(&thd));
  EXPECT_FALSE(rc::begin_commit_admission(&thd, true));
  rc::check_commit_authorization(&thd, true);
  rc::consume_commit_authorization(&thd, true, true, true);
  rc::end_commit_admission(&thd, false);
  EXPECT_FALSE(rc::commit_admission_open_for_test());
  EXPECT_EQ(0U, rc::commit_admission_count_for_test());

  EXPECT_FALSE(rc::may_initialize_system_tables(nullptr));
  for (const auto kind : {NON_SYSTEM_THREAD, SYSTEM_THREAD_DD_RESTART,
                          SYSTEM_THREAD_SERVER_INITIALIZE,
                          SYSTEM_THREAD_INIT_FILE,
                          SYSTEM_THREAD_SERVER_UPGRADE}) {
    thd.system_thread = kind;
    EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
  }
  thd.system_thread = SYSTEM_THREAD_DD_INITIALIZE;
  opt_initialize = true;
  EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
  opt_initialize = false;
  rc::reset_commit_admission_for_test(true);
  EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
  rc::reset_commit_admission_for_test(false);
  ASSERT_TRUE(rc::may_initialize_system_tables(&thd));
  rc::shutdown();
  EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       BootstrapPfsPermissionIsScopedAndRechecksLifecycle) {
  const bool saved_initialize = opt_initialize;
  auto restore = create_scope_guard([&] { opt_initialize = saved_initialize; });
  opt_initialize = false;
  THD thd(false);
  thd.system_thread = SYSTEM_THREAD_BACKGROUND;
  initialize(false);
  epoch_proof.head_body.clear();
  epoch_proof.head_etag.clear();
  epoch_proof.head_generation = 0;
  adopt(rc::StartupEpochAdoptionRole::BOOTSTRAP_SNAPSHOT);

  EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
  const auto compiled_pfs_call = [&] {
    rc::Scoped_startup_pfs_initialization scope(&thd);
    EXPECT_EQ(SYSTEM_THREAD_DD_INITIALIZE, thd.system_thread);
    ASSERT_TRUE(rc::may_initialize_system_tables(&thd));
    thd.lex->sql_command = SQLCOM_CREATE_TABLE;
    thd.lex->no_write_to_binlog = true;
    EXPECT_FALSE(rc::enforce_sql_command_admission(&thd));
    EXPECT_FALSE(rc::begin_commit_admission(&thd, true));
    rc::check_commit_authorization(&thd, true);
    rc::consume_commit_authorization(&thd, true, true, true);
    rc::end_commit_admission(&thd, false);
    EXPECT_FALSE(rc::commit_admission_open_for_test());
    EXPECT_EQ(0U, rc::commit_admission_count_for_test());
    rc::reset_commit_admission_for_test(true);
    EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
    rc::reset_commit_admission_for_test(false);
    EXPECT_TRUE(rc::may_initialize_system_tables(&thd));
    return;
  };
  compiled_pfs_call();
  EXPECT_EQ(SYSTEM_THREAD_BACKGROUND, thd.system_thread);
  EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
  {
    rc::Scoped_startup_pfs_initialization scope(&thd);
    ASSERT_TRUE(rc::may_initialize_system_tables(&thd));
    rc::shutdown();
    EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
  }
  EXPECT_EQ(SYSTEM_THREAD_BACKGROUND, thd.system_thread);
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       BootstrapPfsScopeExcludesClientsPreflightAndPublishedRoots) {
  const bool saved_initialize = opt_initialize;
  auto restore = create_scope_guard([&] { opt_initialize = saved_initialize; });
  opt_initialize = false;
  THD thd(false);
  thd.system_thread = SYSTEM_THREAD_BACKGROUND;
  const auto expect_denied = [&] {
    const auto original_kind = thd.system_thread;
    {
      rc::Scoped_startup_pfs_initialization scope(&thd);
      EXPECT_EQ(original_kind, thd.system_thread);
      EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
    }
    EXPECT_EQ(original_kind, thd.system_thread);
  };
  rc::Scoped_startup_pfs_initialization null_scope(nullptr);
  initialize(false);
  expect_denied();
  epoch_proof.head_body.clear();
  epoch_proof.head_etag.clear();
  epoch_proof.head_generation = 0;
  adopt(rc::StartupEpochAdoptionRole::BOOTSTRAP_SNAPSHOT);
  for (const auto kind : {NON_SYSTEM_THREAD, SYSTEM_THREAD_DD_RESTART,
                          SYSTEM_THREAD_SERVER_INITIALIZE,
                          SYSTEM_THREAD_INIT_FILE,
                          SYSTEM_THREAD_SERVER_UPGRADE}) {
    thd.system_thread = kind;
    expect_denied();
  }
  thd.system_thread = SYSTEM_THREAD_BACKGROUND;
  opt_initialize = true;
  expect_denied();
  opt_initialize = false;
  rc::reset_commit_admission_for_test(true);
  expect_denied();
  rc::reset_commit_admission_for_test(false);

  rc::reset_startup_lifecycle_for_test();
  epoch_proof.head_body = head_body;
  epoch_proof.head_etag = head_etag;
  epoch_proof.head_generation = head.generation;
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::TAKEOVER_RECOVERY);
  expect_denied();
  rc::reset_startup_lifecycle_for_test();
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  expect_denied();
  ASSERT_FALSE(rc::activate_installed_root(activation)) << rc::startup_error();
  expect_denied();
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       PublishedRootRolesDoNotGetBootstrapDictionaryMutation) {
  const bool saved_initialize = opt_initialize;
  auto restore = create_scope_guard([&] { opt_initialize = saved_initialize; });
  opt_initialize = false;
  THD thd(false);
  thd.system_thread = SYSTEM_THREAD_DD_INITIALIZE;
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::TAKEOVER_RECOVERY);
  EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
  rc::reset_startup_lifecycle_for_test();
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
  ASSERT_FALSE(rc::activate_installed_root(activation)) << rc::startup_error();
  EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       ExistingRootDictionaryCacheCreateIsNotAMutationGrant) {
  auto &context = dd::bootstrap::DD_bootstrap_ctx::instance();
  const auto saved_context = context;
  const bool saved_initialize = opt_initialize;
  auto restore = create_scope_guard([&] {
    context = saved_context;
    opt_initialize = saved_initialize;
  });
  opt_initialize = false;
  context.set_stage(dd::bootstrap::Stage::FETCHED_PROPERTIES);
  context.set_actual_dd_version(dd::DD_VERSION);
  context.set_upgraded_server_version(MYSQL_VERSION_ID);
  if (dd::System_tables::instance()->find_type("mysql", "dd_properties") == nullptr)
    dd::System_tables::instance()->add_inert_dd_tables();
  THD thd(false);
  thd.system_thread = SYSTEM_THREAD_DD_INITIALIZE;
  thd.lex->sql_command = SQLCOM_CREATE_TABLE;
  thd.lex->no_write_to_binlog = true;
  HA_CREATE_INFO create_info;
  thd.lex->create_info = &create_info;
  Table_ref table;
  table.db = "mysql";
  table.table_name = "dd_properties";
  thd.lex->query_tables = &table;
  auto clear_lex = create_scope_guard([&] {
    thd.lex->query_tables = nullptr;
    thd.lex->create_info = nullptr;
  });
  initialize(true);
  EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  ASSERT_FALSE(rc::activate_installed_root(activation)) << rc::startup_error();
  const auto expect_cache_only = [&] {
    EXPECT_TRUE(rc::may_rebuild_startup_dictionary_cache(&thd));
    EXPECT_FALSE(rc::enforce_sql_command_admission(&thd));
    EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
    EXPECT_FALSE(rc::commit_admission_open_for_test());
    EXPECT_EQ(0U, rc::commit_admission_count_for_test());
  };
  expect_cache_only();
  EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(nullptr));
  for (const auto kind : {NON_SYSTEM_THREAD, SYSTEM_THREAD_BACKGROUND,
                          SYSTEM_THREAD_DD_RESTART,
                          SYSTEM_THREAD_INIT_FILE,
                          SYSTEM_THREAD_SERVER_UPGRADE}) {
    thd.system_thread = kind;
    EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  }
  thd.system_thread = SYSTEM_THREAD_DD_INITIALIZE;
  for (const auto stage : {dd::bootstrap::Stage::CREATED_TABLESPACES,
                           dd::bootstrap::Stage::CREATED_TABLES,
                           dd::bootstrap::Stage::SYNCED,
                           dd::bootstrap::Stage::FINISHED}) {
    context.set_stage(stage);
    EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  }
  context.set_stage(dd::bootstrap::Stage::FETCHED_PROPERTIES);
  context.set_actual_dd_version(dd::DD_VERSION - 1);
  EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  context.set_actual_dd_version(dd::DD_VERSION);
  context.set_upgraded_server_version(MYSQL_VERSION_ID - 1);
  EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  context.set_upgraded_server_version(MYSQL_VERSION_ID);
  opt_initialize = true;
  EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  opt_initialize = false;
  thd.lex->sql_command = SQLCOM_DROP_TABLE;
  EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  thd.lex->sql_command = SQLCOM_CREATE_TABLE;
  table.db = "user_schema";
  EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  table.db = "mysql";
  table.table_name = "user_table";
  EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  table.table_name = "dd_properties";
  table.next_global = &table;
  EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  table.next_global = nullptr;
  create_info.options |= HA_LEX_CREATE_TMP_TABLE;
  EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  create_info.options &= ~HA_LEX_CREATE_TMP_TABLE;
  rc::reset_commit_admission_for_test(true);
  EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  rc::reset_commit_admission_for_test(false);
  expect_cache_only();
  rc::shutdown();
  EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));

  rc::reset_startup_lifecycle_for_test();
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::TAKEOVER_RECOVERY);
  expect_cache_only();
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       DictionaryCacheFkChecksAreSingleLiteralSessionSettings) {
  auto &context = dd::bootstrap::DD_bootstrap_ctx::instance();
  const auto saved_context = context;
  const bool saved_initialize = opt_initialize;
  my_testing::Server_initializer initializer;
  initializer.SetUp();
  auto restore = create_scope_guard([&] {
    initializer.thd()->lex->var_list.clear();
    initializer.thd()->system_thread = NON_SYSTEM_THREAD;
    initializer.TearDown();
    context = saved_context;
    opt_initialize = saved_initialize;
  });
  opt_initialize = false;
  context.set_stage(dd::bootstrap::Stage::FETCHED_PROPERTIES);
  context.set_actual_dd_version(dd::DD_VERSION);
  context.set_upgraded_server_version(MYSQL_VERSION_ID);
  THD *thd = initializer.thd();
  thd->system_thread = SYSTEM_THREAD_DD_INITIALIZE;
  thd->lex->sql_command = SQLCOM_SET_OPTION;
  thd->lex->no_write_to_binlog = true;
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  ASSERT_FALSE(rc::activate_installed_root(activation)) << rc::startup_error();
  for (const auto scope : {OPT_DEFAULT, OPT_SESSION, OPT_GLOBAL,
                           OPT_PERSIST, OPT_PERSIST_ONLY}) {
    for (const char *name : {"foreign_key_checks", "sql_log_bin"}) {
      for (const int number : {-1, 0, 1, 2}) {
        Item_int *value = new (thd->mem_root) Item_int(number);
        set_var assignment(scope, System_variable_tracker::make_tracker(name),
                           value);
        thd->lex->var_list.push_back(&assignment, thd->mem_root);
        const bool allowed =
            (scope == OPT_DEFAULT || scope == OPT_SESSION) &&
            std::string_view(name) == "foreign_key_checks" &&
            (number == 0 || number == 1);
        EXPECT_EQ(allowed, rc::may_rebuild_startup_dictionary_cache(thd));
        if (allowed) {
          EXPECT_FALSE(rc::enforce_sql_command_admission(thd));
          EXPECT_FALSE(rc::may_initialize_system_tables(thd));
          context.set_stage(dd::bootstrap::Stage::CREATED_TABLES);
          EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(thd));
          context.set_stage(dd::bootstrap::Stage::FETCHED_PROPERTIES);
          thd->lex->var_list.push_back(&assignment, thd->mem_root);
          EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(thd));
        }
        thd->lex->var_list.clear();
      }
    }
  }
  set_var default_value(
      OPT_SESSION, System_variable_tracker::make_tracker("foreign_key_checks"),
      nullptr);
  thd->lex->var_list.push_back(&default_value, thd->mem_root);
  EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(thd));
  thd->lex->var_list.clear();

  const std::pair<const char *, bool> statements[] = {
      {"SET FOREIGN_KEY_CHECKS=0", true},
      {"SET FOREIGN_KEY_CHECKS=1", true},
      {"SET SESSION foreign_key_checks=0", true},
      {"SET @@session.foreign_key_checks=1", true},
      {"SET GLOBAL foreign_key_checks=1", false},
      {"SET PERSIST foreign_key_checks=1", false},
      {"SET PERSIST_ONLY foreign_key_checks=1", false},
      {"SET foreign_key_checks=DEFAULT", false},
      {"SET foreign_key_checks='1'", false},
      {"SET foreign_key_checks=0+1", false},
      {"SET foreign_key_checks=2", false},
      {"SET foreign_key_checks=1, sql_log_bin=0", false},
      {"SET sql_log_bin=0", false},
      {"SET @foreign_key_checks=1", false}};
  for (const auto &[query, allowed] : statements) {
    SCOPED_TRACE(query);
    std::string sql(query);
    Parser_state parser;
    ASSERT_FALSE(parser.init(thd, sql.data(), sql.size()));
    lex_start(thd);
    mysql_reset_thd_for_next_command(thd);
    ASSERT_FALSE(parse_sql(thd, &parser, nullptr));
    thd->lex->no_write_to_binlog = true;
    EXPECT_EQ(allowed, rc::may_rebuild_startup_dictionary_cache(thd));
    if (allowed) EXPECT_FALSE(rc::enforce_sql_command_admission(thd));
    thd->lex->var_list.clear();
  }
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
       SyncedDictionaryAllowsOnlyCacheFlushAndReadOnlyValidation) {
  auto &context = dd::bootstrap::DD_bootstrap_ctx::instance();
  const auto saved_context = context;
  const bool saved_initialize = opt_initialize;
  auto restore = create_scope_guard([&] {
    context = saved_context;
    opt_initialize = saved_initialize;
  });
  opt_initialize = false;
  context.set_stage(dd::bootstrap::Stage::SYNCED);
  context.set_actual_dd_version(dd::DD_VERSION);
  context.set_upgraded_server_version(MYSQL_VERSION_ID);
  THD thd(false);
  thd.system_thread = SYSTEM_THREAD_DD_INITIALIZE;
  thd.lex->sql_command = SQLCOM_FLUSH;
  thd.lex->type = REFRESH_TABLES;
  thd.lex->no_write_to_binlog = false;
  const auto expect_allowed = [&] {
    EXPECT_TRUE(rc::may_rebuild_startup_dictionary_cache(&thd));
    EXPECT_TRUE(rc::may_validate_startup_dictionary_contents(&thd));
    EXPECT_FALSE(rc::enforce_sql_command_admission(&thd));
    EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
    EXPECT_FALSE(rc::commit_admission_open_for_test());
    EXPECT_EQ(0U, rc::commit_admission_count_for_test());
  };
  initialize(true);
  EXPECT_FALSE(rc::may_validate_startup_dictionary_contents(&thd));
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  EXPECT_FALSE(rc::may_validate_startup_dictionary_contents(&thd));
  ASSERT_FALSE(rc::activate_installed_root(activation));
  expect_allowed();
  const ulong rejected_flags[] = {REFRESH_LOG,
                                  REFRESH_TABLES | REFRESH_READ_LOCK,
                                  REFRESH_TABLES | REFRESH_FOR_EXPORT};
  for (const auto flags : rejected_flags) {
    thd.lex->type = flags;
    EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  }
  thd.lex->type = REFRESH_TABLES;
  thd.lex->no_write_to_binlog = true;
  EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  thd.lex->no_write_to_binlog = false;
  Table_ref table;
  thd.lex->query_tables = &table;
  EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  thd.lex->query_tables = nullptr;
  for (const auto kind : {NON_SYSTEM_THREAD, SYSTEM_THREAD_BACKGROUND,
                          SYSTEM_THREAD_SERVER_UPGRADE}) {
    thd.system_thread = kind;
    EXPECT_FALSE(rc::may_validate_startup_dictionary_contents(&thd));
    EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  }
  thd.system_thread = SYSTEM_THREAD_DD_INITIALIZE;
  for (const auto stage : {dd::bootstrap::Stage::FETCHED_PROPERTIES,
                           dd::bootstrap::Stage::POPULATED,
                           dd::bootstrap::Stage::FINISHED}) {
    context.set_stage(stage);
    EXPECT_FALSE(rc::may_validate_startup_dictionary_contents(&thd));
    EXPECT_FALSE(rc::may_rebuild_startup_dictionary_cache(&thd));
  }
  context.set_stage(dd::bootstrap::Stage::SYNCED);
  context.set_upgraded_server_version(MYSQL_VERSION_ID - 1);
  EXPECT_FALSE(rc::may_validate_startup_dictionary_contents(&thd));
  context.set_upgraded_server_version(MYSQL_VERSION_ID);
  opt_initialize = true;
  EXPECT_FALSE(rc::may_validate_startup_dictionary_contents(&thd));
  opt_initialize = false;
  rc::reset_commit_admission_for_test(true);
  EXPECT_FALSE(rc::may_validate_startup_dictionary_contents(&thd));
  rc::reset_commit_admission_for_test(false);
  rc::shutdown();
  EXPECT_FALSE(rc::may_validate_startup_dictionary_contents(&thd));
  rc::reset_startup_lifecycle_for_test();
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::TAKEOVER_RECOVERY);
  expect_allowed();
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       ResourceGroupsRequireCompletedAuthenticatedDictionaryRestart) {
  auto &context = dd::bootstrap::DD_bootstrap_ctx::instance();
  const auto saved_context = context;
  const bool saved_initialize = opt_initialize;
  auto restore = create_scope_guard([&] {
    context = saved_context;
    opt_initialize = saved_initialize;
  });
  opt_initialize = false;
  context.set_stage(dd::bootstrap::Stage::FINISHED);
  context.set_actual_dd_version(dd::DD_VERSION);
  context.set_upgraded_server_version(MYSQL_VERSION_ID);
  THD thd(false);
  thd.system_thread = SYSTEM_THREAD_DD_INITIALIZE;
  initialize(true);
  EXPECT_FALSE(rc::may_validate_startup_resource_groups(&thd));
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  EXPECT_FALSE(rc::may_validate_startup_resource_groups(&thd));
  ASSERT_FALSE(rc::activate_installed_root(activation));
  const auto expect_allowed = [&] {
    EXPECT_TRUE(rc::may_validate_startup_resource_groups(&thd));
    EXPECT_FALSE(rc::may_validate_startup_dictionary_contents(&thd));
    EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
    EXPECT_FALSE(rc::commit_admission_open_for_test());
    EXPECT_EQ(0U, rc::commit_admission_count_for_test());
  };
  expect_allowed();
  EXPECT_FALSE(rc::may_validate_startup_resource_groups(nullptr));
  for (const auto kind : {NON_SYSTEM_THREAD, SYSTEM_THREAD_BACKGROUND,
                          SYSTEM_THREAD_DD_RESTART,
                          SYSTEM_THREAD_SERVER_UPGRADE}) {
    thd.system_thread = kind;
    EXPECT_FALSE(rc::may_validate_startup_resource_groups(&thd));
  }
  thd.system_thread = SYSTEM_THREAD_DD_INITIALIZE;
  for (const auto stage : {dd::bootstrap::Stage::FETCHED_PROPERTIES,
                           dd::bootstrap::Stage::SYNCED,
                           dd::bootstrap::Stage::POPULATED}) {
    context.set_stage(stage);
    EXPECT_FALSE(rc::may_validate_startup_resource_groups(&thd));
  }
  context.set_stage(dd::bootstrap::Stage::FINISHED);
  context.set_upgraded_server_version(MYSQL_VERSION_ID - 1);
  EXPECT_FALSE(rc::may_validate_startup_resource_groups(&thd));
  context.set_upgraded_server_version(MYSQL_VERSION_ID);
  opt_initialize = true;
  EXPECT_FALSE(rc::may_validate_startup_resource_groups(&thd));
  opt_initialize = false;
  rc::reset_commit_admission_for_test(true);
  EXPECT_FALSE(rc::may_validate_startup_resource_groups(&thd));
  rc::reset_commit_admission_for_test(false);
  rc::shutdown();
  EXPECT_FALSE(rc::may_validate_startup_resource_groups(&thd));
  rc::reset_startup_lifecycle_for_test();
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::TAKEOVER_RECOVERY);
  expect_allowed();
  rc::reset_startup_lifecycle_for_test();
  initialize(false);
  epoch_proof.head_body.clear();
  epoch_proof.head_etag.clear();
  epoch_proof.head_generation = 0;
  adopt(rc::StartupEpochAdoptionRole::BOOTSTRAP_SNAPSHOT);
  EXPECT_FALSE(rc::may_validate_startup_resource_groups(&thd));
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       RestoredPfsScopeBindsOneThreadDefinitionAndStartupAuthority) {
  auto &context = dd::bootstrap::DD_bootstrap_ctx::instance();
  const auto saved_context = context;
  const bool saved_initialize = opt_initialize;
  auto restore = create_scope_guard([&] {
    context = saved_context;
    opt_initialize = saved_initialize;
  });
  opt_initialize = false;
  context.set_stage(dd::bootstrap::Stage::FINISHED);
  context.set_actual_dd_version(dd::DD_VERSION);
  context.set_upgraded_server_version(MYSQL_VERSION_ID);
  THD thd(false);
  thd.system_thread = SYSTEM_THREAD_BACKGROUND;
  const Plugin_table definition("performance_schema", "innodb_redo_log_files",
                                "FILE_ID BIGINT NOT NULL",
                                "engine = 'performance_schema'", nullptr);
  const Plugin_table changed("performance_schema", "innodb_redo_log_files",
                             "FILE_ID INT NOT NULL",
                             "engine = 'performance_schema'", nullptr);
  initialize(true);
  {
    rc::Scoped_startup_pfs_restore scope(&thd, &definition);
    EXPECT_FALSE(scope.active());
  }
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  {
    rc::Scoped_startup_pfs_restore scope(&thd, &definition);
    EXPECT_FALSE(scope.active());
  }
  ASSERT_FALSE(rc::activate_installed_root(activation));
  const auto expect_scope = [&] {
    EXPECT_FALSE(rc::startup_pfs_restore_active(&thd));
    {
      rc::Scoped_startup_pfs_restore scope(&thd, &definition);
      ASSERT_TRUE(scope.active());
      EXPECT_TRUE(rc::startup_pfs_restore_active(&thd));
      EXPECT_FALSE(rc::startup_pfs_restore_active(nullptr));
      EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
      EXPECT_EQ(SYSTEM_THREAD_BACKGROUND, thd.system_thread);
      EXPECT_TRUE(rc::validate_startup_pfs_table(&thd, &changed));
      std::thread other([&] {
        EXPECT_FALSE(rc::startup_pfs_restore_active(&thd));
      });
      other.join();
      rc::reset_commit_admission_for_test(true);
      EXPECT_TRUE(rc::startup_pfs_restore_active(&thd));
      EXPECT_TRUE(rc::validate_startup_pfs_table(&thd, &definition));
      rc::reset_commit_admission_for_test(false);
      EXPECT_EQ(0U, rc::commit_admission_count_for_test());
    }
    EXPECT_FALSE(rc::startup_pfs_restore_active(&thd));
    EXPECT_EQ(SYSTEM_THREAD_BACKGROUND, thd.system_thread);
  };
  expect_scope();
  for (const auto kind : {NON_SYSTEM_THREAD, SYSTEM_THREAD_DD_INITIALIZE,
                          SYSTEM_THREAD_DD_RESTART,
                          SYSTEM_THREAD_SERVER_UPGRADE}) {
    thd.system_thread = kind;
    rc::Scoped_startup_pfs_restore scope(&thd, &definition);
    EXPECT_FALSE(scope.active());
  }
  thd.system_thread = SYSTEM_THREAD_BACKGROUND;
  context.set_stage(dd::bootstrap::Stage::SYNCED);
  {
    rc::Scoped_startup_pfs_restore scope(&thd, &definition);
    EXPECT_FALSE(scope.active());
  }
  context.set_stage(dd::bootstrap::Stage::FINISHED);
  context.set_upgraded_server_version(MYSQL_VERSION_ID - 1);
  {
    rc::Scoped_startup_pfs_restore scope(&thd, &definition);
    EXPECT_FALSE(scope.active());
  }
  context.set_upgraded_server_version(MYSQL_VERSION_ID);
  opt_initialize = true;
  {
    rc::Scoped_startup_pfs_restore scope(&thd, &definition);
    EXPECT_FALSE(scope.active());
  }
  opt_initialize = false;
  rc::shutdown();
  {
    rc::Scoped_startup_pfs_restore scope(&thd, &definition);
    EXPECT_FALSE(scope.active());
  }
  rc::reset_startup_lifecycle_for_test();
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::TAKEOVER_RECOVERY);
  expect_scope();
  rc::reset_startup_lifecycle_for_test();
  initialize(false);
  epoch_proof.head_body.clear();
  epoch_proof.head_etag.clear();
  epoch_proof.head_generation = 0;
  adopt(rc::StartupEpochAdoptionRole::BOOTSTRAP_SNAPSHOT);
  rc::Scoped_startup_pfs_restore scope(&thd, &definition);
  EXPECT_FALSE(scope.active());
}

TEST(RemoteCommitStartupDictionary, PfsDefinitionComparisonRejectsMetadataChanges) {
  namespace rc = wesql::remote_commit;
  THD thd(false);
  dd::Table_impl stored;
  stored.set_name("innodb_redo_log_files");
  stored.set_engine("performance_schema");
  stored.set_collation_id(my_charset_utf8mb4_0900_ai_ci.number);
  auto *column = stored.add_column();
  column->set_name("FILE_ID");
  column->set_type(dd::enum_column_types::LONGLONG);
  column->set_nullable(false);
  const auto clone = [&] {
    return std::unique_ptr<dd::Table>(static_cast<const dd::Table &>(stored).clone());
  };
  const auto matches = [&](dd::Table *expected) {
    return rc::startup_pfs_table_definition_matches(
        &thd, &stored, expected, "performance_schema");
  };
  auto expected = clone();
  ASSERT_TRUE(matches(expected.get()));
  stored.set_id(12345);
  expected->set_created(123);
  expected->set_last_altered(456);
  EXPECT_TRUE(matches(expected.get()));
  expected->set_engine("InnoDB");
  EXPECT_FALSE(matches(expected.get()));
  expected = clone();
  expected->set_name("other_table");
  EXPECT_FALSE(matches(expected.get()));
  expected = clone();
  expected->set_comment("changed");
  EXPECT_FALSE(matches(expected.get()));
  expected = clone();
  (*expected->columns()->begin())->set_type(dd::enum_column_types::LONG);
  EXPECT_FALSE(matches(expected.get()));
  expected = clone();
  (*expected->columns()->begin())->set_nullable(true);
  EXPECT_FALSE(matches(expected.get()));
  expected = clone();
  (*expected->columns()->begin())->set_comment("changed");
  EXPECT_FALSE(matches(expected.get()));
  expected = clone();
  (*expected->columns()->begin())->set_default_option("CURRENT_TIMESTAMP");
  EXPECT_FALSE(matches(expected.get()));
  expected = clone();
  expected->add_column()->set_name("EXTRA");
  EXPECT_FALSE(matches(expected.get()));
  EXPECT_FALSE(matches(nullptr));
}

TEST(RemoteCommitStartupDictionary, ResourceGroupCpuRangesPreserveTheHighestBit) {
  namespace rc = wesql::remote_commit;
  constexpr size_t highest = dd::CPU_MASK_SIZE - 1;
  const auto roundtrip = [&](const std::bitset<dd::CPU_MASK_SIZE> &mask) {
    auto ranges = rc::startup_resource_group_cpu_ranges(mask);
    dd::Resource_group_impl stored;
    stored.set_cpu_id_mask(ranges);
    EXPECT_EQ(mask, stored.cpu_id_mask());
    return ranges;
  };
  std::bitset<dd::CPU_MASK_SIZE> mask;
  EXPECT_TRUE(roundtrip(mask).empty());
  for (size_t bit = 0; bit <= highest; ++bit) {
    mask.reset();
    mask.set(bit);
    const auto ranges = roundtrip(mask);
    ASSERT_EQ(1U, ranges.size());
    EXPECT_EQ(bit, ranges[0].m_start);
    EXPECT_EQ(bit, ranges[0].m_end);
  }
  mask.set(highest - 1);
  mask.set(highest - 2);
  auto ranges = roundtrip(mask);
  ASSERT_EQ(1U, ranges.size());
  EXPECT_EQ(highest - 2, ranges[0].m_start);
  EXPECT_EQ(highest, ranges[0].m_end);
  mask.set(0);
  mask.set(2);
  ranges = roundtrip(mask);
  ASSERT_EQ(3U, ranges.size());
  EXPECT_EQ(0U, ranges[0].m_start);
  EXPECT_EQ(0U, ranges[0].m_end);
  EXPECT_EQ(2U, ranges[1].m_start);
  EXPECT_EQ(2U, ranges[1].m_end);
  EXPECT_EQ(highest - 2, ranges[2].m_start);
  EXPECT_EQ(highest, ranges[2].m_end);
  mask.set();
  ranges = roundtrip(mask);
  ASSERT_EQ(1U, ranges.size());
  EXPECT_EQ(0U, ranges[0].m_start);
  EXPECT_EQ(highest, ranges[0].m_end);
}

TEST(RemoteCommitStartupDictionary, DefaultResourceGroupsMatchEveryStoredField) {
  namespace rc = wesql::remote_commit;
  using resourcegroups::Type;
  EXPECT_FALSE(rc::startup_default_resource_group_matches(nullptr, false));
  for (const bool system : {false, true}) {
    dd::Resource_group_impl group;
    const char *name = system ? "SYS_default" : "USR_default";
    const auto type = system ? Type::SYSTEM_RESOURCE_GROUP
                             : Type::USER_RESOURCE_GROUP;
    group.set_name(name);
    group.set_resource_group_type(type);
    group.set_resource_group_enabled(true);
    group.set_thread_priority(0);
    group.set_cpu_id_mask({});
    ASSERT_TRUE(rc::startup_default_resource_group_matches(&group, system));
    EXPECT_FALSE(rc::startup_default_resource_group_matches(&group, !system));
    group.set_name(system ? "sys_default" : "usr_default");
    EXPECT_FALSE(rc::startup_default_resource_group_matches(&group, system));
    group.set_name(name);
    group.set_resource_group_type(system ? Type::USER_RESOURCE_GROUP
                                         : Type::SYSTEM_RESOURCE_GROUP);
    EXPECT_FALSE(rc::startup_default_resource_group_matches(&group, system));
    group.set_resource_group_type(type);
    group.set_resource_group_enabled(false);
    EXPECT_FALSE(rc::startup_default_resource_group_matches(&group, system));
    group.set_resource_group_enabled(true);
    group.set_thread_priority(1);
    EXPECT_FALSE(rc::startup_default_resource_group_matches(&group, system));
    group.set_thread_priority(0);
    group.set_cpu_id_mask({resourcegroups::Range(0, 0)});
    EXPECT_FALSE(rc::startup_default_resource_group_matches(&group, system));
    group.set_cpu_id_mask({});
    EXPECT_TRUE(rc::startup_default_resource_group_matches(&group, system));
  }
}

TEST(RemoteCommitStartupDictionary, RejectsRowSetAndEveryPersistedFieldMismatch) {
  CHARSET_INFO primary = my_charset_latin1;
  CHARSET_INFO secondary = my_charset_latin1_bin;
  primary.state = MY_CS_PRIMARY | MY_CS_AVAILABLE | MY_CS_COMPILED;
  secondary.state = MY_CS_AVAILABLE | MY_CS_COMPILED;
  const CHARSET_INFO *compiled[] = {&primary, &secondary};
  dd::Charset_impl charset;
  charset.set_id(8);
  charset.set_name("latin1");
  charset.set_default_collation_id(8);
  charset.set_mb_max_length(1);
  charset.set_comment("cp1252 West European");
  dd::Collation_impl swedish;
  swedish.set_id(8);
  swedish.set_name("latin1_swedish_ci");
  swedish.set_charset_id(8);
  swedish.set_is_compiled(true);
  swedish.set_sort_length(1);
  swedish.set_pad_attribute(dd::Collation::PA_PAD_SPACE);
  dd::Collation_impl binary;
  binary.set_id(47);
  binary.set_name("latin1_bin");
  binary.set_charset_id(8);
  binary.set_is_compiled(true);
  binary.set_sort_length(1);
  binary.set_pad_attribute(dd::Collation::PA_PAD_SPACE);
  std::vector<const dd::Charset *> charsets{&charset};
  std::vector<const dd::Collation *> collations{&swedish, &binary};
  const auto matches = [&] {
    return rc::startup_character_sets_match(charsets, collations, compiled);
  };
  ASSERT_TRUE(matches());
  for (int field = 0; field != 5; ++field) {
    if (field == 0) charset.set_id(9);
    if (field == 1) charset.set_name("other");
    if (field == 2) charset.set_default_collation_id(47);
    if (field == 3) charset.set_mb_max_length(2);
    if (field == 4) charset.set_comment("changed");
    EXPECT_FALSE(matches()) << field;
    charset.set_id(8);
    charset.set_name("latin1");
    charset.set_default_collation_id(8);
    charset.set_mb_max_length(1);
    charset.set_comment("cp1252 West European");
  }
  for (int field = 0; field != 6; ++field) {
    if (field == 0) binary.set_id(48);
    if (field == 1) binary.set_name("other");
    if (field == 2) binary.set_charset_id(47);
    if (field == 3) binary.set_is_compiled(false);
    if (field == 4) binary.set_sort_length(2);
    if (field == 5) binary.set_pad_attribute(dd::Collation::PA_NO_PAD);
    EXPECT_FALSE(matches()) << field;
    binary.set_id(47);
    binary.set_name("latin1_bin");
    binary.set_charset_id(8);
    binary.set_is_compiled(true);
    binary.set_sort_length(1);
    binary.set_pad_attribute(dd::Collation::PA_PAD_SPACE);
  }
  collations.pop_back();
  EXPECT_FALSE(matches());
  collations.push_back(&binary);
  collations.push_back(&swedish);
  EXPECT_FALSE(matches());
  collations.back() = nullptr;
  EXPECT_FALSE(matches());
  collations.pop_back();
  charsets.clear();
  EXPECT_FALSE(matches());
  charsets.push_back(&charset);
  charsets.push_back(&charset);
  EXPECT_FALSE(matches());
  charsets.pop_back();
  secondary.state = 0;
  EXPECT_FALSE(matches());
  collations.pop_back();
  EXPECT_TRUE(matches());
}

TEST(RemoteCommitStartupDictionary, TemporaryTablespaceRequiresExactSingleFile) {
  dd::Tablespace_impl space;
  space.set_name("innodb_temporary");
  space.set_engine("InnoDB");
  const auto matches = [&](std::string_view filename = "./ibtmp1") {
    return rc::startup_temporary_tablespace_matches(&space, filename);
  };
  EXPECT_FALSE(rc::startup_temporary_tablespace_matches(nullptr, "./ibtmp1"));
  EXPECT_FALSE(matches());
  auto *file = space.add_file();
  file->set_filename("./ibtmp1");
  EXPECT_TRUE(matches());
  EXPECT_FALSE(matches(""));
  EXPECT_FALSE(matches("ibtmp1"));
  EXPECT_FALSE(matches("./ibtmp2"));
  EXPECT_FALSE(matches("/other/root/ibtmp1"));
  file->set_filename("./ibtmp2");
  EXPECT_FALSE(matches());
  file->set_filename("./ibtmp1");
  space.set_name("user_tablespace");
  EXPECT_FALSE(matches());
  space.set_name("innodb_temporary");
  space.set_engine("OTHER");
  EXPECT_FALSE(matches());
  space.set_engine("InnoDB");
  EXPECT_TRUE(matches());
  space.add_file()->set_filename("./ibtmp1");
  EXPECT_FALSE(matches());
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
       BootstrapEvidenceSamplingSelectsExactRoleAndRechecksAuthority) {
  auto &context = dd::bootstrap::DD_bootstrap_ctx::instance();
  const auto saved_context = context;
  const bool saved_initialize = opt_initialize;
  auto restore = create_scope_guard([&] {
    context = saved_context;
    opt_initialize = saved_initialize;
  });
  opt_initialize = false;
  context.set_stage(dd::bootstrap::Stage::FINISHED);
  context.set_actual_dd_version(dd::DD_VERSION);
  context.set_upgraded_server_version(MYSQL_VERSION_ID);
  THD thd(false);
  thd.system_thread = SYSTEM_THREAD_BACKGROUND;
  const auto allowed = [&](bool installed) {
    return rc::may_collect_bootstrap_root_evidence(&thd, installed);
  };

  initialize(false);
  EXPECT_FALSE(allowed(false));
  EXPECT_FALSE(allowed(true));
  epoch_proof.head_body.clear();
  epoch_proof.head_etag.clear();
  epoch_proof.head_generation = 0;
  adopt(rc::StartupEpochAdoptionRole::BOOTSTRAP_SNAPSHOT);
  EXPECT_TRUE(allowed(false));
  EXPECT_FALSE(allowed(true));
  rc::reset_commit_admission_for_test(true);
  EXPECT_FALSE(allowed(false));
  rc::reset_commit_admission_for_test(false);
  EXPECT_TRUE(allowed(false));
  rc::shutdown();
  EXPECT_FALSE(allowed(false));

  rc::reset_startup_lifecycle_for_test();
  initialize(true);
  epoch_proof.head_body = head_body;
  epoch_proof.head_etag = head_etag;
  epoch_proof.head_generation = head.generation;
  adopt(rc::StartupEpochAdoptionRole::TAKEOVER_RECOVERY);
  EXPECT_FALSE(allowed(false));
  EXPECT_FALSE(allowed(true));

  rc::reset_startup_lifecycle_for_test();
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  EXPECT_FALSE(allowed(true));
  ASSERT_FALSE(rc::activate_installed_root(activation));
  EXPECT_TRUE(allowed(true));
  EXPECT_FALSE(allowed(false));
  EXPECT_FALSE(rc::may_collect_bootstrap_root_evidence(nullptr, true));
  EXPECT_FALSE(rc::may_initialize_system_tables(&thd));
  EXPECT_FALSE(rc::commit_admission_open_for_test());
  EXPECT_EQ(0U, rc::commit_admission_count_for_test());
  for (const auto kind : {NON_SYSTEM_THREAD, SYSTEM_THREAD_DD_INITIALIZE,
                          SYSTEM_THREAD_DD_RESTART,
                          SYSTEM_THREAD_SERVER_UPGRADE}) {
    thd.system_thread = kind;
    EXPECT_FALSE(allowed(true));
  }
  thd.system_thread = SYSTEM_THREAD_BACKGROUND;
  context.set_stage(dd::bootstrap::Stage::SYNCED);
  EXPECT_FALSE(allowed(true));
  context.set_stage(dd::bootstrap::Stage::FINISHED);
  context.set_actual_dd_version(dd::DD_VERSION - 1);
  EXPECT_FALSE(allowed(true));
  context.set_actual_dd_version(dd::DD_VERSION);
  context.set_upgraded_server_version(MYSQL_VERSION_ID - 1);
  EXPECT_FALSE(allowed(true));
  context.set_upgraded_server_version(MYSQL_VERSION_ID);
  opt_initialize = true;
  EXPECT_FALSE(allowed(true));
  opt_initialize = false;
  opt_binlog_archive_remote_commit = false;
  EXPECT_FALSE(allowed(true));
  opt_binlog_archive_remote_commit = true;
  EXPECT_TRUE(allowed(true));
  rc::reset_commit_admission_for_test(true);
  EXPECT_FALSE(allowed(true));
  rc::reset_commit_admission_for_test(false);
  EXPECT_TRUE(allowed(true));
  // The admission-only reset replaces the published recovery window.
  // Rebuild the authenticated lifecycle before exercising a real reopen.
  rc::reset_startup_lifecycle_for_test();
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  ASSERT_FALSE(rc::activate_installed_root(activation));
  ASSERT_FALSE(rc::verify_installed_root_post_engine(full_proof));
  EXPECT_FALSE(allowed(true));
  EXPECT_FALSE(rc::commit_admission_open_for_test());
  rc::open_commit_admission();
  EXPECT_FALSE(allowed(true));
  rc::shutdown();
  EXPECT_FALSE(allowed(true));
}

TEST_F(RemoteCommitServerHooksLifecycleTest,
       BootstrapEvidenceSamplingRejectsMismatchedEpochHeadAndMarker) {
  auto &context = dd::bootstrap::DD_bootstrap_ctx::instance();
  const auto saved_context = context;
  const bool saved_initialize = opt_initialize;
  auto restore = create_scope_guard([&] {
    context = saved_context;
    opt_initialize = saved_initialize;
  });
  opt_initialize = false;
  context.set_stage(dd::bootstrap::Stage::FINISHED);
  context.set_actual_dd_version(dd::DD_VERSION);
  context.set_upgraded_server_version(MYSQL_VERSION_ID);
  THD thd(false);
  thd.system_thread = SYSTEM_THREAD_BACKGROUND;
  const auto allowed = [&] {
    return rc::may_collect_bootstrap_root_evidence(&thd, true);
  };
  for (int field = 0; field != 6; ++field) {
    rc::reset_startup_lifecycle_for_test();
    initialize(true);
    auto wrong = epoch_proof;
    switch (field) {
      case 0: ++wrong.value.epoch; break;
      case 1: wrong.body += " "; break;
      case 2: wrong.etag += "x"; break;
      case 3: ++wrong.head_generation; break;
      case 4: wrong.head_body += " "; break;
      case 5: wrong.head_etag += "x"; break;
    }
    EXPECT_TRUE(rc::adopt_startup_epoch(
        wrong, rc::StartupEpochAdoptionRole::INSTALLED_ROOT));
    EXPECT_FALSE(allowed());
  }
  rc::reset_startup_lifecycle_for_test();
  initialize(true);
  adopt(rc::StartupEpochAdoptionRole::INSTALLED_ROOT);
  for (int field = 0; field != 3; ++field) {
    auto wrong = activation;
    switch (field) {
      case 0: wrong.head_body += " "; break;
      case 1: wrong.head_etag += "x"; break;
      case 2: ++wrong.marker.installed_head.generation; break;
    }
    EXPECT_TRUE(rc::activate_installed_root(wrong));
    EXPECT_FALSE(allowed());
  }
  ASSERT_FALSE(rc::activate_installed_root(activation));
  EXPECT_TRUE(allowed());
  rc::shutdown();
  EXPECT_FALSE(allowed());
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
