/* Copyright (c) 2026, WeSQL and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License, version 2.0, for more details. */

#include <array>
#include <string>

#include <gtest/gtest.h>

#define LOG_COMPONENT_TAG "Clone"
#include "clone0api.h"
#include "clone0remotecuts.h"
#include "mysql/components/services/log_builtins.h"
#include "plugin/clone/include/clone_local.h"
#include "sql/mysqld.h"

namespace remote_clone_cut_unittest {

struct ObserverProbe {
  const unsigned char *locator{nullptr};
  unsigned int locator_length{0};
  uint64_t request_id{0};
  std::string phases;
  int bind_result{0};
  int complete_result{0};
};

int bind_local_clone(THD *, uint64_t request_id,
                     const unsigned char *locator,
                     unsigned int locator_length, void *context) {
  auto *probe = static_cast<ObserverProbe *>(context);
  probe->request_id = request_id;
  probe->locator = locator;
  probe->locator_length = locator_length;
  probe->phases.push_back('B');
  return probe->bind_result;
}

int complete_local_clone(THD *, uint64_t request_id,
                         const unsigned char *locator,
                         unsigned int locator_length, void *context) {
  auto *probe = static_cast<ObserverProbe *>(context);
  probe->request_id = request_id;
  probe->locator = locator;
  probe->locator_length = locator_length;
  probe->phases.push_back('C');
  return probe->complete_result;
}

TEST(RemoteCloneObserverTest,
     BindsThenCompletesUniqueInnodbLocatorWithExactRequestId) {
  handlerton innodb{};
  innodb.db_type = DB_TYPE_INNODB;
  handlerton other{};
  other.db_type = DB_TYPE_MYISAM;
  const unsigned char innodb_locator[]{1, 2, 3};
  const unsigned char other_locator[]{4, 5};
  myclone::Storage_Vector storage{
      {&other, other_locator, sizeof(other_locator)},
      {&innodb, innodb_locator, sizeof(innodb_locator)}};
  ObserverProbe probe;
  probe.bind_result = 71;
  probe.complete_result = 73;
  Mysql_clone_local_observer observer{9001, bind_local_clone,
                                      complete_local_clone, &probe};

  EXPECT_EQ(myclone::bind_local_clone_observer(nullptr, storage, &observer),
            71);
  EXPECT_EQ(probe.phases, "B");
  EXPECT_EQ(probe.request_id, 9001U);
  EXPECT_EQ(probe.locator, innodb_locator);
  EXPECT_EQ(probe.locator_length, sizeof(innodb_locator));

  EXPECT_EQ(myclone::complete_local_clone_observer(nullptr, storage, &observer),
            73);
  EXPECT_EQ(probe.phases, "BC");
  EXPECT_EQ(probe.request_id, 9001U);
  EXPECT_EQ(probe.locator, innodb_locator);
  EXPECT_EQ(probe.locator_length, sizeof(innodb_locator));
}

TEST(RemoteCloneObserverTest, RejectsInvalidCallbacksAndLocatorCardinality) {
  handlerton innodb{};
  innodb.db_type = DB_TYPE_INNODB;
  const unsigned char innodb_locator[]{1, 2, 3};
  myclone::Storage_Vector storage;
  ObserverProbe probe;
  Mysql_clone_local_observer observer{9001, bind_local_clone,
                                      complete_local_clone, &probe};

  EXPECT_EQ(myclone::bind_local_clone_observer(nullptr, storage, &observer),
            ER_INTERNAL_ERROR);
  EXPECT_TRUE(probe.phases.empty());

  EXPECT_EQ(myclone::bind_local_clone_observer(nullptr, storage, nullptr), 0);
  EXPECT_EQ(myclone::complete_local_clone_observer(nullptr, storage, nullptr),
            0);
  EXPECT_TRUE(probe.phases.empty());

  storage.push_back({&innodb, innodb_locator, sizeof(innodb_locator)});
  Mysql_clone_local_observer missing_bind{9001, nullptr, complete_local_clone,
                                         &probe};
  EXPECT_EQ(
      myclone::complete_local_clone_observer(nullptr, storage, &missing_bind),
            ER_INTERNAL_ERROR);
  Mysql_clone_local_observer missing_complete{9001, bind_local_clone, nullptr,
                                             &probe};
  EXPECT_EQ(
      myclone::bind_local_clone_observer(nullptr, storage, &missing_complete),
      ER_INTERNAL_ERROR);
  Mysql_clone_local_observer zero_request{0, bind_local_clone,
                                         complete_local_clone, &probe};
  EXPECT_EQ(myclone::bind_local_clone_observer(nullptr, storage, &zero_request),
            ER_INTERNAL_ERROR);
  EXPECT_TRUE(probe.phases.empty());

  storage.push_back({&innodb, innodb_locator, sizeof(innodb_locator)});
  EXPECT_EQ(myclone::bind_local_clone_observer(nullptr, storage, &observer),
            ER_INTERNAL_ERROR);
  EXPECT_TRUE(probe.phases.empty());
}

std::string to_hex(const Remote_clone_digest &digest) {
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  result.reserve(2 * digest.size());
  for (const auto value : digest) {
    result.push_back(hex[value >> 4]);
    result.push_back(hex[value & 0x0f]);
  }
  return result;
}

wesql::remote_commit::CloneCutState make_barrier_state() {
  wesql::remote_commit::CloneCutState state;
  state.request_id = 9001;
  state.file = "binlog.000123";
  state.pos = 4567;
  state.canonical_gtid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-7";
  state.gtid_sha256 =
      "42ca87d1dab7cb22c00e3649af75825522e09b0fc561735782af390f52781077";
  state.head_generation = 9;
  state.head_body_sha256 =
      "37e4cb85bb0fd4313f22fe105e3b5771cd40eb2ef87ca8f8ac8c45f07730c759";
  return state;
}

Remote_clone_redo_locator make_redo_locator() {
  Remote_clone_redo_locator locator;
  locator.start_lsn = 100;
  locator.end_lsn = 200;
  locator.trailer_offset = 300;
  locator.trailer_length = 400;
  return locator;
}

void expect_cut_equals(const Remote_clone_cut &actual,
                       const Remote_clone_cut &expected) {
  EXPECT_EQ(actual.clone_handle_id, expected.clone_handle_id);
  EXPECT_EQ(actual.request_id, expected.request_id);
  EXPECT_EQ(actual.binlog_file, expected.binlog_file);
  EXPECT_EQ(actual.binlog_position, expected.binlog_position);
  EXPECT_EQ(actual.gtid_executed, expected.gtid_executed);
  EXPECT_EQ(actual.gtid_digest, expected.gtid_digest);
  EXPECT_EQ(actual.head_generation, expected.head_generation);
  EXPECT_EQ(actual.head_body_sha256, expected.head_body_sha256);
  EXPECT_EQ(actual.redo_locator.version, expected.redo_locator.version);
  EXPECT_EQ(actual.redo_locator.start_lsn, expected.redo_locator.start_lsn);
  EXPECT_EQ(actual.redo_locator.end_lsn, expected.redo_locator.end_lsn);
  EXPECT_EQ(actual.redo_locator.trailer_offset,
            expected.redo_locator.trailer_offset);
  EXPECT_EQ(actual.redo_locator.trailer_length,
            expected.redo_locator.trailer_length);
  EXPECT_EQ(actual.redo_locator_digest, expected.redo_locator_digest);
}

TEST(RemoteCloneCutTest, RedoLocatorDigestHasStableCanonicalEncoding) {
  Remote_clone_redo_locator locator;
  locator.start_lsn = 0x0102030405060708ULL;
  locator.end_lsn = 0x1112131415161718ULL;
  locator.trailer_offset = 0x2122232425262728ULL;
  locator.trailer_length = 0x31323334U;

  constexpr char expected[] =
      "c23b4bcc0be7bf5f07f7a61867f63d323f5ca4fe38c4226c43d5bbec2c407ce5";
  EXPECT_EQ(to_hex(innodb_clone_redo_locator_digest(locator)), expected);
}

TEST(RemoteCloneCutTest, RedoLocatorDigestBindsEveryCoordinate) {
  Remote_clone_redo_locator locator;
  locator.start_lsn = 101;
  locator.end_lsn = 202;
  locator.trailer_offset = 303;
  locator.trailer_length = 404;
  const auto expected = innodb_clone_redo_locator_digest(locator);

  auto changed = locator;
  ++changed.version;
  EXPECT_NE(innodb_clone_redo_locator_digest(changed), expected);

  changed = locator;
  ++changed.start_lsn;
  EXPECT_NE(innodb_clone_redo_locator_digest(changed), expected);

  changed = locator;
  ++changed.end_lsn;
  EXPECT_NE(innodb_clone_redo_locator_digest(changed), expected);

  changed = locator;
  ++changed.trailer_offset;
  EXPECT_NE(innodb_clone_redo_locator_digest(changed), expected);

  changed = locator;
  ++changed.trailer_length;
  EXPECT_NE(innodb_clone_redo_locator_digest(changed), expected);
}

TEST(RemoteCloneCutTest, BuilderRequiresSynchronizedInnodbCursor) {
  const auto barrier = make_barrier_state();
  const auto redo = make_redo_locator();
  Remote_clone_cut cut;
  std::string error;

  ASSERT_FALSE(
      innodb_clone_build_remote_cut(barrier, barrier.file, barrier.pos, 42,
                                    redo, cut, &error));
  EXPECT_TRUE(error.empty());
  EXPECT_EQ(cut.clone_handle_id, 42U);
  EXPECT_EQ(cut.request_id, barrier.request_id);
  EXPECT_EQ(cut.binlog_file, barrier.file);
  EXPECT_EQ(cut.binlog_position, barrier.pos);
  EXPECT_EQ(cut.gtid_executed, barrier.canonical_gtid);
  EXPECT_EQ(to_hex(cut.gtid_digest), barrier.gtid_sha256);
  EXPECT_EQ(cut.head_generation, barrier.head_generation);
  EXPECT_EQ(cut.head_body_sha256, barrier.head_body_sha256);
  EXPECT_EQ(cut.redo_locator.start_lsn, redo.start_lsn);
  EXPECT_EQ(cut.redo_locator.end_lsn, redo.end_lsn);
}

TEST(RemoteCloneCutTest, BuilderFailureLeavesOutputUnchanged) {
  auto barrier = make_barrier_state();
  auto redo = make_redo_locator();
  Remote_clone_cut output;
  output.clone_handle_id = 999;
  output.binlog_file = "sentinel";
  std::string error;

  barrier.gtid_sha256[0] = 'f';
  EXPECT_TRUE(innodb_clone_build_remote_cut(
      barrier, barrier.file, barrier.pos, 42, redo, output, &error));
  EXPECT_EQ(output.clone_handle_id, 999U);
  EXPECT_EQ(output.binlog_file, "sentinel");

  barrier = make_barrier_state();
  ++redo.version;
  EXPECT_TRUE(innodb_clone_build_remote_cut(
      barrier, barrier.file, barrier.pos, 42, redo, output, &error));
  EXPECT_EQ(output.clone_handle_id, 999U);
  EXPECT_EQ(output.binlog_file, "sentinel");

  redo = make_redo_locator();
  redo.end_lsn = redo.start_lsn - 1;
  EXPECT_TRUE(innodb_clone_build_remote_cut(
      barrier, barrier.file, barrier.pos, 42, redo, output, &error));
  EXPECT_EQ(output.clone_handle_id, 999U);
  EXPECT_EQ(output.binlog_file, "sentinel");

  redo = make_redo_locator();
  EXPECT_TRUE(innodb_clone_build_remote_cut(
      barrier, barrier.file, barrier.pos, 0, redo, output, &error));
  EXPECT_EQ(output.clone_handle_id, 999U);
  EXPECT_EQ(output.binlog_file, "sentinel");

  barrier = make_barrier_state();
  barrier.request_id = 0;
  EXPECT_TRUE(innodb_clone_build_remote_cut(
      barrier, barrier.file, barrier.pos, 42, redo, output, &error));
  EXPECT_EQ(output.clone_handle_id, 999U);
  EXPECT_EQ(output.binlog_file, "sentinel");
}

TEST(RemoteCloneCutTest, BuilderRejectsActualCursorMismatchWithoutOutput) {
  const auto barrier = make_barrier_state();
  const auto redo = make_redo_locator();
  Remote_clone_cut output;
  output.clone_handle_id = 999;
  output.binlog_file = "sentinel";
  std::string error;

  EXPECT_TRUE(innodb_clone_build_remote_cut(
      barrier, "binlog.000124", barrier.pos, 42, redo, output, &error));
  EXPECT_EQ(output.clone_handle_id, 999U);
  EXPECT_EQ(output.binlog_file, "sentinel");
  EXPECT_NE(error.find("does not match"), std::string::npos);

  EXPECT_TRUE(innodb_clone_build_remote_cut(
      barrier, barrier.file, barrier.pos + 1, 42, redo, output, &error));
  EXPECT_EQ(output.clone_handle_id, 999U);
  EXPECT_EQ(output.binlog_file, "sentinel");
  EXPECT_NE(error.find("does not match"), std::string::npos);

  EXPECT_TRUE(innodb_clone_build_remote_cut(barrier, "", 0, 42, redo, output,
                                            &error));
  EXPECT_EQ(output.clone_handle_id, 999U);
  EXPECT_EQ(output.binlog_file, "sentinel");
  EXPECT_NE(error.find("invalid synchronized"), std::string::npos);
}

TEST(RemoteCloneCutTest, SlotInstallsMatchingCutOnlyOnce) {
  const auto barrier = make_barrier_state();
  const auto redo = make_redo_locator();
  Remote_clone_cut first;
  Remote_clone_cut second;
  std::string error;
  ASSERT_FALSE(innodb_clone_build_remote_cut(
      barrier, barrier.file, barrier.pos, 42, redo, first, &error));
  ASSERT_FALSE(innodb_clone_build_remote_cut(
      barrier, barrier.file, barrier.pos, 42, redo, second, &error));
  const auto expected = first;

  Remote_clone_cut_slot slot;
  Remote_clone_cut output;
  output.binlog_file = "sentinel";
  EXPECT_FALSE(slot.get(42, barrier.request_id, output));
  EXPECT_EQ(output.binlog_file, "sentinel");
  EXPECT_FALSE(slot.install(7, barrier.request_id, std::move(first)));
  EXPECT_FALSE(slot.install(42, 0, std::move(first)));
  EXPECT_FALSE(slot.install(42, barrier.request_id + 1, std::move(first)));
  EXPECT_TRUE(slot.install(42, barrier.request_id, std::move(first)));
  EXPECT_FALSE(slot.install(42, barrier.request_id, std::move(second)));

  ASSERT_TRUE(slot.get(42, barrier.request_id, output));
  expect_cut_equals(output, expected);
  output.binlog_file = "mutated-copy";
  ASSERT_TRUE(slot.get(42, barrier.request_id, output));
  expect_cut_equals(output, expected);

  Remote_clone_cut wrong_handle;
  wrong_handle.binlog_file = "unchanged";
  EXPECT_FALSE(slot.get(7, barrier.request_id, wrong_handle));
  EXPECT_FALSE(slot.get(42, 0, wrong_handle));
  EXPECT_FALSE(slot.get(42, barrier.request_id + 1, wrong_handle));
  EXPECT_EQ(wrong_handle.binlog_file, "unchanged");
}

class RemoteCloneCutStageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    m_saved_remote_commit = opt_binlog_archive_remote_commit;
    opt_binlog_archive_remote_commit = true;
    m_barrier = make_barrier_state();
    wesql::remote_commit::reset_commit_admission_for_test(true);
    wesql::remote_commit::set_clone_cut_source_for_test(&m_barrier);
  }

  void TearDown() override {
    wesql::remote_commit::set_clone_cut_source_for_test(nullptr);
    wesql::remote_commit::reset_commit_admission_for_test(false);
    opt_binlog_archive_remote_commit = m_saved_remote_commit;
  }

  wesql::remote_commit::CloneCutState m_barrier;
  bool m_saved_remote_commit{false};
};

TEST_F(RemoteCloneCutStageTest, TransfersClosedBarrierWithVerifiedCut) {
  Remote_clone_cut_slot slot;
  wesql::remote_commit::CloneCutBarrierLease lease;
  Remote_clone_cut expected;
  std::string error;

  {
    Remote_clone_cut_stage stage(42, m_barrier.request_id);
    ASSERT_FALSE(stage.begin(&error)) << error;
    EXPECT_TRUE(wesql::remote_commit::clone_cut_barrier_active_for_test());
    EXPECT_FALSE(wesql::remote_commit::commit_admission_open_for_test());
    ASSERT_FALSE(stage.capture(m_barrier.file, m_barrier.pos,
                               make_redo_locator(), &error));
    ASSERT_FALSE(stage.verify(&error));
    ASSERT_FALSE(stage.take(expected, lease, &error));
    ASSERT_TRUE(slot.install(42, m_barrier.request_id,
                             Remote_clone_cut(expected)));
  }

  EXPECT_TRUE(lease.active());
  EXPECT_TRUE(wesql::remote_commit::clone_cut_barrier_active_for_test());
  EXPECT_FALSE(wesql::remote_commit::commit_admission_open_for_test());
  Remote_clone_cut installed;
  ASSERT_TRUE(slot.get(42, m_barrier.request_id, installed));
  expect_cut_equals(installed, expected);

  wesql::remote_commit::end_clone_cut_barrier(&lease);
  EXPECT_FALSE(lease.active());
  EXPECT_FALSE(wesql::remote_commit::clone_cut_barrier_active_for_test());
  EXPECT_TRUE(wesql::remote_commit::commit_admission_open_for_test());
}

TEST_F(RemoteCloneCutStageTest, VerificationFailureReopensWithoutInstalling) {
  Remote_clone_cut_slot slot;
  std::string error;
  {
    Remote_clone_cut_stage stage(42, m_barrier.request_id);
    ASSERT_FALSE(stage.begin(&error)) << error;
    ASSERT_FALSE(stage.capture(m_barrier.file, m_barrier.pos,
                               make_redo_locator(), &error));

    auto changed = m_barrier;
    ++changed.pos;
    wesql::remote_commit::set_clone_cut_source_for_test(&changed);
    EXPECT_TRUE(stage.verify(&error));
  }

  EXPECT_FALSE(wesql::remote_commit::clone_cut_barrier_active_for_test());
  EXPECT_TRUE(wesql::remote_commit::commit_admission_open_for_test());
  Remote_clone_cut output;
  output.binlog_file = "unchanged";
  EXPECT_FALSE(slot.get(42, m_barrier.request_id, output));
  EXPECT_EQ(output.binlog_file, "unchanged");
}

TEST_F(RemoteCloneCutStageTest, ActualCursorMismatchCannotInstall) {
  std::string error;
  {
    Remote_clone_cut_stage stage(42, m_barrier.request_id);
    ASSERT_FALSE(stage.begin(&error)) << error;
    EXPECT_TRUE(stage.capture("binlog.000124", m_barrier.pos,
                              make_redo_locator(), &error));
    EXPECT_NE(error.find("does not match"), std::string::npos);
    EXPECT_TRUE(stage.active());
  }

  EXPECT_FALSE(wesql::remote_commit::clone_cut_barrier_active_for_test());
  EXPECT_TRUE(wesql::remote_commit::commit_admission_open_for_test());
}

TEST_F(RemoteCloneCutStageTest,
       VerificationRecapturesExecutedGtidBeforeInstallation) {
  std::string error;
  {
    Remote_clone_cut_stage stage(42, m_barrier.request_id);
    ASSERT_FALSE(stage.begin(&error)) << error;
    ASSERT_FALSE(stage.capture(m_barrier.file, m_barrier.pos,
                               make_redo_locator(), &error));

    auto changed = m_barrier;
    changed.canonical_gtid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-8";
    wesql::remote_commit::GtidSetDigest digest;
    ASSERT_TRUE(wesql::remote_commit::gtid_digest(
        changed.canonical_gtid, &digest, &error));
    changed.canonical_gtid = std::move(digest.canonical);
    changed.gtid_sha256 = std::move(digest.sha256);
    wesql::remote_commit::set_clone_cut_source_for_test(&changed);

    EXPECT_TRUE(stage.verify(&error));
    EXPECT_NE(error.find("GTID"), std::string::npos);
  }

  EXPECT_FALSE(wesql::remote_commit::clone_cut_barrier_active_for_test());
  EXPECT_TRUE(wesql::remote_commit::commit_admission_open_for_test());
}

TEST_F(RemoteCloneCutStageTest, RejectsZeroAndMismatchedRequestAuthority) {
  std::string error;
  {
    Remote_clone_cut_stage zero(42, 0);
    EXPECT_TRUE(zero.begin(&error));
    EXPECT_FALSE(zero.active());
    EXPECT_TRUE(wesql::remote_commit::commit_admission_open_for_test());
  }

  {
    Remote_clone_cut_stage mismatched(42, m_barrier.request_id + 1);
    EXPECT_TRUE(mismatched.begin(&error));
    EXPECT_FALSE(mismatched.active());
    EXPECT_NE(error.find("request ID"), std::string::npos);
    EXPECT_TRUE(wesql::remote_commit::commit_admission_open_for_test());
  }
}

}  // namespace remote_clone_cut_unittest
