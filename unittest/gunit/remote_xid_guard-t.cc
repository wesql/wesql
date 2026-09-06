/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA */

#include <gtest/gtest.h>

#include <array>
#include <cstring>

#include "sql/mysqld.h"
#include "sql/xa.h"
#include "sql/xa/recovery.h"
#include "unittest/gunit/handler-t.h"

namespace remote_xid_guard_unittest {
namespace {

int recover_calls;
int mutation_calls;
int recover_prepared_in_tc_calls;
bool change_heuristic_during_recover;

int fake_recover(handlerton *, XA_recover_txn *transactions, uint length,
                 MEM_ROOT *) {
  ++recover_calls;
  if (change_heuristic_during_recover)
    tc_heuristic_recover = TC_HEURISTIC_RECOVER_COMMIT;
  if (length < 3) return -1;

  constexpr std::size_t mysql_xid_prefix_length = 8;
  std::array<char, mysql_xid_prefix_length + sizeof(server_id) +
                       sizeof(my_xid)>
      internal_xid{};
  const ulong internal_server_id = 1;
  const my_xid internal_id = 17;
  std::memcpy(internal_xid.data(), MYSQL_XID_PREFIX,
              mysql_xid_prefix_length);
  std::memcpy(internal_xid.data() + mysql_xid_prefix_length,
              &internal_server_id, sizeof(internal_server_id));
  std::memcpy(internal_xid.data() + mysql_xid_prefix_length +
                  sizeof(internal_server_id),
              &internal_id, sizeof(internal_id));
  transactions[0].id.set(1, internal_xid.data(), internal_xid.size(), "", 0);
  transactions[0].mod_tables = nullptr;
  transactions[1].id.set(41, "external-a", 10, "", 0);
  transactions[1].mod_tables = nullptr;
  transactions[2].id.set(42, "external-b", 10, "", 0);
  transactions[2].mod_tables = nullptr;
  return 3;
}

int fake_recover_prepared_in_tc(handlerton *, Xa_state_list &) {
  ++recover_prepared_in_tc_calls;
  return 0;
}

xa_status_code fake_direct_xid_mutation(handlerton *, XID *) {
  ++mutation_calls;
  return XA_OK;
}

class RemoteXidGuardTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_remote_commit = opt_binlog_archive_remote_commit;
    saved_heuristic_recover = tc_heuristic_recover;
    saved_total_ha_2pc = total_ha_2pc;
    opt_binlog_archive_remote_commit = false;
    tc_heuristic_recover = TC_HEURISTIC_NOT_USED;
    recover_calls = 0;
    mutation_calls = 0;
    recover_prepared_in_tc_calls = 0;
    change_heuristic_during_recover = false;
  }

  void TearDown() override {
    opt_binlog_archive_remote_commit = saved_remote_commit;
    tc_heuristic_recover = saved_heuristic_recover;
    total_ha_2pc = saved_total_ha_2pc;
  }

  void configure_handlerton(Fake_handlerton *ht) {
    ht->state = SHOW_OPTION_YES;
    ht->recover = fake_recover;
    ht->recover_prepared_in_tc = fake_recover_prepared_in_tc;
    ht->commit_by_xid = fake_direct_xid_mutation;
    ht->rollback_by_xid = fake_direct_xid_mutation;
    ht->set_prepared_in_tc_by_xid = fake_direct_xid_mutation;
  }

 private:
  bool saved_remote_commit;
  long saved_heuristic_recover;
  ulong saved_total_ha_2pc;
};

TEST_F(RemoteXidGuardTest, DisabledModePreservesLegacyPath) {
  tc_heuristic_recover = TC_HEURISTIC_RECOVER_ROLLBACK;

  EXPECT_FALSE(xa::recovery::reject_direct_xid_operation(
      xa::recovery::Direct_xid_operation::COMMIT_BY_XID));
  EXPECT_EQ(TC_HEURISTIC_RECOVER_ROLLBACK, tc_heuristic_recover);
}

TEST_F(RemoteXidGuardTest, EnabledModeRejectsDirectXidMutation) {
  opt_binlog_archive_remote_commit = true;

  EXPECT_TRUE(xa::recovery::reject_direct_xid_operation(
      xa::recovery::Direct_xid_operation::COMMIT_BY_XID));
  EXPECT_TRUE(xa::recovery::reject_direct_xid_operation(
      xa::recovery::Direct_xid_operation::ROLLBACK_BY_XID));
  EXPECT_TRUE(xa::recovery::reject_direct_xid_operation(
      xa::recovery::Direct_xid_operation::SET_PREPARED_IN_TC_BY_XID));
  EXPECT_EQ(0, mutation_calls);
}

TEST_F(RemoteXidGuardTest, StockRecoveryRejectedBeforeHeuristicMutation) {
  opt_binlog_archive_remote_commit = true;
  tc_heuristic_recover = TC_HEURISTIC_NOT_USED;
  total_ha_2pc = static_cast<ulong>(opt_bin_log) + 1;

  EXPECT_EQ(1, ha_recover());
  EXPECT_EQ(TC_HEURISTIC_NOT_USED, tc_heuristic_recover);
}

TEST_F(RemoteXidGuardTest, InventoryOnlyCallsRecoverAndCountsEntries) {
  Fake_handlerton ht;
  configure_handlerton(&ht);
  xa::recovery::Prepared_xid_inventory inventory;
  opt_binlog_archive_remote_commit = true;

  EXPECT_FALSE(xa::recovery::enumerate_prepared_transactions_in_engine(
      &ht, &inventory));
  EXPECT_EQ(1U, inventory.internal_entries);
  EXPECT_EQ(2U, inventory.external_entries);
  EXPECT_EQ(1, recover_calls);
  EXPECT_EQ(0, recover_prepared_in_tc_calls);
  EXPECT_EQ(0, mutation_calls);
}

TEST_F(RemoteXidGuardTest, InventoryRejectsHeuristicBeforeRecover) {
  Fake_handlerton ht;
  configure_handlerton(&ht);
  xa::recovery::Prepared_xid_inventory inventory;
  tc_heuristic_recover = TC_HEURISTIC_RECOVER_ROLLBACK;

  EXPECT_TRUE(xa::recovery::enumerate_prepared_transactions_in_engine(
      &ht, &inventory));
  EXPECT_EQ(0, recover_calls);
  EXPECT_TRUE(inventory.empty());
  EXPECT_EQ(0, mutation_calls);
}

TEST_F(RemoteXidGuardTest, InventoryDetectsHeuristicChangeAfterRecover) {
  Fake_handlerton ht;
  configure_handlerton(&ht);
  xa::recovery::Prepared_xid_inventory inventory;
  change_heuristic_during_recover = true;

  EXPECT_TRUE(xa::recovery::enumerate_prepared_transactions_in_engine(
      &ht, &inventory));
  EXPECT_EQ(1, recover_calls);
  EXPECT_TRUE(inventory.empty());
  EXPECT_EQ(0, mutation_calls);
}

}  // namespace
}  // namespace remote_xid_guard_unittest
