/*
 * Copyright (c) 2024, ApeCloud Inc Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "db/db_impl.h"
#include "db/replay_task.h"
#include "db/replay_thread_pool.h"
#include "db/db_test_util.h"
#include "env/mock_env.h"
#include "port/port.h"
#include "util/stack_trace.h"
#include "util/sync_point.h"
#include "util/testutil.h"
#include "table/table.h"
#include "transactions/transaction.h"
#include "transactions/transaction_db.h"

using namespace smartengine;
using namespace common;
using namespace util;

namespace smartengine {
namespace db {

static const std::string test_dir =
    ::smartengine::util::test::TmpDir() + "/db_recover_test";
static const std::string test_local_obs_basepath = test_dir + "/local_obs";
static const std::string test_local_obs_bucket = "db_bucket";

class RecoveryTest : public testing::TestWithParam<bool>  {
public:
  RecoveryTest() = default;
  ~RecoveryTest() = default;

  virtual void SetUp() override {
    db_name_ = test::TmpDir() + "/recover_test";
    options_.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
    options_.parallel_wal_recovery = false;
    options_.allow_2pc = true;

    use_obj_ = GetParam();
    if (use_obj_) {
      objstore::ObjectStore *obs = nullptr;
      auto s = options_.env->InitObjectStore("local",
                                             test_local_obs_basepath /* use test_dir as the basepath */,
                                             nullptr,
                                             false,
                                             test_local_obs_bucket,
                                             "repo/branch",
                                             0);
      ASSERT_OK(s);
      s = options_.env->GetObjectStore(obs);
      ASSERT_OK(s);
      auto ss = obs->delete_bucket(test_local_obs_bucket);
      ASSERT_TRUE(ss.is_succ());
      ss = obs->create_bucket(test_local_obs_bucket);
      ASSERT_TRUE(ss.is_succ());
    }

    DestroyDB(db_name_, options_);
  }

  virtual void TearDown() override { ASSERT_OK(DestroyDB(db_name_, options_)); }

 protected:
  std::string db_name_;
  Options options_;
  bool use_obj_{false};
};

INSTANTIATE_TEST_CASE_P(FileOrObjstore, RecoveryTest,
                        testing::Values(false, true));

TEST_P(RecoveryTest, simple_put_then_restart) {
  DBOptions db_options;
  ColumnFamilyOptions cf_options;
  table::BlockBasedTableOptions table_options;
  table_options.cluster_id = "repo/branch";
  cf_options.table_factory.reset(table::NewExtentBasedTableFactory(table_options));

  Options options1(db_options, cf_options);
  options1.write_buffer_size = 4096 * 4096;
  options1.db_total_write_buffer_size =  4 * 4096 * 4096;
  options1.db_write_buffer_size = 2 * 4096 * 4096;
  options1.dump_memtable_limit_size = 200;
  options1.allow_2pc = true;
  options1.parallel_wal_recovery = true;
  TransactionDB* db;
  ASSERT_OK(test_open_trans_db(options1, TransactionDBOptions(), db_name_, &db));

  common::WriteOptions write_options;
  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_NE(nullptr, txn);
  ASSERT_OK(txn->SetName("simple_recovery_test"));
  ASSERT_OK(txn->Put("foo00", "v0"));
  ASSERT_OK(txn->Put("foo01", "v1"));
  ASSERT_OK(txn->Prepare());
  ASSERT_OK(txn->Commit());
  ASSERT_OK(db->Flush(FlushOptions{}));
  delete txn;
  delete db;

  // recovery the db
  db = nullptr;
  ASSERT_OK(test_open_trans_db(options1, TransactionDBOptions(), db_name_, &db));
  ReadOptions read_options1;
  std::string value;
  ASSERT_OK(db->Get(read_options1, db->DefaultColumnFamily(), "foo00", &value));
  ASSERT_EQ(value, "v0");
  ASSERT_OK(db->Get(read_options1, db->DefaultColumnFamily(), "foo01", &value));
  ASSERT_EQ(value, "v1");
  ASSERT_EQ(Status::kNotFound, db->Get(read_options1, db->DefaultColumnFamily(), "foo02", &value).code());
  delete db;
}

} // namespace db
} // namespace smartengine

int main(int argc, char **argv) {
  smartengine::util::StackTrace::install_handler();
  std::string log_path =
      smartengine::util::test::TmpDir() + "/db_recover_test.log";
  smartengine::logger::Logger::get_log().init(log_path.c_str(),
                                              smartengine::logger::DEBUG_LEVEL);
  ::testing::InitGoogleTest(&argc, argv);
  // smartengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
