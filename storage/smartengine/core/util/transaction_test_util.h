// Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include "options/options.h"
#include "transactions/optimistic_transaction_db.h"
#include "transactions/transaction_db.h"

namespace smartengine {

namespace db {
class DB;
}  // namespace db

namespace util {

class Random64;

// Utility class for stress testing transactions.  Can be used to write many
// transactions in parallel and then validate that the data written is logically
// consistent.  This class assumes the input common::DB is initially empty.
//
// Each call to TransactionDBInsert()/OptimisticTransactionDBInsert() will
// increment the value of a key in #num_sets sets of keys.  Regardless of
// whether the transaction succeeds, the total sum of values of keys in each
// set is an invariant that should remain equal.
//
// After calling TransactionDBInsert()/OptimisticTransactionDBInsert() many
// times, Verify() can be called to validate that the invariant holds.
//
// To test writing common::Transaction in parallel, multiple threads can create
// a
// RandomTransactionInserter with similar arguments using the same common::DB.
class RandomTransactionInserter {
 public:
  // num_keys is the number of keys in each set.
  // num_sets is the number of sets of keys.
  explicit RandomTransactionInserter(
      Random64* rand,
      const common::WriteOptions& write_options = common::WriteOptions(),
      const common::ReadOptions& read_options = common::ReadOptions(),
      uint64_t num_keys = 1000, uint16_t num_sets = 3);

  ~RandomTransactionInserter();

  // Increment a key in each set using a common::Transaction on a TransactionDB.
  //
  // Returns true if the transaction succeeded OR if any error encountered was
  // expected (eg a write-conflict). Error status may be obtained by calling
  // GetLastStatus();
  bool TransactionDBInsert(
      TransactionDB* db,
      const TransactionOptions& txn_options = TransactionOptions());

  // Increment a key in each set using a common::Transaction on an
  // common::OptimisticTransactionDB
  //
  // Returns true if the transaction succeeded OR if any error encountered was
  // expected (eg a write-conflict). Error status may be obtained by calling
  // GetLastStatus();
  bool OptimisticTransactionDBInsert(
      OptimisticTransactionDB* db,
      const OptimisticTransactionOptions& txn_options =
          OptimisticTransactionOptions());
  // Increment a key in each set without using a transaction.  If this function
  // is called in parallel, then Verify() may fail.
  //
  // Returns true if the write succeeds.
  // Error status may be obtained by calling GetLastStatus().
  bool DBInsert(db::DB* db);

  // Returns OK if Invariant is true.
  static common::Status Verify(db::DB* db, uint16_t num_sets);

  // Returns the status of the previous Insert operation
  common::Status GetLastStatus() { return last_status_; }

  // Returns the number of successfully written calls to
  // TransactionDBInsert/OptimisticTransactionDBInsert/DBInsert
  uint64_t GetSuccessCount() { return success_count_; }

  // Returns the number of calls to
  // TransactionDBInsert/OptimisticTransactionDBInsert/DBInsert that did not
  // write any data.
  uint64_t GetFailureCount() { return failure_count_; }

 private:
  // Input options
  Random64* rand_;
  const common::WriteOptions write_options_;
  const common::ReadOptions read_options_;
  const uint64_t num_keys_;
  const uint16_t num_sets_;

  // Number of successful insert batches performed
  uint64_t success_count_ = 0;

  // Number of failed insert batches attempted
  uint64_t failure_count_ = 0;

  // common::Status returned by most recent insert operation
  common::Status last_status_;

  // optimization: re-use allocated transaction objects.
  Transaction* txn_ = nullptr;
  Transaction* optimistic_txn_ = nullptr;

  bool DoInsert(db::DB* db, Transaction* txn, bool is_optimistic);
};

}  // namespace util
}  // namespace smartengine