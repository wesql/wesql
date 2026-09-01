/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include "util/types.h"

namespace smartengine
{
namespace storage
{
class ExtentLayerVersion;
struct ExtentLayer;
struct LayerPosition;
}

namespace db {

class DB;

// Abstract handle to particular state of a DB.
// A Snapshot is an immutable object and can therefore be safely
// accessed from multiple threads without any external synchronization.
//
// To Create a Snapshot, call DB::GetSnapshot().
// To Destroy a Snapshot, call DB::ReleaseSnapshot(snapshot).
class Snapshot {
 public:
  // returns Snapshot's sequence number
  virtual common::SequenceNumber GetSequenceNumber() const = 0;
  virtual storage::ExtentLayerVersion *get_extent_layer_version(const int64_t level) const = 0;
  virtual storage::ExtentLayer *get_extent_layer(const storage::LayerPosition &layer_position) const = 0;
  virtual int64_t get_total_extent_count() const = 0;

protected:
  virtual ~Snapshot() {}
};

// Simple RAII wrapper class for Snapshot.
// Constructing this object will create a snapshot.  Destructing will
// release the snapshot.
class ManagedSnapshot {
 public:
  explicit ManagedSnapshot(DB* db);

  // Instead of creating a snapshot, take ownership of the input snapshot.
  ManagedSnapshot(DB* db, const Snapshot* _snapshot);

  ~ManagedSnapshot();

  const Snapshot* snapshot();

 private:
  DB* db_;
  const Snapshot* snapshot_;
};

}  // namespace db
}  // namespace smartengine
