//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/snapshot_impl.h"
#include "db/db.h"
#include "logger/log_module.h"
#include "storage/multi_version_extent_meta_layer.h"

namespace smartengine {
using namespace common;
using namespace storage;
namespace db {

SnapshotImpl::SnapshotImpl() : index_id_(-1), number_(0), ref_(0), backup_ref_(0)
{
  for (int64_t level = 0; level < storage::MAX_TIER_COUNT; ++level) {
    extent_layer_versions_[level] = nullptr;
  }
}

SnapshotImpl::~SnapshotImpl()
{
}

int SnapshotImpl::init(int64_t index_id,
                       int64_t meta_snapshot_id,
                       storage::ExtentLayerVersion **extent_layer_versions,
                       common::SequenceNumber seq_num)
{
  int ret = common::Status::kOk;

  if (IS_NULL(extent_layer_versions)) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(extent_layer_versions));
  } else {
    index_id_ = index_id;
    meta_snapshot_id_ = meta_snapshot_id;
    number_ = seq_num;
    for (int64_t level = 0; level < storage::MAX_TIER_COUNT; ++level) {
      extent_layer_versions_[level] = extent_layer_versions[level];
      extent_layer_versions_[level]->ref();
    }
  }

  return ret;
}

void SnapshotImpl::destroy(util::autovector<storage::ExtentLayerVersion *> &recyle_extent_layer_versions)
{
  for (int64_t level = 0; level < storage::MAX_TIER_COUNT; ++level) {
    if (extent_layer_versions_[level]->unref()) {
      recyle_extent_layer_versions.push_back(extent_layer_versions_[level]);
    }
  }
}
bool SnapshotImpl::ref(int32_t *old_ref) {
  std::unique_lock lock(ref_mutex_);
  if (old_ref) {
    *old_ref = ref_;
  }
  return 0 == ref_++ ? true : false;
}

bool SnapshotImpl::unref(int32_t *old_ref) {
  std::unique_lock lock(ref_mutex_);
  if (old_ref) {
    *old_ref = ref_;
  }
  return 0 == --ref_ && 0 == backup_ref_;
}

bool SnapshotImpl::backup_ref() {
  std::unique_lock lock(ref_mutex_);
  return 0 == backup_ref_++ ? true : false;
}

bool SnapshotImpl::backup_unref() {
  std::unique_lock lock(ref_mutex_);
  return 0 == --backup_ref_ && 0 == ref_;
}

int64_t SnapshotImpl::get_total_extent_count() const
{
  int64_t total_extent_count = 0;
  for (int64_t level = 0; level < storage::MAX_TIER_COUNT; ++level) {
    total_extent_count += extent_layer_versions_[level]->get_total_extent_size();
  }
  return total_extent_count;
}

int SnapshotImpl::extent_layer_versions_serialize(char *buf, int64_t buf_len, int64_t &pos) const {
  int ret = Status::kOk;
  int64_t size = extent_layer_versions_get_serialize_size();
  int64_t max_level = storage::MAX_TIER_COUNT;
  storage::ExtentLayerVersion *extent_layer_version = nullptr;

  if (IS_NULL(buf) || buf_len < 0 || pos + size > buf_len) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos), K(size));
  } else {
    *(reinterpret_cast<int64_t *>(buf + pos)) = size;
    pos += sizeof(size);
    if (FAILED(util::serialize(buf, buf_len, pos, max_level))) {
      SE_LOG(WARN, "fail to serialize MAX_LEVEL", K(ret));
    } else {
      for (int64_t level = 0; SUCCED(ret) && level < max_level; ++level) {
        if (IS_NULL(extent_layer_version = extent_layer_versions_[level])) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN, "unexpected error, extent layer version must not nullptr", K(ret), K(level));
        } else if (FAILED(extent_layer_version->serialize(buf, buf_len, pos))) {
          SE_LOG(WARN, "fail to serialize ExtentLayerVersion", K(ret), K(level));
        }
      }
    }
  }
  return ret;
}

int SnapshotImpl::extent_layer_versions_deserialize(char *buf, int64_t buf_len, int64_t &pos) {
  int ret = Status::kOk;
  int64_t size = 0;
  int64_t max_level = 0;
  int64_t current_level = 0;
  int64_t level_extent_layer_count = 0;
  ExtentLayerVersion *current_extent_layer_version = nullptr;

  if (IS_NULL(buf) || buf_len < 0 || pos >= buf_len) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    size = *((int64_t *)(buf + pos));
    pos += sizeof(size);

    if (FAILED(util::deserialize(buf, buf_len, pos, max_level))) {
      SE_LOG(WARN, "fail to deserialize max level", K(ret));
    } else {
      for (int64_t level = 0; SUCCED(ret) && level < max_level; ++level) {
        if (IS_NULL(current_extent_layer_version = extent_layer_versions_[level])) {
          ret = Status::kErrorUnexpected;
          SE_LOG(WARN, "unexpected error, extent layer version must not nullptr", K(ret), K(current_level));
        } else if (FAILED(current_extent_layer_version->deserialize(buf, buf_len, pos))) {
          SE_LOG(WARN, "fail to deserialize ExtentLayerVersion", K(ret), K(level));
        }
      }
    }
  }

  return ret;
}

int64_t SnapshotImpl::extent_layer_versions_get_serialize_size() const {
  int64_t size = 0;
  int64_t max_level = storage::MAX_TIER_COUNT;
  ExtentLayerVersion *extent_layer_version = nullptr;

  size += sizeof(int64_t); // size
  size += util::get_serialize_size(max_level);
  for (int64_t level = 0; level < max_level; ++level) {
    size += extent_layer_versions_[level]->get_serialize_size();
  }
  return size;
}

int SnapshotImpl::deserialize(char *buf, int64_t buf_len, int64_t &pos, const util::Comparator *cmp)
{
  int ret = Status::kOk;
  int64_t size = 0;
  int64_t version = 0;
  ExtentLayerVersion *extent_layer_version = nullptr;

  if (IS_NULL(buf) || buf_len < 0 || pos >= buf_len) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (IS_NULL(cmp)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "cmp is null", K(ret));
  } else {
    for (int32_t level = 0; SUCCED(ret) && level < MAX_TIER_COUNT; ++level) {
      if (IS_NULL(extent_layer_version = MOD_NEW_OBJECT(memory::ModId::kStorageMgr, ExtentLayerVersion, level, cmp))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for ExtentLayerVersion", K(ret), K(level));
      } else {
        extent_layer_versions_[level] = extent_layer_version;
        extent_layer_versions_[level]->ref();
      }
    }

    if (SUCCED(ret)) {
      size = *((int64_t *)(buf + pos));
      pos += sizeof(size);
      version = *((int64_t *)(buf + pos));
      pos += sizeof(version);
      index_id_ = *((int64_t *)(buf + pos));
      pos += sizeof(index_id_);

      if (FAILED(extent_layer_versions_deserialize(buf, buf_len, pos))) {
        SE_LOG(WARN, "failed to deserialize extent layer versions", K(ret));
      }
    }
  }
  if (SUCCED(ret)) {
    assert(pos < buf_len);
  }
  return ret;
}

int SnapshotImpl::serialize(char *buf, int64_t buf_len, int64_t &pos) const {
  int ret = Status::kOk;
  int64_t size = get_serialize_size();
  int64_t version = META_SNAPSHOT_VERSION;
  storage::ExtentLayerVersion *extent_layer_version = nullptr;
  int64_t old_pos = pos;

  if (IS_NULL(buf) || buf_len < 0 || pos + size > buf_len) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos), K(size));
  } else {
    *reinterpret_cast<int64_t *>(buf + pos) = size;
    pos += sizeof(int64_t);
    *reinterpret_cast<int64_t *>(buf + pos) = version;
    pos += sizeof(int64_t);
    *reinterpret_cast<int64_t *>(buf + pos) = index_id_;
    pos += sizeof(int64_t);

    if (FAILED(extent_layer_versions_serialize(buf, buf_len, pos))) {
      SE_LOG(WARN, "failed to serialize extent layer versions", K(ret));
    }
  }
  if (SUCCED(ret) && (pos - old_pos != size)) {
    ret = Status::kCorruption;
    SE_LOG(WARN, "serialize size not match", K(ret), K(pos), K(old_pos), K(size));
  }
  return ret;
}

int64_t SnapshotImpl::get_serialize_size() const {
  int64_t size = 0;

  size += 2 * sizeof(int64_t); // size and version
  size += sizeof(index_id_);
  size += extent_layer_versions_get_serialize_size();
  return size;
}

DEFINE_TO_STRING(SnapshotImpl, K_(number));

ManagedSnapshot::ManagedSnapshot(DB *db) : db_(db), snapshot_(db->GetSnapshot()) {}

ManagedSnapshot::ManagedSnapshot(DB *db, const Snapshot *_snapshot) : db_(db), snapshot_(_snapshot) {}

ManagedSnapshot::~ManagedSnapshot() {
  if (snapshot_) {
    db_->ReleaseSnapshot(snapshot_);
  }
}

const Snapshot *ManagedSnapshot::snapshot() { return snapshot_; }
} // namespace db
}  // namespace smartengine
