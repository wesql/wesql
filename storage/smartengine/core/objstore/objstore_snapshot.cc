/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
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

#include "objstore/objstore_snapshot.h"
#include "objstore/objstore_layout.h"
#include "backup/hotbackup_impl.h"
#include "db/snapshot_impl.h"
#include "memory/mod_info.h"
#include "logger/log_module.h"

namespace smartengine {
namespace objstore {

ObjStoreSnapshotOperator::ObjStoreSnapshotOperator()
{
}

ObjStoreSnapshotOperator::~ObjStoreSnapshotOperator()
{
}

int ObjStoreSnapshotOperator::init(::objstore::ObjectStore *object_store, std::string_view repo_id, std::string_view bucket)
{
    object_store_ = object_store;
    repo_id_ = repo_id;
    bucket_ = bucket;
    is_inited_ = true;
    return 0;
}

int ObjStoreSnapshotOperator::get_snapshot(const int64_t snapshot_id, util::BackupSnapshotInfo &backup_snapshot_info)
{
    int ret = Status::kOk;
    std::string err_msg;
    if (!is_inited_) {
      ret = Status::kNotInit;
    } else {
      std::string key = util::make_snapshot_key(repo_id_, snapshot_id);
      std::string data;
      ::objstore::Status status = object_store_->get_object(bucket_, key, data);
      if (!status.is_succ()) {
        if (status.error_code() == ::objstore::Errors::SE_NO_SUCH_KEY) {
          ret = Status::kNotFound;
        } else {
          ret = Status::kObjStoreError;
          err_msg = status.error_message();
          SE_LOG(WARN, "failed to get snapshot", K(ret), K(snapshot_id), K(err_msg));
        }
      } else {
        int64_t pos = 0;
        if (FAILED(backup_snapshot_info.deserialize(data.data(), data.size(), pos))) {
          SE_LOG(WARN, "failed to deserialize backup snapshot info", K(ret), K(snapshot_id));
        }
      }
    }
    return ret;
}

int ObjStoreSnapshotOperator::write_snapshot(const int64_t snapshot_id, const util::BackupSnapshotInfo &backup_snapshot_info)
{
    int ret = Status::kOk;
    if (!is_inited_) {
      ret = Status::kNotInit;
    } else {
      int64_t size = backup_snapshot_info.get_serialize_size();
      char* buf = static_cast<char*>(memory::base_malloc(size, memory::ModId::kObjectStore));
      if (buf == nullptr) {
          ret = Status::kMemoryLimit;
          SE_LOG(WARN, "failed to allocate memory for snapshot", K(ret), K(backup_snapshot_info));
      } else {
        int64_t pos = 0;
        if (FAILED(backup_snapshot_info.serialize(buf, size, pos))) {
          SE_LOG(WARN, "failed to serialize backup snapshot info", K(ret), K(backup_snapshot_info));
        } else {
          std::string key = util::make_snapshot_key(repo_id_, snapshot_id);
          ::objstore::Status status = object_store_->put_object(bucket_, key, buf);
          if (!status.is_succ()) {
            ret = status.error_code();
            std::string err_msg = std::string("failed to write snapshot:") + status.error_message().data();
            SE_LOG(WARN, err_msg.data(), K(ret), K(backup_snapshot_info));
          }
        }
        memory::base_free(buf);
      }
    }
    return ret;
}

int ObjStoreSnapshotOperator::read_snapshot(std::string_view snapshot_data)
{
    return 0;
}

int ObjStoreSnapshotOperator::delete_snapshot()
{
    return 0;
}

int ObjStoreSnapshotOperator::list_snapshots(std::vector<uint64_t> &snapshot_ids)
{
    return 0;
}

int ObjStoreSnapshotOperator::get_meta_snapshot(const int64_t meta_snapshot_id, db::SnapshotImpl &meta_snapshot, const util::Comparator *cmp)
{
  int ret = Status::kOk;
  if (!is_inited_) {
    ret = Status::kNotInit;
  } else {
    std::string key = util::make_metasnapshot_key(repo_id_, meta_snapshot_id);
    std::string data;
    ::objstore::Status status = object_store_->get_object(bucket_, key, data);
    if (!status.is_succ()) {
      ret = status.error_code();
      std::string err_msg = std::string("failed to get meta snapshot:") + status.error_message().data();
      SE_LOG(WARN, err_msg.data(), K(ret), K(meta_snapshot_id));
    } else {
      int64_t pos = 0;
      // todo
      // meta_snapshot->deserialize(buf, buf_size, pos, global_ctx_->options_.comparator)))
      if (FAILED(meta_snapshot.deserialize(data.data(), data.size(), pos, cmp))) {
        SE_LOG(WARN, "failed to deserialize meta snapshot", K(ret), K(meta_snapshot_id));
      }
    }
  }
  return ret;
}

int ObjStoreSnapshotOperator::write_meta_snapshot(const int64_t meta_snapshot_id, db::SnapshotImpl* meta_snapshot)
{
    int ret = Status::kOk;
    if (!is_inited_) {
      ret = Status::kNotInit;
    } else if (meta_snapshot == nullptr) {
      ret = Status::kInvalidArgument;
    } else {
      int64_t size = meta_snapshot->get_serialize_size();
      char* buf = static_cast<char*>(memory::base_malloc(size, memory::ModId::kObjectStore));
      if (buf == nullptr) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "failed to allocate memory for meta snapshot", K(ret), K(meta_snapshot_id));
      } else {
        int64_t pos = 0;
        if (FAILED(meta_snapshot->serialize(buf, size, pos))) {
          SE_LOG(WARN, "failed to serialize meta snapshot", K(ret), K(meta_snapshot_id));
        } else {
          std::string key = util::make_metasnapshot_key(repo_id_, meta_snapshot_id);
          ::objstore::Status status = object_store_->put_object(bucket_, key, buf);
          if (!status.is_succ()) {
            ret = status.error_code();
            std::string err_msg = std::string("failed to write meta snapshot:") + status.error_message().data();
            SE_LOG(WARN, err_msg.data(), K(ret), K(meta_snapshot_id));
          }
        }
        memory::base_free(buf);
      }
    }
    return ret;
}

int ObjStoreSnapshotOperator::read_meta_snapshot(std::string_view meta_snapshot_data)
{
    return 0;
}

int ObjStoreSnapshotOperator::delete_meta_snapshot()
{
    return 0;
}

int ObjStoreSnapshotOperator::list_meta_snapshots(std::vector<uint64_t> &meta_snapshot_ids)
{
    return 0;
}

} // namespace objstore
} // namespace smartengine
