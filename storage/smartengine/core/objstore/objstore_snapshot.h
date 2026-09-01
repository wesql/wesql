/*
 * Copyright (c) 2023, ApeCloud Inc Holding Limited
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

#ifndef SMARTENGINE_INCLUDE_OBJSTORE_OBJSTORE_SNAPSHOT_H_
#define SMARTENGINE_INCLUDE_OBJSTORE_OBJSTORE_SNAPSHOT_H_

#include "objstore.h"

namespace smartengine {
namespace db {
class Snapshot;
class SnapshotImpl;
}
namespace util {
struct BackupSnapshotInfo;
class Comparator;
}

namespace objstore {

class ObjStoreSnapshotOperator {
public:
    ObjStoreSnapshotOperator();
    ~ObjStoreSnapshotOperator();

    int init(::objstore::ObjectStore *object_store, std::string_view repo_id, std::string_view bucket);

    int get_snapshot(const int64_t snapshot_id, util::BackupSnapshotInfo &backup_snapshot_info);

    // backup snapshot
    int write_snapshot(const int64_t snapshot_id, const util::BackupSnapshotInfo &backup_snapshot_info);

    int delete_snapshot();

    int list_snapshots(std::vector<uint64_t> &snapshot_ids);

    int get_meta_snapshot(const int64_t meta_snapshot_id, db::SnapshotImpl &meta_snapshot, const util::Comparator *cmp);

    // meta snapshot
    // TODO(ljc): add err msg argument
    int write_meta_snapshot(const int64_t meta_snapshot_id, db::SnapshotImpl* meta_snapshot);

    int read_meta_snapshot(std::string_view meta_snapshot_data);

    int delete_meta_snapshot();

    int list_meta_snapshots(std::vector<uint64_t> &meta_snapshot_ids);

private:
    ::objstore::ObjectStore *object_store_;
    std::string repo_id_;
    std::string bucket_;

    bool is_inited_ = false;
};

} // namespace objstore
} // namespace smartengine

#endif