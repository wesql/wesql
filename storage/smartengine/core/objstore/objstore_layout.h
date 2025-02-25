//  Copyright (c) 2023, ApeCloud Inc Holding Limited

#ifndef SMARTENGINE_INCLUDE_OBJSTORE_LAYOUT_H_
#define SMARTENGINE_INCLUDE_OBJSTORE_LAYOUT_H_

#include <string>

namespace smartengine {
namespace util {

// ================ Sharing at the repo level =================
std::string make_repo_prefix(const std::string &repo_id);

// Make the snapshot key
std::string make_snapshot_key(const std::string &repo_id, const int64_t snapshot_id);

// Make the metasnapshot key
std::string make_metasnapshot_key(const std::string &repo_id, const int64_t meta_snapshot_id);

// Make extent prefix name
std::string make_data_prefix(const std::string &repo_id);

// Make the latest snapshot id key
std::string make_latest_snapshot_key(const std::string &repo_id);

// Make the latest metasnapshot id key
std::string make_latest_metasnapshot_key(const std::string &repo_id);

// Make the latest extent id key
std::string make_latest_extent_key(const std::string &repo_id);

// ================ Sharing at the branch level =================
std::string make_branch_prefix(const std::string &cluster_id);

// Make index prefix name
std::string make_index_key(const std::string &cluster_id, const int64_t index_id);

// Make object store lock prefix name
std::string make_lock_prefix(const std::string &cluster_id);

std::string get_lease_lock_key(const std::string_view cluster_id);

std::string get_lease_lock_version_file_prefix(const std::string_view cluster_id);

} // namespace util
} // namespace smartengine

#endif