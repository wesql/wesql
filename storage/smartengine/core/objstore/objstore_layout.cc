//  Copyright (c) 2023, ApeCloud Inc Holding Limited

#include "objstore_layout.h"

namespace smartengine {

namespace util {

std::string make_repo_prefix(const std::string &repo_id) 
{
  std::string prefix = repo_id +
                       "/" + "smartengine" +
                       "/" + "v2" +
                       "/";
  return prefix;
}

std::string make_branch_prefix(const std::string &cluster_id)
{
  // clang-format off
  std::string prefix = cluster_id + 
                       "/" + "smartengine" +
                       "/" + "v2" +
                       "/";
  // clang-format on
  return prefix;
}

std::string make_snapshot_key(const std::string &repo_id, const int64_t snapshot_id)
{
  std::string key = make_repo_prefix(repo_id) + "snapshots" + "/" + std::to_string(snapshot_id);
  return key;
}

std::string make_metasnapshot_key(const std::string &repo_id, const int64_t meta_snapshot_id)
{
  std::string key = make_repo_prefix(repo_id) + "metasnapshots" + "/" + std::to_string(meta_snapshot_id);
  return key;
}

std::string make_data_prefix(const std::string &repo_id)
{
  std::string prefix = make_repo_prefix(repo_id) + 
                      "/" + "table_space" + 
                      "/" + "data" + 
                      "/";
  return prefix;
}

std::string make_latest_snapshot_key(const std::string &repo_id)
{
  std::string key = make_repo_prefix(repo_id) + "/" + "latest_snapshot";
  return key;
}

std::string make_latest_metasnapshot_key(const std::string &repo_id)
{
  std::string key = make_repo_prefix(repo_id) + "/" + "latest_metasnapshot";
  return key;
}

std::string make_latest_extent_key(const std::string &repo_id)
{
  std::string key = make_repo_prefix(repo_id) + "/" + "latest_extent";
  return key;
}

std::string make_index_key(const std::string &cluster_id, const int64_t index_id)
{
  // TODO(ljc): use repo_id instead of cluster_id
  size_t pos = cluster_id.find("/");
  std::string repo_id = cluster_id.substr(0, pos);
  std::string branch = cluster_id.substr(pos + 1);
  // clang-format off
  std::string prefix = make_branch_prefix(repo_id) + 
                      "/" + "branches" + 
                      "/" + branch + 
                      "/" + std::to_string(index_id) + 
                      "/";
  // clang-format on
  return prefix;
}

// Make object store lock prefix name
std::string make_lock_prefix(const std::string &cluster_id)
{
  std::string prefix = make_branch_prefix(cluster_id) + "locks" + "/";
  return prefix;
}

std::string get_lease_lock_key(const std::string_view cluster_id)
{
  return util::make_lock_prefix(cluster_id.data()) + "lease_lock";
}

std::string get_lease_lock_version_file_prefix(const std::string_view cluster_id)
{
  return util::make_lock_prefix(cluster_id.data()) + "lease_lock_version_";
}

} // namespace util
} // namespace smartengine