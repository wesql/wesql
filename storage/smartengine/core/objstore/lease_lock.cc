/*
   Copyright (c) 2024, ApeCloud Inc Holding Limited.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#include "lease_lock.h"
#include <atomic>
#include <chrono>
#include <cstring>
#include "objstore_layout.h"
#include "util/sync_point.h"

namespace smartengine {
namespace objstore {

using ObjectMeta = ::objstore::ObjectMeta;
using Status = ::objstore::Status;
using Errors = ::objstore::Errors;

static std::atomic<bool> is_lease_lock_owner = false;
static std::atomic<bool> should_abort_if_not_owner = false;
static std::chrono::milliseconds lease_lock_last_lease_time = std::chrono::milliseconds(0);

#ifdef NDEBUG
namespace {
#endif

int get_single_data_node_lease_lock_expire_time(ObjectStore *objstore,
                                                const std::string_view bucket_dir,
                                                const std::string_view cluster_objstore_id,
                                                std::string &err_msg,
                                                std::chrono::milliseconds &expire_time)
{
  int ret = 0;
  if (objstore == nullptr) {
    ret = Errors::SE_INVALID;
  } else {
    std::string lease_lock_key = util::get_lease_lock_key(cluster_objstore_id);
    std::string value;
    Status status = objstore->get_object(bucket_dir, lease_lock_key, value);
    if (status.error_code() == Errors::SE_NO_SUCH_KEY) {
      // lease lock file not found, it means the lease lock is expired
      expire_time = std::chrono::milliseconds(0);
      ret = 0;
    } else if (!status.is_succ()) {
      ret = status.error_code();
      err_msg = status.error_message();
    } else {
      // convert the value to a timestamp with milliseconds
      errno = 0;
      int64_t time_ms = std::strtoll(value.data(), nullptr, 10);
      if (errno == ERANGE || 0 == time_ms) {
        err_msg = lease_lock_key + ": invalid lease time: " + value;
        ret = Errors::SE_INVALID;
      } else if (time_ms > 0) {
        expire_time = std::chrono::milliseconds(time_ms);
      } else {
        err_msg = lease_lock_key + ": invalid lease time: " + value;
        ret = Errors::SE_INVALID;
      }
    }
  }
  return ret;
}

int try_single_data_node_lease_lock_if_expired(ObjectStore *objstore,
                                               const std::string_view bucket_dir,
                                               const std::string_view cluster_objstore_id,
                                               const LeaseLockSettings& lease_lock_settings,
                                               std::string &err_msg,
                                               std::chrono::milliseconds &new_lease_time)
{
  int ret = 0;
  Status status;

  if (objstore == nullptr) {
    err_msg = "object store is not initialized";
    ret = Errors::SE_INVALID;
  } else {
    int64_t max_version = 0;

    std::string start_after;
    bool finished = false;
    std::vector<ObjectMeta> stale_lock_version_files;
    const std::string lock_version_file_prefix = util::get_lease_lock_version_file_prefix(cluster_objstore_id);
    do {
      std::vector<ObjectMeta> results;
      status = objstore->list_object(bucket_dir, lock_version_file_prefix, false, start_after, finished, results);
      if (!status.is_succ()) {
        err_msg = std::string("fail to list all lock version files:") + status.error_message().data();
        ret = status.error_code();
      } else if (results.empty()) {
        if (!finished) {
          err_msg = "no lock version file found but not finished, impossible";
          ret = Errors::SE_UNEXPECTED;
        } else {
          // max_version = 0, no lock version file found
        }
      } else {
        for (const ObjectMeta &object_meta : results) {
          std::string_view key = object_meta.key;
          int64_t version = std::strtoll(key.substr(strlen(lock_version_file_prefix.data())).data(), nullptr, 10);
          if (errno == ERANGE || version <= 0) {
            err_msg = std::string(key) + ": invalid lock version file suffix";
            ret = Errors::SE_INVALID;
          } else if (version > 0) {
            if (version > max_version) {
              max_version = version;
            }
          }

          if (0 != ret) {
            break;
          }
        }
        if (0 == ret) {
          stale_lock_version_files.insert(stale_lock_version_files.end(), results.begin(), results.end());
        }
      }
    } while (0 == ret && !finished);

    if (0 == ret) {
      // try to create new lock version file with forbid-overwrite option
      int64_t new_max_version = max_version + 1;
      std::string new_max_version_lock_version_key = lock_version_file_prefix + std::to_string(new_max_version);

      bool forbid_overwrite = true;
      status = objstore->put_object(bucket_dir, new_max_version_lock_version_key, "", forbid_overwrite);
      if (status.error_code() == Errors::SE_OBJECT_FORBID_OVERWRITE) {
        err_msg = new_max_version_lock_version_key +
                  ":fail to create new lock version key, already exists:" + status.error_message().data();
        ret = Errors::SE_OHTER_DATA_NODE_MAYBE_RUNNING;
      } else if (!status.is_succ()) {
        err_msg = new_max_version_lock_version_key +
                  ":fail to put new lock version key:" + status.error_message().data();
        ret = status.error_code();
      } else {
        // new lock version file is created successfully

        // caculate new lease time and update the lease lock file
        std::chrono::milliseconds now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
        std::chrono::milliseconds lease_time = now + lease_lock_settings.lease_timeout;
        std::string lease_lock_key = util::get_lease_lock_key(cluster_objstore_id);
        status = objstore->put_object(bucket_dir, lease_lock_key, std::to_string(lease_time.count()), false);
        if (!status.is_succ()) {
          err_msg = lease_lock_key + ":fail to update lease lock file:" + status.error_message().data();
          ret = status.error_code();
        } else {
          std::vector<std::string_view> stale_lock_version_keys;
          stale_lock_version_keys.reserve(stale_lock_version_files.size());
          for (const ObjectMeta &object_meta : stale_lock_version_files) {
            assert(object_meta.key != new_max_version_lock_version_key);
            stale_lock_version_keys.emplace_back(object_meta.key);
          }
          if (!stale_lock_version_keys.empty()) {
            status = objstore->delete_objects(bucket_dir, stale_lock_version_keys);
            if (!status.is_succ()) {
              err_msg = std::string("fail to delete stale lock version files:") + status.error_message().data();
              ret = status.error_code();
            }
          }
          if (0 == ret) {
            // stale lock version keys are deleted successfully
            new_lease_time = lease_time;
          }
        }
      }
    }
  }
  return ret;
}

int check_if_lease_time_expired(ObjectStore *objstore,
                                const std::string_view bucket_dir,
                                const std::string_view cluster_objstore_id,
                                const LeaseLockSettings &lease_lock_settings,
                                std::string &err_msg,
                                std::chrono::milliseconds &expire_time,
                                bool &lease_expired)
{
  int ret = get_single_data_node_lease_lock_expire_time(objstore,
                                                        bucket_dir,
                                                        cluster_objstore_id,
                                                        err_msg,
                                                        expire_time);
  if (ret != 0) {
    err_msg = "fail to get lease lock expire time:" + err_msg;
  } else {
    std::chrono::milliseconds now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    if (now.count() >= expire_time.count()) {
      // lease time is expired
      lease_expired = true;
    } else if (now + lease_lock_settings.lease_timeout < expire_time) {
      err_msg = "lease lock has an unexpected expire time:" + std::to_string(expire_time.count()) +
                ", now:" + std::to_string(now.count());
      ret = Errors::SE_UNEXPECTED;
    } else {
      // lease time is not expired
      lease_expired = false;
    }
  }
  return ret;
}

int check_lease_and_try_lease_lock_if_expired(ObjectStore *objstore,
                                              const std::string_view bucket_dir,
                                              const std::string_view cluster_objstore_id,
                                              const LeaseLockSettings& lease_lock_settings,
                                              std::chrono::milliseconds &new_lease_time,
                                              std::string &err_msg)
{
  std::chrono::milliseconds expire_time;
  bool lease_expired = false;
  int ret = check_if_lease_time_expired(objstore, bucket_dir, cluster_objstore_id, lease_lock_settings, err_msg, expire_time, lease_expired);
  if (ret != 0) {
    err_msg = "fail to check lease time:" + err_msg;
  } else if (lease_expired) {
    ret = try_single_data_node_lease_lock_if_expired(objstore,
                                                     bucket_dir,
                                                     cluster_objstore_id,
                                                     lease_lock_settings,
                                                     err_msg,
                                                     new_lease_time);
    if (ret != 0) {
      err_msg = "fail to get lease lock:" + err_msg;
    } else {
      // success to get lease lock
    }
  } else {
    // lease time is not expired

    // the acutal lease time in object store is not equal to last lease time of this node in memory,
    // it must be owned by another node
    if (lease_lock_last_lease_time != expire_time) {
      err_msg = "lease time is valid and owned by another node: " + std::to_string(expire_time.count());
      ret = Errors::SE_OHTER_DATA_NODE_MAYBE_RUNNING;
    } else {
      err_msg = "lease time is valid and owned by this node: " + std::to_string(expire_time.count());
      ret = Errors::SE_LEASE_LOCK_RENEWAL_TIMEOUT;
    }
  }
  return ret;
}

// this is used for lease lock owner to renewal the lease lock
int renewal_single_data_node_lease_lock(ObjectStore *objstore,
                                        const std::string_view bucket_dir,
                                        const std::string_view cluster_objstore_id,
                                        const LeaseLockSettings &lease_lock_settings,
                                        std::chrono::milliseconds &new_lease_time,
                                        std::string &err_msg)
{
  int ret = 0;
  const std::chrono::milliseconds last_lease_time = lease_lock_last_lease_time;
  if (objstore == nullptr) {
    err_msg = "object store is not initialized";
    ret = Errors::SE_INVALID;
  } else if (!is_lease_lock_owner) {
    err_msg = "this node is not lease lock owner";
    ret = Errors::SE_INVALID;
  } else if (last_lease_time.count() < lease_lock_settings.lease_timeout.count()) {
    err_msg = "last lease time is invalid:" + std::to_string(last_lease_time.count());
    ret = Errors::SE_INVALID;
  } else {
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    const std::chrono::milliseconds last_renewal_time = last_lease_time - lease_lock_settings.lease_timeout;
    if (now.count() < last_renewal_time.count()) {
      err_msg = "current time:" + std::to_string(now.count()) +
                "is less than last renewal time:" + std::to_string(last_renewal_time.count()) + ", impossible";
      ret = Errors::SE_INVALID;
    } else if (now.count() - last_renewal_time.count() < lease_lock_settings.renewal_timeout.count()) {
      // lease lock renewal interval is not reached
      new_lease_time = now + lease_lock_settings.lease_timeout;
      std::string new_lease_time_str = std::to_string(new_lease_time.count());
      std::string lease_lock_key = util::get_lease_lock_key(cluster_objstore_id);
      Status status = objstore->put_object(bucket_dir, lease_lock_key, new_lease_time_str, false);
      if (!status.is_succ()) {
        err_msg = lease_lock_key + ":fail to put lease lock key when renew lease time" + status.error_message().data();
        ret = status.error_code();
      } else {
        // success to renewal lease lock
      }
    } else {
      // renewal timeout reached, or lease time is expired
      ret = check_lease_and_try_lease_lock_if_expired(objstore,
                                                      bucket_dir,
                                                      cluster_objstore_id,
                                                      lease_lock_settings,
                                                      new_lease_time,
                                                      err_msg);
      if (ret != 0) {
        err_msg = "fail to get lease lock:" + err_msg;
      } else {
        // success to get lease lock
      }
    }
  }
  return ret;
}

int remove_lease_lock_key(ObjectStore *objstore, const std::string_view bucket, const std::string_view cluster_objstore_id, std::string &err_msg) 
{
  int ret = 0;
  if (objstore == nullptr) {
    ret = Errors::SE_INVALID;
    err_msg = "object store is not initialized";
  } else {
    std::string lease_lock_key = util::get_lease_lock_key(cluster_objstore_id);
    Status status = objstore->delete_object(bucket, lease_lock_key);
    if (!status.is_succ()) {
      ret = status.error_code();
      err_msg = status.error_message();
    }
  }
  return ret;
}

void TEST_unset_lease_lock_owner()
{
  is_lease_lock_owner = false;
  lease_lock_last_lease_time = std::chrono::milliseconds(0);
}

#ifdef NDEBUG
} // namespace
#endif

bool is_lease_lock_owner_node() { return is_lease_lock_owner; }

int try_single_data_node_lease_lock(ObjectStore *objstore,
                                    const std::string_view bucket_dir,
                                    const std::string_view cluster_objstore_id,
                                    const LeaseLockSettings &lease_lock_settings,
                                    std::string &err_msg,
                                    std::string &important_msg,
                                    bool &need_abort)
{
  int ret = 0;
  std::chrono::milliseconds new_lease_time;

  need_abort = false;
  if (is_lease_lock_owner) {
    ret = renewal_single_data_node_lease_lock(objstore, bucket_dir, cluster_objstore_id, lease_lock_settings, new_lease_time, err_msg);
#ifndef NDEBUG
    TEST_SYNC_POINT_CALLBACK("objstore::try_single_data_node_lease_lock", &ret);
#endif
  } else {
    ret = check_lease_and_try_lease_lock_if_expired(objstore, bucket_dir, cluster_objstore_id, lease_lock_settings, new_lease_time, err_msg);
  }

  if (ret == 0) {
    if (!is_lease_lock_owner) {
      important_msg = "this node becomes the lease lock owner! new lease time:" +
                      std::to_string(new_lease_time.count());
    }
    // success to get/renewal the lease lock
    is_lease_lock_owner = true;
    lease_lock_last_lease_time = new_lease_time;
    // if a data node get lease lock successfully, it will go into running state,
    // since we can't running multiple data nodes at the same time,
    // should abort when we can make sure lease lock is owned by other node
    should_abort_if_not_owner = true;
  } else {
    err_msg = "fail to renewal lease lock:" + err_msg;

    bool lease_lock_owned_by_other = false;
    if (ret == Errors::SE_OHTER_DATA_NODE_MAYBE_RUNNING) {
      lease_lock_owned_by_other = true;
    }
    if (should_abort_if_not_owner && lease_lock_owned_by_other) {
      important_msg = "lease lock is owned by other node now!!!";
      // should abort as soon as possible
      need_abort = true;
    }
    is_lease_lock_owner = false;
  }

  return ret;
}

} // namespace objstore
} // namespace smartengine
