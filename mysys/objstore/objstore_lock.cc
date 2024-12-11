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

#include <cstring>
#include <thread>
#include "objstore.h"

namespace objstore {

static constexpr std::string_view data_lock_file{"data.lock"};

int ensure_object_store_lock(const std::string_view &provider,
                             const std::string_view &region,
                             const std::string_view *endpoint,
                             const std::string_view &bucket_dir,
                             const std::string_view &store_id,
                             const std::string_view &branch_id,
                             const bool should_exist, std::string &err_msg) {
  Status status;
  std::string data_lock;
  int error = 0;

  objstore::init_objstore_provider(provider);

  ObjectStore *objstore =
      create_object_store(provider, region, endpoint, false, err_msg);
  if (objstore == nullptr) {
    return 1;
  }

  std::string branch_path(store_id.data());
  branch_path.append("/");
  branch_path.append(branch_id.data());
  branch_path.append("/");

  std::string data_lock_path(branch_path);
  data_lock_path.append(data_lock_file.data());

  status = objstore->get_object(bucket_dir, data_lock_path, data_lock);
  if (!status.is_succ() &&
      status.error_code() != objstore::Errors::SE_NO_SUCH_KEY) {
    err_msg = status.error_message();
    error = 1;
  } else if (should_exist &&
             status.error_code() == objstore::Errors::SE_NO_SUCH_KEY) {
    err_msg = data_lock_path + ": lock file should exist but not";
    error = 1;
  } else if (!should_exist && status.is_succ()) {
    err_msg = data_lock_path + ": lock file should not exist";
    error = 1;
  } else if (!should_exist) {
    status = objstore->put_object(bucket_dir, branch_path, "");
    if (!status.is_succ()) {
      err_msg = status.error_message();
      error = 1;
    }
    status = objstore->put_object(bucket_dir, data_lock_path, "");
    if (!status.is_succ()) {
      err_msg = status.error_message();
      error = 1;
    }
  }

  delete objstore;

  return error;
}

}  // namespace objstore