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

#include "objstore.h"

#include "mysys/objstore/aliyun_oss.h"
#include "mysys/objstore/local.h"
#include "mysys/objstore/s3.h"

#include <filesystem>

namespace objstore {

namespace fs = std::filesystem;

std::string remove_prefix(const std::string &str, const std::string &prefix) {
  int pos = str.find(prefix);
  if (pos == 0) {
    return str.substr(prefix.size());
  }
  return str;
}

/*
this function check whether a key is frist-level sub key in a s3/oss/...
directory (like `aws s3 ls ...`).

if prefix is "a", the following are first-level sub keys:
====
"a"
"ab"
"a/"
"aa"
"aa/"
"ab/"
====

if prefix is "a/", the following are first level sub keys:
====
"a/"
"a/b"
"a/b/"
====
and the following are not:
====
"aa/"
"a/b/c"
====
*/
bool is_first_level_sub_key(const std::string_view &key,
                            const std::string_view &prefix) {
  if (key.size() >= prefix.size()) {
    if (key.find('/', prefix.size()) == std::string::npos ||
        key.find('/', prefix.size()) == key.size() - 1) {
      return true;
    }
  }
  return false;
}

int mkdir_p(std::string_view path) {
  std::error_code errcode;
  fs::create_directories(path, errcode);
  // bool created = fs::create_directories(path, errcode);
  //  assert(created);
  return errcode.value();
}

int rm_f(std::string_view path) {
  std::error_code errcode;
  fs::remove_all(path, errcode);
  // bool deleted = fs::remove_all(path, errcode);
  // assert(deleted);
  return errcode.value();
}

void init_objstore_provider(const std::string_view &provider) {
  if (ObjectStore::use_s3_sdk(provider)) {
    init_aws_api();
  } else if (provider == "aliyun") {
    init_aliyun_api();
  } else if (provider == "local") {
    // do nothing
  }
}

void cleanup_objstore_provider(ObjectStore *objstore) {
  if (objstore != nullptr) {
    std::string_view provider = objstore->get_provider();
    if (ObjectStore::use_s3_sdk(provider)) {
      shutdown_aws_api();
    } else if (provider == "aliyun") {
      shutdown_aliyun_api();
    } else if (provider == "local") {
      // do nothing
    }
  }
}

char *get_src_access_key_id() {
  char *src_access_key_id = std::getenv("SOURCE_ACCESS_KEY_ID");
  if (src_access_key_id) {
    return src_access_key_id;
  }
  return nullptr;
}

char *get_src_access_secret_key() {
  char *src_access_secret_key = std::getenv("SOURCE_SECRET_ACCESS_KEY");
  if (src_access_secret_key) {
    return src_access_secret_key;
  }
  return nullptr;
}

char *get_dest_access_key_id() {
  char *dst_access_key_id = std::getenv("DEST_ACCESS_KEY_ID");
  if (dst_access_key_id) {
    return dst_access_key_id;
  }
  return nullptr;
}

char *get_dest_access_secret_key() {
  char *dst_access_secret_key = std::getenv("DEST_SECRET_ACCESS_KEY");
  if (dst_access_secret_key) {
    return dst_access_secret_key;
  }
  return nullptr;
}

bool ObjectStore::is_valid_key(const std::string_view &key) {
  // for s3, a key should be no more than 1024 bytes.
  //
  // for aliyun, a key should not start with '/' or '\', and key length must be
  // in [1,1023].
  return key.size() > 0 && key.size() < 1024 && key[0] != '/' && key[0] != '\\';
}

Status ObjectStore::delete_directory(const std::string_view &bucket,
                                     const std::string_view &prefix) {
  std::string dir_prefix(prefix);
  if (!prefix.empty() && prefix.back() != '/') {
    dir_prefix.append("/");
  }
  std::string start_after;
  bool finished = false;
  std::vector<ObjectMeta> objects;
  while (!finished) {
    Status s =
        list_object(bucket, dir_prefix, true, start_after, finished, objects);
    if (!s.is_succ()) {
      return s;
    }
    if (!objects.empty()) {
      std::vector<std::string_view> object_keys;
      for (const ObjectMeta &object_meta : objects) {
        object_keys.emplace_back(object_meta.key);
      }
      s = delete_objects(bucket, object_keys);
      if (!s.is_succ()) {
        return s;
      }
      objects.clear();
    }
  }
  return Status();
}

Status ObjectStore::put_objects_from_dir(const std::string_view &src_dir,
                                         const std::string_view &dst_objstore_bucket,
                                         const std::string_view &dst_objstore_dir) {
  fs::path src_dir_path = fs::path(src_dir).lexically_normal();
  std::string dst_objstore_dir_path(dst_objstore_dir);
  if (!dst_objstore_dir_path.empty() && dst_objstore_dir_path.back() != '/') {
    dst_objstore_dir_path.append("/");
  }
  // check if local dir path exists
  if (!fs::exists(src_dir_path)) {
    std::string err_msg = src_dir_path.native() + " does not exist";
    return Status(Errors::SE_INVALID, EINVAL, err_msg.c_str());
  }
  // check if local dir path is a directory
  if (!fs::is_directory(src_dir_path)) {
    std::string err_msg = src_dir_path.native() + " is not a directory";
    return Status(Errors::SE_INVALID, ENOTDIR, err_msg.c_str());
  }

  if (!dst_objstore_dir_path.empty()) {
    Status s = put_object(dst_objstore_bucket, dst_objstore_dir_path, "");
    if (!s.is_succ()) {
      return s;
    }
  }
  for (const fs::directory_entry &entry :
       fs::recursive_directory_iterator(src_dir_path)) {
    std::string key = fs::relative(entry.path(), src_dir_path);
    if (!dst_objstore_dir_path.empty()) {
      key = dst_objstore_dir_path + key;
    }

    if (entry.is_directory()) {
      if (key.back() != '/') {
        key.append("/");
      }
      Status s = put_object(dst_objstore_bucket, key, "");
      if (!s.is_succ()) {
        return s;
      }
    } else if (entry.is_regular_file()) {
      std::string abs_src_file_path = fs::absolute(entry.path());
      Status s =
          put_object_from_file(dst_objstore_bucket, key, abs_src_file_path);
      if (!s.is_succ()) {
        return s;
      }
    }
  }
  return Status();
}

Status ObjectStore::get_objects_to_dir(const std::string_view &src_objstore_bucket,
                                       const std::string_view &src_objstore_dir,
                                       const std::string_view &dst_dir) {
  fs::path dst_dir_path = fs::path(dst_dir).lexically_normal();
  std::string src_objstore_dir_path(src_objstore_dir);
  if (!src_objstore_dir_path.empty() && src_objstore_dir_path.back() != '/') {
    src_objstore_dir_path.append("/");
  }
  // check if the dst directory exists, if not, create it.
  if (!fs::exists(dst_dir_path)) {
    int ret = mkdir_p(dst_dir_path.native());
    if (ret != 0) {
      return Status(Errors::SE_IO_ERROR, ret, std::generic_category().message(ret));
    }
  }
  if (!fs::is_directory(dst_dir_path)) {
    std::string err_msg = dst_dir_path.native() + " is not a directory";
    return Status(Errors::SE_INVALID, ENOTDIR, err_msg.c_str());
  }

  bool finished = false;
  std::vector<ObjectMeta> object_metas;
  Status s;
  std::string start_after;
  fs::path last_parent_path;
  while (!finished) {
    s = list_object(src_objstore_bucket, src_objstore_dir_path, true,
                    start_after, finished, object_metas);
    if (!s.is_succ()) {
      return s;
    }
    fs::path abs_dst_dir_path = fs::absolute(dst_dir_path);
    for (const ObjectMeta &obj : object_metas) {
      std::string key = obj.key;
      if (!src_objstore_dir_path.empty()) {
        key = remove_prefix(key, src_objstore_dir_path);
      }
      fs::path dst_file_path = abs_dst_dir_path / key;

      if (obj.key.back() == '/') {
        // this is a sub directory of object store bucket
        if (!fs::exists(dst_file_path)) {
          int ret = mkdir_p(dst_file_path.native());
          if (ret != 0) {
            return Status(Errors::SE_IO_ERROR, ret,
                          std::generic_category().message(ret));
          }
        }
      } else {
        if (dst_file_path.parent_path() != last_parent_path &&
            !fs::exists(dst_file_path.parent_path())) {
          int ret = mkdir_p(dst_file_path.parent_path().native());
          if (ret != 0) {
            return Status(Errors::SE_IO_ERROR, ret,
                          std::generic_category().message(ret));
          }
          last_parent_path = dst_file_path.parent_path();
        }
        s = get_object_to_file(src_objstore_bucket, obj.key,
                               dst_file_path.native());
        if (!s.is_succ()) {
          return s;
        }
      }
    }
    object_metas.clear();
  }
  return Status();
}

ObjectStore *create_object_store(const std::string_view &provider,
                                 const std::string_view region,
                                 const std::string_view *endpoint,
                                 bool use_https, std::string &err_msg) {
  if (ObjectStore::use_s3_sdk(provider)) {
    return create_s3_objstore(region, endpoint, use_https, err_msg);
  } else if (provider == "aliyun") {
    return create_aliyun_oss_objstore(region, endpoint, err_msg);
  } else if (provider == "local") {
    return create_local_objstore(region, endpoint, use_https);
  } else {
    return nullptr;
  }
}

ObjectStore *create_source_object_store(const std::string_view &provider,
                                        const std::string_view region,
                                        const std::string_view *endpoint,
                                        bool use_https, std::string &err_msg) {
  if (ObjectStore::use_s3_sdk(provider)) {
    return create_source_s3_objstore(region, endpoint, use_https, err_msg);
  } else if (provider == "aliyun") {
    return create_source_aliyun_oss_objstore(region, endpoint, err_msg);
  } else if (provider == "local") {
    return create_local_objstore(region, endpoint, use_https);
  } else {
    return nullptr;
  }
}

ObjectStore *create_dest_object_store(const std::string_view &provider,
                                      const std::string_view region,
                                      const std::string_view *endpoint,
                                      bool use_https, std::string &err_msg) {
  if (ObjectStore::use_s3_sdk(provider)) {
    return create_dest_s3_objstore(region, endpoint, use_https, err_msg);
  } else if (provider == "aliyun") {
    return create_dest_aliyun_oss_objstore(region, endpoint, err_msg);
  } else if (provider == "local") {
    return create_local_objstore(region, endpoint, use_https);
  } else {
    return nullptr;
  }
}

ObjectStore *create_object_store_for_test(const std::string_view &provider,
                                          const std::string_view region,
                                          const std::string_view *endpoint,
                                          bool use_https,
                                          const std::string_view bucket_dir,
                                          std::string &err_msg) {
  if (ObjectStore::use_s3_sdk(provider)) {
    return create_s3_objstore_for_test(region, endpoint, use_https, bucket_dir,
                                       err_msg);
  } else if (provider == "aliyun") {
    return create_aliyun_oss_objstore_for_test(region, endpoint, bucket_dir,
                                               err_msg);
  } else if (provider == "local") {
    return create_local_objstore(region, endpoint, use_https);
  } else {
    return nullptr;
  }
}

void destroy_object_store(ObjectStore *obj_store) {
  // provide a register mechanism to create/destroy the object store
  delete obj_store;
}

int init_object_store(const std::string_view &provider,
                      const std::string_view &region,
                      const std::string_view &bucket_dir, std::string &err_msg,
                      ObjectStore *&objstore) {
  Status status;
  int ret = 0;
  init_objstore_provider(provider);
  objstore = create_object_store(provider, region, nullptr, false, err_msg);
  if (objstore == nullptr) {
    ret = 1;
    cleanup_objstore_provider(objstore);
    return ret;
  }
  return 0;
}

void cleanup_object_store(ObjectStore *&objstore) {
  cleanup_objstore_provider(objstore);
  delete objstore;
}

}  // namespace objstore