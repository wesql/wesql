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

#include "mysys/objstore/local.h"

#include <sys/errno.h>
#include <algorithm>
#include <cerrno>
#include <chrono>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <system_error>

namespace objstore {

namespace fs = std::filesystem;

namespace {

Errors std_err_code_to_objstore_error(std::error_code err_code) {
  if (!err_code) {
    return Errors::SE_SUCCESS;
  }
  if (err_code.value() == ENOENT) {
    return Errors::SE_NO_SUCH_KEY;
  }
  return Errors::SE_IO_ERROR;
}

bool path_is_within(const fs::path &root, const fs::path &path) {
  auto root_component = root.begin();
  auto path_component = path.begin();
  for (; root_component != root.end(); ++root_component, ++path_component) {
    if (path_component == path.end() || *path_component != *root_component) {
      return false;
    }
  }
  return true;
}

int get_obj_meta_from_file(const fs::path &path, ObjectMeta &meta, bool is_dir) {
  std::error_code errcode;

  fs::file_time_type ftime = fs::last_write_time(path, errcode);
  if (errcode) {
    return errcode.value();
  }
  const auto system_time = fs::file_time_type::clock::to_sys(ftime);
  const int64_t epoch_in_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          system_time.time_since_epoch())
          .count();

  std::uintmax_t fsize = 0;
  if (is_dir) {
    fsize = 0;
  } else {
    errcode.clear();
    fsize = fs::file_size(path, errcode);
    if (errcode) {
      return errcode.value();
    }
    if (fsize > static_cast<std::uintmax_t>(
                    std::numeric_limits<long long>::max())) {
      return EOVERFLOW;
    }
  }

  meta.last_modified = epoch_in_ms;
  meta.size = static_cast<long long>(fsize);

  return 0;
}

}  // anonymous namespace

bool LocalObjectStore::is_valid_local_bucket(std::string_view bucket) {
  return is_valid_key(bucket) &&
         bucket.find('\0') == std::string_view::npos && bucket != "." &&
         bucket != ".." &&
         bucket.find('/') == std::string_view::npos &&
         bucket.find('\\') == std::string_view::npos;
}

bool LocalObjectStore::is_valid_local_key(std::string_view key) {
  if (!is_valid_key(key) || key.find('\0') != std::string_view::npos) {
    return false;
  }

  const fs::path path{std::string(key)};
  if (path.is_absolute() || path.has_root_path()) return false;
  for (const fs::path &component : path) {
    if (component == "." || component == "..") return false;
  }
  return true;
}

bool LocalObjectStore::is_path_in_bucket(std::string_view bucket,
                                         std::string_view key) {
  std::error_code error;
  fs::path root = fs::weakly_canonical(fs::absolute(basepath_, error), error);
  if (error) return false;

  const fs::path bucket_root = (root / std::string(bucket)).lexically_normal();
  const fs::path candidate =
      fs::weakly_canonical(bucket_root / std::string(key), error);
  return !error && path_is_within(bucket_root, candidate);
}

Status LocalObjectStore::create_bucket(const std::string_view &bucket) {
  const std::lock_guard<std::mutex> _(mutex_);

  if (!is_valid_local_bucket(bucket) || !is_path_in_bucket(bucket, "")) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid bucket");
  }

  int ret = mkdir_p(generate_path(bucket));
  Errors error_code = ret == 0 ? Errors::SE_SUCCESS : Errors::SE_IO_ERROR;
  return Status(error_code, ret, std::generic_category().message(ret));
}

Status LocalObjectStore::delete_bucket(const std::string_view &bucket) {
  const std::lock_guard<std::mutex> _(mutex_);

  if (!is_valid_local_bucket(bucket) || !is_path_in_bucket(bucket, "")) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid bucket");
  }

  int ret = rm_f(generate_path(bucket));
  Errors error_code = ret == 0 ? Errors::SE_SUCCESS : Errors::SE_IO_ERROR;
  return Status(error_code, ret, std::generic_category().message(ret));
}

Status LocalObjectStore::put_object_from_file(
    const std::string_view &bucket, const std::string_view &key,
    const std::string_view &data_file_path) {
  const std::lock_guard<std::mutex> _(mutex_);

  if (data_file_path.find('\0') != std::string_view::npos) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid input file path");
  }
  if (!is_valid_local_bucket(bucket) || !is_path_in_bucket(bucket, "")) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid bucket");
  }
  if (!is_valid_local_key(key) || !is_path_in_bucket(bucket, key)) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid key");
  }

  const std::string key_path = generate_path(bucket, key);

  std::error_code errcode;
  if (!fs::is_regular_file(fs::path{std::string(data_file_path)}, errcode)) {
    const int error = errcode ? errcode.value() : EINVAL;
    const Errors object_error =
        errcode ? std_err_code_to_objstore_error(errcode) : Errors::SE_INVALID;
    return Status(object_error, error,
                  "Couldn't open input file");
  }

  // key may contains '/', so if its parent directory does not exists, we
  // create for it.
  const int ret = mkdir_p(fs::path(key_path).parent_path().native());
  if (ret != 0) {
    return Status(Errors::SE_IO_ERROR, ret,
                  std::generic_category().message(ret));
  }

  errcode.clear();
  fs::copy_file(fs::path{std::string(data_file_path)}, fs::path{key_path},
                fs::copy_options::overwrite_existing, errcode);
  return errcode ? Status(std_err_code_to_objstore_error(errcode),
                          errcode.value(), errcode.message())
                 : Status();
}

Status LocalObjectStore::get_object_to_file(
    const std::string_view &bucket, const std::string_view &key,
    const std::string_view &output_file_path) {
  const std::lock_guard<std::mutex> _(mutex_);

  if (output_file_path.find('\0') != std::string_view::npos) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid output file path");
  }
  if (!is_valid_local_bucket(bucket)) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid bucket");
  }
  if (!is_valid_local_key(key) || !is_path_in_bucket(bucket, key)) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid key");
  }

  const std::string key_path = generate_path(bucket, key);
  std::error_code errcode;
  fs::copy_file(fs::path{key_path}, fs::path{std::string(output_file_path)},
                fs::copy_options::overwrite_existing, errcode);
  return errcode ? Status(std_err_code_to_objstore_error(errcode),
                          errcode.value(), errcode.message())
                 : Status();
}

Status LocalObjectStore::put_object(const std::string_view &bucket,
                                    const std::string_view &key,
                                    const std::string_view &data,
                                    bool forbid_overwrite) {
  const std::lock_guard<std::mutex> _(mutex_);

  if (!is_valid_local_bucket(bucket)) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid bucket");
  }
  if (!is_valid_local_key(key) || !is_path_in_bucket(bucket, key)) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid key");
  }

  std::string key_path = generate_path(bucket, key);

  // NOTICE:
  // when forbid_overwrite is true, we need to check whether the key exists
  // before write. if it exists, we return error.
  //
  // for local mode, there isn't a atomic operation to check and write, so we
  // just do check and write in two steps. since local mode is only used for
  // test, so it's ok.
  //
  // c++ 23 provide std::ios::noreplace flag for std::ofstream, which can
  // simplify this logic later if we support it.
  std::error_code errcode;
  if (forbid_overwrite && fs::exists(key_path, errcode)) {
    return Status(Errors::SE_OBJECT_FORBID_OVERWRITE, EEXIST, "file exists");
  }
  if (errcode) {
    return Status(std_err_code_to_objstore_error(errcode), errcode.value(),
                  errcode.message());
  }

  if (key_path.back() == '/') {
    if (data.size() != 0) {
      return Status(Errors::SE_INVALID, EINVAL,
                    std::string("key is directory but have data:") + key_path);
    }
    int ret = mkdir_p(fs::path(key_path).native());
    if (ret != 0) {
      return Status(Errors::SE_IO_ERROR, ret,
                    std::generic_category().message(ret));
    }
    return Status();
  } else {
    // key may contains '/', so if its parent directory does not exists, we
    // create for it.
    const int ret = mkdir_p(fs::path(key_path).parent_path().native());
    if (ret != 0) {
      return Status(Errors::SE_IO_ERROR, ret,
                    std::generic_category().message(ret));
    }
    if (data.size() >
        static_cast<size_t>(std::numeric_limits<std::streamsize>::max())) {
      return Status(Errors::SE_INVALID, EOVERFLOW, "object is too large");
    }
    std::ofstream output_file(key_path, std::ios::binary | std::ios::trunc);
    if (!output_file) {
      const int error = errno == 0 ? EIO : errno;
      return Status(Errors::SE_IO_ERROR, error, "Couldn't open file");
    }

    output_file.write(data.data(), static_cast<std::streamsize>(data.size()));
    output_file.flush();
    const bool fail = !output_file;
    output_file.close();
    return fail ? Status(Errors::SE_IO_ERROR, EINVAL, "write fail") : Status();
  }
}

Status LocalObjectStore::get_object(const std::string_view &bucket,
                                    const std::string_view &key,
                                    std::string &body) {
  const std::lock_guard<std::mutex> _(mutex_);

  if (!is_valid_local_bucket(bucket)) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid bucket");
  }
  if (!is_valid_local_key(key) || !is_path_in_bucket(bucket, key)) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid key");
  }

  std::string key_path = generate_path(bucket, key);
  if (key_path.back() == '/') {
    std::error_code errcode;
    if (!fs::is_directory(key_path, errcode)) {
      if (errcode) {
        return Status(std_err_code_to_objstore_error(errcode), errcode.value(),
                      errcode.message());
      }
      return Status(Errors::SE_NO_SUCH_KEY, ENOENT, "key not found");
    }
    body.clear();
    return Status();
  } else {
    std::ifstream input_file(key_path, std::ios::in | std::ios::binary);
    if (!input_file.is_open()) {
      std::error_code errcode(errno, std::generic_category());
      // if (ENOENT == errcode.value() && !fs::exists(generate_path(bucket))) {
      //   return Status(Errors::SE_NO_SUCH_BUCKET, ENOENT, "bucket not found");
      // }
      Errors error_code = std_err_code_to_objstore_error(errcode);
      return Status(error_code, errcode.value(), "Couldn't open file");
    }

    std::string result{std::istreambuf_iterator<char>{input_file},
                       std::istreambuf_iterator<char>{}};
    if (input_file.bad()) {
      return Status(Errors::SE_IO_ERROR, EIO, "read fail");
    }
    body = std::move(result);
    return Status();
  }
}

Status LocalObjectStore::get_object(const std::string_view &bucket,
                                    const std::string_view &key, size_t off,
                                    size_t len, std::string &body) {
  const std::lock_guard<std::mutex> _(mutex_);

  if (!is_valid_local_bucket(bucket)) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid bucket");
  }
  if (!is_valid_local_key(key) || !is_path_in_bucket(bucket, key)) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid key");
  }

  std::string key_path = generate_path(bucket, key);
  if (key_path.back() == '/') {
    std::error_code errcode;
    if (!fs::is_directory(key_path, errcode)) {
      if (errcode) {
        return Status(std_err_code_to_objstore_error(errcode), errcode.value(),
                      errcode.message());
      }
      return Status(Errors::SE_NO_SUCH_KEY, ENOENT, "key not found");
    }
    body.clear();
    return Status();
  } else {
    std::ifstream input_file(key_path, std::ios::in | std::ios::binary);
    if (!input_file.is_open()) {
      std::error_code errcode(errno, std::generic_category());
      // if (ENOENT == errcode.value() && !fs::exists(generate_path(bucket))) {
      //   return Status(Errors::SE_NO_SUCH_BUCKET, ENOENT, "bucket not found");
      // }
      Errors error_code = std_err_code_to_objstore_error(errcode);
      return Status(error_code, errcode.value(), "Couldn't open file");
    }

    std::error_code errcode;
    const std::uintmax_t file_size = fs::file_size(key_path, errcode);
    if (errcode) {
      return Status(std_err_code_to_objstore_error(errcode), errcode.value(),
                    errcode.message());
    }
    if (off >= file_size) {
      return Status(Errors::SE_INVALID, ERANGE, "offset out of range");
    }

    const std::uintmax_t remaining = file_size - off;
    const size_t read_size =
        static_cast<size_t>(std::min<std::uintmax_t>(remaining, len));
    if (off > static_cast<size_t>(
                  std::numeric_limits<std::streamoff>::max()) ||
        read_size >
        static_cast<size_t>(std::numeric_limits<std::streamsize>::max())) {
      return Status(Errors::SE_INVALID, EOVERFLOW, "range too large");
    }

    input_file.seekg(static_cast<std::streamoff>(off));
    if (!input_file) {
      return Status(Errors::SE_IO_ERROR, EIO, "seek fail");
    }
    std::string result(read_size, '\0');
    input_file.read(result.data(), static_cast<std::streamsize>(read_size));
    if (input_file.gcount() != static_cast<std::streamsize>(read_size)) {
      return Status(Errors::SE_IO_ERROR, EIO, "read fail");
    }
    body = std::move(result);
    return Status();
  }
}

Status LocalObjectStore::get_object_meta(const std::string_view &bucket,
                                         const std::string_view &key,
                                         ObjectMeta &meta) {
  const std::lock_guard<std::mutex> _(mutex_);
  bool is_dir = false;

  if (!is_valid_local_bucket(bucket) || !is_path_in_bucket(bucket, "")) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid bucket");
  }
  if (!is_valid_local_key(key) || !is_path_in_bucket(bucket, key)) {
    return Status(Errors::SE_INVALID, EINVAL,
                  std::string("invalid key:") + std::string(key));
  }
  fs::path key_path = fs::path(generate_path(bucket, key));
  std::error_code errcode;
  if (key.back() == '/' && fs::is_directory(key_path, errcode)) {
    is_dir = true;
  } else if (!errcode && key.back() != '/' &&
             fs::is_regular_file(key_path, errcode)) {
    // empty
  } else {
    return Status(Errors::SE_INVALID, EINVAL,
                  std::string("invalid key:") + std::string(key));
  }

  int ret = get_obj_meta_from_file(key_path, meta, is_dir);
  if (ret != 0) {
    return Status(Errors::SE_IO_ERROR, ret, "fail to get object meta");
  }

  std::string bucket_path = generate_path(bucket);
  // use lexically_relative() to remove the bucket prefix
  // example: entry: bucket/key_prefix_dir/key -> key_prefix_dir/key
  meta.key = key_path.lexically_relative(bucket_path).generic_string();
  return Status();
}

Status process_local_obj_meta(ObjectMeta &meta,
                              const fs::directory_entry &entry,
                              std::vector<ObjectMeta> &objects,
                              std::string_view bucket_path,
                              const std::string_view &prefix, bool recursive) {
  bool is_dir = false;
  std::error_code errcode;
  if (entry.is_symlink(errcode)) {
    return Status(Errors::SE_INVALID, ELOOP,
                  "symbolic links are not valid local objects");
  }
  if (!errcode && entry.is_directory(errcode)) {
    is_dir = true;
    meta.key =
        entry.path().lexically_relative(bucket_path).generic_string();
    if (meta.key.back() != '/') {
      meta.key.append("/");
    }
  } else if (!errcode && entry.is_regular_file(errcode)) {
    // use lexically_relative() to remove the bucket prefix
    // example: entry: bucket/key_prefix_dir/key -> key_prefix_dir/key
    meta.key =
        entry.path().lexically_relative(bucket_path).generic_string();
  } else {
    const int error = errcode ? errcode.value() : EINVAL;
    return Status(Errors::SE_INVALID, error, "invalid file type");
  }

  if (meta.key.starts_with(prefix)) {
    if (recursive || is_first_level_sub_key(meta.key, prefix)) {
      int ret = get_obj_meta_from_file(entry.path(), meta, is_dir);
      if (ret != 0) {
        return Status(Errors::SE_IO_ERROR, ret, "fail to get object meta");
      }
      objects.push_back(meta);
    }
  }

  return Status();
}

Status LocalObjectStore::list_object(const std::string_view &bucket,
                                     const std::string_view &prefix,
                                     bool recursive,
                                     std::string &start_after,
                                     bool &finished,
                                     std::vector<ObjectMeta> &objects) {
  const std::lock_guard<std::mutex> _(mutex_);

  if (!is_valid_local_bucket(bucket) || !is_path_in_bucket(bucket, "")) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid bucket");
  }
  if ((!prefix.empty() &&
       (!is_valid_local_key(prefix) || !is_path_in_bucket(bucket, prefix))) ||
      (!start_after.empty() && !is_valid_local_key(start_after))) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid list key");
  }

  const std::string bucket_path = generate_path(bucket);
  objects.clear();

  std::error_code errcode;
  if (!fs::is_directory(bucket_path, errcode)) {
    if (errcode && errcode.value() != ENOENT) {
      return Status(Errors::SE_IO_ERROR, errcode.value(), errcode.message());
    }
    return Status(Errors::SE_NO_SUCH_BUCKET, ENOENT, "bucket not found");
  }

  fs::recursive_directory_iterator entry(bucket_path, errcode);
  const fs::recursive_directory_iterator end;
  if (errcode) {
    return Status(Errors::SE_IO_ERROR, errcode.value(), errcode.message());
  }
  for (; entry != end; entry.increment(errcode)) {
    if (errcode) {
      return Status(Errors::SE_IO_ERROR, errcode.value(), errcode.message());
    }
    ObjectMeta meta;
    Status s = process_local_obj_meta(meta, *entry, objects, bucket_path,
                                      prefix, recursive);
    if (!s.is_succ()) {
      return s;
    }
  }
  if (errcode) {
    return Status(Errors::SE_IO_ERROR, errcode.value(), errcode.message());
  }

  // sort the objects by key in lexicographical order.
  std::sort(
      objects.begin(), objects.end(),
      [](const ObjectMeta &a, const ObjectMeta &b) { return a.key < b.key; });
  if (!start_after.empty()) {
    objects.erase(
        objects.begin(),
        std::upper_bound(
            objects.begin(), objects.end(), start_after,
            [](std::string_view key, const ObjectMeta &object) {
              return key < object.key;
            }));
  }

  finished = true;
  return Status();
}

Status LocalObjectStore::delete_object(const std::string_view &bucket,
                                       const std::string_view &key) {
  const std::lock_guard<std::mutex> _(mutex_);

  if (!is_valid_local_bucket(bucket)) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid bucket");
  }
  if (!is_valid_local_key(key) || !is_path_in_bucket(bucket, key)) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid key");
  }

  size_t len = key.size();
  if (key.back() == '/') {
    if (len > 1) {
      // should remove the last '/', otherwise the parent directory will be the
      // same as the key.
      len -= 1;
    } else {
      return Status(Errors::SE_INVALID, EINVAL,
                    std::string("invalid key:") + std::string(key));
    }
  }

  const std::string key_path_str = generate_path(bucket, key.substr(0, len));
  fs::path key_path = fs::path(key_path_str);

  int ret = rm_f(key_path.c_str());
  if (ret != 0) {
    return Status(Errors::SE_IO_ERROR, ret,
                      std::generic_category().message(ret));
  }
  return Status();
}

Status LocalObjectStore::delete_objects(const std::string_view &bucket,
                                        const std::vector<std::string_view> &keys) {
  for (const std::string_view &key : keys) {
    Status s = delete_object(bucket, key);
    if (!s.is_succ()) {
      return s;
    }
  }
  return Status();
}

Status LocalObjectStore::delete_directory(const std::string_view &bucket, 
                                          const std::string_view &prefix) {
  std::string dir_prefix(prefix);
  if (!dir_prefix.empty() && dir_prefix.back() != '/') {
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

std::string LocalObjectStore::generate_path(const std::string_view &bucket) {
  return std::string(basepath_) + "/" + std::string(bucket);
}

std::string LocalObjectStore::generate_path(const std::string_view &bucket,
                                            const std::string_view &key) {
  std::string key_buf(key);
  return std::string(basepath_) + "/" + std::string(bucket) + '/' +
         std::string(key_buf);
}

LocalObjectStore *create_local_objstore(const std::string_view region,
                                        const std::string_view *endpoint
                                        [[maybe_unused]],
                                        bool use_https [[maybe_unused]]) {
  int ret = mkdir_p(region);
  if (ret != 0) {
    return nullptr;
  }

  LocalObjectStore *lobs =
      new LocalObjectStore(region /* use region parameter as basepath */);

  if (lobs == nullptr) {
    rm_f(region);
  }

  return lobs;
}

LocalObjectStore *create_local_objstore(const std::string_view &access_key
                                        [[maybe_unused]],
                                        const std::string_view &secret_key
                                        [[maybe_unused]],
                                        const std::string_view region,
                                        const std::string_view *endpoint,
                                        bool use_https) {
  return create_local_objstore(region, endpoint, use_https);
}

void destroy_local_objstore(LocalObjectStore *local_objstore) {
  if (local_objstore) {
    delete local_objstore;
  }
  // keep the data there.
  return;
}

}  // namespace objstore
