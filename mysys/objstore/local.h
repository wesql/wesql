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

#ifndef MY_OBJSTORE_LOCAL_H_INCLUDED
#define MY_OBJSTORE_LOCAL_H_INCLUDED

#include <mutex>

#include "objstore.h"

namespace objstore {

class LocalObjectStore : public ObjectStore {
 public:
  explicit LocalObjectStore(const std::string_view basepath)
      : basepath_(basepath) {}
  virtual ~LocalObjectStore() = default;

  Status create_bucket(const std::string_view &bucket) override;

  Status delete_bucket(const std::string_view &bucket) override;

  Status put_object_from_file(const std::string_view &bucket,
                              const std::string_view &key,
                              const std::string_view &data_file_path) override;
  Status get_object_to_file(const std::string_view &bucket,
                            const std::string_view &key,
                            const std::string_view &output_file_path) override;

  Status put_object(const std::string_view &bucket, const std::string_view &key,
                    const std::string_view &data,
                    bool forbid_overwrite = false) override;
  Status get_object(const std::string_view &bucket, const std::string_view &key,
                    std::string &input) override;
  Status get_object(const std::string_view &bucket, const std::string_view &key,
                    size_t off, size_t len, std::string &body) override;
  Status get_object_meta(const std::string_view &bucket,
                         const std::string_view &key,
                         ObjectMeta &meta) override;

  Status list_object(const std::string_view &bucket,
                     const std::string_view &prefix, bool recursive,
                     std::string &start_after, bool &finished,
                     std::vector<ObjectMeta> &objects) override;

  Status delete_object(const std::string_view &bucket,
                       const std::string_view &key) override;

  Status delete_objects(const std::string_view &bucket,
                        const std::vector<std::string_view> &keys) override;

  Status delete_directory(const std::string_view &bucket,
                          const std::string_view &prefix) override;

  std::string_view get_provider() const override { return provider_; }

 private:
  bool is_valid_local_bucket(std::string_view bucket);
  bool is_valid_local_key(std::string_view key);
  bool is_path_in_bucket(std::string_view bucket, std::string_view key);

  std::string generate_path(const std::string_view &bucket);
  std::string generate_path(const std::string_view &bucket,
                            const std::string_view &key);

 private:
  constexpr static std::string_view provider_{"local"};
  std::mutex mutex_;
  std::string basepath_;
};

LocalObjectStore *create_local_objstore(const std::string_view region,
                                        const std::string_view *endpoint,
                                        bool useHttps = true);

void destroy_local_objstore(LocalObjectStore *s3_obj_store);

}  // namespace objstore

#endif  // MY_OBJSTORE_LOCAL_H_INCLUDED
