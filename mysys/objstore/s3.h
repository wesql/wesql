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

#ifndef MY_OBJSTORE_S3_H_INCLUDED
#define MY_OBJSTORE_S3_H_INCLUDED

#include <string>

#include <aws/s3/S3Client.h>

#include "objstore.h"

namespace objstore {

class S3ObjectStore : public ObjectStore {
 public:
  explicit S3ObjectStore(const std::string_view region,
                         Aws::S3::S3Client &&s3_client)
      : region_(region), s3_client_(s3_client) {}
  virtual ~S3ObjectStore() = default;

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

  // for aws s3, the list object interface will return at most 1000 objects,
  // if there are more objects, the `finished` parameter will be set to false,
  // and the caller should call this function again with the last returned
  // object's key as the `start_after` parameter.

  // NOTICE:
  // 1. by default, aws s3 will return the objects in lexicographical order.
  // 2. the `start_after` key is not included in the `objects` returned.

  // param[in] bucket: the bucket name
  // param[in] prefix: the prefix of the object key
  // param[in] recursive: list all sub keys or only the first-level sub keys
  // param[in & out] start_after: the object key to start after, and return the
  // object key for the next time to start after if there are more objects.
  // param[out] finished: whether the list is finished
  // param[out] objects: the object list returned.
  Status list_object(const std::string_view &bucket,
                     const std::string_view &prefix, bool recursive,
                     std::string &start_after, bool &finished,
                     std::vector<ObjectMeta> &objects) override;

  Status delete_object(const std::string_view &bucket,
                       const std::string_view &key) override;

  Status delete_objects(
      const std::string_view &bucket,
      const std::vector<std::string_view> &object_keys) override;

  std::string_view get_provider() const override { return provider_; }

 private:
  static constexpr std::string_view provider_{"aws"};
  static constexpr int kDeleteObjsNumEach = 1000;
  std::string region_;
  Aws::S3::S3Client s3_client_;
  // TODO(ljc): may add an configuration setting
  int retry_times_on_error_ = 10;
};

std::string remove_prefix(const std::string &str, const std::string &prefix);

void init_aws_api();

void shutdown_aws_api();

S3ObjectStore *create_s3_objstore(const std::string_view region,
                                  const std::string_view *endpoint,
                                  bool useHttps, std::string &err_msg);

S3ObjectStore *create_source_s3_objstore(const std::string_view region,
                                         const std::string_view *endpoint,
                                         bool use_https, std::string &err_msg);

S3ObjectStore *create_dest_s3_objstore(const std::string_view region,
                                       const std::string_view *endpoint,
                                       bool use_https, std::string &err_msg);

void destroy_s3_objstore(S3ObjectStore *s3_obj_store);

}  // namespace objstore

#endif  // MY_OBJSTORE_S3_H_INCLUDED
