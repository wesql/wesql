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

#ifndef MY_OBJSTORE_ALIYUN_OSS_H_INCLUDED
#define MY_OBJSTORE_ALIYUN_OSS_H_INCLUDED

#include <string>

#include <alibabacloud/oss/OssClient.h>
#include <alibabacloud/oss/ServiceRequest.h>
#include "objstore.h"

namespace objstore {

enum class AliyunOSSErrorCode {
  AccessDenied = 1,
  SymlinkTargetNotExist,
  InvalidTargetType,
  InvalidObjectState,
  InvalidArgument,
  InvalidObjectName,

  BucketAlreadyExists,
  NoSuchBucket,
  NoSuchKey,
  TooManyBuckets,
  FileAlreadyExists
};

class AliyunOssObjectStore : public ObjectStore {
 public:
  explicit AliyunOssObjectStore(const std::string_view region,
                                AlibabaCloud::OSS::OssClient &&oss_client)
      : region_(region), oss_client_(oss_client) {}
  virtual ~AliyunOssObjectStore() = default;

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

  std::string_view get_provider() const override { return provider_; }

  Status delete_objects(const std::string_view &bucket,
                        const std::vector<std::string_view> &object_keys) override;

 private:
  static constexpr std::string_view provider_{"aliyun"};
  static constexpr int kDeleteObjsNumEach = 1000;

  std::string region_;
  AlibabaCloud::OSS::OssClient oss_client_;
  static constexpr int LIST_MAX_KEYS = 1000;
};

void init_aliyun_api();

void shutdown_aliyun_api();

AliyunOssObjectStore *create_aliyun_oss_objstore(
    const std::string_view region, const std::string_view *endpoint,
    std::string &err_msg);

AliyunOssObjectStore *create_source_aliyun_oss_objstore(
    const std::string_view region, const std::string_view *endpoint,
    std::string &err_msg);

AliyunOssObjectStore *create_dest_aliyun_oss_objstore(
    const std::string_view region, const std::string_view *endpoint,
    std::string &err_msg);

void destroy_aliyun_oss_objstore(AliyunOssObjectStore *oss_obj_store);

}  // namespace objstore

#endif