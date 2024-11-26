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

#include "mysys/objstore/s3.h"
#include "aws/s3/S3Errors.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <errno.h>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>

namespace objstore {

namespace { // anonymous namespace

Errors aws_error_to_objstore_error(const Aws::S3::S3Error &aws_error) {
  if (aws_error.ShouldRetry()) {
    // if the error is retryable, and we have retried enough times, we treat it
    // as an unrecoverable error of cloud provider.
    return Errors::CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED;
  }
  if (aws_error.GetResponseCode() ==
      Aws::Http::HttpResponseCode::PRECONDITION_FAILED) {
    return Errors::SE_OBJECT_FORBID_OVERWRITE;
  }
  int aws_error_code = static_cast<int>(aws_error.GetErrorType());
  switch (aws_error_code) {
    case static_cast<int>(Aws::S3::S3Errors::ACCESS_DENIED):
      return Errors::SE_ACCESS_DENIED;
    case static_cast<int>(Aws::S3::S3Errors::BUCKET_ALREADY_EXISTS):
      return Errors::SE_BUCKET_ALREADY_EXISTS;
    case static_cast<int>(Aws::S3::S3Errors::BUCKET_ALREADY_OWNED_BY_YOU):
      return Errors::SE_BUCKET_ALREADY_OWNED_BY_YOU;
    case static_cast<int>(Aws::S3::S3Errors::INVALID_OBJECT_STATE):
      return Errors::SE_INVALID_OBJECT_STATE;
    case static_cast<int>(Aws::S3::S3Errors::NO_SUCH_BUCKET):
      return Errors::SE_NO_SUCH_BUCKET;
    case static_cast<int>(Aws::S3::S3Errors::NO_SUCH_KEY):
      return Errors::SE_NO_SUCH_KEY;
    case static_cast<int>(Aws::S3::S3Errors::NO_SUCH_UPLOAD):
      return Errors::SE_NO_SUCH_UPLOAD;
    case static_cast<int>(Aws::S3::S3Errors::OBJECT_ALREADY_IN_ACTIVE_TIER):
      return Errors::SE_OBJECT_ALREADY_IN_ACTIVE_TIER;
    case static_cast<int>(Aws::S3::S3Errors::OBJECT_NOT_IN_ACTIVE_TIER):
      return Errors::SE_OBJECT_NOT_IN_ACTIVE_TIER;
    case static_cast<int>(Aws::S3::S3Errors::UNKNOWN):
      return Errors::CLOUD_PROVIDER_UNRECOVERABLE_ERROR;
    default:
      // if the error is not-retryable and not a S3 specific error, it's an
      // error of cloud provider, we treat it as an unrecoverable error.
      return Errors::CLOUD_PROVIDER_UNRECOVERABLE_ERROR;
  }
}

}  // namespace

namespace fs = std::filesystem;

Status S3ObjectStore::create_bucket(const std::string_view &bucket) {
  Aws::S3::Model::CreateBucketRequest request;
  request.SetBucket(std::string(bucket));

  Aws::S3::Model::CreateBucketConfiguration createBucketConfig;
  createBucketConfig.SetLocationConstraint(
      Aws::S3::Model::BucketLocationConstraintMapper::
          GetBucketLocationConstraintForName(region_));
  request.SetCreateBucketConfiguration(createBucketConfig);

  Aws::S3::Model::CreateBucketOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.CreateBucket(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  return Status();
}

Status S3ObjectStore::delete_bucket(const std::string_view &bucket) {
  Aws::S3::Model::DeleteBucketRequest request;
  request.SetBucket(std::string(bucket));

  Aws::S3::Model::DeleteBucketOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.DeleteBucket(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  return Status();
}

Status S3ObjectStore::put_object_from_file(
    const std::string_view &bucket, const std::string_view &key,
    const std::string_view &data_file_path) {
  Aws::S3::Model::PutObjectRequest request;
  Aws::String full_key;
  if (bucket_dir_.empty()) {
    full_key = key;
  } else {
    full_key.append(bucket_dir_).append("/").append(key);
  }
  request.SetKey(full_key);
  request.SetBucket(Aws::String(bucket));

  std::shared_ptr<Aws::IOStream> input_data = Aws::MakeShared<Aws::FStream>(
      "IOStreamAllocationTag", data_file_path.data(),
      std::ios_base::in | std::ios_base::binary);

  if (!*input_data) {
    return Status(Errors::SE_IO_ERROR, 0, "Error unable to open input file");
  }

  request.SetBody(input_data);

  Aws::S3::Model::PutObjectOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.PutObject(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  return Status();
}

Status S3ObjectStore::get_object_to_file(
    const std::string_view &bucket, const std::string_view &key,
    const std::string_view &output_file_path) {
  std::string result;
  Status status = get_object(bucket, key, result);

  std::shared_ptr<Aws::IOStream> output_stream = Aws::MakeShared<Aws::FStream>(
      "IOStreamAllocationTag", output_file_path.data(),
      std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
  if (!*output_stream) {
    return Status(Errors::SE_IO_ERROR, 0, "Error unable to open output file");
  }

  bool fail =
      output_stream->write(result.c_str(), result.length()).flush().fail();
  if (fail) {
    return Status(Errors::SE_IO_ERROR, 0,
                  "unable to write key's value into file");
  }

  return Status();
}

Status S3ObjectStore::put_object(const std::string_view &bucket,
                                 const std::string_view &key,
                                 const std::string_view &data,
                                 bool forbid_overwrite) {
  if (!is_valid_key(key)) {
    return Status(Errors::SE_INVALID, EINVAL, "invalid key");
  }
  Aws::S3::Model::PutObjectRequest request;
  Aws::String full_key;
  if (bucket_dir_.empty()) {
    full_key = key;
  } else {
    full_key.append(bucket_dir_).append("/").append(key);
  }
  request.SetKey(full_key);
  request.SetBucket(Aws::String(bucket));
  if (forbid_overwrite) {
    request.SetAdditionalCustomHeaderValue("If-None-Match", "*");
  }

  const std::shared_ptr<Aws::IOStream> data_stream =
      Aws::MakeShared<Aws::StringStream>("SStreamAllocationTag");
  if (!*data_stream) {
    return Status(Errors::SE_IO_ERROR, 0,
                  "unable to create data stream to hold input data");
  }

  *data_stream << data;
  if (!*data_stream) {
    return Status(Errors::SE_IO_ERROR, 0,
                  "unable to write data into data stream");
  }

  request.SetBody(data_stream);

  Aws::S3::Model::PutObjectOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.PutObject(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  return Status();
}

Status S3ObjectStore::get_object(const std::string_view &bucket,
                                 const std::string_view &key,
                                 std::string &body) {
  Aws::S3::Model::GetObjectRequest request;
  Aws::String full_key;
  if (bucket_dir_.empty()) {
    full_key = key;
  } else {
    full_key.append(bucket_dir_).append("/").append(key);
  }
  request.SetKey(full_key);
  request.SetBucket(Aws::String(bucket));

  Aws::S3::Model::GetObjectOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.GetObject(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);

      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  std::ostringstream oss;
  if (outcome.GetResult().GetBody().rdbuf()->in_avail() > 0) {
    oss << outcome.GetResult().GetBody().rdbuf();
    if (!oss) {
      return Status(Errors::SE_IO_ERROR, 0,
                    "unable to read data from response stream");
    }
    body = oss.str();
  } else {
    body = "";
  }

  return Status();
}

Status S3ObjectStore::get_object(const std::string_view &bucket,
                                 const std::string_view &key, size_t off,
                                 size_t len, std::string &body) {
  Aws::S3::Model::GetObjectRequest request;
  Aws::String full_key;
  if (bucket_dir_.empty()) {
    full_key = key;
  } else {
    full_key.append(bucket_dir_).append("/").append(key);
  }
  request.SetKey(full_key);
  request.SetBucket(Aws::String(bucket));
  std::string byte_range =
      "bytes=" + std::to_string(off) + "-" + std::to_string(off + len - 1);
  request.SetRange(byte_range);

  Aws::S3::Model::GetObjectOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.GetObject(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  std::ostringstream oss;
  if (outcome.GetResult().GetBody().rdbuf()->in_avail() > 0) {
    oss << outcome.GetResult().GetBody().rdbuf();
    if (!oss) {
      return Status(Errors::SE_IO_ERROR, 0,
                    "unable to read data from response stream");
    }
    body = oss.str();
  } else {
    body = "";
  }
  return Status();
}

Status S3ObjectStore::get_object_meta(const std::string_view &bucket,
                                      const std::string_view &key,
                                      ObjectMeta &meta) {
  Aws::S3::Model::HeadObjectRequest request;
  Aws::String full_key;
  if (bucket_dir_.empty()) {
    full_key = key;
  } else {
    full_key.append(bucket_dir_).append("/").append(key);
  }
  request.SetKey(full_key);
  request.SetBucket(Aws::String(bucket));

  Aws::S3::Model::HeadObjectOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.HeadObject(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  meta.key = key;
  meta.last_modified = outcome.GetResult().GetLastModified().Millis();
  meta.size = outcome.GetResult().GetContentLength();

  return Status();
}

Status S3ObjectStore::list_object(const std::string_view &bucket,
                                  const std::string_view &prefix,
                                  bool recursive, std::string &start_after,
                                  bool &finished,
                                  std::vector<ObjectMeta> &objects) {
  Aws::S3::Model::ListObjectsV2Request request;
  Aws::String full_prefix;
  if (bucket_dir_.empty()) {
    full_prefix = prefix;
  } else {
    full_prefix.append(bucket_dir_).append("/").append(prefix);
  }
  request.SetBucket(Aws::String(bucket));
  request.SetPrefix(full_prefix);
  if (!start_after.empty()) {
    request.SetContinuationToken(Aws::String(start_after));
  }

  Aws::S3::Model::ListObjectsV2Outcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.ListObjectsV2(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }

      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }
  const Aws::Vector<Aws::S3::Model::Object> &s3_objects =
      outcome.GetResult().GetContents();

  for (const auto &obj : s3_objects) {
    // only list first-level sub keys
    if (!recursive && !is_first_level_sub_key(obj.GetKey(), full_prefix)) {
      continue;
    }
    ObjectMeta meta;
    meta.key = obj.GetKey();
    meta.last_modified = obj.GetLastModified().Millis();
    meta.size = obj.GetSize();
    objects.push_back(meta);
  }
  finished = !outcome.GetResult().GetIsTruncated();
  if (finished) {
    start_after = "";
  } else {
    if (!s3_objects.empty()) {
      start_after = outcome.GetResult().GetNextContinuationToken();
    } else {
      Errors err_type = Errors::CLOUD_PROVIDER_UNRECOVERABLE_ERROR;
      return Status(err_type, 0,
                    "list object returned empty objects but should not");
    }
  }

  return Status();
}

Status S3ObjectStore::delete_object(const std::string_view &bucket,
                                    const std::string_view &key) {
  Aws::S3::Model::DeleteObjectRequest request;
  Aws::String full_key;
  if (bucket_dir_.empty()) {
    full_key = key;
  } else {
    full_key.append(bucket_dir_).append("/").append(key);
  }
  request.SetKey(full_key);
  request.SetBucket(Aws::String(bucket));

  Aws::S3::Model::DeleteObjectOutcome outcome;

  int retry_times = retry_times_on_error_;
  while (true) {
    outcome = s3_client_.DeleteObject(request);
    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error &err = outcome.GetError();
      bool should_retry = err.ShouldRetry();
      if (retry_times-- > 0 && should_retry) {
        continue;
      }
      Errors err_type = aws_error_to_objstore_error(err);
      return Status(err_type, static_cast<int>(err.GetErrorType()),
                    err.GetMessage());
    } else {
      break;
    }
  }

  return Status();
}

Status S3ObjectStore::delete_objects(
    const std::string_view &bucket,
    const std::vector<std::string_view> &object_keys) {
  Aws::String full_key;
  Aws::String common_prefix;
  
  if (!bucket_dir_.empty()) {
    common_prefix = bucket_dir_;
    common_prefix.append("/");
  }

  Aws::Vector<Aws::S3::Model::ObjectIdentifier> object_identifiers;

  for (size_t i = 0; i < object_keys.size(); i++) {
    const std::string_view &object_key = object_keys[i];
    if (bucket_dir_.empty()) {
      full_key = object_key;
    } else {
      full_key = common_prefix;
      full_key.append(object_key);
    }
    object_identifiers.emplace_back(
        Aws::S3::Model::ObjectIdentifier().WithKey(full_key));
    if (object_identifiers.size() == kDeleteObjsNumEach || i == object_keys.size() - 1) {
      Aws::S3::Model::DeleteObjectsRequest request;
      Aws::S3::Model::Delete delete_object;
      delete_object.SetObjects(std::move(object_identifiers));
      request.SetDelete(std::move(delete_object));
      request.SetBucket(Aws::String(bucket));

      int retry_times = retry_times_on_error_;
      Aws::S3::Model::DeleteObjectsOutcome outcome;
      while (true) {
        outcome = s3_client_.DeleteObjects(request);
        if (!outcome.IsSuccess()) {
          const Aws::S3::S3Error &err = outcome.GetError();
          bool should_retry = err.ShouldRetry();
          if (retry_times-- > 0 && should_retry) {
            continue;
          }
          Errors err_type = aws_error_to_objstore_error(err);
          return Status(err_type, static_cast<int>(err.GetErrorType()),
                        err.GetMessage());
        } else {
          break;
        }
      }
      object_identifiers.clear();
    }    
  }
  
  return Status();
}

void init_aws_api() {
  Aws::SDKOptions options;
  Aws::InitAPI(options);
}

void shutdown_aws_api() {
  Aws::SDKOptions options;
  Aws::ShutdownAPI(options);
}

char *get_s3_access_key_id() {
  char *access_key_id = std::getenv("AWS_ACCESS_KEY_ID");
  if (access_key_id) {
    return access_key_id;
  }
  access_key_id = std::getenv("ACCESS_KEY_ID");
  if (access_key_id) {
    return access_key_id;
  }
  return nullptr;
}

char *get_s3_access_secret_key() {
  char *access_secret_key = std::getenv("AWS_SECRET_ACCESS_KEY");
  if (access_secret_key) {
    return access_secret_key;
  }
  access_secret_key = std::getenv("SECRET_ACCESS_KEY");
  if (access_secret_key) {
    return access_secret_key;
  }
  return nullptr;
}

S3ObjectStore *create_s3_objstore_helper(
    const std::string_view region, const std::string_view *endpoint,
    char *access_key_id, char *access_secret_key, bool use_https,
    const std::string_view bucket_dir, std::string &err_msg) {
  Aws::Client::ClientConfiguration clientConfig;
  clientConfig.region = region;
  if (endpoint != nullptr) {
    clientConfig.endpointOverride = *endpoint;
  }
  clientConfig.scheme =
      use_https ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;

  if (access_key_id && access_secret_key) {
    // if both access_key_id and access_secret_key are not empty, we use them to
    // create the client.
    Aws::String access_key_id_str(access_key_id);
    Aws::String access_secret_key_str(access_secret_key);
    Aws::Auth::AWSCredentials credentials(access_key_id_str,
                                          access_secret_key_str);
    Aws::S3::S3Client client(credentials, nullptr, clientConfig);
    return new S3ObjectStore(region, std::move(client), bucket_dir);
  } else if (access_key_id || access_secret_key) {
    // if one of the access_key_id and access_secret_key is empty, we treat it
    // as an invalid input.
    err_msg =
        "access key id and access secret key environment variables should be "
        "both set or both empty for s3";
    return nullptr;
  } else {
    // if both access_key_id and access_secret_key are empty, will find the
    // credentials by default credential provider chain for AWS. see
    // https://github.com/aws/aws-sdk-cpp/blob/main/docs/Credentials_Providers.md
    // for details
    Aws::S3::S3Client client(clientConfig);
    return new S3ObjectStore(region, std::move(client), bucket_dir);
  }
}

S3ObjectStore *create_s3_objstore(const std::string_view region,
                                  const std::string_view *endpoint,
                                  bool use_https, std::string &err_msg) {
  char *access_key_id = get_s3_access_key_id();
  char *access_secret_key = get_s3_access_secret_key();
  S3ObjectStore *s3_objstore =
      create_s3_objstore_helper(region, endpoint, access_key_id,
                                access_secret_key, use_https, "", err_msg);
  if (!s3_objstore) {
    err_msg = "failed to create s3 object store:" + err_msg;
  }
  return s3_objstore;
}

S3ObjectStore *create_source_s3_objstore(const std::string_view region,
                                         const std::string_view *endpoint,
                                         bool use_https, std::string &err_msg) {
  char *source_access_key_id = get_src_access_key_id();
  char *source_access_secret_key = get_src_access_secret_key();
  S3ObjectStore *s3_objstore = create_s3_objstore_helper(
      region, endpoint, source_access_key_id, source_access_secret_key,
      use_https, "", err_msg);
  if (!s3_objstore) {
    err_msg = "failed to create source s3 object store:" + err_msg;
  }
  return s3_objstore;
}

S3ObjectStore *create_dest_s3_objstore(const std::string_view region,
                                       const std::string_view *endpoint,
                                       bool use_https, std::string &err_msg) {
  char *dest_access_key_id = get_dest_access_key_id();
  char *dest_access_secret_key = get_dest_access_secret_key();
  S3ObjectStore *s3_objstore =
      create_s3_objstore_helper(region, endpoint, dest_access_key_id,
                                dest_access_secret_key, use_https, "", err_msg);
  if (!s3_objstore) {
    err_msg = "failed to create destination s3 object store:" + err_msg;
  }
  return s3_objstore;
}

S3ObjectStore *create_s3_objstore_for_test(const std::string_view region,
                                           const std::string_view *endpoint,
                                           bool use_https,
                                           const std::string_view bucket_dir,
                                           std::string &err_msg) {
  char *access_key_id = get_s3_access_key_id();
  char *access_secret_key = get_s3_access_secret_key();
  S3ObjectStore *s3_objstore = create_s3_objstore_helper(
      region, endpoint, access_key_id, access_secret_key, use_https, bucket_dir,
      err_msg);
  if (!s3_objstore) {
    err_msg = "failed to create s3 object store for test:" + err_msg;
  }
  return s3_objstore;
}

void destroy_s3_objstore(S3ObjectStore *s3_objstore) {
  if (s3_objstore) {
    delete s3_objstore;
  }
  return;
}

}  // namespace objstore
