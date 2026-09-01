/*
 * Copyright (c) 2024, ApeCloud Inc Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SMARTENGINE_INCLUDE_EXTENT_SPACE_OBJ_H_
#define SMARTENGINE_INCLUDE_EXTENT_SPACE_OBJ_H_

#include <map>
#include <unordered_set>

#include "storage/extent_space.h"

namespace smartengine {
namespace storage {

class ObjectExtentSpace : public ExtentSpace {
 public:
  ObjectExtentSpace(util::Env *env, const util::EnvOptions &env_options,
                    ::objstore::ObjectStore *objstore);
  virtual ~ObjectExtentSpace() override;

  void destroy() override;

  // ddl relative function
  int create(const CreateExtentSpaceArgs &arg) override;
  int remove() override;

  // extent relative function
  int allocate(const std::string prefix, ExtentIOInfo &io_info) override;
  int recycle(const std::string prefix, const ExtentId extent_id) override;
  // TODO(ljc): Change back to the original implementation. see https://github.com/wesql/wesql-server/pull/101/files
  // mark the extent used, only used during recovery
  int reference_if_need(const std::string prefix,
                        const ExtentId extent_id,
                        ExtentIOInfo &io_info,
                        bool& existed) override;

  // shrink relative function
  int get_shrink_info_if_need(const ShrinkCondition &shrink_condition,
                              bool &need_shrink,
                              ShrinkInfo &shrink_info) override;

  int move_extens_to_front(
      const int64_t move_extent_count,
      std::unordered_map<int64_t, ExtentIOInfo> &replace_map) override;
  int shrink(const int64_t shrink_extent_count) override;

  // statistic relative function
  bool is_free() override;
  int get_data_file_stats(
      std::vector<DataFileStatistics> &data_file_stats) override;

  // recovery relative function
  int add_data_file(DataFile *data_file) override;
  int rebuild() override;

 private:
  void update_last_alloc_ts() { last_alloc_ts_ = env_->NowMicros(); }

  int find_available_extent_offset(int32_t &extent_offset);

  std::string make_extent_key(const int32_t extent_offset_id) const;

  friend class ExtentSpaceTest_find_extent_offset_Test;

  static constexpr size_t EXTENT_OFFSET_LARGEST_VAL = INT32_MAX;

private:
  bool is_inited_;
  util::Env *env_;
  const util::EnvOptions &env_options_;
  ::objstore::ObjectStore *objstore_;
  std::string extent_bucket_;
  int64_t table_space_id_;
  int32_t extent_space_type_;
  int64_t total_extent_count_;
  int64_t used_extent_count_;
  // TODO(cnut): maintain the used extent count
  int64_t free_extent_count_;
  uint64_t last_alloc_ts_; // timestamp of last allocate extent
  std::set<int64_t> inused_extent_set_;
};

}  // namespace storage
}  // namespace smartengine

#endif
