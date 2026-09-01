//  Copyright (c) 2023, ApeCloud Inc Holding Limited.

#include "db/snapshot_impl_objstore.h"

namespace smartengine {
namespace db {

SnapshotImplObjStore::SnapshotImplObjStore()
{
}

SnapshotImplObjStore::~SnapshotImplObjStore()
{
}

void SnapshotImplObjStore::destroy(util::autovector<storage::ExtentLayerVersion *> &recyle_extent_layer_versions)
{
  for (int64_t level = 0; level < storage::MAX_TIER_COUNT; ++level) {
    if (extent_layer_versions_[level]->unref()) {
      //TODO(ljc): remove this and recyle extents 
      //   recyle_extent_layer_versions.push_back(extent_layer_versions_[level]);
      MOD_DELETE_OBJECT(ExtentLayerVersion, extent_layer_versions_[level]);
    }
  }

}

}
}