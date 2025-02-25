//  Copyright (c) 2023, ApeCloud Inc Holding Limited.

#pragma once

#include "db/snapshot_impl.h"

namespace smartengine {
namespace db {

class SnapshotImplObjStore : public SnapshotImpl {
public:
  SnapshotImplObjStore();
  virtual ~SnapshotImplObjStore() override;

  virtual void destroy(util::autovector<storage::ExtentLayerVersion *> &recyle_extent_layer_versions) override;
};

}
}