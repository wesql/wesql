//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "db/db_impl.h"
#include "db/job_context.h"

namespace smartengine
{
using namespace common;
using namespace monitor;
using namespace util;

namespace db
{
Status DBImpl::DisableFileDeletions() {
  InstrumentedMutexLock l(&mutex_);
  ++disable_delete_obsolete_files_;
  if (disable_delete_obsolete_files_ == 1) {
    SE_LOG(INFO, "File Deletions Disabled");
  } else {
    __SE_LOG(SYSTEM, "File Deletions Disabled, but already disabled. Counter: %d", disable_delete_obsolete_files_);
  }
  return Status::OK();
}

Status DBImpl::EnableFileDeletions(bool force) {
  // Job id == 0 means that this is not our background process, but rather
  // user thread
  JobContext job_context(0);
  bool should_purge_files = false;
  {
    InstrumentedMutexLock l(&mutex_);
    if (force) {
      // if force, we need to enable file deletions right away
      disable_delete_obsolete_files_ = 0;
    } else if (disable_delete_obsolete_files_ > 0) {
      --disable_delete_obsolete_files_;
    }
    if (disable_delete_obsolete_files_ == 0) {
      SE_LOG(INFO, "File Deletions Enabled");
      should_purge_files = true;
      FindObsoleteFiles(&job_context, true);
    } else {
      __SE_LOG(SYSTEM, "File Deletions Enable, but not really enabled. Counter: %d", disable_delete_obsolete_files_);
    }
  }
  if (should_purge_files) {
    PurgeObsoleteFiles(job_context);
  }
  job_context.Clean();
  return Status::OK();
}

int DBImpl::IsFileDeletionsEnabled() const {
  return disable_delete_obsolete_files_;
}

}
}