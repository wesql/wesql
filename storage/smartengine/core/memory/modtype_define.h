/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
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
//
//#ifndef UTIL_MODTYPE_DEFINE_H_
//#define UTIL_MODTYPE_DEFINE_H_
//
//
//
//#endif /* UTIL_MODTYPE_DEFINE_H_ */

#ifdef DEFINE_MOD_TYPE
DEFINE_MOD_TYPE(kDefaultMod)
DEFINE_MOD_TYPE(kCache)
DEFINE_MOD_TYPE(kPersistentCache)
DEFINE_MOD_TYPE(kZlibCache)
DEFINE_MOD_TYPE(kBZip2Cache)
DEFINE_MOD_TYPE(kBZSTDCache)
DEFINE_MOD_TYPE(kLZ4Cache)
DEFINE_MOD_TYPE(kTableCache)
DEFINE_MOD_TYPE(kRowCache)
DEFINE_MOD_TYPE(kMemtable)
DEFINE_MOD_TYPE(kCompaction)
DEFINE_MOD_TYPE(kFlushBuffer)
DEFINE_MOD_TYPE(kObjectPool)
DEFINE_MOD_TYPE(kDbIter)
DEFINE_MOD_TYPE(kWritableBuffer)
DEFINE_MOD_TYPE(kAIOBuffer)
DEFINE_MOD_TYPE(kLruCache)
DEFINE_MOD_TYPE(kJemallocDump)
DEFINE_MOD_TYPE(kTestMod)
DEFINE_MOD_TYPE(kFilterBlockCache)
DEFINE_MOD_TYPE(kIndexBlockCache)
DEFINE_MOD_TYPE(kDataBlockCache)
DEFINE_MOD_TYPE(kDefaultBlockCache)
DEFINE_MOD_TYPE(kQueryTrace)
DEFINE_MOD_TYPE(kExtentSpaceMgr)
DEFINE_MOD_TYPE(kStorageMgr)
DEFINE_MOD_TYPE(kStorageLogger)
DEFINE_MOD_TYPE(kInformationSchema)
DEFINE_MOD_TYPE(kLargeObject)
DEFINE_MOD_TYPE(kClockCache)
DEFINE_MOD_TYPE(kAllSubTable)
DEFINE_MOD_TYPE(kAutoVector)
DEFINE_MOD_TYPE(kArenaVector)
DEFINE_MOD_TYPE(kBackupCheck)
DEFINE_MOD_TYPE(kRep)
DEFINE_MOD_TYPE(kVersionSet)
DEFINE_MOD_TYPE(kColumnFamilySet)
DEFINE_MOD_TYPE(kSubTable)
DEFINE_MOD_TYPE(kObjectStore)
DEFINE_MOD_TYPE(kFlush)
DEFINE_MOD_TYPE(kSuperVersion)
DEFINE_MOD_TYPE(kWrapAllocator)
DEFINE_MOD_TYPE(kSnapshotImpl)
DEFINE_MOD_TYPE(kDBImpl)
DEFINE_MOD_TYPE(kWriteRequest)
DEFINE_MOD_TYPE(kWriteBatch)
DEFINE_MOD_TYPE(kRecoveredTransaction)
DEFINE_MOD_TYPE(kRecovery)
DEFINE_MOD_TYPE(kEnv)
DEFINE_MOD_TYPE(kLookupKey)
DEFINE_MOD_TYPE(kTransactionLockMgr)
DEFINE_MOD_TYPE(kCacheHashTable)
DEFINE_MOD_TYPE(kDDLSort)
DEFINE_MOD_TYPE(kParallelRead)
DEFINE_MOD_TYPE(kShrinkJob)
DEFINE_MOD_TYPE(kMetaDescriptor)
DEFINE_MOD_TYPE(kBlock)
DEFINE_MOD_TYPE(kRowBlockWriter)
DEFINE_MOD_TYPE(kIndexBlockWriter)
DEFINE_MOD_TYPE(kExtentWriter)
DEFINE_MOD_TYPE(kIndexBlockReader)
DEFINE_MOD_TYPE(kExtentReader)
DEFINE_MOD_TYPE(kCompressor)
DEFINE_MOD_TYPE(kColumn)
DEFINE_MOD_TYPE(kColumnUnitWriter)
DEFINE_MOD_TYPE(kColumnUnitReader)
DEFINE_MOD_TYPE(kColumnBlockWriter)
DEFINE_MOD_TYPE(kColumnBlockReader)
DEFINE_MOD_TYPE(kBloomFilter)
DEFINE_MOD_TYPE(kIOExtent)
DEFINE_MOD_TYPE(kLogWriter)

DEFINE_MOD_TYPE(kMaxMod)
#endif
