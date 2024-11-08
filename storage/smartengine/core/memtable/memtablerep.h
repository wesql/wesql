/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file contains the interface that must be implemented by any collection
// to be used as the backing store for a MemTable. Such a collection must
// satisfy the following properties:
//  (1) It does not store duplicate items.
//  (2) It uses MemTableRep::KeyComparator to compare items for iteration and
//     equality.
//  (3) It can be accessed concurrently by multiple readers and can support
//     during reads. However, it needn't support multiple concurrent writes.
//  (4) Items are never deleted.
// The liberal use of assertions is encouraged to enforce (1).
//
// The factory will be passed an memtable::MemTableAllocator object when a new
// MemTableRep
// is requested.
//
// Users can implement their own memtable representations. We include three
// types built in:
//  - SkipListRep: This is the default; it is backed by a skip list.
//  - HashSkipListRep: The memtable rep that is best used for keys that are
//  structured like "prefix:suffix" where iteration within a prefix is
//  common and iteration across different prefixes is rare. It is backed by
//  a hash map where each bucket is a skip list.
//  - VectorRep: This is backed by an unordered std::vector. On iteration, the
// vector is sorted. It is intelligent about sorting; once the MarkReadOnly()
// has been called, the vector will only be sorted once. It is optimized for
// random-write-heavy workloads.
//
// The last four implementations are designed for situations in which
// iteration over the entire collection is rare since doing so requires all the
// keys to be copied into a sorted data structure.

#pragma once

#include <stdint.h>
#include <stdlib.h>

namespace smartengine {
namespace util {
class Arena;
class Logger;
}

namespace db {
class LookupKey;
}

namespace common {
class Slice;
}

namespace memtable {
class MemTableAllocator;

typedef void* KeyHandle;

class MemTableRep {
 public:
  // KeyComparator provides a means to compare keys, which are internal keys
  // concatenated with values.
  class KeyComparator {
   public:
    // Compare a and b. Return a negative value if a is less than b, 0 if they
    // are equal, and a positive value if a is greater than b
    virtual int operator()(const char* prefix_len_key1,
                           const char* prefix_len_key2) const = 0;

    virtual int operator()(const char* prefix_len_key,
                           const common::Slice& key) const = 0;
    KeyComparator() = default;
    KeyComparator(const KeyComparator &) = default;
    virtual ~KeyComparator() {}
  };

  explicit MemTableRep(memtable::MemTableAllocator* allocator)
      : allocator_(allocator) {}

  // Allocate a buf of len size for storing key. The idea is that a
  // specific memtable representation knows its underlying data structure
  // better. By allowing it to allocate memory, it can possibly put
  // correlated stuff in consecutive memory area to make processor
  // prefetching more efficient.
  virtual KeyHandle Allocate(const size_t len, char** buf);

  // Insert key into the collection. (The caller will pack key and value into a
  // single buffer and pass that in as the parameter to Insert).
  // REQUIRES: nothing that compares equal to key is currently in the
  // collection, and no concurrent modifications to the table in progress
  virtual void Insert(KeyHandle handle) = 0;

  // Same as Insert(), but in additional pass a hint to insert location for
  // the key. If hint points to nullptr, a new hint will be populated.
  // otherwise the hint will be updated to reflect the last insert location.
  //
  // Currently only skip-list based memtable implement the interface. Other
  // implementations will fallback to Insert() by default.
  virtual void InsertWithHint(KeyHandle handle, void** hint) {
    // Ignore the hint by default.
    Insert(handle);
  }

  // Like Insert(handle), but may be called concurrent with other calls
  // to InsertConcurrently for other handles
  virtual void InsertConcurrently(KeyHandle handle) = 0;

  // Returns true iff an entry that compares equal to key is in the collection.
  virtual bool Contains(const char* key) const = 0;

  // Notify this table rep that it will no longer be added to. By default,
  // does nothing.  After MarkReadOnly() is called, this table rep will
  // not be written to (ie No more calls to Allocate(), Insert(),
  // or any writes done directly to entries accessed through the iterator.)
  virtual void MarkReadOnly() {}

  // Look up key from the mem table, since the first key in the mem table whose
  // user_key matches the one given k, call the function callback_func(), with
  // callback_args directly forwarded as the first parameter, and the mem table
  // key as the second parameter. If the return value is false, then terminates.
  // Otherwise, go through the next key.
  //
  // It's safe for Get() to terminate after having finished all the potential
  // key for the k.user_key(), or not.
  //
  // Default:
  // Get() function with a default value of dynamically construct an iterator,
  // seek and call the call back function.
  virtual void Get(const db::LookupKey& k, void* callback_args,
                   bool (*callback_func)(void* arg, const char* entry));

  virtual uint64_t ApproximateNumEntries(const common::Slice& start_ikey,
                                         const common::Slice& end_key) {
    return 0;
  }

  // Report an approximation of how much memory has been used other than memory
  // that was allocated through the allocator.  Safe to call from any thread.
  virtual size_t ApproximateMemoryUsage() = 0;

  virtual ~MemTableRep() {}

  // Iteration over the contents of a skip collection
  class Iterator {
   public:
    // Initialize an iterator over the specified collection.
    // The returned iterator is not valid.
    // explicit Iterator(const MemTableRep* collection);
    virtual ~Iterator() {}

    // Returns true iff the iterator is positioned at a valid node.
    virtual bool Valid() const = 0;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    virtual const char* key() const = 0;

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual void Next() = 0;

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual void Prev() = 0;

    // Advance to the first entry with a key >= target
    virtual void Seek(const common::Slice& internal_key,
                      const char* memtable_key) = 0;

    // retreat to the first entry with a key <= target
    virtual void SeekForPrev(const common::Slice& internal_key,
                             const char* memtable_key) = 0;

    // Position at the first entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToFirst() = 0;

    // Position at the last entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    virtual void SeekToLast() = 0;
  };

  // Return an iterator over the keys in this representation.
  // arena: If not null, the arena needs to be used to allocate the Iterator.
  //        When destroying the iterator, the caller will not call "delete"
  //        but Iterator::~Iterator() directly. The destructor needs to destroy
  //        all the states but those allocated in arena.
  virtual Iterator* GetIterator(util::Arena* arena = nullptr) = 0;

  // Return an iterator that has a special Seek semantics. The result of
  // a Seek might only include keys with the same prefix as the target key.
  // arena: If not null, the arena is used to allocate the Iterator.
  //        When destroying the iterator, the caller will not call "delete"
  //        but Iterator::~Iterator() directly. The destructor needs to destroy
  //        all the states but those allocated in arena.
  virtual Iterator* GetDynamicPrefixIterator(util::Arena* arena = nullptr) {
    return GetIterator(arena);
  }

  // Return true if the current MemTableRep supports snapshot
  // Default: true
  virtual bool IsSnapshotSupported() const { return true; }

 protected:
  // When *key is an internal key concatenated with the value, returns the
  // user key.
  virtual common::Slice UserKey(const char* key) const;

  memtable::MemTableAllocator* allocator_;
};

// This is the base class for all factories that are used by RocksDB to create
// new MemTableRep objects
class MemTableRepFactory {
 public:
  virtual ~MemTableRepFactory() {}
  virtual MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator&,
                                         memtable::MemTableAllocator*) = 0;
  virtual const char* Name() const = 0;

  // Return true if the current MemTableRep supports concurrent inserts
  // Default: false
  virtual bool IsInsertConcurrentlySupported() const { return false; }
};

// This uses a skip list to store keys. It is the default.
//
// Parameters:
//   lookahead: If non-zero, each iterator's seek operation will start the
//     search from the previously visited record (doing at most 'lookahead'
//     steps). This is an optimization for the access pattern including many
//     seeks with consecutive keys.
class SkipListFactory : public MemTableRepFactory {
 public:
  explicit SkipListFactory() {}

  virtual MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator&,
                                         memtable::MemTableAllocator*) override;
  virtual const char* Name() const override { return "SkipListFactory"; }

  bool IsInsertConcurrentlySupported() const override { return true; }
};

class ARTFactory : public MemTableRepFactory {
 public:
  explicit ARTFactory() {}

  virtual MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator&,
                                         memtable::MemTableAllocator*) override;
  virtual const char* Name() const override { return "ARTFactory"; }

  bool IsInsertConcurrentlySupported() const override { return true; }
};

}  // namespace memtable
}  // namespace smartengine
