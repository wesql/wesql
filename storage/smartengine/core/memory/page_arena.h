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

#ifndef IS_UTIL_PAGE_ARENA_H_
#define IS_UTIL_PAGE_ARENA_H_

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "allocator.h"
#include "base_malloc.h"
#include "mod_info.h"
#include "util/macro_utils.h"

namespace smartengine {
namespace memory {

// convenient function for memory alignment
inline size_t get_align_offset(void *p) {
  size_t size = 0;
  if (sizeof(void *) == 8) {
    size = 8 - (((uint64_t)p) & 0x7);
  } else {
    size = 4 - (((uint64_t)p) & 0x3);
  }
  return size;
}

struct DefaultPageAllocator : public SimpleAllocator {
  DefaultPageAllocator(size_t mod_id = ModId::kDefaultMod) : mod_id_(mod_id) {}
  DefaultPageAllocator(const DefaultPageAllocator &) = default;
  virtual ~DefaultPageAllocator() override {}
  virtual void *alloc(const int64_t sz) override {
    return base_malloc(sz, mod_id_);
  }
  void free(void *p) override { base_free(p); }
  int64_t size() const override { return 0; }
  void set_mod_id(const size_t mod_id) override { mod_id_ = mod_id; }
  size_t mod_id_;
};

struct WrapAllocator: public SimpleAllocator
{
  explicit WrapAllocator(const size_t mod_id = ModId::kWrapAllocator)
      : mod_id_(mod_id),
        allocator_(nullptr) {}
  explicit WrapAllocator(SimpleAllocator &allocator) : allocator_(&allocator) {}
  WrapAllocator(const WrapAllocator &) = default;
  virtual ~WrapAllocator() override {}
  void set_mod_id(const size_t mod_id) override { mod_id_ = mod_id; }
  void *alloc(const int64_t sz) override {
    return (nullptr == allocator_) ? base_malloc(sz, mod_id_) : allocator_->alloc(sz);
  }
  void free(void *p) override {
    (nullptr == allocator_) ? base_free(p) : allocator_->free(p);
  }
  int64_t size() const override {
    return (nullptr == allocator_) ? 0 : allocator_->size();
  }
  private:
  int32_t mod_id_;
  SimpleAllocator *allocator_;
};

/**
 * A simple/fast allocator to avoid individual deletes/frees
 * Good for usage patterns that just:
 * load, use and free the entire container repeatedly.
 */
template <typename CharT = char, class PageAllocatorT = DefaultPageAllocator>
class PageArena {
 private:  // types
  typedef PageArena<CharT, PageAllocatorT> Self;

  struct Page {
    int64_t magic_;
    Page *next_page_;
    char *alloc_end_;
    const char *page_end_;
    char buf_[0];

    Page(const char *end)
        : magic_(0x1234abcddbca4321), next_page_(0), page_end_(end) {
      alloc_end_ = buf_;
    }

    inline int64_t remain() const { return page_end_ - alloc_end_; }
    inline int64_t used() const { return alloc_end_ - buf_; }
    inline int64_t raw_size() const { return page_end_ - buf_ + sizeof(Page); }
    inline int64_t reuse_size() const { return page_end_ - buf_; }

    inline CharT *alloc(int64_t sz) {
      CharT *ret = nullptr;
      if (sz <= remain()) {
        char *start = alloc_end_;
        alloc_end_ += sz;
        ret = (CharT *)start;
      }
      return ret;
    }

    inline CharT *alloc_down(int64_t sz) {
      assert(sz <= remain());
      page_end_ -= sz;
      return (CharT *)page_end_;
    }

    inline void reuse() { alloc_end_ = buf_; }
  };

 public:
  static const int64_t DEFAULT_NORMAL_BLOCK_SIZE = 64 * 1024;
  static const int64_t DEFAULT_PAGE_SIZE =
      DEFAULT_NORMAL_BLOCK_SIZE - sizeof(Page);  // default 64KB

 private:  // data
  Page *cur_page_;
  Page *header_;
  Page *tailer_;
  int64_t page_limit_;  // capacity in bytes of an empty page
  int64_t page_size_;   // page size in number of bytes
  int64_t pages_;       // number of pages allocated
  int64_t used_;        // total number of bytes allocated by users
  int64_t total_;       // total number of bytes occupied by pages
  PageAllocatorT page_allocator_;

 private:  // helpers
  Page *insert_head(Page *page) {
    if (nullptr != header_) {
      page->next_page_ = header_;
    }
    header_ = page;

    return page;
  }

  Page *insert_tail(Page *page) {
    if (nullptr != tailer_) {
      tailer_->next_page_ = page;
    }
    tailer_ = page;

    return page;
  }

  Page *alloc_new_page(const int64_t sz) {
    Page *page = nullptr;
    void *ptr = page_allocator_.alloc(sz);

    if (nullptr != ptr) {
      page = new (ptr) Page((char *)ptr + sz);

      total_ += sz;
      ++pages_;
    } else {
      fprintf(stderr, "cannot allocate memory.sz=%ld, pages_=%ld,total_=%ld",
              sz, pages_, total_);
    }

    return page;
  }

  Page *extend_page(const int64_t sz) {
    Page *page = cur_page_;
    if (nullptr != page) {
      page = page->next_page_;
      if (nullptr != page) {
        page->reuse();
      } else {
        page = alloc_new_page(sz);
        if (nullptr == page) {
          fprintf(stderr, "extend_page sz =%ld cannot alloc new page", sz);
        } else {
          insert_tail(page);
        }
      }
    }
    return page;
  }

  inline bool lookup_next_page(const int64_t sz) {
    bool ret = false;
    if (nullptr != cur_page_ && nullptr != cur_page_->next_page_ &&
        cur_page_->next_page_->reuse_size() >= sz) {
      cur_page_->next_page_->reuse();
      cur_page_ = cur_page_->next_page_;
      ret = true;
    }
    return ret;
  }

  inline bool ensure_cur_page() {
    if (nullptr == cur_page_) {
      header_ = cur_page_ = tailer_ = alloc_new_page(page_size_);
      if (nullptr != cur_page_) page_limit_ = cur_page_->remain();
    }

    return (nullptr != cur_page_);
  }

  inline bool is_normal_overflow(const int64_t sz) { return sz <= page_limit_; }

  inline bool is_large_page(Page *page) {
    return nullptr == page ? false : page->raw_size() > page_size_;
  }

  CharT *alloc_big(const int64_t sz) {
    CharT *ptr = nullptr;
    // big enough object to have their own page
    Page *p = alloc_new_page(sz + sizeof(Page));
    if (nullptr != p) {
      insert_head(p);
      ptr = p->alloc(sz);
    }
    return ptr;
  }

  void free_large_pages() {
    Page **current = &header_;
    while (nullptr != *current) {
      Page *entry = *current;
      if (is_large_page(entry)) {
        *current = entry->next_page_;
        pages_ -= 1;
        total_ -= entry->raw_size();
        page_allocator_.free(entry);
      } else {
        tailer_ = *current;
        current = &entry->next_page_;
      }
    }
    if (nullptr == header_) {
      tailer_ = nullptr;
    }
  }

  Self &assign(Self &rhs) {
    if (this != &rhs) {
      free();

      header_ = rhs.header_;
      cur_page_ = rhs.cur_page_;
      tailer_ = rhs.tailer_;

      pages_ = rhs.pages_;
      used_ = rhs.used_;
      total_ = rhs.total_;
      page_size_ = rhs.page_size_;
      page_limit_ = rhs.page_limit_;
      page_allocator_ = rhs.page_allocator_;
    }
    return *this;
  }

 public:  // API
  /** constructor */
  PageArena(const int64_t psz = DEFAULT_PAGE_SIZE,
            const size_t mod_id = ModId::kDefaultMod,
            const PageAllocatorT &allocator = PageAllocatorT())
      : cur_page_(nullptr),
        header_(nullptr),
        tailer_(nullptr),
        page_limit_(0),
        page_size_(psz),
        pages_(0),
        used_(0),
        total_(0),
        page_allocator_(allocator) {
    assert(psz > (int64_t)sizeof(Page));
    page_allocator_.set_mod_id(mod_id);
  }
  ~PageArena() { free(); }
  void set_mod_id(const size_t mod_id) { page_allocator_.set_mod_id(mod_id); }

  Self &join(Self &rhs) {
    if (this != &rhs && rhs.used_ == 0) {
      if (nullptr == header_) {
        assign(rhs);
      } else if (nullptr != rhs.header_) {
        tailer_->next_page_ = rhs.header_;
        tailer_ = rhs.tailer_;

        pages_ += rhs.pages_;
        total_ += rhs.total_;
      }
      rhs.reset();
    }
    return *this;
  }

  int64_t page_size() const { return page_size_; }

  /** allocate sz bytes */
  CharT *alloc(const int64_t sz) {
    ensure_cur_page();

    // common case
    CharT *ret = nullptr;
    if (nullptr != cur_page_) {
      if (sz <= cur_page_->remain()) {
        ret = cur_page_->alloc(sz);
      } else if (is_normal_overflow(sz)) {
        Page *new_page = extend_page(page_size_);
        if (nullptr != new_page) {
          cur_page_ = new_page;
        }
        if (nullptr != cur_page_) {
          ret = cur_page_->alloc(sz);
        }
      } else if (lookup_next_page(sz)) {
        ret = cur_page_->alloc(sz);
      } else {
        ret = alloc_big(sz);
      }

      if (nullptr != ret) {
        used_ += sz;
      }
    }
    return ret;
  }

  template <class T>
  T *new_object() {
    T *ret = nullptr;
    void *tmp = (void *)alloc_aligned(sizeof(T));
    if (nullptr == tmp) {
      fprintf(stderr, "fail to alloc mem for T");
    } else {
      ret = new (tmp) T();
    }
    return ret;
  }

  /** allocate sz bytes */
  CharT *alloc_aligned(const int64_t sz) {
    ensure_cur_page();

    // common case
    CharT *ret = nullptr;
    if (nullptr != cur_page_) {
      int64_t align_offset = get_align_offset(cur_page_->alloc_end_);
      int64_t adjusted_sz = sz + align_offset;

      if (adjusted_sz <= cur_page_->remain()) {
        used_ += align_offset;
        ret = cur_page_->alloc(adjusted_sz) + align_offset;
      } else if (is_normal_overflow(sz)) {
        Page *new_page = extend_page(page_size_);
        if (nullptr != new_page) {
          cur_page_ = new_page;
        }
        if (nullptr != cur_page_) {
          ret = cur_page_->alloc(sz);
        }
      } else if (lookup_next_page(sz)) {
        ret = cur_page_->alloc(sz);
      } else {
        ret = alloc_big(sz);
      }

      if (nullptr != ret) {
        used_ += sz;
      }
    }
    return ret;
  }

  /**
   * allocate from the end of the page.
   * - allow better packing/space saving for certain scenarios
   */
  CharT *alloc_down(const int64_t sz) {
    used_ += sz;
    ensure_cur_page();

    // common case
    CharT *ret = nullptr;
    if (nullptr != cur_page_) {
      if (sz <= cur_page_->remain()) {
        ret = cur_page_->alloc_down(sz);
      } else if (is_normal_overflow(sz)) {
        Page *new_page = extend_page(page_size_);
        if (nullptr != new_page) {
          cur_page_ = new_page;
        }
        if (nullptr != cur_page_) {
          ret = cur_page_->alloc_down(sz);
        }
      } else if (lookup_next_page(sz)) {
        ret = cur_page_->alloc_down(sz);
      } else {
        ret = alloc_big(sz);
      }
    }
    return ret;
  }

  /** realloc for newsz bytes */
  CharT *realloc(CharT *p, const int64_t oldsz, const int64_t newsz) {
    assert(cur_page_);
    CharT *ret = p;
    // if we're the last one on the current page with enough space
    if (p + oldsz == cur_page_->alloc_end_ &&
        p + newsz < cur_page_->page_end_) {
      cur_page_->alloc_end_ = (char *)p + newsz;
      ret = p;
    } else {
      ret = alloc(newsz);
      if (nullptr != ret) ::memcpy(ret, p, newsz > oldsz ? oldsz : newsz);
    }
    return ret;
  }

  /** duplicate a null terminated string s */
  CharT *dup(const char *s) {
    if (nullptr == s) return nullptr;

    int64_t len = strlen(s) + 1;
    CharT *copy = alloc(len);
    if (nullptr != copy) memcpy(copy, s, len);
    return copy;
  }

  /** duplicate a buffer of size len */
  CharT *dup(const void *s, const int64_t len) {
    CharT *copy = nullptr;
    if (nullptr != s && len > 0) {
      copy = alloc(len);
      if (nullptr != copy) memcpy(copy, s, len);
    }

    return copy;
  }

  /** free the whole arena */
  void free() {
    Page *page = nullptr;

    while (nullptr != header_) {
      page = header_;
      header_ = header_->next_page_;
      page_allocator_.free(page);
    }

    cur_page_ = nullptr;
    tailer_ = nullptr;
    used_ = 0;
    pages_ = 0;
    total_ = 0;
  }

  /**
   * free some of pages. remain memory can be reuse.
   *
   * @param sleep_pages force sleep when pages are freed every time.
   * @param sleep_interval_us sleep interval in microseconds.
   * @param remain_size keep size of memory pages less than %remain_size
   *
   */
  void partial_slow_free(const int64_t sleep_pages,
                         const int64_t sleep_interval_us,
                         const int64_t remain_size = 0) {
    Page *page = nullptr;

    int64_t current_sleep_pages = 0;

    while (nullptr != header_ && (remain_size == 0 || total_ > remain_size)) {
      page = header_;
      header_ = header_->next_page_;

      total_ -= page->raw_size();

      page_allocator_.free(page);

      ++current_sleep_pages;
      --pages_;

      if (sleep_pages > 0 && current_sleep_pages >= sleep_pages) {
        ::usleep(static_cast<useconds_t>(sleep_interval_us));
        current_sleep_pages = 0;
      }
    }

    // reset allocate start point, important.
    // once slow_free called, all memory allocated before
    // CANNOT use anymore.
    cur_page_ = header_;
    if (nullptr == header_) tailer_ = nullptr;
    used_ = 0;
  }

  void free(CharT *ptr) { UNUSED(ptr); }

  void fast_reuse() {
    used_ = 0;
    cur_page_ = header_;
    if (nullptr != cur_page_) {
      cur_page_->reuse();
    }
  }

  void reuse() {
    free_large_pages();
    fast_reuse();
  }

  void dump() const {
    Page *page = header_;
    int64_t count = 0;
    while (nullptr != page) {
      fprintf(stderr,
              "DUMP PAGEARENA page[%ld]:rawsize[%ld],used[%ld],remain[%ld]",
              count++, page->raw_size(), page->used(), page->remain());
      page = page->next_page_;
    }
  }

  /** stats accessors */
  int64_t pages() const { return pages_; }
  int64_t used() const { return used_; }
  int64_t size() const { return total_; }

 private:
  PageArena(const PageArena &) = delete;
  PageArena(PageArena &&) = delete;
  PageArena &operator=(const PageArena &) = delete;
};

typedef PageArena<> CharArena;

class ArenaAllocator : public SimpleAllocator {
 public:
  ArenaAllocator(int64_t page_size = CharArena::DEFAULT_PAGE_SIZE,
                 const size_t mod_id = ModId::kDefaultMod)
      : arena_(page_size, mod_id){}
  virtual ~ArenaAllocator() override {}

 public:
  virtual void *alloc(const int64_t sz) override {
    return arena_.alloc_aligned(sz);
  }
  virtual void free(void *ptr) override {
    arena_.free(reinterpret_cast<char *>(ptr));
  }
  virtual int64_t size() const override { return arena_.size(); }
  virtual void set_mod_id(const size_t mod_id) override {
    arena_.set_mod_id(mod_id);
  }
  void reuse() { arena_.reuse(); }
  void reuse_one_page() { arena_.fast_reuse(); }
  void clear() { arena_.free(); }
  CharArena &get_arena() { return arena_; }

 private:
  CharArena arena_;
};

}  // namespace memory
}  // namespace smartengine
#endif  // end if IS_UTIL_PAGE_ARENA_H_
