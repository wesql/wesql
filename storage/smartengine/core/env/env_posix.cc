//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#if defined(OS_LINUX)
#include <linux/fs.h>
#endif
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#ifdef OS_LINUX
#include <sys/statfs.h>
#include <sys/syscall.h>
#endif
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <algorithm>
// Get nano time includes
#if defined(OS_LINUX) || defined(OS_FREEBSD)
#elif defined(__MACH__)
#include <mach/clock.h>
#include <mach/mach.h>
#else
#include <chrono>
#endif
#include <set>
#include <vector>

#include "env/io_posix.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_updater.h"
#include "objstore.h"
#include "options/options.h"
#include "util/random.h"
#include "util/se_constants.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/thread_local.h"
#include "util/threadpool_imp.h"

#if !defined(TMPFS_MAGIC)
#define TMPFS_MAGIC 0x01021994
#endif
#if !defined(XFS_SUPER_MAGIC)
#define XFS_SUPER_MAGIC 0x58465342
#endif
#if !defined(EXT4_SUPER_MAGIC)
#define EXT4_SUPER_MAGIC 0xEF53
#endif

namespace smartengine {
using namespace common;

namespace util {
namespace {

ThreadStatusUpdater* CreateThreadStatusUpdater() {
  return new ThreadStatusUpdater();
}

// list of pathnames that are locked
static std::set<std::string> lockedFiles;
static port::Mutex mutex_lockedFiles;

static int LockOrUnlock(const std::string& fname, int fd, bool lock) {
  mutex_lockedFiles.Lock();
  if (lock) {
    // If it already exists in the lockedFiles set, then it is already locked,
    // and fail this lock attempt. Otherwise, insert it into lockedFiles.
    // This check is needed because fcntl() does not detect lock conflict
    // if the fcntl is issued by the same thread that earlier acquired
    // this lock.
    if (lockedFiles.insert(fname).second == false) {
      mutex_lockedFiles.Unlock();
      errno = ENOLCK;
      return -1;
    }
  } else {
    // If we are unlocking, then verify that we had locked it earlier,
    // it should already exist in lockedFiles. Remove it from lockedFiles.
    if (lockedFiles.erase(fname) != 1) {
      mutex_lockedFiles.Unlock();
      errno = ENOLCK;
      return -1;
    }
  }
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;  // Lock/unlock entire file
  int value = fcntl(fd, F_SETLK, &f);
  if (value == -1 && lock) {
    // if there is an error in locking, then remove the pathname from
    // lockedfiles
    lockedFiles.erase(fname);
  }
  mutex_lockedFiles.Unlock();
  return value;
}

class PosixFileLock : public FileLock
{
 public:
  int fd_;
  std::string filename;
};

class PosixEnv : public Env
{
 public:
  PosixEnv();
  virtual ~PosixEnv() override
  {
    for (const auto tid : threads_to_join_) {
      pthread_join(tid, nullptr);
    }
    for (int pool_id = 0; pool_id < Env::Priority::TOTAL; ++pool_id) {
      thread_pools_[pool_id].JoinAllThreads();
    }
    // Delete the thread_status_updater_ only when the current Env is not
    // Env::Default().  This is to avoid the free-after-use error when
    // Env::Default() is destructed while some other child threads are
    // still trying to update thread status.
    if (this != Env::Default()) {
      delete thread_status_updater_;
    }
  }

  void SetFD_CLOEXEC(int fd) {
    if (fd > 0) {
      fcntl(fd, F_SETFD, fcntl(fd, F_GETFD) | FD_CLOEXEC);
    }
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile *&result,
                                   const EnvOptions& options) override {
    int fd = -1;
    int flags = O_RDONLY;
    FILE* file = nullptr;

    if (options.use_direct_reads) {
#if !defined(OS_MACOSX) && !defined(OS_OPENBSD)
      flags |= O_DIRECT;
#endif
    }

    do {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(fname.c_str(), flags, 0644);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
      return IOError(fname, errno);
    }

    SetFD_CLOEXEC(fd);

    if (options.use_direct_reads) {
#ifdef OS_MACOSX
      if (fcntl(fd, F_NOCACHE, 1) == -1) {
        close(fd);
        return IOError(fname, errno);
      }
#endif
    } else {
      do {
        IOSTATS_TIMER_GUARD(open_nanos);
        file = fdopen(fd, "r");
      } while (file == nullptr && errno == EINTR);
      if (file == nullptr) {
        close(fd);
        return IOError(fname, errno);
      }
    }
    if (nullptr != options.arena) {
      result = ALLOC_OBJECT(PosixSequentialFile, *options.arena, fname, file, fd, options);
    } else {
      result = MOD_NEW_OBJECT(memory::ModId::kEnv, PosixSequentialFile, fname, file, fd, options);
    }
    return Status::OK();
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile *&result,
                                     const EnvOptions& options) override {
//    result->reset();
    Status s;
    int fd;
    int flags = O_RDONLY;
    if (options.use_direct_reads) {
#if !defined(OS_MACOSX) && !defined(OS_OPENBSD)
      flags |= O_DIRECT;
      TEST_SYNC_POINT_CALLBACK("NewRandomAccessFile:O_DIRECT", &flags);
#endif
    }

    do {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(fname.c_str(), flags, 0644);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
      return IOError(fname, errno);
    }
    SetFD_CLOEXEC(fd);

    if (options.use_direct_reads) {
#ifdef OS_MACOSX
      if (fcntl(fd, F_NOCACHE, 1) == -1) {
        close(fd);
        return IOError(fname, errno);
      }
#endif
    }

    if (nullptr != options.arena) {
      result = ALLOC_OBJECT(PosixRandomAccessFile, *options.arena, fname, fd, options);
    } else {
      result = MOD_NEW_OBJECT(memory::ModId::kEnv, PosixRandomAccessFile, fname, fd, options);
    }

    return s;
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile *&result,
                                 const EnvOptions& options) override {
    Status s;
    int fd = -1;
    int flags = O_CREAT | O_TRUNC;
    // Direct IO mode with O_DIRECT flag or F_NOCAHCE (MAC OSX)
    if (options.use_direct_writes) {
// Note: we should avoid O_APPEND here due to ta the following bug:
// POSIX requires that opening a file with the O_APPEND flag should
// have no affect on the location at which pwrite() writes data.
// However, on Linux, if a file is opened with O_APPEND, pwrite()
// appends data to the end of the file, regardless of the value of
// offset.
// More info here: https://linux.die.net/man/2/pwrite
      flags |= O_WRONLY;
#if !defined(OS_MACOSX) && !defined(OS_OPENBSD)
      flags |= O_DIRECT;
#endif
      TEST_SYNC_POINT_CALLBACK("NewWritableFile:O_DIRECT", &flags);
    } else {
      flags |= O_WRONLY;
    }

    do {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(fname.c_str(), flags, 0644);
    } while (fd < 0 && errno == EINTR);

    if (fd < 0) {
      s = IOError(fname, errno);
      return s;
    }
    SetFD_CLOEXEC(fd);

    if (options.use_direct_writes) {
#ifdef OS_MACOSX
      if (fcntl(fd, F_NOCACHE, 1) == -1) {
        close(fd);
        s = IOError(fname, errno);
        return s;
      }
#endif
      if (nullptr != options.arena) {
        result = ALLOC_OBJECT(PosixWritableFile, *options.arena, fname, fd, options);
      } else {
        result = MOD_NEW_OBJECT(memory::ModId::kEnv, PosixWritableFile, fname, fd, options);
      }
    } else {
      EnvOptions no_mmap_writes_options = options;
      if (nullptr != options.arena) {
        result = ALLOC_OBJECT(PosixWritableFile, *options.arena, fname, fd, no_mmap_writes_options);
      } else {
        result = MOD_NEW_OBJECT(memory::ModId::kEnv, PosixWritableFile, fname, fd, no_mmap_writes_options);
      }
    }

    return s;
  }

  virtual Status NewRandomRWFile(const std::string& fname,
                                 RandomRWFile *&result,
                                 const EnvOptions& options) override {
    int fd = -1;
    int flag = options.use_direct_reads ? O_CREAT | O_RDWR | O_DIRECT : O_CREAT | O_RDWR;
    while (fd < 0) {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(fname.c_str(), flag, 0644);
      if (fd < 0) {
        // Error while opening the file
        if (errno == EINTR) {
          continue;
        }
        return IOError(fname, errno);
      }
    }

    SetFD_CLOEXEC(fd);
    if (nullptr != options.arena) {
      result = ALLOC_OBJECT(PosixRandomRWFile, *options.arena, fname, fd, options);
    } else {
      result = MOD_NEW_OBJECT(memory::ModId::kEnv, PosixRandomRWFile, fname, fd, options);
    }
    return Status::OK();
  }

  virtual Status NewDirectory(const std::string& name,
                              Directory *&result) override {
    int fd;
    {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(name.c_str(), 0);
    }
    if (fd < 0) {
      return IOError(name, errno);
    } else {
      result = MOD_NEW_OBJECT(memory::ModId::kEnv, PosixDirectory, fd);
    }
    return Status::OK();
  }

  virtual Status FileExists(const std::string& fname) override {
    int result = access(fname.c_str(), F_OK);

    if (result == 0) {
      return Status::OK();
    }

    switch (errno) {
      case EACCES:
      case ELOOP:
      case ENAMETOOLONG:
      case ENOENT:
      case ENOTDIR:
        return Status::NotFound();
      default:
        assert(result == EIO || result == ENOMEM);
        return Status::IOError("Unexpected error(" + ToString(result) +
                               ") accessing file `" + fname + "' ");
    }
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) override {
    result->clear();
    DIR* d = opendir(dir.c_str());
    if (d == nullptr) {
      switch (errno) {
        case EACCES:
        case ENOENT:
        case ENOTDIR:
          return Status::NotFound();
        default:
          return IOError(dir, errno);
      }
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != nullptr) {
      result->push_back(entry->d_name);
    }
    closedir(d);
    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) override {
    Status result;
    if (unlink(fname.c_str()) != 0) {
      result = IOError(fname, errno);
    }
    return result;
  }

  virtual Status CreateDir(const std::string& name) override {
    Status result;
    if (mkdir(name.c_str(), 0755) != 0) {
      result = IOError(name, errno);
    }
    return result;
  }

  virtual Status CreateDirIfMissing(const std::string &name) override
  {
    int ret = Status::kOk;
    mode_t mode = 0755;
    char *parent_ptr = nullptr;
    char *cursor_ptr = nullptr;
    int64_t path_length = name.size();
    char *copy_path = new char[path_length+1];
    memset(copy_path, 0, path_length+1);
    memcpy(copy_path, name.c_str(), path_length);
    parent_ptr = copy_path;

    //system("mkdir -p name") can do the same thing as step1 and step 2, but it not cross-platform
    //step 1:create the parent directory if not exist
    while (SUCCED(ret) && (nullptr != (cursor_ptr = strchr(parent_ptr, '/')))) {
      if (parent_ptr != cursor_ptr) {
        *cursor_ptr = '\0';
        do_mkdir(copy_path, mode);
        *cursor_ptr = '/';
      }
      parent_ptr = cursor_ptr + 1;
    }

    //step 2:create the target directory
    if (SUCCED(ret)) {
      ret = do_mkdir(name.c_str(), mode);
    }

    if (nullptr != copy_path) {
      delete[] copy_path;
      copy_path = nullptr;
    }

    return ret;
  }

  virtual Status DeleteDir(const std::string& name) override {
    Status result;
    if (rmdir(name.c_str()) != 0) {
      result = IOError(name, errno);
    }
    return result;
  }

  virtual Status GetFileSize(const std::string& fname,
                             uint64_t* size) override {
    Status s;
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      *size = 0;
      s = IOError(fname, errno);
    } else {
      *size = sbuf.st_size;
    }
    return s;
  }

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime) override {
    struct stat s;
    if (stat(fname.c_str(), &s) != 0) {
      return IOError(fname, errno);
    }
    *file_mtime = static_cast<uint64_t>(s.st_mtime);
    return Status::OK();
  }
  virtual Status RenameFile(const std::string& src,
                            const std::string& target) override {
    Status result;
    if (rename(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LinkFile(const std::string& src,
                          const std::string& target) override {
    Status result;
    if (link(src.c_str(), target.c_str()) != 0) {
      if (errno == EXDEV) {
        return Status::NotSupported("No cross FS links allowed");
      }
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) override {
    *lock = nullptr;
    Status result;
    int fd;
    {
      IOSTATS_TIMER_GUARD(open_nanos);
      fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
    }
    if (fd < 0) {
      result = IOError(fname, errno);
    } else if (LockOrUnlock(fname, fd, true) == -1) {
      result = IOError("lock " + fname, errno);
      close(fd);
    } else {
      SetFD_CLOEXEC(fd);
      PosixFileLock* my_lock = new PosixFileLock;
      my_lock->fd_ = fd;
      my_lock->filename = fname;
      *lock = my_lock;
    }
    return result;
  }

  virtual Status UnlockFile(FileLock* lock) override {
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    Status result;
    if (LockOrUnlock(my_lock->filename, my_lock->fd_, false) == -1) {
      result = IOError("unlock", errno);
    }
    close(my_lock->fd_);
    delete my_lock;
    return result;
  }

  virtual void Schedule(void (*function)(void* arg1), void* arg,
                        Priority pri = LOW, void* tag = nullptr,
                        void (*unschedFunction)(void* arg) = 0) override;

  virtual int UnSchedule(void* arg, Priority pri) override;

  virtual void StartThread(void (*function)(void* arg), void* arg) override;

  virtual void WaitForJoin() override;

  virtual unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override;

  virtual Status GetTestDirectory(std::string* result) override {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      CreateDir("./tmp");
      char buf[100];
      snprintf(buf, sizeof(buf), "./tmp/smartengine-test-%d", int(geteuid()));
      *result = buf;
    }
    // Directory may already exist
    CreateDir(*result);
    return Status::OK();
  }

  virtual Status GetThreadList(
      std::vector<ThreadStatus>* thread_list) override {
    assert(thread_status_updater_);
    return thread_status_updater_->GetThreadList(thread_list);
  }

  static uint64_t gettid(pthread_t tid) {
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }

  static uint64_t gettid() {
    pthread_t tid = pthread_self();
    return gettid(tid);
  }

  virtual uint64_t GetThreadID() const override {
    return gettid(pthread_self());
  }

  virtual common::Status InitObjectStore(const std::string_view provider,
                                         const std::string_view region,
                                         const std::string_view *endpoint,
                                         bool use_https,
                                         const std::string_view bucket,
                                         const std::string &cluster_objstore_id,
                                         const uint32_t lease_lock_timeout) override
  {
    Status result;

    objstore::init_objstore_provider(provider);

    std::string obj_err_msg;
    obj_store_ = objstore::create_object_store(provider, region, endpoint, use_https, obj_err_msg);
    if (obj_store_ == nullptr) {
      result = common::Status::InvalidArgument(obj_err_msg);
    } else {
      obj_store_bucket_ = bucket;
      cluster_objstore_id_ = cluster_objstore_id;
      objstore_lease_lock_timeout_ = lease_lock_timeout;
    }
    return result;
  }

  virtual common::Status DestroyObjectStore() override {

    if (obj_store_ != nullptr) {
      objstore::cleanup_objstore_provider(obj_store_);

      objstore::destroy_object_store(obj_store_);
      obj_store_ = nullptr;
    }
    return common::Status::OK();
  }

  virtual bool IsObjectStoreSupported() const override { return true; }

  virtual bool IsObjectStoreInited() const override { return obj_store_ != nullptr; }

  virtual common::Status GetObjectStore(
      objstore::ObjectStore*& object_store) override {
    object_store = obj_store_;
    return object_store != nullptr ? common::Status::OK() : common::Status::NotInited();
  }

  virtual std::string& GetObjectStoreBucket() override {
    return obj_store_bucket_;
  }

  virtual std::string &GetClusterObjstoreId() override { return cluster_objstore_id_; }

  virtual uint32_t GetObjstoreLeaseLockTimeout() override { return objstore_lease_lock_timeout_; }

  virtual uint64_t NowMicros() override {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  virtual uint64_t NowNanos() override {
#if defined(OS_LINUX) || defined(OS_FREEBSD)
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
#elif defined(__MACH__)
    clock_serv_t cclock;
    mach_timespec_t ts;
    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
    clock_get_time(cclock, &ts);
    mach_port_deallocate(mach_task_self(), cclock);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
#else
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
#endif
  }

  virtual void SleepForMicroseconds(int micros) override { usleep(micros); }

  virtual Status GetHostName(char* name, uint64_t len) override {
    int ret = gethostname(name, static_cast<size_t>(len));
    if (ret < 0) {
      if (errno == EFAULT || errno == EINVAL)
        return Status::InvalidArgument(strerror(errno));
      else
        return IOError("GetHostName", errno);
    }
    return Status::OK();
  }

  virtual Status GetCurrentTime(int64_t* unix_time) override {
    time_t ret = time(nullptr);
    if (ret == (time_t)-1) {
      return IOError("GetCurrentTime", errno);
    }
    *unix_time = (int64_t)ret;
    return Status::OK();
  }

  virtual Status GetAbsolutePath(const std::string& db_path,
                                 std::string* output_path) override {
    if (db_path.find('/') == 0) {
      *output_path = db_path;
      return Status::OK();
    }

    char the_path[256];
    char* ret = getcwd(the_path, 256);
    if (ret == nullptr) {
      return Status::IOError(strerror(errno));
    }

    *output_path = ret;
    return Status::OK();
  }

  // Allow increasing the number of worker threads.
  virtual void SetBackgroundThreads(int num, Priority pri) override {
    assert(pri >= Priority::STATS && pri < Priority::TOTAL);
    thread_pools_[pri].SetBackgroundThreads(num);
  }

  virtual void WaitForJobsAndJoinAllThreads(Priority pri) override {
    assert(pri >= Priority::STATS && pri < Priority::TOTAL);
    thread_pools_[pri].WaitForJobsAndJoinAllThreads();
  }

  // Allow increasing the number of worker threads.
  virtual void IncBackgroundThreadsIfNeeded(int num, Priority pri) override {
    assert(pri >= Priority::STATS && pri <= Priority::FILTER);
    thread_pools_[pri].IncBackgroundThreadsIfNeeded(num);
  }

  virtual void LowerThreadPoolIOPriority(Priority pool = LOW) override {
    assert(pool >= Priority::STATS && pool <= Priority::FILTER);
#ifdef OS_LINUX
    thread_pools_[pool].LowerIOPriority();
#endif
  }

  virtual std::string TimeToString(uint64_t secondsSince1970) override {
    const time_t seconds = (time_t)secondsSince1970;
    struct tm t;
    int maxsize = 64;
    std::string dummy;
    dummy.reserve(maxsize);
    dummy.resize(maxsize);
    char* p = &dummy[0];
    localtime_r(&seconds, &t);
    snprintf(p, maxsize, "%04d/%02d/%02d-%02d:%02d:%02d ", t.tm_year + 1900,
             t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);
    return dummy;
  }

  EnvOptions OptimizeForLogWrite(const EnvOptions& env_options,
                                 const DBOptions& db_options) const override {
    EnvOptions optimized = env_options;
    optimized.concurrent_writable_file_buffer_num =
        db_options.concurrent_writable_file_buffer_num;
    optimized.concurrent_writable_file_single_buffer_size =
        db_options.concurrent_writable_file_single_buffer_size;
    optimized.concurrent_writable_file_buffer_switch_limit =
        db_options.concurrent_writable_file_buffer_switch_limit;
    optimized.use_direct_writes = db_options.use_direct_write_for_wal;

    optimized.bytes_per_sync = db_options.wal_bytes_per_sync;
    // TODO(icanadi) it's faster if fallocate_with_keep_size is false, but it
    // breaks TransactionLogIteratorStallAtLastRecord unit test. Fix the unit
    // test and make this false
    optimized.fallocate_with_keep_size = true;
    return optimized;
  }

  EnvOptions OptimizeForManifestWrite(
      const EnvOptions& env_options) const override {
    EnvOptions optimized = env_options;
    optimized.fallocate_with_keep_size = true;
    optimized.use_direct_writes = true;
    optimized.concurrent_writable_file_buffer_num = 2;
    optimized.concurrent_writable_file_single_buffer_size = 1024U * 1024U;
    optimized.concurrent_writable_file_buffer_switch_limit =
        optimized.concurrent_writable_file_buffer_switch_limit;
    return optimized;
  }

private:
  int do_mkdir(const char *path, mode_t mode)
  {
    int ret = Status::kOk;
    struct stat st;

    if (0 != stat(path, &st)) {
      //directory not exist, create it. EEXIST for race condition, more than one  thread create the same directory
      if (0 != mkdir(path, mode) && EEXIST != errno) {
        ret = Status::kIOError;
      }
    } else if (!S_ISDIR(st.st_mode)) {
      ret = Status::kIOError;
    }

    return ret;
  }

 private:
  // Returns true iff the named directory exists and is a directory.
  virtual bool DirExists(const std::string& dname) {
    struct stat statbuf;
    if (stat(dname.c_str(), &statbuf) == 0) {
      return S_ISDIR(statbuf.st_mode);
    }
    return false;  // stat() failed return false
  }

  bool SupportsFastAllocate(const std::string& path) {
#ifdef ROCKSDB_FALLOCATE_PRESENT
    struct statfs s;
    if (statfs(path.c_str(), &s)) {
      return false;
    }
    switch (s.f_type) {
      case EXT4_SUPER_MAGIC:
        return true;
      case XFS_SUPER_MAGIC:
        return true;
      case TMPFS_MAGIC:
        return true;
      default:
        return false;
    }
#else
    return false;
#endif
  }

  std::vector<ThreadPoolImpl> thread_pools_;
  pthread_mutex_t mu_;
  std::vector<pthread_t> threads_to_join_;

  objstore::ObjectStore* obj_store_;
  std::string obj_store_bucket_;
  std::string cluster_objstore_id_;
  uint32_t objstore_lease_lock_timeout_;
};

PosixEnv::PosixEnv() : thread_pools_(Priority::TOTAL)
{
  ThreadPoolImpl::PthreadCall("mutex_init", pthread_mutex_init(&mu_, nullptr));
  for (int pool_id = 0; pool_id < Env::Priority::TOTAL; ++pool_id) {
    thread_pools_[pool_id].SetThreadPriority(
        static_cast<Env::Priority>(pool_id));
    // This allows later initializing the thread-local-env of each thread.
    thread_pools_[pool_id].SetHostEnv(this);
  }
  thread_status_updater_ = CreateThreadStatusUpdater();
}

void PosixEnv::Schedule(void (*function)(void* arg1), void* arg, Priority pri,
                        void* tag, void (*unschedFunction)(void* arg)) {
  assert(pri >= Priority::STATS && pri < Priority::TOTAL);
  thread_pools_[pri].Schedule(function, arg, tag, unschedFunction);
}

int PosixEnv::UnSchedule(void* arg, Priority pri) {
  return thread_pools_[pri].UnSchedule(arg);
}

unsigned int PosixEnv::GetThreadPoolQueueLen(Priority pri) const {
  assert(pri >= Priority::STATS && pri <= Priority::HIGH);
  return thread_pools_[pri].GetQueueLen();
}

struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};

static void* StartThreadWrapper(void* arg) {
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return nullptr;
}

void PosixEnv::StartThread(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  ThreadPoolImpl::PthreadCall(
      "start thread", pthread_create(&t, nullptr, &StartThreadWrapper, state));
  ThreadPoolImpl::PthreadCall("lock", pthread_mutex_lock(&mu_));
  threads_to_join_.push_back(t);
  ThreadPoolImpl::PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void PosixEnv::WaitForJoin() {
  for (const auto tid : threads_to_join_) {
    pthread_join(tid, nullptr);
  }
  threads_to_join_.clear();
}

}  // namespace

std::string Env::GenerateUniqueId() {
  std::string uuid_file = "/proc/sys/kernel/random/uuid";

  Status s = FileExists(uuid_file);
  if (s.ok()) {
    std::string uuid;
    s = ReadFileToString(this, uuid_file, &uuid);
    if (s.ok()) {
      return uuid;
    }
  }
  // Could not read uuid_file - generate uuid using "nanos-random"
  Random64 r(time(nullptr));
  uint64_t random_uuid_portion =
      r.Uniform(std::numeric_limits<uint64_t>::max());
  uint64_t nanos_uuid_portion = NowNanos();
  char uuid2[200];
  snprintf(uuid2, 200, "%lx-%lx", (unsigned long)nanos_uuid_portion,
           (unsigned long)random_uuid_portion);
  return uuid2;
}

//
// Default Posix Env
//
Env* Env::Default() {
  // The following function call initializes the singletons of ThreadLocalPtr
  // right before the static default_env.  This guarantees default_env will
  // always being destructed before the ThreadLocalPtr singletons get
  // destructed as C++ guarantees that the destructions of static variables
  // is in the reverse order of their constructions.
  //
  // Since static members are destructed in the reverse order
  // of their construction, having this call here guarantees that
  // the destructor of static PosixEnv will go first, then the
  // the singletons of ThreadLocalPtr.
  ThreadLocalPtr::InitSingletons();
  static PosixEnv default_env;
  return &default_env;
}

}  // namespace util
}  // namespace smartengine
