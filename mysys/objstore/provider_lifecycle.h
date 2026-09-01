/*
   Copyright (c) 2026, ApeCloud Inc Holding Limited.

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

#ifndef MYSYS_OBJSTORE_PROVIDER_LIFECYCLE_H_
#define MYSYS_OBJSTORE_PROVIDER_LIFECYCLE_H_

#include <cstddef>
#include <mutex>

namespace objstore::detail {

class ProviderLifecycle {
 public:
  using Callback = void (*)();

  ProviderLifecycle(Callback initialize, Callback shutdown)
      : initialize_(initialize), shutdown_(shutdown) {}

  ProviderLifecycle(const ProviderLifecycle &) = delete;
  ProviderLifecycle &operator=(const ProviderLifecycle &) = delete;

  void acquire() {
    std::lock_guard<std::mutex> guard(mutex_);
    if (references_ == 0) initialize_();
    ++references_;
  }

  void release() {
    std::lock_guard<std::mutex> guard(mutex_);
    if (references_ == 0) return;
    --references_;
    if (references_ == 0) shutdown_();
  }

 private:
  Callback initialize_;
  Callback shutdown_;
  std::mutex mutex_;
  std::size_t references_{0};
};

}  // namespace objstore::detail

#endif  // MYSYS_OBJSTORE_PROVIDER_LIFECYCLE_H_
