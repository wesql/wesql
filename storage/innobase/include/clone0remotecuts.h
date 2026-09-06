/* Copyright (c) 2026, WeSQL and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License, version 2.0, for more details. */

#ifndef CLONE_REMOTE_CUTS_INCLUDE
#define CLONE_REMOTE_CUTS_INCLUDE

#ifdef WESQL

#include <optional>
#include <string>
#include <string_view>

#include "clone0api.h"
#include "sql/remote_commit/server_hooks.h"

/** Build one immutable clone cut from an already-drained barrier snapshot.
The output is unchanged on failure.
@return true on error */
bool innodb_clone_build_remote_cut(
    const wesql::remote_commit::CloneCutState &barrier_state,
    std::string_view synchronized_file, uint64_t synchronized_position,
    uint64_t clone_id, const Remote_clone_redo_locator &redo_locator,
    Remote_clone_cut &cut, std::string *error);

/** Owns the barrier state and lease while REDO_COPY is in progress. */
class Remote_clone_cut_stage {
 public:
  Remote_clone_cut_stage(uint64_t clone_id, uint64_t request_id)
      : m_clone_id(clone_id), m_request_id(request_id) {}

  Remote_clone_cut_stage(const Remote_clone_cut_stage &) = delete;
  Remote_clone_cut_stage &operator=(const Remote_clone_cut_stage &) = delete;

  /** Close and drain commit admission, then capture the committed cut state.
  @return true on error */
  bool begin(std::string *error);

  /** Add the post-synchronization InnoDB cursor and stopped redo coordinates to
  the staged cut. The persisted cursor must exactly equal the pinned barrier
  cursor.
  @return true on error */
  bool capture(std::string_view synchronized_file,
               uint64_t synchronized_position,
               const Remote_clone_redo_locator &redo_locator,
               std::string *error);

  /** Verify the barrier and all captured state immediately before install.
  @return true on error */
  bool verify(std::string *error);

  /** Transfer the verified cut and active lease to its clone handle.
  @return true on error */
  bool take(Remote_clone_cut &cut,
            wesql::remote_commit::CloneCutBarrierLease &lease,
            std::string *error);

  /** @return true if this stage owns an active barrier lease. */
  bool active() const { return m_barrier_lease.active(); }

 private:
  uint64_t m_clone_id;
  uint64_t m_request_id;
  wesql::remote_commit::CloneCutState m_barrier_state;
  wesql::remote_commit::CloneCutBarrierLease m_barrier_lease;
  std::optional<Remote_clone_cut> m_cut;
  bool m_verified{false};
};

/** Install-once storage used by each live copy-clone handle. */
class Remote_clone_cut_slot {
 public:
  /** Install a matching cut exactly once. */
  bool install(uint64_t clone_id, uint64_t request_id,
               Remote_clone_cut &&cut);

  /** Copy a matching installed cut. The output is unchanged when absent. */
  bool get(uint64_t clone_id, uint64_t request_id,
           Remote_clone_cut &cut) const;

 private:
  std::optional<Remote_clone_cut> m_cut;
};

#endif /* WESQL */

#endif /* CLONE_REMOTE_CUTS_INCLUDE */
