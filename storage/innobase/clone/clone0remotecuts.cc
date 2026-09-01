/* Copyright (c) 2026, WeSQL and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License, version 2.0, for more details. */

#include "clone0remotecuts.h"

#include <algorithm>
#include <cstring>
#include <string_view>

#include "clone0desc.h"
#include "log0types.h"
#include "sha2.h"
#include "sql/remote_commit/protocol_codec.h"

namespace {

constexpr char REMOTE_REDO_LOCATOR_DOMAIN[] =
    "wesql.remote_clone.redo_locator";

void store_u32_be(unsigned char *buffer, uint32_t value) {
  buffer[0] = static_cast<unsigned char>(value >> 24);
  buffer[1] = static_cast<unsigned char>(value >> 16);
  buffer[2] = static_cast<unsigned char>(value >> 8);
  buffer[3] = static_cast<unsigned char>(value);
}

void store_u64_be(unsigned char *buffer, uint64_t value) {
  for (size_t index = 0; index < sizeof(value); ++index) {
    const auto shift = 8 * (sizeof(value) - index - 1);
    buffer[index] = static_cast<unsigned char>(value >> shift);
  }
}

bool set_error(std::string *error, std::string_view message) {
  if (error != nullptr) error->assign(message);
  return true;
}

std::string digest_to_hex(const Remote_clone_digest &digest) {
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  result.reserve(2 * digest.size());
  for (const auto value : digest) {
    result.push_back(hex[value >> 4]);
    result.push_back(hex[value & 0x0f]);
  }
  return result;
}

bool is_lowercase_sha256(std::string_view value) {
  return value.size() == 64 &&
         std::all_of(value.begin(), value.end(), [](const char character) {
           return (character >= '0' && character <= '9') ||
                  (character >= 'a' && character <= 'f');
         });
}

unsigned char lowercase_hex_value(char character) {
  return character <= '9' ? static_cast<unsigned char>(character - '0')
                          : static_cast<unsigned char>(character - 'a' + 10);
}

Remote_clone_digest digest_from_lowercase_hex(std::string_view value) {
  ut_ad(is_lowercase_sha256(value));
  Remote_clone_digest digest{};
  for (size_t index = 0; index < digest.size(); ++index) {
    digest[index] = static_cast<unsigned char>(
        (lowercase_hex_value(value[2 * index]) << 4) |
        lowercase_hex_value(value[2 * index + 1]));
  }
  return digest;
}

bool valid_remote_cut(const Remote_clone_cut &cut, uint64_t clone_id,
                      uint64_t request_id) {
  if (clone_id == CLONE_LOC_INVALID_ID || request_id == 0 ||
      cut.clone_handle_id != clone_id || cut.request_id != request_id ||
      cut.binlog_file.empty() || cut.binlog_position == 0 ||
      cut.head_generation == 0 ||
      !is_lowercase_sha256(cut.head_body_sha256) ||
      cut.redo_locator.version != REMOTE_CLONE_REDO_LOCATOR_VERSION ||
      cut.redo_locator.start_lsn == LSN_MAX ||
      cut.redo_locator.end_lsn == LSN_MAX ||
      cut.redo_locator.end_lsn < cut.redo_locator.start_lsn) {
    return false;
  }

  wesql::remote_commit::GtidSetDigest gtid_digest;
  return wesql::remote_commit::gtid_digest(cut.gtid_executed, &gtid_digest,
                                           nullptr) &&
         gtid_digest.canonical == cut.gtid_executed &&
         gtid_digest.sha256 == digest_to_hex(cut.gtid_digest) &&
         innodb_clone_redo_locator_digest(cut.redo_locator) ==
             cut.redo_locator_digest;
}

}  // namespace

Remote_clone_digest innodb_clone_redo_locator_digest(
    const Remote_clone_redo_locator &locator) {
  constexpr size_t domain_length = sizeof(REMOTE_REDO_LOCATOR_DOMAIN) - 1;
  constexpr size_t encoded_length = domain_length + sizeof(uint32_t) +
                                    3 * sizeof(uint64_t) + sizeof(uint32_t);
  std::array<unsigned char, encoded_length> encoded{};

  std::memcpy(encoded.data(), REMOTE_REDO_LOCATOR_DOMAIN, domain_length);
  size_t offset = domain_length;
  store_u32_be(encoded.data() + offset, locator.version);
  offset += sizeof(uint32_t);
  store_u64_be(encoded.data() + offset, locator.start_lsn);
  offset += sizeof(uint64_t);
  store_u64_be(encoded.data() + offset, locator.end_lsn);
  offset += sizeof(uint64_t);
  store_u64_be(encoded.data() + offset, locator.trailer_offset);
  offset += sizeof(uint64_t);
  store_u32_be(encoded.data() + offset, locator.trailer_length);

  Remote_clone_digest digest{};
  SHA_EVP256(encoded.data(), encoded.size(), digest.data());
  return digest;
}

bool innodb_clone_build_remote_cut(
    const wesql::remote_commit::CloneCutState &barrier_state,
    std::string_view synchronized_file, uint64_t synchronized_position,
    uint64_t clone_id, const Remote_clone_redo_locator &redo_locator,
    Remote_clone_cut &remote_cut, std::string *error) {
  if (error != nullptr) error->clear();
  if (clone_id == CLONE_LOC_INVALID_ID || barrier_state.request_id == 0 ||
      barrier_state.file.empty() || barrier_state.pos == 0 ||
      barrier_state.head_generation == 0 ||
      !is_lowercase_sha256(barrier_state.gtid_sha256) ||
      !is_lowercase_sha256(barrier_state.head_body_sha256)) {
    return set_error(error, "Remote clone cut has invalid barrier identity");
  }
  if (synchronized_file.empty() || synchronized_position == 0) {
    return set_error(
        error, "Remote clone cut has an invalid synchronized InnoDB cursor");
  }
  if (synchronized_file != barrier_state.file ||
      synchronized_position != barrier_state.pos) {
    return set_error(
        error,
        "Remote clone cut synchronized InnoDB cursor does not match the barrier");
  }
  if (redo_locator.version != REMOTE_CLONE_REDO_LOCATOR_VERSION ||
      redo_locator.start_lsn == LSN_MAX ||
      redo_locator.end_lsn == LSN_MAX ||
      redo_locator.end_lsn < redo_locator.start_lsn) {
    return set_error(error, "Remote clone cut has an invalid redo locator");
  }

  Remote_clone_cut cut;
  cut.clone_handle_id = clone_id;
  cut.request_id = barrier_state.request_id;
  cut.binlog_file = synchronized_file;
  cut.binlog_position = synchronized_position;
  wesql::remote_commit::GtidSetDigest gtid_digest;
  if (!wesql::remote_commit::gtid_digest(barrier_state.canonical_gtid,
                                         &gtid_digest, nullptr) ||
      gtid_digest.canonical != barrier_state.canonical_gtid ||
      gtid_digest.sha256 != barrier_state.gtid_sha256) {
    return set_error(error,
                     "Remote clone cut GTID digest does not match the barrier");
  }
  cut.gtid_executed = std::move(gtid_digest.canonical);
  cut.gtid_digest = digest_from_lowercase_hex(gtid_digest.sha256);

  cut.head_generation = barrier_state.head_generation;
  cut.head_body_sha256 = barrier_state.head_body_sha256;
  cut.redo_locator = redo_locator;
  cut.redo_locator_digest = innodb_clone_redo_locator_digest(redo_locator);
  remote_cut = std::move(cut);
  return false;
}

bool Remote_clone_cut_stage::begin(std::string *error) {
  if (m_request_id == 0) {
    return set_error(error,
                     "Remote clone cut stage has an invalid snapshot request ID");
  }
  if (active() || m_cut.has_value() || m_verified) {
    return set_error(error, "Remote clone cut stage was started twice");
  }
  return wesql::remote_commit::begin_clone_cut_barrier(
      m_request_id, &m_barrier_state, &m_barrier_lease, error);
}

bool Remote_clone_cut_stage::capture(
    std::string_view synchronized_file, uint64_t synchronized_position,
    const Remote_clone_redo_locator &redo_locator, std::string *error) {
  if (!active() || m_cut.has_value() || m_verified) {
    return set_error(error, "Remote clone cut stage is not ready for capture");
  }

  Remote_clone_cut cut;
  if (innodb_clone_build_remote_cut(
          m_barrier_state, synchronized_file, synchronized_position, m_clone_id,
          redo_locator, cut, error)) {
    return true;
  }
  m_cut.emplace(std::move(cut));
  return false;
}

bool Remote_clone_cut_stage::verify(std::string *error) {
  if (!active() || !m_cut.has_value() || m_verified) {
    return set_error(error,
                     "Remote clone cut stage is not ready for verification");
  }
  if (wesql::remote_commit::verify_clone_cut_barrier(
          m_barrier_state, m_barrier_lease, error)) {
    return true;
  }
  m_verified = true;
  return false;
}

bool Remote_clone_cut_stage::take(
    Remote_clone_cut &cut,
    wesql::remote_commit::CloneCutBarrierLease &lease, std::string *error) {
  if (!active() || !m_cut.has_value() || !m_verified || lease.active()) {
    return set_error(error,
                     "Remote clone cut stage is not ready for installation");
  }

  cut = std::move(*m_cut);
  m_cut.reset();
  lease = std::move(m_barrier_lease);
  m_verified = false;
  return false;
}

bool Remote_clone_cut_slot::install(uint64_t clone_id, uint64_t request_id,
                                    Remote_clone_cut &&cut) {
  if (m_cut.has_value() || !valid_remote_cut(cut, clone_id, request_id)) {
    return false;
  }
  m_cut.emplace(std::move(cut));
  return true;
}

bool Remote_clone_cut_slot::get(uint64_t clone_id, uint64_t request_id,
                                Remote_clone_cut &cut) const {
  if (!m_cut.has_value() ||
      !valid_remote_cut(*m_cut, clone_id, request_id))
    return false;
  cut = *m_cut;
  return true;
}
