/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_RECOVERY_INCLUDED
#define SQL_REMOTE_COMMIT_RECOVERY_INCLUDED

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "sql/remote_commit/protocol_codec.h"
#include "sql/remote_commit/publisher.h"

namespace wesql::remote_commit {

enum class RecoveryReadOutcome : uint8_t {
  READY,
  EMPTY,
  BLOCKED,
  CORRUPT,
};

struct VerifiedManifest {
  ObjectRef object;
  TransitionManifest value;
  uint64_t body_size{0};
};

struct RecoveryPlan {
  PublishedBytes head_object;
  Head head;
  PublishedBytes epoch_object;
  WriterEpoch epoch;
  PublishedBytes snapshot_object;
  SnapshotManifest snapshot;

  // Newest first, ending at the exact recovery-window stop boundary.
  std::vector<VerifiedManifest> manifests;
  // Strictly chronological and exactly covering (snapshot, HEAD].
  std::vector<SegmentRef> replay_segments;
};

struct RecoveryReadResult {
  RecoveryReadOutcome outcome{RecoveryReadOutcome::CORRUPT};
  std::string detail;

  bool ready() const { return outcome == RecoveryReadOutcome::READY; }
};

// Reads HEAD before WRITER_EPOCH and validates the bounded immutable recovery
// window without consulting LIST, legacy indexes, or local state.
class RecoveryChainReader {
 public:
  RecoveryChainReader(ConditionalIo *io, StreamIdentity stream)
      : store_(io), stream_(std::move(stream)) {}

  RecoveryReadResult read(RecoveryPlan *plan);

 private:
  RecoveryReadResult read_required(std::string_view key, size_t max_bytes,
                                   PublishedBytes *object);
  RecoveryReadResult read_snapshot(const SnapshotRef &ref,
                                   PublishedBytes *object,
                                   SnapshotManifest *snapshot);
  RecoveryReadResult corrupt(std::string detail) const;

  ProtocolStore store_;
  StreamIdentity stream_;
};

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_RECOVERY_INCLUDED
