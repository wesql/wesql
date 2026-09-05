/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#ifndef SQL_REMOTE_COMMIT_SEGMENT_SEALER_INCLUDED
#define SQL_REMOTE_COMMIT_SEGMENT_SEALER_INCLUDED

#include <cstdint>
#include <string>
#include <vector>
#ifdef WESQL_TEST
#include <functional>
#endif

#include "sql/remote_commit/protocol_codec.h"
#include "sql/remote_commit/publisher.h"

namespace wesql::remote_commit {

struct NativeRangeMetadata {
  uint64_t transaction_count{0};
  std::string gtid_set;
  std::vector<uint64_t> xids;
  std::string native_sha256;
};

struct NativeBinlogRange {
  std::string local_path;
  SegmentSource source;
  NativeRangeMetadata metadata;
};

struct SealedSegments {
  std::vector<SegmentRef> segments;
  SegmentTip tip;
  Cursor durable_cursor;
};

// Validates native event framing. This does not reinterpret or rewrite bytes.
// CRC32 and GTID/XID equivalence are additionally checked by the server-side
// Binlog_file_reader before constructing NativeRangeMetadata.
bool validate_native_binlog_range(std::string_view body,
                                  const SegmentSource &source,
                                  std::string *error);

class SegmentSealer {
 public:
  SegmentSealer(ProtocolStore *store, StreamIdentity stream,
                uint64_t max_segment_bytes)
      : store_(store),
        stream_(std::move(stream)),
        max_segment_bytes_(max_segment_bytes) {}

  PublishResult seal(const Writer &writer, uint64_t first_sequence,
                     const SegmentTip &previous_tip,
                     const std::vector<NativeBinlogRange> &ranges,
                     SealedSegments *sealed);

#ifdef WESQL_TEST
  void set_after_read_for_test(std::function<void()> hook) {
    after_read_for_test_ = std::move(hook);
  }
#endif

 private:
  PublishResult read_range(const NativeBinlogRange &range, std::string *body);

  ProtocolStore *store_;
  StreamIdentity stream_;
  uint64_t max_segment_bytes_;
#ifdef WESQL_TEST
  std::function<void()> after_read_for_test_;
#endif
};

}  // namespace wesql::remote_commit

#endif  // SQL_REMOTE_COMMIT_SEGMENT_SEALER_INCLUDED
