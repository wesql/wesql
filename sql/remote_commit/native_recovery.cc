/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/remote_commit/native_recovery.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cctype>
#include <filesystem>
#include <limits>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <utility>

#ifndef WESQL_NATIVE_RECOVERY_STATE_MACHINE_TEST
#include "my_inttypes.h"
#include "my_psi_config.h"
#include "mysql/binlog/event/trx_boundary_parser.h"
#include "sql/auto_thd.h"
#include "sql/binlog_reader.h"
#include "sql/log_event.h"
#include "sql/mysqld.h"
#include "sql/remote_commit/server_hooks.h"
#include "sql/rpl_filter.h"
#include "sql/rpl_info_dummy.h"
#include "sql/rpl_mta_submode.h"
#include "sql/rpl_replica.h"
#include "sql/rpl_rli.h"
#include "sql/sql_class.h"
#endif

namespace wesql::remote_commit {
namespace {

namespace fs = std::filesystem;

NativeRecoveryResult result(NativeRecoveryOutcome outcome, std::string detail,
                            uint64_t applied = 0) {
  return {outcome, std::move(detail), applied};
}

bool parse_binlog_name(std::string_view file, std::string_view *basename,
                       uint64_t *sequence) {
  const size_t dot = file.rfind('.');
  if (dot == std::string_view::npos || dot == 0 || dot + 1 == file.size())
    return false;
  uint64_t parsed = 0;
  const auto converted = std::from_chars(file.data() + dot + 1,
                                         file.data() + file.size(), parsed);
  if (converted.ec != std::errc() || converted.ptr != file.data() + file.size())
    return false;
  if (basename != nullptr) *basename = file.substr(0, dot);
  if (sequence != nullptr) *sequence = parsed;
  return true;
}

int compare_cursor(const Cursor &left, const Cursor &right) {
  std::string_view left_base;
  std::string_view right_base;
  uint64_t left_sequence = 0;
  uint64_t right_sequence = 0;
  if (!parse_binlog_name(left.file, &left_base, &left_sequence) ||
      !parse_binlog_name(right.file, &right_base, &right_sequence) ||
      left_base != right_base)
    return left.file < right.file ? -1 : left.file == right.file ? 0 : 1;
  if (left_sequence != right_sequence)
    return left_sequence < right_sequence ? -1 : 1;
  if (left.pos != right.pos) return left.pos < right.pos ? -1 : 1;
  return 0;
}

bool exact_segment_coverage(const NativeRecoveryRequest &request,
                            std::string *error) {
  const auto &segments = request.candidate->replay_segments;
  if (request.exclusive_start == request.inclusive_end) {
    if (!segments.empty()) {
      *error = "empty replay bound carries native segments";
      return false;
    }
    return true;
  }
  if (segments.empty()) {
    *error = "non-empty replay bound has no native segments";
    return false;
  }

  Cursor expected = request.exclusive_start;
  for (const SegmentRef &segment : segments) {
    if (segment.source.file.empty() ||
        segment.source.start_pos >= segment.source.end_pos ||
        segment.size != segment.source.end_pos - segment.source.start_pos ||
        !segment.ends_at_transaction_boundary ||
        segment.payload_format != "native-mysql-binlog-range-v1" ||
        segment.compression != "none") {
      *error = "candidate contains an invalid native segment descriptor";
      return false;
    }
    if (segment.source.file == expected.file) {
      if (segment.source.start_pos != expected.pos) {
        *error = "candidate native segments contain a gap or overlap";
        return false;
      }
    } else {
      std::string_view expected_base;
      std::string_view segment_base;
      uint64_t expected_sequence = 0;
      uint64_t segment_sequence = 0;
      if (segment.source.start_pos != 0 ||
          !parse_binlog_name(expected.file, &expected_base,
                             &expected_sequence) ||
          !parse_binlog_name(segment.source.file, &segment_base,
                             &segment_sequence) ||
          expected_base != segment_base ||
          expected_sequence == std::numeric_limits<uint64_t>::max() ||
          segment_sequence != expected_sequence + 1) {
        *error =
            "candidate cross-file segment is not the exact next binlog";
        return false;
      }
    }
    expected = {segment.source.file, segment.source.end_pos};
  }
  if (expected != request.inclusive_end) {
    *error = "candidate native segments do not reach the exact HEAD bound";
    return false;
  }
  return true;
}

bool validate_request(const NativeRecoveryRequest &request,
                      std::string *head_body_sha256, std::string *error) {
  if (request.candidate == nullptr || request.materialized == nullptr ||
      request.stream_id.empty() || request.max_event_bytes == 0) {
    *error = "native recovery request is incomplete";
    return false;
  }
  const RecoveryPlan &candidate = *request.candidate;
  if (candidate.head.generation == 0 || candidate.head_object.body.empty() ||
      candidate.snapshot.snapshot_id.empty() ||
      request.exclusive_start != candidate.snapshot.cursor ||
      request.inclusive_end != candidate.head.durable_cursor ||
      compare_cursor(request.exclusive_start, request.inclusive_end) > 0) {
    *error = "native recovery bounds do not equal snapshot-to-candidate HEAD";
    return false;
  }
  if (!sha256_hex(candidate.head_object.body, head_body_sha256, error))
    return false;
  return exact_segment_coverage(request, error);
}

bool validate_transactions(
    const NativeRecoveryRequest &request,
    const std::vector<NativeRecoveryTransaction> &transactions,
    std::string *error) {
  if (request.exclusive_start == request.inclusive_end)
    return transactions.empty();
  if (transactions.empty()) {
    *error = "non-empty replay range contains no complete transaction";
    return false;
  }

  Cursor previous = request.exclusive_start;
  for (const NativeRecoveryTransaction &transaction : transactions) {
    GtidSetDigest recomputed;
    if (transaction.first_event.file != transaction.endpoint.file ||
        compare_cursor(transaction.first_event, transaction.endpoint) >= 0 ||
        compare_cursor(transaction.first_event, previous) < 0 ||
        compare_cursor(transaction.endpoint, previous) <= 0 ||
        compare_cursor(transaction.endpoint, request.inclusive_end) > 0 ||
        !gtid_digest(transaction.gtid.canonical, &recomputed, error) ||
        recomputed != transaction.gtid) {
      if (error->empty())
        *error = "native recovery transaction descriptor is invalid";
      return false;
    }
    previous = transaction.endpoint;
  }
  if (previous != request.inclusive_end) {
    *error = "last complete transaction does not end at the HEAD bound";
    return false;
  }
  return true;
}

NativeRecoveryOutcome scan_outcome(NativeRecoveryScanOutcome outcome) {
  switch (outcome) {
    case NativeRecoveryScanOutcome::INCOMPLETE_TRANSACTION:
      return NativeRecoveryOutcome::INCOMPLETE_TRANSACTION;
    case NativeRecoveryScanOutcome::LOCAL_IO_ERROR:
      return NativeRecoveryOutcome::LOCAL_IO_ERROR;
    case NativeRecoveryScanOutcome::READY:
    case NativeRecoveryScanOutcome::CORRUPT:
      return NativeRecoveryOutcome::CORRUPT;
  }
  return NativeRecoveryOutcome::CORRUPT;
}

}  // namespace

NativeRecoveryResult run_bounded_native_recovery(
    const NativeRecoveryRequest &request, NativeRecoveryExecutor *executor,
    NativeRecoveryPreparedVerifier *prepared_verifier) {
  if (executor == nullptr || prepared_verifier == nullptr)
    return result(NativeRecoveryOutcome::CORRUPT,
                  "native recovery dependencies are null");

  std::string head_body_sha256;
  std::string error;
  if (!validate_request(request, &head_body_sha256, &error))
    return result(NativeRecoveryOutcome::CORRUPT, std::move(error));
  if (!prepared_verifier->prepared_sets_empty(&error))
    return result(NativeRecoveryOutcome::PREPARED_SET_NOT_EMPTY,
                  error.empty() ? "prepared transaction set is not empty"
                                : std::move(error));

  std::vector<NativeRecoveryTransaction> transactions;
  NativeRecoveryScanResult scan = executor->scan(request, &transactions);
  if (!scan.ready())
    return result(scan_outcome(scan.outcome), std::move(scan.detail));
  if (!validate_transactions(request, transactions, &error))
    return result(NativeRecoveryOutcome::CORRUPT, std::move(error));
  if (transactions.empty()) {
    if (!prepared_verifier->prepared_sets_empty(&error))
      return result(NativeRecoveryOutcome::PREPARED_SET_NOT_EMPTY,
                    error.empty() ? "prepared set changed during empty replay"
                                  : std::move(error));
    return result(NativeRecoveryOutcome::EMPTY, {});
  }

  if (!executor->start_session(&error))
    return result(NativeRecoveryOutcome::THD_LIFECYCLE_ERROR,
                  error.empty() ? "cannot start native recovery THD"
                                : std::move(error));

  uint64_t applied = 0;
  NativeRecoveryOutcome outcome = NativeRecoveryOutcome::APPLIED;
  for (const NativeRecoveryTransaction &transaction : transactions) {
    std::vector<uint64_t> xids;
    if (transaction.xid.has_value()) xids.push_back(*transaction.xid);
    XidDigest xid;
    if (!xid_digest(xids, &xid, &error)) {
      outcome = NativeRecoveryOutcome::CORRUPT;
      break;
    }
    CommitBinding binding{request.stream_id,
                          request.candidate->head.generation,
                          head_body_sha256,
                          transaction.endpoint.file,
                          transaction.endpoint.pos,
                          transaction.gtid.sha256,
                          xid.sha256};
    if (!executor->apply_transaction(transaction, binding, &error)) {
      outcome = NativeRecoveryOutcome::APPLY_ERROR;
      break;
    }
    ++applied;
  }

  std::string finish_error;
  if (!executor->finish_session(&finish_error)) {
    if (!error.empty()) finish_error.append("; prior error: ").append(error);
    return result(NativeRecoveryOutcome::THD_LIFECYCLE_ERROR,
                  finish_error.empty()
                      ? "native recovery THD did not restore its lifecycle"
                      : std::move(finish_error),
                  applied);
  }

  std::string prepared_error;
  if (!prepared_verifier->prepared_sets_empty(&prepared_error)) {
    if (!error.empty()) prepared_error.append("; prior error: ").append(error);
    return result(NativeRecoveryOutcome::PREPARED_SET_NOT_EMPTY,
                  prepared_error.empty()
                      ? "prepared transaction set is not empty after replay"
                      : std::move(prepared_error),
                  applied);
  }
  if (outcome != NativeRecoveryOutcome::APPLIED)
    return result(outcome,
                  error.empty() ? "native recovery transaction apply failed"
                                : std::move(error),
                  applied);
  return result(NativeRecoveryOutcome::APPLIED, {}, applied);
}

#ifndef WESQL_NATIVE_RECOVERY_STATE_MACHINE_TEST
namespace {

bool fail(std::string *error, std::string detail) {
  if (error != nullptr) *error = std::move(detail);
  return false;
}

NativeRecoveryScanResult scan_failure(NativeRecoveryScanOutcome outcome,
                                      std::string detail) {
  return {outcome, std::move(detail)};
}

bool append_gtid(const Gtid_log_event &event, std::string *gtid,
                 std::string *error) {
  if (event.get_type() != ASSIGNED_GTID)
    return fail(error, "native recovery contains an anonymous GTID");
  std::array<char, mysql::gtid::tsid_max_length + 1> tsid{};
  const int tsid_length = event.get_tsid().to_string(tsid.data());
  if (tsid_length <= 0 || static_cast<size_t>(tsid_length) >= tsid.size())
    return fail(error, "cannot render recovery GTID TSID");
  std::array<char, 32> gno{};
  const auto converted = std::to_chars(gno.data(), gno.data() + gno.size(),
                                       event.get_gno());
  if (converted.ec != std::errc())
    return fail(error, "cannot render recovery GTID GNO");
  gtid->assign(tsid.data(), static_cast<size_t>(tsid_length));
  gtid->push_back(':');
  gtid->append(gno.data(), static_cast<size_t>(converted.ptr - gno.data()));
  return true;
}

std::string_view trim_ascii(std::string_view value) {
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.front())) != 0)
    value.remove_prefix(1);
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.back())) != 0)
    value.remove_suffix(1);
  return value;
}

bool ascii_equal(std::string_view left, std::string_view right) {
  return left.size() == right.size() &&
         std::equal(left.begin(), left.end(), right.begin(),
                    [](unsigned char lhs, unsigned char rhs) {
                      return std::toupper(lhs) == std::toupper(rhs);
                    });
}

bool ascii_starts_token(std::string_view value, std::string_view token) {
  value = trim_ascii(value);
  if (value.size() < token.size() ||
      !ascii_equal(value.substr(0, token.size()), token))
    return false;
  return value.size() == token.size() ||
         std::isspace(static_cast<unsigned char>(value[token.size()])) != 0;
}

bool is_row_mutation(mysql::binlog::event::Log_event_type type) {
  using mysql::binlog::event::DELETE_ROWS_EVENT;
  using mysql::binlog::event::PARTIAL_UPDATE_ROWS_EVENT;
  using mysql::binlog::event::UPDATE_ROWS_EVENT;
  using mysql::binlog::event::WRITE_ROWS_EVENT;
  return type == WRITE_ROWS_EVENT || type == UPDATE_ROWS_EVENT ||
         type == DELETE_ROWS_EVENT || type == PARTIAL_UPDATE_ROWS_EVENT;
}

bool is_metadata_event(mysql::binlog::event::Log_event_type type) {
  return type == mysql::binlog::event::FORMAT_DESCRIPTION_EVENT ||
         type == mysql::binlog::event::PREVIOUS_GTIDS_LOG_EVENT ||
         type == mysql::binlog::event::ROTATE_EVENT;
}

struct CurrentTransaction {
  NativeRecoveryTransaction value;
  std::string gtid;
  bool saw_mutation{false};
};

class MysqlNativeRecoveryExecutor final : public NativeRecoveryExecutor {
 public:
  NativeRecoveryScanResult scan(
      const NativeRecoveryRequest &request,
      std::vector<NativeRecoveryTransaction> *transactions) override {
    if (transactions == nullptr)
      return scan_failure(NativeRecoveryScanOutcome::CORRUPT,
                          "null native recovery transaction output");
    transactions->clear();
    files_.clear();
    max_event_bytes_ = request.max_event_bytes;

    for (const fs::path &path : request.materialized->binlog_files) {
      const std::string name = path.filename().string();
      if (name.empty() || !files_.emplace(name, path).second)
        return scan_failure(NativeRecoveryScanOutcome::CORRUPT,
                            "materialized binlog filename is duplicated");
    }
    for (size_t index = 0; index < request.candidate->replay_segments.size();
         ++index) {
      const SegmentRef &segment = request.candidate->replay_segments[index];
      const auto found = files_.find(segment.source.file);
      if (found == files_.end())
        return scan_failure(NativeRecoveryScanOutcome::CORRUPT,
                            "candidate segment has no materialized file");
      std::string_view next_file;
      if (index + 1 < request.candidate->replay_segments.size()) {
        const SegmentRef &next = request.candidate->replay_segments[index + 1];
        if (next.source.file != segment.source.file) next_file = next.source.file;
      }
      NativeRecoveryScanResult scanned =
          scan_segment(found->second, segment, next_file, transactions);
      if (!scanned.ready()) return scanned;
    }
    return {NativeRecoveryScanOutcome::READY, {}};
  }

  bool start_session(std::string *error) override {
    if (started_) return fail(error, "native recovery THD was started twice");
    auto_thd_ = std::make_unique<Auto_THD>();
    THD *thd = auto_thd_->thd;
    if (thd == nullptr) return fail(error, "Auto_THD creation returned null");

    rli_ = std::make_unique<Relay_log_info>(
        false,
#ifdef HAVE_PSI_INTERFACE
        &key_relay_log_info_run_lock, &key_relay_log_info_data_lock,
        &key_relay_log_info_sleep_lock, &key_relay_log_info_thd_lock,
        &key_relay_log_info_data_cond, &key_relay_log_info_start_cond,
        &key_relay_log_info_stop_cond, &key_relay_log_info_sleep_cond,
#endif
        1, "", false);
    if (rli_ == nullptr) return fail(error, "cannot allocate recovery RLI");
    // Query events use this even on a serial SQL applier, as in
    // handle_slave_sql. The non-fake RLI owns and destroys the submode.
    rli_->current_mts_submode = new Mts_submode_logical_clock();
    rli_->set_rpl_info_handler(
        new Rpl_info_dummy(Relay_log_info::get_number_info_rli_fields()));
    filter_ = std::make_unique<Rpl_filter>();
    rli_->set_filter(filter_.get());
    rli_->info_thd = thd;
    thd->rli_slave = rli_.get();

    if (init_replica_thread(thd, SLAVE_THD_SQL) != 0) {
      restore_background_thd();
      return fail(error, "cannot initialize dedicated recovery applier THD");
    }
    set_slave_thread_default_charset(thd, rli_.get());
    thd->variables.option_bits &= ~OPTION_BIN_LOG;
    thd->variables.sql_log_bin = false;
    started_ = true;
    return true;
  }

  bool apply_transaction(const NativeRecoveryTransaction &transaction,
                         const CommitBinding &binding,
                         std::string *error) override {
    if (!started_ || auto_thd_ == nullptr || rli_ == nullptr)
      return fail(error, "native recovery THD is not active");
    const auto found = files_.find(transaction.first_event.file);
    if (found == files_.end())
      return fail(error, "native recovery transaction file disappeared");

    Format_description_log_event *fdle = nullptr;
    std::vector<BufferedEvent> events;
    if (!read_complete_transaction(found->second, transaction, &fdle, &events,
                                   error))
      return false;
    std::unique_ptr<Format_description_log_event> description(fdle);
    {
      mysql_mutex_lock(&rli_->data_lock);
      const int set_error = rli_->set_rli_description_event(description.get());
      mysql_mutex_unlock(&rli_->data_lock);
      if (set_error != 0)
        return fail(error, "cannot install native recovery FDE");
      (void)description.release();
    }

    THD *thd = auto_thd_->thd;
    if (install_recovery_commit_authorization(thd, binding, error))
      return false;
    bool applied = true;
    for (BufferedEvent &buffered : events) {
      Log_event *event = buffered.event.get();
      mysql_mutex_lock(&rli_->data_lock);
      rli_->set_event_start_pos(static_cast<my_off_t>(buffered.start));
      rli_->set_event_relay_log_name(transaction.first_event.file.c_str());
      rli_->set_event_relay_log_pos(buffered.start);
      rli_->set_future_event_relay_log_pos(buffered.end);
      mysql_mutex_unlock(&rli_->data_lock);
      event->thd = thd;
      RLI_current_event_raii current_event(rli_.get(), event);
      const int apply_error = event->apply_event(rli_.get());
      if (event->get_type_code() ==
          mysql::binlog::event::ROWS_QUERY_LOG_EVENT)
        (void)buffered.event.release();
      if (apply_error != 0 || thd->is_slave_error || thd->is_error()) {
        applied = false;
        if (error != nullptr && error->empty())
          *error = "Log_event::apply_event failed during native recovery";
        break;
      }
    }
    if (!applied) {
      discard_recovery_commit_authorization(thd);
      return false;
    }
    if (finish_recovery_commit_authorization(
            thd, transaction.requires_durable_authorization, error))
      return false;
    return true;
  }

  bool finish_session(std::string *error) override {
    if (!started_) return fail(error, "native recovery THD was not started");
    THD *thd = auto_thd_->thd;
    discard_recovery_commit_authorization(thd);
    if (rli_->is_in_group() ||
        thd->get_transaction()->is_active(Transaction_ctx::SESSION) ||
        !thd->owned_gtid_is_empty())
      rli_->cleanup_context(thd, true);
    rli_->slave_close_thread_tables(thd);
    const bool unfinished =
        rli_->is_in_group() ||
        thd->get_transaction()->is_active(Transaction_ctx::SESSION) ||
        !thd->owned_gtid_is_empty();
    restore_background_thd();
    started_ = false;
    if (unfinished)
      return fail(error,
                  "native recovery THD retained an unfinished transaction");
    return true;
  }

  ~MysqlNativeRecoveryExecutor() override {
    if (started_) {
      std::string ignored;
      (void)finish_session(&ignored);
    }
  }

 private:
  struct BufferedEvent {
    uint64_t start{0};
    uint64_t end{0};
    std::unique_ptr<Log_event> event;
  };

  NativeRecoveryScanResult scan_segment(
      const fs::path &path, const SegmentRef &segment,
      std::string_view expected_next_file,
      std::vector<NativeRecoveryTransaction> *transactions) {
    std::error_code filesystem_error;
    const uint64_t file_size = fs::file_size(path, filesystem_error);
    if (filesystem_error || segment.source.end_pos > file_size)
      return scan_failure(NativeRecoveryScanOutcome::LOCAL_IO_ERROR,
                          "native segment is outside its materialized file");

    Binlog_file_reader reader(true, max_event_bytes_);
    const my_off_t scan_start = static_cast<my_off_t>(
        segment.source.start_pos == 0 ? BIN_LOG_HEADER_SIZE
                                      : segment.source.start_pos);
    if (reader.open(path.c_str(), scan_start))
      return scan_failure(NativeRecoveryScanOutcome::LOCAL_IO_ERROR,
                          std::string("cannot open native recovery range: ") +
                              reader.get_error_str());

    mysql::binlog::event::Transaction_boundary_parser parser(
        mysql::binlog::event::Transaction_boundary_parser::
            TRX_BOUNDARY_PARSER_RECEIVER);
    std::optional<CurrentTransaction> current;
    std::vector<std::string> gtids;
    std::vector<uint64_t> xids;
    uint64_t transaction_count = 0;
    bool saw_rotate = false;
    while (static_cast<uint64_t>(reader.position()) < segment.source.end_pos) {
      const uint64_t event_start = static_cast<uint64_t>(reader.position());
      std::unique_ptr<Log_event> event(reader.read_event_object());
      if (event == nullptr)
        return scan_failure(NativeRecoveryScanOutcome::LOCAL_IO_ERROR,
                            std::string("cannot decode recovery event: ") +
                                reader.get_error_str());
      const uint64_t event_end = static_cast<uint64_t>(reader.position());
      if (event_end <= event_start || event_end > segment.source.end_pos ||
          !event->is_valid())
        return scan_failure(NativeRecoveryScanOutcome::CORRUPT,
                            "native segment ends inside an event");

      const auto type = event->get_type_code();
      if (type == mysql::binlog::event::ANONYMOUS_GTID_LOG_EVENT ||
          type == mysql::binlog::event::XA_PREPARE_LOG_EVENT ||
          type == mysql::binlog::event::TRANSACTION_PAYLOAD_EVENT)
        return scan_failure(NativeRecoveryScanOutcome::CORRUPT,
                            "native recovery contains a disallowed event");

      const auto [info_error, event_info] =
          extract_log_event_basic_info(event.get());
      if (info_error || parser.feed_event(event_info, false))
        return scan_failure(NativeRecoveryScanOutcome::CORRUPT,
                            "native recovery transaction ordering is invalid");

      if (mysql::binlog::event::Log_event_type_helper::
              is_assigned_gtid_event(type)) {
        if (current.has_value())
          return scan_failure(NativeRecoveryScanOutcome::CORRUPT,
                              "nested GTID in native recovery transaction");
        const auto *gtid = dynamic_cast<const Gtid_log_event *>(event.get());
        CurrentTransaction started;
        std::string gtid_error;
        if (gtid == nullptr || !append_gtid(*gtid, &started.gtid, &gtid_error))
          return scan_failure(NativeRecoveryScanOutcome::CORRUPT,
                              std::move(gtid_error));
        started.value.first_event = {segment.source.file, event_start};
        if (!gtid_digest(started.gtid, &started.value.gtid, &gtid_error))
          return scan_failure(NativeRecoveryScanOutcome::CORRUPT,
                              std::move(gtid_error));
        gtids.push_back(started.gtid);
        current = std::move(started);
      } else if (!current.has_value()) {
        if (!is_metadata_event(type))
          return scan_failure(
              NativeRecoveryScanOutcome::CORRUPT,
              "native recovery event outside an assigned-GTID transaction");
        if (type == mysql::binlog::event::ROTATE_EVENT) {
          const auto *rotate =
              dynamic_cast<const Rotate_log_event *>(event.get());
          const std::string target =
              rotate != nullptr && rotate->new_log_ident != nullptr
                  ? std::string(rotate->new_log_ident, rotate->ident_len)
                  : std::string();
          if (saw_rotate || expected_next_file.empty() || rotate == nullptr ||
              rotate->pos != BIN_LOG_HEADER_SIZE ||
              target != expected_next_file ||
              event_end != segment.source.end_pos)
            return scan_failure(NativeRecoveryScanOutcome::CORRUPT,
                                "native recovery Rotate is not the exact "
                                "terminal next-file boundary");
          saw_rotate = true;
        }
      } else {
        if (type == mysql::binlog::event::QUERY_EVENT) {
          const auto *query = dynamic_cast<const Query_log_event *>(event.get());
          const std::string_view text =
              query == nullptr ? std::string_view()
                               : std::string_view(query->query, query->q_len);
          if (query == nullptr || ascii_starts_token(text, "XA") ||
              ascii_equal(trim_ascii(text), "ROLLBACK"))
            return scan_failure(NativeRecoveryScanOutcome::CORRUPT,
                                "native recovery contains disallowed SQL");
          if (!ascii_equal(trim_ascii(text), "BEGIN") &&
              !ascii_equal(trim_ascii(text), "COMMIT"))
            current->saw_mutation = true;
        } else if (type == mysql::binlog::event::XID_EVENT) {
          const auto *xid = dynamic_cast<const Xid_log_event *>(event.get());
          if (xid == nullptr || current->value.xid.has_value())
            return scan_failure(NativeRecoveryScanOutcome::CORRUPT,
                                "native recovery XID is invalid or repeated");
          current->value.xid = xid->xid;
          xids.push_back(xid->xid);
          current->saw_mutation = true;
        } else if (is_row_mutation(type)) {
          current->saw_mutation = true;
        }
      }

      if (current.has_value() && parser.is_not_inside_transaction()) {
        if (type != mysql::binlog::event::QUERY_EVENT &&
            type != mysql::binlog::event::XID_EVENT)
          return scan_failure(NativeRecoveryScanOutcome::CORRUPT,
                              "transaction has an invalid terminal event");
        current->value.endpoint = {segment.source.file, event_end};
        current->value.requires_durable_authorization = current->saw_mutation;
        transactions->push_back(std::move(current->value));
        current.reset();
        ++transaction_count;
      }
    }

    if (static_cast<uint64_t>(reader.position()) != segment.source.end_pos)
      return scan_failure(NativeRecoveryScanOutcome::CORRUPT,
                          "native recovery reader missed the exact bound");
    if (current.has_value() || parser.is_inside_transaction())
      return scan_failure(NativeRecoveryScanOutcome::INCOMPLETE_TRANSACTION,
                          "native recovery segment ends inside a transaction");
    if (parser.is_error())
      return scan_failure(NativeRecoveryScanOutcome::CORRUPT,
                          "native recovery boundary parser is in error");
    if (!expected_next_file.empty() && !saw_rotate)
      return scan_failure(NativeRecoveryScanOutcome::CORRUPT,
                          "cross-file native segment has no terminal Rotate");

    std::string joined_gtids;
    for (size_t index = 0; index < gtids.size(); ++index) {
      if (index != 0) joined_gtids.push_back(',');
      joined_gtids.append(gtids[index]);
    }
    GtidSetDigest gtid_digest_value;
    XidDigest xid_digest_value;
    std::string digest_error;
    if (!gtid_digest(joined_gtids, &gtid_digest_value, &digest_error) ||
        !xid_digest(xids, &xid_digest_value, &digest_error) ||
        transaction_count != segment.transaction_count ||
        gtid_digest_value != segment.gtid_set ||
        xid_digest_value != segment.xids)
      return scan_failure(
          NativeRecoveryScanOutcome::CORRUPT,
          digest_error.empty()
              ? "native segment GTID/XID metadata differs from candidate"
              : std::move(digest_error));
    return {NativeRecoveryScanOutcome::READY, {}};
  }

  bool read_complete_transaction(
      const fs::path &path, const NativeRecoveryTransaction &transaction,
      Format_description_log_event **fdle, std::vector<BufferedEvent> *events,
      std::string *error) {
    Binlog_file_reader reader(true, max_event_bytes_);
    if (reader.open(path.c_str(), static_cast<my_off_t>(transaction.first_event.pos),
                    fdle))
      return fail(error, std::string("cannot reopen recovery transaction: ") +
                             reader.get_error_str());
    if (*fdle == nullptr)
      return fail(error, "recovery transaction has no preceding FDE");

    mysql::binlog::event::Transaction_boundary_parser parser(
        mysql::binlog::event::Transaction_boundary_parser::
            TRX_BOUNDARY_PARSER_RECEIVER);
    std::string observed_gtid;
    std::optional<uint64_t> observed_xid;
    while (static_cast<uint64_t>(reader.position()) < transaction.endpoint.pos) {
      const uint64_t start = static_cast<uint64_t>(reader.position());
      std::unique_ptr<Log_event> event(reader.read_event_object());
      if (event == nullptr)
        return fail(error, std::string("cannot reread recovery event: ") +
                               reader.get_error_str());
      const uint64_t end = static_cast<uint64_t>(reader.position());
      if (end <= start || end > transaction.endpoint.pos)
        return fail(error, "recovery transaction changed after validation");
      const auto type = event->get_type_code();
      const auto [info_error, event_info] =
          extract_log_event_basic_info(event.get());
      if (info_error || parser.feed_event(event_info, false))
        return fail(error, "recovery transaction ordering changed");
      if (mysql::binlog::event::Log_event_type_helper::
              is_assigned_gtid_event(type)) {
        const auto *gtid = dynamic_cast<const Gtid_log_event *>(event.get());
        if (!observed_gtid.empty() || gtid == nullptr ||
            !append_gtid(*gtid, &observed_gtid, error))
          return false;
      } else if (type == mysql::binlog::event::XID_EVENT) {
        const auto *xid = dynamic_cast<const Xid_log_event *>(event.get());
        if (xid == nullptr || observed_xid.has_value())
          return fail(error, "recovery transaction XID changed");
        observed_xid = xid->xid;
      } else if (type == mysql::binlog::event::ANONYMOUS_GTID_LOG_EVENT ||
                 type == mysql::binlog::event::XA_PREPARE_LOG_EVENT ||
                 type == mysql::binlog::event::TRANSACTION_PAYLOAD_EVENT)
        return fail(error, "recovery transaction became disallowed");
      events->push_back({start, end, std::move(event)});
    }
    GtidSetDigest observed_digest;
    if (static_cast<uint64_t>(reader.position()) != transaction.endpoint.pos ||
        parser.is_error() || !parser.is_not_inside_transaction() ||
        !gtid_digest(observed_gtid, &observed_digest, error) ||
        observed_digest != transaction.gtid || observed_xid != transaction.xid)
      return fail(error, "recovery transaction changed after first-pass proof");
    return true;
  }

  void restore_background_thd() {
    if (auto_thd_ == nullptr) return;
    THD *thd = auto_thd_->thd;
    if (thd != nullptr) {
      discard_recovery_commit_authorization(thd);
      thd->rli_slave = nullptr;
    }
    if (rli_ != nullptr) rli_->set_filter(nullptr);
    rli_.reset();
    filter_.reset();
    if (thd != nullptr) {
      thd->slave_thread = false;
      thd->system_thread = SYSTEM_THREAD_BACKGROUND;
    }
    auto_thd_.reset();
  }

  std::unordered_map<std::string, fs::path> files_;
  uint32_t max_event_bytes_{0};
  std::unique_ptr<Auto_THD> auto_thd_;
  std::unique_ptr<Relay_log_info> rli_;
  std::unique_ptr<Rpl_filter> filter_;
  bool started_{false};
};

}  // namespace

NativeRecoveryResult replay_bounded_native_tail(
    const NativeRecoveryRequest &request,
    NativeRecoveryPreparedVerifier *prepared_verifier) {
  MysqlNativeRecoveryExecutor executor;
  return run_bounded_native_recovery(request, &executor, prepared_verifier);
}

#ifdef WESQL_TEST
bool exercise_native_recovery_query_context_for_test(std::string *error) {
  MysqlNativeRecoveryExecutor executor;
  if (!executor.start_session(error)) return false;
  THD *thd = current_thd;
  Relay_log_info *rli = thd == nullptr ? nullptr : thd->rli_slave;
  if (rli == nullptr || rli->current_mts_submode == nullptr ||
      rli->is_parallel_exec()) {
    std::string ignored;
    (void)executor.finish_session(&ignored);
    return fail(error, "serial recovery Query context was not initialized");
  }
  constexpr char statement[] = "CREATE DATABASE recovery_context_test";
  Query_log_event event(thd, statement, sizeof(statement) - 1, false, true,
                        true, 0);
  event.attach_temp_tables_worker(thd, rli);
  event.detach_temp_tables_worker(thd, rli);
  return executor.finish_session(error);
}
#endif
#endif  // WESQL_NATIVE_RECOVERY_STATE_MACHINE_TEST

}  // namespace wesql::remote_commit
