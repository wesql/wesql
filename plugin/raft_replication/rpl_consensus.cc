/*
   Portions Copyright (c) 2024, ApeCloud Inc Holding Limited

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

#include "rpl_consensus.h"
#include "bl_consensus_log.h"
#include "my_thread.h"
#include "system_variables.h"

#include "paxos.h"
#include "paxos_log.h"
#include "paxos_server.h"

std::shared_ptr<BLConsensusLog> consensus_log =
    std::make_shared<BLConsensusLog>();
std::shared_ptr<alisql::AliSQLServer> alisql_server;
alisql::Paxos *consensus_ptr = nullptr;
std::atomic<bool> rpl_consensus_inited = false;
std::atomic<bool> rpl_consensus_ready = false;

extern void stateChangeCb(StateType state, uint64_t term, uint64_t commitIndex);
extern uint32 binlog_checksum_crc32_callback(uint32 crc,
                                             const unsigned char *pos,
                                             size_t length);

static void state_change_callback(alisql::Paxos::StateType state, uint64_t term,
                                  uint64_t commitIndex) {
  stateChangeCb(static_cast<StateType>(state), term, commitIndex);
}

int rpl_consensus_init(bool is_learner, uint64 mock_start_index,
                       ConsensusLogManager *consensus_log_manager,
                       ConsensusMeta *consensus_meta,
                       ConsensusStateProcess *consensus_state_process) {
  std::string empty_str = "";
  std::vector<std::string> cluster_str_config;
  int error = 0;

  consensus_log->init(mock_start_index, consensus_log_manager, consensus_meta,
                      consensus_state_process);
  alisql_server = std::make_shared<alisql::AliSQLServer>(0);
  consensus_ptr =
      new alisql::Paxos(opt_consensus_election_timeout, consensus_log);
  consensus_ptr->setStateChangeCb(state_change_callback);
  consensus_ptr->setMaxPacketSize(opt_consensus_max_packet_size);
  consensus_ptr->setPipeliningTimeout(opt_consensus_pipelining_timeout);
  consensus_ptr->setLargeBatchRatio(opt_consensus_large_batch_ratio);
  consensus_ptr->setMaxDelayIndex(opt_consensus_max_delay_index);
  consensus_ptr->setMinDelayIndex(opt_consensus_min_delay_index);
  consensus_ptr->setSyncFollowerMetaInterval(
      opt_consensus_sync_follower_meta_interval);
  consensus_ptr->setConsensusAsync(opt_weak_consensus_mode);
  consensus_ptr->setReplicateWithCacheLog(
      opt_consensus_replicate_with_cache_log);
  consensus_ptr->setAlertLogLevel(
      alisql::Paxos::AlertLogLevel(opt_consensus_log_level + 3));
  consensus_ptr->setForceSyncEpochDiff(opt_consensus_force_sync_epoch_diff);
  consensus_ptr->setChecksumMode(opt_consensus_checksum);
  consensus_ptr->setChecksumCb(binlog_checksum_crc32_callback);
  consensus_ptr->setWorkerCb(my_thread_init, my_thread_end);
  consensus_ptr->setConfigureChangeTimeout(
      opt_consensus_configure_change_timeout);
  consensus_ptr->setMaxDelayIndex4NewMember(
      opt_consensus_new_follower_threshold);
  consensus_ptr->setEnableDynamicEasyIndex(opt_consensus_dynamic_easyindex);
  consensus_ptr->setEnableLearnerPipelining(opt_consensus_learner_pipelining);
  consensus_ptr->setEnableLearnerHeartbeat(opt_consensus_learner_heartbeat);
  consensus_ptr->setEnableAutoResetMatchIndex(
      opt_consensus_auto_reset_match_index);
  consensus_ptr->setEnableAutoLeaderTransfer(
      opt_consensus_auto_leader_transfer);
  consensus_ptr->setAutoLeaderTransferCheckSeconds(
      opt_consensus_auto_leader_transfer_check_seconds);
  if (!is_learner) {
    // startup as normal node
    error = consensus_ptr->init(
        cluster_str_config, 0, nullptr, opt_consensus_io_thread_cnt,
        opt_consensus_worker_thread_cnt, alisql_server,
        opt_consensus_easy_pool_size, opt_consensus_heartbeat_thread_cnt);
  } else {
    // startup as learner node, config string arg pass empty
    error = consensus_ptr->initAsLearner(
        empty_str, nullptr, opt_consensus_io_thread_cnt,
        opt_consensus_worker_thread_cnt, alisql_server,
        opt_consensus_easy_pool_size, opt_consensus_heartbeat_thread_cnt);
  }

  if (!error) {
    consensus_ptr->initAutoPurgeLog(false, false,
                                    nullptr);  // disable autoPurge
    consensus_ptr->setAsLogType(opt_cluster_log_type_instance);
  }

  if (!error && opt_cluster_force_single_mode)  // use nuclear weapon
    consensus_ptr->forceSingleLeader();

  if (!error) rpl_consensus_inited = true;

  return error;
}

void rpl_consensus_set_ready() {
  if (rpl_consensus_inited && consensus_ptr != nullptr)
    rpl_consensus_ready = true;
}

bool rpl_consensus_is_ready() { return rpl_consensus_ready; }

void rpl_consensus_shutdown() {
  if (rpl_consensus_inited && !consensus_ptr->isShutdown()) {
    consensus_ptr->shutdown();
  }
}

void rpl_consensus_cleanup() {
  if (consensus_ptr) delete consensus_ptr;
  rpl_consensus_inited = false;
  consensus_ptr = nullptr;
}

bool rpl_consensus_is_shutdown() {
  return (!rpl_consensus_inited || consensus_ptr->isShutdown());
}

bool rpl_consensus_is_leader() {
  return (consensus_ptr->getState() == alisql::Paxos::LEADER);
}

uint64 rpl_consensus_check_commit_index(uint64 index, uint64 term) {
  return consensus_ptr->checkCommitIndex(index - 1, term);
}

uint64 rpl_consensus_wait_commit_index_update(uint64 log_index, uint64 term) {
  return consensus_ptr->waitCommitIndexUpdate(log_index - 1, term);
}

uint64 rpl_consensus_timed_wait_commit_index_update(uint64 log_index,
                                                    uint64 term,
                                                    uint64 timeout) {
  return consensus_ptr->timedWaitCommitIndexUpdate(log_index - 1, term,
                                                   timeout);
}

void rpl_consensus_set_last_noncommit_dep_index(uint64 log_index) {
  alisql_server->setLastNonCommitDepIndex(log_index);
}

void rpl_consensus_update_applied_index(uint64 log_index) {
  consensus_ptr->updateAppliedIndex(log_index);
}

uint64 rpl_consensus_get_applied_index() {
  return consensus_ptr->getAppliedIndex();
}

uint64 rpl_consensus_get_term() { return consensus_ptr->getTerm(); }

uint64 rpl_consensus_get_commit_index() {
  return consensus_ptr->getCommitIndex();
}

uint64 rpl_consensus_log_get_term() { return consensus_log->getCurrentTerm(); }

uint64 rpl_consensus_get_leader_term() {
  uint64 term = 0;
  alisql::LogEntry log_entry;
  // use fake buf_size, tell consenesus layer, the entry is not noop
  BLConsensusLog::packLogEntry(
      reinterpret_cast<uchar *>(const_cast<char *>("any")), 3, term, 0,
      BLConsensusLog::UNCERTAIN, log_entry);
  consensus_ptr->replicateLog(log_entry);
  term = log_entry.term();

  return term;
}

void rpl_consensus_write_log_done_internal(uint64 log_index, bool force_send) {
  alisql_server->writeLogDoneInternal(log_index, force_send);
}

void rpl_consensus_write_log_done(uint64 log_index) {
  alisql_server->writeLogDone(log_index);
}

void rpl_consensus_write_log_cache_done() {
  alisql_server->writeCacheLogDone();
}

void rpl_consensus_set_checksum_mode(bool mode) {
  if (consensus_ptr) consensus_ptr->setChecksumMode(mode);
}

void rpl_consensus_set_disable_election(bool disable_election,
                                        bool disable_stepdown) {
  if (consensus_ptr) {
    consensus_ptr->debugDisableElection = disable_election;
    consensus_ptr->debugDisableStepDown = disable_stepdown;
  }
}

void rpl_consensus_set_enable_dynamic_easy_index(
    bool consensus_dynamic_easyindex) {
  if (consensus_ptr)
    consensus_ptr->setEnableDynamicEasyIndex(consensus_dynamic_easyindex);
}

void rpl_consensus_set_replicate_with_cache_log(
    bool consensus_replicate_with_cache_log) {
  if (consensus_ptr)
    consensus_ptr->setReplicateWithCacheLog(consensus_replicate_with_cache_log);
}

void rpl_consensus_set_compact_old_mode(bool consensus_old_compact_mode) {
  if (consensus_ptr)
    consensus_ptr->setCompactOldMode(consensus_old_compact_mode);
}

void rpl_consensus_set_force_sync_epoch_diff(
    uint64 consensus_force_sync_epoch_diff) {
  if (consensus_ptr)
    consensus_ptr->setForceSyncEpochDiff(consensus_force_sync_epoch_diff);
}

void rpl_consensus_set_new_follower_threshold(
    uint64 consensus_new_follower_threshold) {
  if (consensus_ptr)
    consensus_ptr->setMaxDelayIndex4NewMember(consensus_new_follower_threshold);
}

void rpl_consensus_set_send_packet_timeout(uint64 consensus_send_timeout) {
  if (consensus_ptr)
    consensus_ptr->setSendPacketTimeout(consensus_send_timeout);
}

void rpl_consensus_set_send_learner_timeout(uint64 consensus_learner_timeout) {
  if (consensus_ptr)
    consensus_ptr->setLearnerConnTimeout(consensus_learner_timeout);
}

void rpl_consensus_set_enable_learner_pipelining(
    bool consensus_learner_pipelining) {
  if (consensus_ptr)
    consensus_ptr->setEnableLearnerPipelining(consensus_learner_pipelining);
}

void rpl_consensus_set_configure_change_timeout(
    uint64 consensus_configure_change_timeout) {
  if (consensus_ptr)
    consensus_ptr->setConfigureChangeTimeout(
        consensus_configure_change_timeout);
}

void rpl_consensus_set_max_packet_size(uint64 consensus_max_packet_size) {
  if (consensus_ptr) consensus_ptr->setMaxPacketSize(consensus_max_packet_size);
}

void rpl_consensus_reset_msg_compress_option() {
  if (consensus_ptr) consensus_ptr->resetMsgCompressOption();
}

int rpl_consensus_set_msg_compress_option(int type, size_t threshold,
                                          bool checksum,
                                          const std::string &address) {
  if (!consensus_ptr) return 1;
  return consensus_ptr->setMsgCompressOption(type, threshold, checksum,
                                             address);
}

void rpl_consensus_set_pipelining_timeout(uint64 consensus_pipelining_timeout) {
  if (consensus_ptr)
    consensus_ptr->setPipeliningTimeout(consensus_pipelining_timeout);
}

void rpl_consensus_set_large_batch_ratio(uint64 consensus_large_batch_ratio) {
  if (consensus_ptr)
    consensus_ptr->setLargeBatchRatio(consensus_large_batch_ratio);
}

void rpl_consensus_set_max_delay_index(uint64 consensus_max_delay_index) {
  if (consensus_ptr) consensus_ptr->setMaxDelayIndex(consensus_max_delay_index);
}

void rpl_consensus_set_min_delay_index(uint64 consensus_min_delay_index) {
  if (consensus_ptr) consensus_ptr->setMinDelayIndex(consensus_min_delay_index);
}

void rpl_consensus_set_optimistic_heartbeat(
    bool consensus_optimistic_heartbeat) {
  if (consensus_ptr)
    consensus_ptr->setOptimisticHeartbeat(consensus_optimistic_heartbeat);
}

void rpl_consensus_set_sync_follower_meta_interval(
    uint64 consensus_sync_follower_meta_interval) {
  if (consensus_ptr)
    consensus_ptr->setSyncFollowerMetaInterval(
        consensus_sync_follower_meta_interval);
}

void rpl_consensus_reset_flow_control() {
  if (consensus_ptr) consensus_ptr->reset_flow_control();
}

uint64 rpl_consensus_get_server_id(const std::string &addr) {
  return consensus_ptr->getServerIdFromAddr(addr);
}

void rpl_consensus_set_flow_control(uint64 server_id, int64_t fc) {
  if (consensus_ptr) consensus_ptr->set_flow_control(server_id, fc);
}

void rpl_consensus_set_alert_log_level(uint64 consensus_log_level) {
  if (consensus_ptr)
    consensus_ptr->setAlertLogLevel(
        alisql::Paxos::AlertLogLevel(consensus_log_level + 3));
}

void rpl_consensus_force_promote() { consensus_ptr->forcePromote(); }

void rpl_consensus_set_enable_auto_reset_match_index(
    bool consensus_auto_reset_match_index) {
  if (consensus_ptr)
    consensus_ptr->setEnableAutoResetMatchIndex(
        consensus_auto_reset_match_index);
}

void rpl_consensus_set_enable_learner_heartbeat(
    bool consensus_learner_heartbeat) {
  if (consensus_ptr)
    consensus_ptr->setEnableLearnerHeartbeat(consensus_learner_heartbeat);
}

void rpl_consensus_set_enable_auto_leader_transfer(
    bool consensus_auto_leader_transfer) {
  if (consensus_ptr)
    consensus_ptr->setEnableAutoLeaderTransfer(consensus_auto_leader_transfer);
}

void rpl_consensus_set_auto_leader_transfer_check_seconds(
    uint64 consensus_auto_leader_transfer_check_seconds) {
  if (consensus_ptr)
    consensus_ptr->setAutoLeaderTransferCheckSeconds(
        consensus_auto_leader_transfer_check_seconds);
}

uint64 rpl_consensus_get_easy_alloc_byte() {
  return (uint64)alisql::easy_pool_alloc_byte;
}

uint rpl_consensus_get_cluster_global_info(
    std::vector<cluster_global_info> *cgis, bool check_leader) {
  std::vector<alisql::Paxos::ClusterInfoType> cis;

  if (check_leader) {
    alisql::Paxos::MemberInfoType mi;
    consensus_ptr->getMemberInfo(&mi);
    // if node is not LEADER, global information is empty
    if (mi.role != alisql::Paxos::StateType::LEADER) {
      return 0;
    }
  }

  consensus_ptr->getClusterInfo(cis);
  for (auto e : cis) {
    cluster_global_info cgi;
    cgi.server_id = e.serverId;
    cgi.ip_port = e.ipPort;
    cgi.match_index = e.matchIndex;
    cgi.next_index = e.nextIndex;

    switch (e.role) {
      case alisql::Paxos::StateType::FOLLOWER:
        cgi.role = "Follower";
        break;
      case alisql::Paxos::StateType::CANDIDATE:
        cgi.role = "Candidate";
        break;
      case alisql::Paxos::StateType::LEADER:
        cgi.role = "Leader";
        break;
      case alisql::Paxos::StateType::LEARNER:
        cgi.role = "Learner";
        break;
      default:
        cgi.role = "No Role";
        break;
    }
    cgi.has_voted = e.hasVoted;
    cgi.force_sync = e.forceSync;
    cgi.election_weight = e.electionWeight;
    cgi.applied_index = e.appliedIndex;
    cgi.learner_source = e.learnerSource;
    cgi.pipelining = e.pipelining;
    cgi.use_applied = e.useApplied;
    cgis->push_back(std::move(cgi));
  }
  return cis.size();
}

void rpl_consensus_get_cluster_local_info(cluster_local_info *cli) {
  alisql::Paxos::MemberInfoType mi;
  consensus_ptr->getMemberInfo(&mi);
  cli->server_id = mi.serverId;
  cli->current_term = mi.currentTerm;
  cli->current_leader_ip_port = mi.currentLeaderAddr;
  cli->commit_index = mi.commitIndex;
  cli->last_apply_index = mi.lastAppliedIndex;
  cli->last_log_term = mi.lastLogTerm;
  cli->last_log_index = mi.lastLogIndex;
  switch (mi.role) {
    case alisql::Paxos::StateType::FOLLOWER:
      cli->role = "Follower";
      break;
    case alisql::Paxos::StateType::CANDIDATE:
      cli->role = "Candidate";
      break;
    case alisql::Paxos::StateType::LEADER:
      cli->role = "Leader";
      break;
    case alisql::Paxos::StateType::LEARNER:
      cli->role = "Learner";
      break;
    default:
      cli->role = "No Role";
      break;
  }
  cli->voted_for = mi.votedFor;
}

uint rpl_consensus_get_helthy_info(std::vector<cluster_helthy_info> *chis) {
  cluster_helthy_info chi;
  std::vector<alisql::Paxos::HealthInfoType> hi;
  if (consensus_ptr->getClusterHealthInfo(hi)) return 0;

  for (auto e : hi) {
    chi.server_id = e.serverId;
    chi.addr = e.addr;
    chi.connected = e.connected;
    chi.log_delay = e.logDelayNum;
    chi.apply_delay = e.applyDelayNum;
    switch (e.role) {
      case alisql::Paxos::StateType::FOLLOWER:
        chi.role = "Follower";
        break;
      case alisql::Paxos::StateType::CANDIDATE:
        chi.role = "Candidate";
        break;
      case alisql::Paxos::StateType::LEADER:
        chi.role = "Leader";
        break;
      case alisql::Paxos::StateType::LEARNER:
        chi.role = "Learner";
        break;
      default:
        chi.role = "No Role";
        break;
    }
    chis->push_back(std::move(chi));
  }
  return hi.size();
}

void rpl_consensus_get_stats(cluster_stats *cs) {
  const alisql::Paxos::StatsType &stats = consensus_ptr->getStats();
  const alisql::PaxosLog::StatsType &log_stats = consensus_ptr->getLogStats();
  cs->count_msg_append_log = stats.countMsgAppendLog;
  cs->count_msg_request_vote = stats.countMsgRequestVote;
  cs->count_heartbeat = stats.countHeartbeat;
  cs->count_on_msg_append_log = stats.countOnMsgAppendLog;
  cs->count_on_msg_request_vote = stats.countOnMsgRequestVote;
  cs->count_on_heartbeat = stats.countOnHeartbeat;
  cs->count_replicate_log = stats.countReplicateLog;
  cs->count_log_meta_get_in_cache = log_stats.countMetaGetInCache;
  cs->count_log_meta_get_total = log_stats.countMetaGetTotal;
}

void rpl_consensus_get_member_changes(
    std::vector<cluster_memebership_change> *cmcs) {
  std::vector<alisql::Paxos::MembershipChangeType> mch =
      consensus_ptr->getMembershipChangeHistory();

  for (auto &e : mch) {
    cluster_memebership_change cmc;
    std::string command;

    e.address = "'" + e.address + "'";
    if (e.cctype == alisql::Consensus::CCOpType::CCMemberOp) {
      switch (e.optype) {
        case alisql::Consensus::CCOpType::CCAddNode:
          command = "change consensus_learner " + e.address +
                    " to consensus_follower";
          break;
        case alisql::Consensus::CCOpType::CCDelNode:
          command = "drop consensus_follower " + e.address;
          break;
        case alisql::Consensus::CCOpType::CCDowngradeNode:
          command = "change consensus_follower " + e.address +
                    " to consensus_learner";
          break;
        case alisql::Consensus::CCOpType::CCConfigureNode:
          command =
              "change consensus_node " + e.address + " consensus_force_sync " +
              (e.forceSync ? "true" : "false") + " consensus_election_weight " +
              std::to_string(e.electionWeight);
          break;
        case alisql::Consensus::CCOpType::CCLeaderTransfer:
          command = "change consensus_leader to " + e.address;
          break;
        default:
          break;
      }
    } else {  // alisql::Consensus::CCOpType::CCLearnerOp
      switch (e.optype) {
        case alisql::Consensus::CCOpType::CCAddNode:
        case alisql::Consensus::CCOpType::CCAddLearnerAutoChange:
          command = "add consensus_learner " + e.address;
          break;
        case alisql::Consensus::CCOpType::CCDelNode:
          command = "drop consensus_learner " + e.address;
          break;
        case alisql::Consensus::CCOpType::CCConfigureNode:
          if (e.learnerSource.size())
            e.learnerSource = "'" + e.learnerSource + "'";
          command = "change consensus_node " + e.address +
                    " consensus_learner_source " + e.learnerSource +
                    " consensus_use_applyindex " +
                    (e.sendByAppliedIndex ? "true" : "false");
          break;
        case alisql::Consensus::CCOpType::CCSyncLearnerAll:
          command = "change consensus_learner for consensus_meta";
          break;
        default:
          break;
      }
    }
    command += ";";

    cmc.command = command;
    cmc.time = e.time;
    cmcs->push_back(std::move(cmc));
  }
}

int rpl_consensus_transfer_leader_by_id(uint64 target_id) {
  return consensus_ptr->leaderTransfer(target_id);
}

int rpl_consensus_transfer_leader(const std::string &ip_port) {
  return consensus_ptr->leaderTransfer(ip_port);
}

int rpl_consensus_add_learners(std::vector<std::string> &info_vector) {
  return consensus_ptr->changeLearners(alisql::Paxos::CCOpType::CCAddNode,
                                       info_vector);
}

int rpl_consensus_drop_learners(std::vector<std::string> &info_vector) {
  return consensus_ptr->changeLearners(alisql::Paxos::CCOpType::CCDelNode,
                                       info_vector);
}

int rpl_consensus_upgrade_learner(std::string &addr) {
  return consensus_ptr->changeMember(alisql::Paxos::CCOpType::CCAddNode, addr);
}

int rpl_consensus_get_cluster_size() { return consensus_ptr->getClusterSize(); }

int rpl_consensus_add_follower(std::string &addr) {
  return consensus_ptr->changeMember(
      alisql::Paxos::CCOpType::CCAddLearnerAutoChange, addr);
}

int rpl_consensus_downgrade_follower(const std::string &addr) {
  return consensus_ptr->downgradeMember(addr);
}

int rpl_consensus_sync_all_learners(std::vector<std::string> &info_vector) {
  return consensus_ptr->changeLearners(alisql::Paxos::CCSyncLearnerAll,
                                       info_vector);
}

int rpl_consensus_configure_member(const std::string &addr, bool force_sync,
                                   uint election_weight) {
  return consensus_ptr->configureMember(addr, force_sync, election_weight);
}

int rpl_consensus_set_cluster_id(const std::string &cluster_id) {
  return consensus_ptr->setClusterId(cluster_id);
}

void rpl_consensus_force_fix_match_index(const std::string &addr,
                                         uint64 match_index) {
  return consensus_ptr->forceFixMatchIndex(addr, match_index);
}

int rpl_consensus_force_single_leader() {
  return consensus_ptr->forceSingleLeader();
}

int rpl_consensus_configure_learner(const std::string &addr,
                                    const std::string &source,
                                    bool use_applied) {
  return consensus_ptr->configureLearner(addr, source, use_applied);
}

int rpl_consensus_force_purge_log(bool local, uint64 index) {
  return consensus_ptr->forcePurgeLog(local, index);
}

const char *rpl_consensus_protocol_error(int error_code) {
  return alisql::pxserror(error_code);
}

const char *rpl_consensus_protocol_default_error() {
  return alisql::pxserror(alisql::PaxosErrorCode::PE_DEFAULT);
}
