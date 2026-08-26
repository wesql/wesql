/* Copyright (c) 2026, ApeCloud Inc Holding Limited. */

#include "sql/sys_vars.h"

#include <climits>

#include "my_io.h"
#include "mysql_version.h"
#include "sql/consistent_archive.h"
#include "sql/mysqld.h"
#include "sql/rpl_replica.h"

static char *wesql_version_ptr = nullptr;
static Sys_var_charptr Sys_wesql_version(
    "wesql_version", "Version of WeSQL",
    READ_ONLY GLOBAL_VAR(wesql_version_ptr), NO_CMD_LINE, IN_SYSTEM_CHARSET,
    DEFAULT(WESQL_VERSION));

static Sys_var_bool Sys_binlog_archive(
    "binlog_archive", "Enable binlog archive",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_binlog_archive), CMD_LINE(OPT_ARG),
    DEFAULT(true));
static Sys_var_charptr Sys_binlog_archive_dir(
    "binlog_archive_dir", "Local directory for binlog archive",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_binlog_archive_dir),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT(nullptr));
static Sys_var_bool Sys_binlog_archive_expire_auto_purge(
    "binlog_archive_expire_auto_purge",
    "Automatically purge expired archived binlogs",
    GLOBAL_VAR(opt_binlog_archive_expire_auto_purge), CMD_LINE(OPT_ARG),
    DEFAULT(true));
static Sys_var_ulong Sys_binlog_archive_expire_seconds(
    "binlog_archive_expire_seconds", "Archived binlog retention in seconds",
    GLOBAL_VAR(opt_binlog_archive_expire_seconds), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(0, 0xFFFFFFFF), DEFAULT(2592000), BLOCK_SIZE(1));
static Sys_var_ulong Sys_binlog_archive_slice_max_size(
    "binlog_archive_slice_max_size", "Maximum archived binlog slice size",
    GLOBAL_VAR(opt_binlog_archive_slice_max_size), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(IO_SIZE, 1024 * 1024L * 1024L), DEFAULT(4 * 1024L * 1024L),
    BLOCK_SIZE(IO_SIZE));
static Sys_var_ulong Sys_binlog_archive_period(
    "binlog_archive_period", "Binlog archive polling period in milliseconds",
    GLOBAL_VAR(opt_binlog_archive_period), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(10, ULONG_MAX), DEFAULT(1000), BLOCK_SIZE(1));
static Sys_var_ulong Sys_binlog_archive_parallel_workers(
    "binlog_archive_parallel_workers", "Parallel binlog upload workers",
    PERSIST_AS_READONLY GLOBAL_VAR(opt_binlog_archive_parallel_workers),
    CMD_LINE(REQUIRED_ARG), VALID_RANGE(1, MTS_MAX_WORKERS), DEFAULT(4),
    BLOCK_SIZE(1));

static Sys_var_bool Sys_binlog_archive_replica(
    "binlog_archive_replica", "Enable object-store binlog replay",
    PERSIST_AS_READONLY GLOBAL_VAR(opt_binlog_archive_replica),
    CMD_LINE(OPT_ARG), DEFAULT(false));
static Sys_var_ulong Sys_binlog_archive_replica_flush_period(
    "binlog_archive_replica_flush_period",
    "Object-store replay polling period in seconds",
    GLOBAL_VAR(opt_binlog_archive_replica_flush_period),
    CMD_LINE(REQUIRED_ARG), VALID_RANGE(1, ULONG_MAX), DEFAULT(1),
    BLOCK_SIZE(1));
static Sys_var_charptr Sys_binlog_archive_replica_source_log_file(
    "binlog_archive_replica_source_log_file",
    "Initial source binlog file for object-store replay",
    GLOBAL_VAR(opt_binlog_archive_replica_source_log_file),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT(nullptr));
static Sys_var_ulong Sys_binlog_archive_replica_source_log_pos(
    "binlog_archive_replica_source_log_pos",
    "Initial source binlog position for object-store replay",
    GLOBAL_VAR(opt_binlog_archive_replica_source_log_pos),
    CMD_LINE(REQUIRED_ARG), VALID_RANGE(1, ULONG_MAX), DEFAULT(4),
    BLOCK_SIZE(1));

static Sys_var_bool Sys_snapshot_archive(
    "snapshot_archive", "Enable consistent snapshot archive",
    NON_PERSIST GLOBAL_VAR(opt_consistent_snapshot_archive), CMD_LINE(OPT_ARG),
    DEFAULT(true));
static Sys_var_charptr Sys_snapshot_archive_dir(
    "snapshot_archive_dir", "Local directory for snapshot archive",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_consistent_snapshot_archive_dir),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT(nullptr));
static Sys_var_bool Sys_snapshot_archive_on_objectstore(
    "snapshot_archive_on_objectstore", "Persist snapshots in object storage",
    READ_ONLY NON_PERSIST
        GLOBAL_VAR(opt_consistent_snapshot_persistent_on_objstore),
    CMD_LINE(OPT_ARG), DEFAULT(true));
static Sys_var_ulong Sys_snapshot_archive_period(
    "snapshot_archive_period", "Snapshot archive period in seconds",
    GLOBAL_VAR(opt_consistent_snapshot_archive_period), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(2, LONG_TIMEOUT), DEFAULT(300), BLOCK_SIZE(1));
static Sys_var_bool Sys_snapshot_archive_expire_auto_purge(
    "snapshot_archive_expire_auto_purge",
    "Automatically purge expired snapshots",
    GLOBAL_VAR(opt_consistent_snapshot_expire_auto_purge), CMD_LINE(OPT_ARG),
    DEFAULT(true));
static Sys_var_ulong Sys_snapshot_archive_expire_seconds(
    "snapshot_archive_expire_seconds", "Snapshot retention in seconds",
    GLOBAL_VAR(opt_consistent_snapshot_expire_seconds),
    CMD_LINE(REQUIRED_ARG), VALID_RANGE(0, 0xFFFFFFFF), DEFAULT(0),
    BLOCK_SIZE(1));

static const char *consistent_snapshot_tar_names[] = {
    "OFF", "TAR", "TAR_AND_COMPRESS", NullS};
static Sys_var_enum Sys_snapshot_archive_innodb_tar_mode(
    "snapshot_archive_innodb_tar_mode", "InnoDB snapshot tar mode",
    GLOBAL_VAR(opt_consistent_snapshot_innodb_tar_mode),
    CMD_LINE(REQUIRED_ARG), consistent_snapshot_tar_names,
    DEFAULT(CONSISTENT_SNAPSHOT_NO_TAR));
static Sys_var_enum Sys_snapshot_archive_smartengine_tar_mode(
    "snapshot_archive_smartengine_tar_mode", "SmartEngine snapshot tar mode",
    GLOBAL_VAR(opt_consistent_snapshot_se_tar_mode), CMD_LINE(OPT_ARG),
    consistent_snapshot_tar_names, DEFAULT(CONSISTENT_SNAPSHOT_NO_TAR));
static Sys_var_bool Sys_snapshot_archive_smartengine_backup_checkpoint(
    "snapshot_archive_smartengine_backup_checkpoint",
    "Checkpoint SmartEngine before creating a backup snapshot",
    GLOBAL_VAR(opt_consistent_snapshot_smartengine_backup_checkpoint),
    CMD_LINE(OPT_ARG), DEFAULT(false));

static Sys_var_bool Sys_recovery_snapshot_from_objstore(
    "recovery_snapshot_from_objectstore",
    "Recover snapshot and binlog from object storage",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_recovery_from_objstore),
    CMD_LINE(OPT_ARG), DEFAULT(true));
static Sys_var_charptr Sys_recovery_snapshot_tmpdir(
    "recovery_snapshot_tmpdir", "Temporary directory for snapshot recovery",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_recovery_consistent_snapshot_tmpdir),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT("recovery_tmp"));
static Sys_var_bool Sys_recovery_snapshot_only(
    "recovery_snapshot_only", "Recover the snapshot without archived binlogs",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_recovery_consistent_snapshot_only),
    CMD_LINE(OPT_ARG), DEFAULT(false));
static Sys_var_charptr Sys_recovery_snapshot_timestamp(
    "recovery_snapshot_timestamp", "Snapshot timestamp used for recovery",
    READ_ONLY NON_PERSIST
        GLOBAL_VAR(opt_recovery_consistent_snapshot_timestamp),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT(nullptr));

static Sys_var_bool Sys_initialize_from_objstore(
    "initialize_from_source_objectstore",
    "Initialize from a source repository in object storage",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_initialize_from_source_objectstore),
    CMD_LINE(OPT_ARG), DEFAULT(false));
static Sys_var_charptr Sys_source_objstore_provider(
    "source_objectstore_provider", "Source object-store provider",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_source_objectstore_provider),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT("local"));
static Sys_var_charptr Sys_source_objstore_region(
    "source_objectstore_region", "Source object-store region",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_source_objectstore_region),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET,
    DEFAULT(".local_objectstore_region_1"));
static Sys_var_charptr Sys_source_objstore_endpoint(
    "source_objectstore_endpoint", "Source object-store endpoint",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_source_objectstore_endpoint),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT(nullptr));
static Sys_var_bool Sys_source_objstore_use_https(
    "source_objectstore_use_https", "Use HTTPS for the source object store",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_source_objectstore_use_https),
    CMD_LINE(OPT_ARG), DEFAULT(false));
static Sys_var_charptr Sys_source_objstore_bucket(
    "source_objectstore_bucket", "Source object-store bucket",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_source_objectstore_bucket),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT("objectstore_bucket_1"));
static Sys_var_charptr Sys_source_objstore_repo_id(
    "source_objectstore_repo_id", "Source object-store repository id",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_source_objectstore_repo_id),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT("wesql_serverless_repo"));
static Sys_var_charptr Sys_source_objstore_branch_id(
    "source_objectstore_branch_id", "Source object-store branch id",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_source_objectstore_branch_id),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT("main"));
static Sys_var_bool Sys_source_objectstore_smartengine_data(
    "source_objectstore_smartengine_data",
    "Initialize SmartEngine object-store data from the source repository",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_source_objectstore_smartengine_data),
    CMD_LINE(OPT_ARG), DEFAULT(false));

static Sys_var_bool Sys_serverless(
    "serverless", "Enable WeSQL serverless archive and recovery",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_serverless), CMD_LINE(OPT_ARG),
    DEFAULT(true));
static Sys_var_bool Sys_table_on_objstore(
    "table_on_objectstore", "Store SmartEngine table data in object storage",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_table_on_objstore), CMD_LINE(OPT_ARG),
    DEFAULT(true));
static Sys_var_uint Sys_objstore_lease_lock_timeout(
    "objectstore_lease_lock_timeout", "Object-store lease timeout in seconds",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_objstore_lease_lock_timeout),
    CMD_LINE(OPT_ARG), VALID_RANGE(0, 30), DEFAULT(8), BLOCK_SIZE(1));
static Sys_var_charptr Sys_objstore_provider(
    "objectstore_provider", "Object-store provider",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_objstore_provider),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT("local"));
static Sys_var_charptr Sys_objstore_region(
    "objectstore_region", "Object-store region",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_objstore_region),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET,
    DEFAULT(".local_objectstore_region"));
static Sys_var_charptr Sys_objstore_endpoint(
    "objectstore_endpoint", "Object-store endpoint",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_objstore_endpoint),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT(nullptr));
static Sys_var_bool Sys_objstore_use_https(
    "objectstore_use_https", "Use HTTPS for object storage",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_objstore_use_https), CMD_LINE(OPT_ARG),
    DEFAULT(false));
static Sys_var_charptr Sys_objstore_bucket(
    "objectstore_bucket", "Object-store bucket",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_objstore_bucket),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT("objectstore_bucket"));
static Sys_var_charptr Sys_repo_objstore_id(
    "repo_objectstore_id", "Object-store repository id",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_repo_objstore_id),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT("wesql_serverless_repo"));
static Sys_var_charptr Sys_branch_objstore_id(
    "branch_objectstore_id", "Object-store branch id",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_branch_objstore_id),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT("main"));
static Sys_var_charptr Sys_server_id_on_objstore(
    "server_id_on_objectstore", "Server identifier in object storage",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_server_id_on_objstore),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT(""));

/*
  sql_main is a static archive. This TU only has static Sys_var objects, so
  the linker/LTO can drop it and never run the constructors that register
  --serverless / --repo-objectstore-id. A live symbol referenced from
  binlog_archive.cc pulls the TU into mysqld.
*/
void wesql_sys_vars_force_link() {}
