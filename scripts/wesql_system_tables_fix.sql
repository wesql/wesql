use mysql;

set default_storage_engine=SMARTENGINE;

-- Consensus replication
SET @have_raft_replication= (select count(plugin_name) from information_schema.plugins where plugin_name='raft_replication');

SET @have_consensus_index= (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mysql'
                    AND TABLE_NAME='slave_relay_log_info'
                    AND column_name='Consensus_apply_index');

START TRANSACTION;
SET @cmd = "INSERT INTO consensus_applier_info
  SELECT 3, Consensus_apply_index from slave_relay_log_info
  where Channel_name='raft_replication_applier' and Consensus_apply_index > 0";
SET @str=IF(@have_consensus_index <> 0 AND @have_raft_replication = 1, @cmd, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;
SET @cmd = "DELETE FROM slave_relay_log_info where Channel_name='raft_replication_applier'";
SET @str=IF(@have_consensus_index <> 0, @cmd, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;
SET @cmd = "DELETE FROM slave_master_info where Channel_name='raft_replication_applier'";
SET @str=IF(@have_consensus_index <> 0, @cmd, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;
SET @cmd = "DELETE FROM consensus_applier_info;";
SET @str=IF(@have_raft_replication = 0, @cmd, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;
COMMIT;
SET @cmd = "ALTER TABLE slave_relay_log_info DROP COLUMN Consensus_apply_index";
SET @str=IF(@have_consensus_index <> 0, @cmd, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

SET @have_consensus_index= (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mysql'
                    AND TABLE_NAME='slave_worker_info'
                    AND column_name='Consensus_apply_index');
START TRANSACTION;
SET @cmd = "INSERT INTO consensus_applier_worker
  SELECT 3, Id, Consensus_apply_index from slave_worker_info
  where Channel_name='raft_replication_applier' and Consensus_apply_index > 0";
SET @str=IF(@have_consensus_index <> 0 AND @have_raft_replication = 1, @cmd, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;
SET @cmd = "DELETE FROM slave_worker_info where Channel_name='raft_replication_applier'";
SET @str=IF(@have_consensus_index <> 0, @cmd, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;
SET @cmd = "DELETE FROM consensus_applier_worker;";
SET @str=IF(@have_raft_replication = 0, @cmd, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;
COMMIT;
SET @cmd = "ALTER TABLE slave_worker_info
  DROP COLUMN Checkpoint_consensus_apply_index, DROP COLUMN Consensus_apply_index";
SET @str=IF(@have_consensus_index <> 0, @cmd, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;
