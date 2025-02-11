use mysql;


-- ------------------------------------------------------------------------
-- Following are set of "configuration tables" that used to configure
-- the Consensus replication.
-- ------------------------------------------------------------------------
set @have_innodb = (select count(engine) from information_schema.engines where engine='INNODB' and support != 'NO');
set @have_smartengine = (select count(engine) from information_schema.engines where engine='SMARTENGINE' and support != 'NO');
set @have_nose_gtid_executed = (select count(table_name) from information_schema.tables where table_schema = 'mysql' and table_name = 'gtid_executed' and table_type = 'BASE TABLE' and engine != 'SMARTENGINE');
set @have_nose_slave_table = (select count(table_name) from information_schema.tables where table_schema = 'mysql' and table_name = 'slave_relay_log_info' and table_type = 'BASE TABLE' and engine != 'SMARTENGINE');
set @have_nose_consensus_table = (select count(table_name) from information_schema.tables where table_schema = 'mysql' and table_name = 'consensus_info' and table_type = 'BASE TABLE' and engine != 'SMARTENGINE');
set @is_mysql_encrypted = (select ENCRYPTION from information_schema.INNODB_TABLESPACES where NAME='mysql');

-- Tables below are NOT treated as DD tables by MySQL server yet.

SET FOREIGN_KEY_CHECKS= 1;

# Added sql_mode elements and making it as SET, instead of ENUM

set default_storage_engine=SMARTENGINE;

-- Serverless
SET @cmd = "ALTER TABLE gtid_executed ENGINE = smartengine;";
SET @str = IF(@@global.serverless AND @have_smartengine <> 0 AND @have_nose_gtid_executed <> 0, @cmd, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

SET @cmd = "ALTER TABLE slave_worker_info ENGINE = smartengine;";
SET @str = IF(@@global.serverless AND @have_smartengine <> 0 AND @have_nose_slave_table <> 0, @cmd, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

SET @cmd = "ALTER TABLE slave_relay_log_info ENGINE = smartengine;";
SET @str = IF(@@global.serverless AND @have_smartengine <> 0 AND @have_nose_slave_table <> 0, @cmd, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

SET @cmd = "ALTER TABLE slave_master_info ENGINE = smartengine;";
SET @str = IF(@@global.serverless AND @have_smartengine <> 0 AND @have_nose_slave_table <> 0, @cmd, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

-- Consensus replication

SET @have_raft_replication= (select count(plugin_name) from information_schema.plugins where plugin_name='raft_replication');
SET @cmd = "CREATE TABLE IF NOT EXISTS consensus_info (
  number_of_lines INTEGER UNSIGNED NOT NULL COMMENT 'Number of lines in the file or rows in the table. Used to version table definitions.',
  vote_for BIGINT UNSIGNED COMMENT 'current vote for', current_term BIGINT UNSIGNED COMMENT 'current term',
  recover_status BIGINT UNSIGNED COMMENT 'recover status', last_leader_term BIGINT UNSIGNED COMMENT 'last leader term',
  start_apply_index BIGINT UNSIGNED COMMENT 'start apply index', cluster_id varchar(256) COMMENT 'cluster identifier',
  cluster_info varchar(6000) COMMENT 'cluster config information', cluster_learner_info varchar(6000) COMMENT 'cluster learner config information',
  cluster_config_recover_index BIGINT UNSIGNED COMMENT 'cluster config recover index',
  PRIMARY KEY(number_of_lines)) DEFAULT CHARSET=utf8 COMMENT 'Consensus cluster consensus information'";
SET @str = IF(@@global.serverless AND @have_smartengine <> 0, CONCAT(@cmd, ' ENGINE= SMARTENGINE'), IF(@have_innodb <> 0, CONCAT(@cmd, ' ENGINE= INNODB ROW_FORMAT=DYNAMIC TABLESPACE=mysql ENCRYPTION=\'', @is_mysql_encrypted,'\''), CONCAT(@cmd, ' ENGINE= MYISAM')));
SET @str = IF(@have_raft_replication = 1, @str, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

SET @cmd = "CREATE TABLE IF NOT EXISTS consensus_applier_worker (
  Number_of_lines INTEGER UNSIGNED NOT NULL COMMENT 'Number of lines in the file.',
  Id INTEGER UNSIGNED NOT NULL,
  Consensus_apply_index BIGINT UNSIGNED NOT  NULL COMMENT 'The consensus log applyed index in the consensus log',
  PRIMARY KEY(Id)) DEFAULT CHARSET=utf8 STATS_PERSISTENT=0 COMMENT 'Consensus applier Worker Information'";
SET @str = IF(@@global.serverless AND @have_smartengine <> 0, CONCAT(@cmd, ' ENGINE= SMARTENGINE'), IF(@have_innodb <> 0, CONCAT(@cmd, ' ENGINE= INNODB ROW_FORMAT=DYNAMIC TABLESPACE=mysql ENCRYPTION=\'', @is_mysql_encrypted,'\''), CONCAT(@cmd, ' ENGINE= MYISAM')));
SET @str = IF(@have_raft_replication = 1, @str, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

SET @cmd = "CREATE TABLE IF NOT EXISTS consensus_applier_info (
  Number_of_lines INTEGER UNSIGNED NOT NULL COMMENT 'Number of lines in the file.',
  Number_of_workers INTEGER UNSIGNED,
  Consensus_apply_index BIGINT UNSIGNED NOT  NULL COMMENT 'The consensus log applyed index in the consensus log',
  PRIMARY KEY(number_of_lines)) DEFAULT CHARSET=utf8 STATS_PERSISTENT=0 COMMENT 'Consensus Log Information'";
SET @str = IF(@@global.serverless AND @have_smartengine <> 0, CONCAT(@cmd, ' ENGINE= SMARTENGINE'), IF(@have_innodb <> 0, CONCAT(@cmd, ' ENGINE= INNODB ROW_FORMAT=DYNAMIC TABLESPACE=mysql ENCRYPTION=\'', @is_mysql_encrypted,'\''), CONCAT(@cmd, ' ENGINE= MYISAM')));
SET @str = IF(@have_raft_replication = 1, @str, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

SET @cmd = "ALTER TABLE consensus_info ENGINE = smartengine;";
SET @str = IF(@@global.serverless AND @have_smartengine <> 0 AND @have_raft_replication = 1 AND @have_nose_consensus_table <> 0, @cmd, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

SET @cmd = "ALTER TABLE consensus_applier_info ENGINE = smartengine;";
SET @str = IF(@@global.serverless AND @have_smartengine <> 0 AND @have_raft_replication = 1 AND @have_nose_consensus_table <> 0, @cmd, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;

SET @cmd = "ALTER TABLE consensus_applier_worker ENGINE = smartengine;";
SET @str = IF(@@global.serverless AND @have_smartengine <> 0 AND @have_raft_replication = 1 AND @have_nose_consensus_table <> 0, @cmd, 'SET @dummy = 0');
PREPARE stmt FROM @str;
EXECUTE stmt;
DROP PREPARE stmt;