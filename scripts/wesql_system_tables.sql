use mysql;


-- ------------------------------------------------------------------------
-- Following are set of "configuration tables" that used to configure
-- the Consensus replication.
-- ------------------------------------------------------------------------
set @have_innodb = (select count(engine) from information_schema.engines where engine='INNODB' and support != 'NO');
set @have_smartengine = (select count(engine) from information_schema.engines where engine='SMARTENGINE' and support != 'NO');
set @have_nose_gtid_executed = (select count(table_name) from information_schema.tables where table_schema = 'mysql' and table_name = 'gtid_executed' and table_type = 'BASE TABLE' and engine != 'SMARTENGINE');
set @have_nose_slave_table = (select count(table_name) from information_schema.tables where table_schema = 'mysql' and table_name = 'slave_relay_log_info' and table_type = 'BASE TABLE' and engine != 'SMARTENGINE');
set @is_mysql_encrypted = (select ENCRYPTION from information_schema.INNODB_TABLESPACES where NAME='mysql');

-- Tables below are NOT treated as DD tables by MySQL server yet.

SET FOREIGN_KEY_CHECKS= 1;

# Added sql_mode elements and making it as SET, instead of ENUM

set default_storage_engine=InnoDB;

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

