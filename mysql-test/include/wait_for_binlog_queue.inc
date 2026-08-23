# Wait until the captured writes have reached the persistence queue.
--let $wait_condition= SELECT SUBSTRING_INDEX(LAST_PERSISTING_MYSQL_BINLOG, '/', -1) = '$binlog_file' AND LAST_PERSISTING_MYSQL_BINLOG_READ_POS >= $binlog_pos FROM INFORMATION_SCHEMA.BINLOG_PERSISTENT_TASK_INFO;
--let $wait_timeout= 600;
--source include/wait_condition_or_abort.inc
