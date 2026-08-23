# Capture the end position after the writes under test.
--let $binlog_file= query_get_value(SHOW MASTER STATUS, File, 1)
--let $binlog_pos= query_get_value(SHOW MASTER STATUS, Position, 1)
