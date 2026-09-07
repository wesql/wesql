#!/usr/bin/env python3
"""Make the captured recovery fixture independent of registered engine plugins."""

from pathlib import Path
import struct
import zlib


def normalize(source):
    result = bytearray(source)
    assert result[:4] == b'\xfebin'
    pos = 4
    changed = 0
    while pos < len(result):
        _, event_type, _, size, _, _ = struct.unpack_from('<IBIIIH', result, pos)
        assert size >= 23 and pos + size <= len(result)
        event = result[pos:pos + size]
        assert zlib.crc32(event[:-4]) == struct.unpack_from('<I', event, size - 4)[0]
        if event_type == 2:
            db_size = event[19 + 8]
            status_size = struct.unpack_from('<H', event, 19 + 11)[0]
            query_start = 19 + 13 + status_size + db_size + 1
            assert query_start <= size - 4
            sql = event[query_start:-4]
            clause = b'ENGINE=SmartEngine'
            if clause in sql:
                assert sql.count(clause) == 1
                sql = sql.replace(clause, b' ' * len(clause))
                event[query_start:-4] = sql
                struct.pack_into('<I', event, size - 4, zlib.crc32(event[:-4]))
                result[pos:pos + size] = event
                changed += 1
        pos += size
    assert pos == len(result) and changed == 6
    return result


if __name__ == '__main__':
    directory = Path(__file__).resolve().parent
    source = directory / 'repeated-recovery.binlog'
    (directory / 'repeated-recovery-parser.binlog').write_bytes(
        normalize(source.read_bytes()))
