# MySQL 9.7 远端提交日志设计

授权：task #13 总账与 task #34 实现包。长期分支：`9.7`，固定起点
`4e26b01e194c3b7617c3491b9bc1ac7f94e39716`。本版替代提交
`d1b75916cdfd96e2ae3e4825c38acbf9f2eef74b` 中把 mutable index 当提交点
的已否决首稿。本文定义全新的 remote-commit v2 namespace；首稿及其探索性实现
不得用于 task #35/#37 验收。

本轮用 MinIO 验证协议正确性。MinIO 结果不代表 S3 Express One Zone 的
延迟、费用、单 AZ 可用性或 CreateSession 行为。

## 1. 对外承诺和判定点

远端提交是一个独立、默认关闭的启动模式。只有以下三项同时开启时生效：

```text
serverless=ON
binlog_archive=ON
binlog_archive_remote_commit=ON
```

`binlog_archive_remote_commit` 默认 OFF，只能在启动时设置，运行期不可切换；它是
独立产品开关，不复用 archive period/size 或 provider 类型暗中启用。

已有 `serverless + binlog_archive` 用户仍保持异步归档语义，不能因升级自动变成
同步远端提交。

客户端收到事务成功前，必须同时满足：

1. 所有参与引擎的 prepared WAL 已经 durable，且 flush 错误已检查；
2. 覆盖整个提交组的不可变 binlog segment 已通过 size 和 SHA-256 校验；
3. segment 对应的不可变 manifest 已发布并校验；
4. 唯一权威 `HEAD` 已使用读取到的精确旧 ETag 完成 `If-Match` CAS；
5. 发布者再次确认远端 writer epoch 仍属于本进程；
6. 组内引擎 commit 全部成功；
7. 在发布外部可见状态和 ACK 前再次确认 HEAD 与 epoch 仍属于本组和本进程；
8. 之后才发布外部可见 binlog end position 并唤醒客户端。

逻辑关系为：

```text
CLIENT_OK(tx) => ENGINE_WAL_DURABLE(group)
              && SEGMENT_VERIFIED(group)
              && MANIFEST_VERIFIED(group)
              && HEAD_CAS_COMMITTED(group)
              && EPOCH_STILL_OWNED(writer)
              && ENGINE_COMMIT_OK(tx)
              && ACK_OWNERSHIP_STILL_VALID(writer, group)
```

固定 key 的 `HEAD` CAS 是不可逆决定点。HEAD 已成功后不得回滚该组；此后若
本地发布或引擎 commit 失败，进程必须 fail-stop，恢复流程根据 HEAD 完成事务。

客户端未收到成功的事务允许出现零次或一次；最终状态只由 HEAD 决定。客户端
已收到成功的事务在删掉全部本地状态后必须恰好出现一次。

## 2. 当前实现的 P0 缺口

当前 9.7 路径不能满足上述承诺：

- `MYSQL_BIN_LOG::ordered_commit()` 不等待对象存储；
- `ha_flush_logs(true)` 位于写 binlog 前，但其 boolean 错误被丢弃；
- InnoDB 的 flush 配置为 0 时跳过 group flush，为 2 时只 write 不 fsync；
- SmartEngine 的 flush 配置为 0 时跳过 `sync_wal()`；
- binlog end position 在远端决定前对 dumper/waiter 可见；
- slice 与 `binlog-index.index` 都通过普通 PUT 写入，后者会被旧 writer 覆盖；
- 现有 `data.lock` 只检查对象存在，SmartEngine lease 也不能围栏过期 writer
  的在途 IO；
- PUT 超时后没有通过 exact GET 解歧义；
- `update_index_file()` 在远端发布前移除 slice 状态，失败后可能丢失重试依据；
- 恢复通过 LIST/可变 index 猜测最新内容，没有 HEAD 限界和 hash/连续性校验；
- 当前恢复会清除最后 binlog 的 `LOG_EVENT_BINLOG_IN_USE_F` 并进入本地 2PC；P0
  必须改为按 HEAD 重建 fresh root，不能让旧 root 决定事务；
- stock `ha_recover()` 和 detached XA recovery 会直接调用 handler 的
  `commit_by_xid()`/`rollback_by_xid()`，不经过 `ha_commit_low()`；heuristic COMMIT
  还会在没有 HEAD XID 清单时提交全部 internal prepared XID；
- snapshot index 与 snapshot 数据同样存在可变覆盖和旧 writer 删除风险。
- `sql_log_bin=0`、replica apply/filter、匿名 GTID 或非事务引擎可以产生没有
  binlog endpoint 的持久变更，当前提交组会直接绕过远端协议；
- stock InnoDB Clone 的 `clone_begin()` 只创建 handle，实际一致性 cut 到
  `clone_copy()` 的 REDO_COPY 转换才固定，提前放开 commit 会与 SmartEngine
  snapshot 形成 split image；
- SmartEngine 的 `commit_in_the_middle()` 当前恒为 true，UPDATE/DELETE 达到
  `bulk_load_size` 会经 `flush_batch()`/`commit_no_binlog()` 在 binlog 和 HEAD 前
  持久提交；`write_disable_wal` 还可使 WAL barrier 失效；
- SmartEngine object extent key 只由 `(table_space_id, offset)` 派生，offset
  回收后会复用；extent PUT 允许覆盖，recycle 还会无条件删除。因此旧 writer
  可以覆盖或删除新 writer 的 live extent，HEAD fencing 本身保护不了引擎状态。

因此 P0 不是在现有 waiter 上补一次调用，而是增加一套可条件发布、可恢复验证的
远端事务决定协议。

## 3. 远端命名空间和对象格式

remote commit 模式先把 `repo_objectstore_id` 和 `branch_objectstore_id` 各自验证为一个
ASCII path component：必须匹配 `[A-Za-z0-9][A-Za-z0-9._-]{0,47}`。这样每个 ID 最多
48 bytes，完整 canonical `stream_id` 最多 101 bytes，不超过普通 ID 上限。空值、斜线、
反斜线、百分号、控制字符、`.`、`..` 和任何非 ASCII 输入都在远端 IO 前拒绝；不能
先拼接再转义。设：

```text
P=r=<repo_objectstore_id>/b=<branch_objectstore_id>/remote-commit/v2
S=<cluster_object_prefix>/smartengine/v2/extents
stream_id=r=<repo_objectstore_id>/b=<branch_objectstore_id>
stream_identity_value=["wesql.remote_commit.stream",2,
  "<repo_objectstore_id>","<branch_objectstore_id>"]
stream_sha256=lowercase_hex(SHA256(JCS(stream_identity_value)))
```

`cluster_object_prefix` 同样必须是由上述 grammar 的非空 component 以单个 `/` 连接的
canonical prefix，不允许前后 `/`、空 component、`.`、`..`、反斜线或 percent-encoded
别名。`stream_identity_value` 是一个完整 JSON array value；这里的 JCS bytes 是
`stream_sha256` 唯一 preimage，不能散列裸 `stream_id`、路径拼接结果或 JSON string。

P0 使用以下对象：

```text
P/WRITER_EPOCH
P/HEAD
P/manifests/e<epoch>/g<generation>-<sha256>.json
P/binlog/segments/e<epoch>/<writer_id>/<file_seq20>/
  <start20>-<end20>-<sha256>.seg
P/snapshots/manifests/<snapshot_id>-<sha256>.json
P/snapshots/binlog-seeds/<snapshot_id>/<file_seq20>-<cursor_pos20>-<sha256>.seed
P/snapshots/data/<snapshot_id>/<component>/<ordinal20>-<sha256>.obj
S/s<stream_sha256>/e<epoch>/a<allocation_seq>/
  db=<database_name_hex>/idx=<index_id>/data/<object_id>
```

`WRITER_EPOCH` 和 `HEAD` 是 v2 事务/恢复 namespace 内唯二可变对象，只能条件
PUT。现有 lease/status 对象只能作为 admission/liveness 提示，不能进入恢复 live
set。其余 v2 对象全部不可变，首次创建使用 `If-None-Match: *`。manifest、segment
和 snapshot data key 中的 SHA-256 是对象原始字节的 lowercase hex；SmartEngine
extent 则由 stream digest、全局 epoch 和进程内不重复的 allocation sequence 组成
唯一地址。启动必须从两个已独立验证的 ID 计算唯一 P、`stream_id` 和
`stream_sha256`，逐字段验证 P、S 与所有 manifest 中的 stream；
两个 stream 即使共用 bucket、cluster prefix、epoch 和 allocation sequence，也不能
生成相同 extent key。两类对象都必须 exact GET 校验原始字节、size 和 SHA-256。
ETag 只作为 provider 返回的 CAS token，不能替代内容校验。

对象 key 不是提示信息，producer 和 reader 都必须从已验证字段逐字节重算。epoch、
generation、allocation sequence、index ID 和 object ID 使用无前导零十进制；
`file_seq20`、`start20`、`end20`、`cursor_pos20` 和 `ordinal20` 使用恰好 20 位零填充
十进制；writer ID
和 snapshot ID 是 32 位 lowercase hex，SHA-256 是 64 位 lowercase hex，materialized
snapshot component 只能取 schema 列出的四个 literal。binlog basename 启动时固定并进入
`startup_config_sha256`，必须匹配上述 ASCII component grammar；文件名必须是
`<basename>.<sequence>`，sequence 使用至少 6 位的最短零填充十进制并受 JSON
safe-integer 上限约束，key 中再把解析出的 sequence 编为 `file_seq20`。

transition manifest key 必须等于
`P/manifests/e<writer.epoch>/g<generation>-<SHA256(JCS(body))>.json`；segment key 必须
等于 `P/binlog/segments/e<manifest.writer.epoch>/<manifest.writer.id>/<file_seq20>/`
加 `<start20>-<end20>-<SHA256(raw range bytes)>.seg`；snapshot manifest key 必须等于
`P/snapshots/manifests/<body.snapshot_id>-<SHA256(JCS(body))>.json`；snapshot data key
必须等于 `P/snapshots/data/<body.snapshot_id>/<ref.component>/<ordinal20>-`
`<SHA256(raw object bytes)>.obj`；binlog seed key 必须等于
`P/snapshots/binlog-seeds/<body.snapshot_id>/<file_seq20>-<cursor_pos20>-`
`<SHA256(raw prefix bytes)>.seed`。调用条件 PUT 前和解析 ref 后都要 exact-match 重算
结果；任何别名、大小写变化、重复 `/`、percent encoding 或字段/key 不一致都拒绝。

旧 `binlog-index.index`、`snapshot.index`、`innodb.index` 和
`smartengine.index` 可以保留为 HEAD 成功后的派生目录，但提交等待、恢复、
purge 和 writer 判定都不得信任它们。若同一 repo/branch 只有旧格式而没有
v2 HEAD，启动必须 fail closed，要求显式创建 v2 namespace，不能通过 LIST 猜测
并自动升级。

P0 所有 v2 JSON body 和所有 `*_sha256` 的 JSON preimage 都使用 RFC 8785 JCS
序列化后的 UTF-8 bytes。schema 继续禁止浮点数；optional 字段必须按本文 schema
显式写为 `null`，不能省略。P0 不定义 non-critical extension，parser 对未知字段、
未知 format/version、重复字段、溢出和非法路径一律拒绝。每个 digest 的定义都以
本文点名的完整 JSON value 为 preimage，不允许实现自行拼接字符串；单测必须为
HEAD、三类 manifest、snapshot、XID/GTID 和 object-ref 数组提供
golden body/hash vector，证明两个实现产生相同字节。

格式 v2 的解析与恢复硬上限固定为：WRITER_EPOCH 4 KiB、HEAD 64 KiB、单个
delta manifest 8 MiB、snapshot manifest 256 MiB；单 manifest 最多 4096 个
segment，snapshot 的 materialized objects 与 SmartEngine extent refs 合计最多
1,000,000 项；单个 `recovery_window` 最多
100,000 个 manifest、累计 manifest body
512 MiB、累计 replay segment 1,000,000 个。JSON nesting depth
最多 16，object key/relative path 最多 1024 bytes，普通 ID 最多 128 bytes，
canonical GTID payload 最多 16 MiB。单 segment 和 snapshot object 都不得超过
启动时固定并写入证据的上限，P0 默认 4 GiB；copy 前先用
checked uint64 校验 aggregate staging/temp-root budget，再流式传输和计算 hash。

每个 HEAD 和 transition manifest 都携带 `recovery_window`。它记录从 target
manifest 向后，一直到验证完“引入当前 HEAD.snapshot 的
BOOTSTRAP/SNAPSHOT transition”以及该 snapshot 的 exact `MANIFEST_BOUNDARY` anchor
（含 anchor）所需遍历的 `manifest_count`、原始 `manifest_bytes` 和 replay
`segment_count`；anchor body 参与 ancestry/hash/window 校验，但只取严格晚于 anchor
的 LOG segments 做 replay 和 segment count。`MANIFEST_BOUNDARY` 必须在 chain 中唯一
exact-match，且其 manifest generation 等于 anchor generation、其 durable cursor 等于
`anchor.cursor == snapshot.cursor`。`EMPTY_BASE` 则遍历到引入 snapshot 的唯一
BOOTSTRAP manifest 后停止。generation 1 BOOTSTRAP 的 window 恰好包含该 manifest，
`segment_count=0`。恢复必须重新遍历并复算三项，不能只信计数。

每次 HEAD CAS 前，publisher 必须按 target snapshot 的新停止点计算 window；超过任一
硬上限的对象不得发布。LOG 沿用 prior snapshot，base counters 等于已验证 prior
window。SNAPSHOT 会重锚，base counters 必须从新 snapshot 的 exact anchor 到 prior
HEAD 重新遍历、按实际保留后缀计算，不能沿用 prior total；BOOTSTRAP 的 base counters
为零。三类 transition 都在 base counters 上加本 manifest，segment count 只计算严格
晚于 snapshot cursor、恢复确实要 replay 的 segment。

`manifest_bytes` 包含每个最终 JCS manifest body 自身。serializer 从上述 base raw-byte
sum 开始填入候选值，序列化并改为 `base + actual_body_size`，重复到值与实际 body size
唯一稳定（最多 4 次）；不能稳定、溢出或 exact GET 后 retained-suffix raw-byte sum
不等即 fail closed。HEAD body 不计入 manifest_bytes，BOOTSTRAP/SNAPSHOT manifest body
另限 64 KiB。

P0 没有单独的 ownership-only transition。接管者只在 fresh temp root 已恢复并制作了 cursor 等于
candidate durable cursor 的新 snapshot 后，用一次 SNAPSHOT CAS 同时认领 writer 并
重锚 window；CAS 前崩溃不增长 chain，CAS 后崩溃留下的是已完成重锚、可再次接管的
HEAD。因此不需要预留多次空接管 manifest。每个 LOG 只要 prospective window 超出任一
硬上限就必须在写 segment/HEAD 前关闭新写并先制作 snapshot；80,000 个 window
manifest 时必须主动开始 snapshot。扩大硬格式上限必须升 format version，不能只改
运行参数让旧恢复器无界分配。

takeover snapshot 的 anchor 固定为 candidate HEAD manifest 且 cursor 等于 candidate
durable cursor，所以 rebase window 恰好是 2 个 manifest、最多 `8 MiB + 64 KiB`、
0 个 replay segment；除显式的 generation+1 数值耗尽外，它在当前 window 硬上限内
对任何合法 candidate 都可发布。generation/epoch 数值耗尽一律在写对象前 fail closed，
不允许回绕。

所有 JSON number 都必须是 `0..9007199254740991` 的 canonical 非负整数，保证 RFC
8785/IEEE-754 实现逐字节一致；parser 和 producer 都拒绝越界值。内部十进制解析、
加法和乘法仍使用 checked unsigned 64-bit；溢出或序列化前超过上述 safe-integer 上限
立即 fail closed。
manifest/snapshot 使用 streaming parser，对象边下载边计算 hash，不把大对象
整体读入内存。重复 manifest key、segment sequence/key、非 canonical array order、
snapshot ordinal 或 relative path 都拒绝。

### 3.1 WRITER_EPOCH

```json
{
  "format": "wesql.remote_commit.writer_epoch",
  "version": 2,
  "stream_id": "r=repo-id/b=branch-id",
  "epoch": 37,
  "writer_id": "128-bit-random-hex",
  "previous_epoch": 36
}
```

epoch 严格单调递增，writer_id 每个进程随机生成且不得复用。epoch 1 的
`previous_epoch=0`；每次 CAS 都要求 target `previous_epoch` 等于 exact prior
`epoch`，target `epoch == prior.epoch+1`。该对象创建后永不删除或归零。获取新 epoch
必须 GET body+ETag，再以精确 ETag CAS 到 `epoch+1`。

### 3.2 HEAD

```json
{
  "format": "wesql.remote_commit.head",
  "version": 2,
  "stream_id": "r=repo-id/b=branch-id",
  "state": "READY",
  "generation": 42,
  "writer": {"id": "...", "epoch": 37},
  "parent": {
    "generation": 41,
    "etag": "exact-provider-etag",
    "sha256": "sha256-of-previous-head-body"
  },
  "manifest": {"key": "...", "size": 123, "sha256": "..."},
  "recovery_window": {
    "manifest_count": 42,
    "manifest_bytes": 12345,
    "segment_count": 1001
  },
  "segment_tip": {
    "kind": "SEGMENT|SNAPSHOT_ROOT",
    "key": "...",
    "size": 4096,
    "sha256": "...",
    "sequence": 1001,
    "snapshot_id": null,
    "cursor": null
  },
  "base_cursor": {"file": "binlog.000120", "pos": 789},
  "durable_cursor": {"file": "binlog.000123", "pos": 456},
  "snapshot": {
    "id": "...",
    "manifest_key": "...",
    "manifest_size": 123,
    "manifest_sha256": "...",
    "cursor": {"file": "binlog.000120", "pos": 789}
  }
}
```

P0 的 HEAD 只有 READY 状态，writer、manifest、recovery window、segment tip、base
cursor、durable cursor 与 snapshot 全部必填，且 snapshot cursor 不得晚于 durable
cursor。generation 1 必须由 BOOTSTRAP manifest 创建，`parent=null`；之后每次 HEAD
变化 generation 必须严格 `+1`，parent 必须精确描述 CAS 前读取的 HEAD body 和 ETag。
`HEAD.writer.epoch` 始终大于等于 1，且不得大于当前 WRITER_EPOCH。P0 不把未完成的
本地迁移或 prepared decision 写入 HEAD；首次 HEAD 发布前两类 prepared set 必须为空。

旧 mutable HEAD body 在后续 CAS 后不能从对象存储重新取得，因此 `parent` 只用于
本次运行时的 CAS 审计，不作为重启后的可恢复历史。恢复所验证的历史由 immutable
manifest `previous` chain 提供；文档和实现不得声称恢复会验证完整 HEAD body 链。

### 3.3 增量 manifest 和 segment

每次 HEAD 更新先写一个不可变 manifest。日志 manifest 至少包含：

```json
{
  "format": "wesql.remote_commit.manifest",
  "version": 2,
  "stream_id": "r=repo-id/b=branch-id",
  "kind": "BOOTSTRAP|LOG|SNAPSHOT",
  "generation": 42,
  "writer": {"id": "...", "epoch": 37},
  "head_parent": {"generation": 41, "etag": "...", "sha256": "..."},
  "previous": {"generation": 41, "key": "...", "size": 123, "sha256": "..."},
  "recovery_window": {
    "manifest_count": 42,
    "manifest_bytes": 12345,
    "segment_count": 1001
  },
  "segment_tip": {
    "kind": "SEGMENT|SNAPSHOT_ROOT",
    "key": "...",
    "size": 4096,
    "sha256": "...",
    "sequence": 1001,
    "snapshot_id": null,
    "cursor": null
  },
  "snapshot": {
    "id": "...",
    "manifest_key": "...",
    "manifest_size": 123,
    "manifest_sha256": "...",
    "cursor": {"file": "binlog.000120", "pos": 789}
  },
  "base_cursor": {"file": "binlog.000120", "pos": 789},
  "durable_cursor": {"file": "binlog.000123", "pos": 456},
  "segments": [
    {
      "sequence": 1001,
      "key": "...",
      "size": 4096,
      "sha256": "...",
      "source": {
        "file": "binlog.000123",
        "start_pos": 4,
        "end_pos": 4100
      },
      "previous_segment": {
        "kind": "SEGMENT|SNAPSHOT_ROOT",
        "key": "...",
        "size": 4096,
        "sha256": "...",
        "sequence": 1000,
        "snapshot_id": null,
        "cursor": null
      },
      "transaction_count": 8,
      "gtid_set": {
        "canonical": "uuid:intervals",
        "sha256": "sha256-of-jcs-canonical-string"
      },
      "xids": {
        "count": 8,
        "sha256": "sha256-of-jcs-sorted-decimal-string-array"
      },
      "ends_at_transaction_boundary": true,
      "payload_format": "native-mysql-binlog-range-v1",
      "compression": "none"
    }
  ]
}
```

manifest 是 delta chain，不在每个提交组重复上传全部历史。恢复从 HEAD 指向的
manifest 反向遍历；cursor coverage 完成只是数据范围条件，不是停止条件。恢复还
必须定位并验证引入 `HEAD.snapshot` 的 exact BOOTSTRAP/SNAPSHOT transition，随后在
`MANIFEST_BOUNDARY` 情况继续走到并验证 snapshot manifest 指定的 exact anchor 是
ancestor；只有两项都完成才可停止。每个 snapshot 周期把自动恢复窗口限制在该
anchor 之后；历史链仅供审计，P0 不删除。

所有 transition 必须满足以下通用不变量：

- BOOTSTRAP target generation 为 1；其余 target HEAD generation 等于 prior
  generation `+1`，且恰好引用一个新 manifest；
- manifest 的 stream、generation、writer 与 target HEAD 逐字段相同；key 中的
  epoch/generation/SHA 与 body 和原始字节相同，且 key 位于同一 `P/`；
- BOOTSTRAP 的 target `parent`、manifest `head_parent` 和 `previous` 全部为 null；
  其余 target HEAD `parent` 与 manifest `head_parent` 必须逐字段相等，且两者都等于
  `{prior generation, exact prior ETag, SHA256(prior JCS HEAD body)}`；其
  `previous` 必须精确引用 prior HEAD manifest 的
  generation/key/size/SHA，不得跳代、分叉或跨 stream；
- target HEAD 的 manifest、recovery window、segment tip、snapshot、base cursor 和
  durable cursor 必须与 transition manifest 逐字段一致；window 三项
  必须等于本 transition 发布前的 prospective checked counters；
- cursor 比较解析 binlog stream identity、数值 file sequence 和 event position，
  禁止按普通字符串排序。

三种 transition 的合法变化固定如下：

| kind | prior HEAD | segments | snapshot/base/durable | target HEAD |
| --- | --- | --- | --- | --- |
| `BOOTSTRAP` | 不存在 | 空 | freshly initialized EMPTY_SOURCE 已证明 internal/external prepared 都为 0；新 snapshot 已完整校验，`base == durable == snapshot.cursor`；tip 建为该 snapshot root | 以 `If-None-Match:*` 创建 generation 1 `READY` |
| `LOG` | 本 writer 的 `READY` | 非空 | snapshot/base 不变；首段 start 等于 prior durable 且 previous_segment 等于 prior tip，全部连续；new tip/ durable 等于末段/end 并严格前进 | `READY` |
| `SNAPSHOT` | `READY` | 空 | durable 和 segment tip 不变；新 snapshot cursor 位于 prior `[base,durable]` 且是已验证边界，base 改为该 cursor | `READY`；writer 可保持不变，或在接管时原子改为本 epoch |

SNAPSHOT 不得重写 immutable 历史 segment 的链接，也不重置 physical tip。若新
snapshot cursor 早于 prior durable，原 `(snapshot cursor,durable]` tail 必须完整
保留；下一 LOG 首段仍引用 prior HEAD segment tip。反向遍历时 SNAPSHOT 的
空 delta 继续跟随 previous，LOG 的 segments 逆序收集；只有严格连续覆盖
`(HEAD.snapshot.cursor, HEAD.durable_cursor]`，验证引入当前 snapshot 的 exact
transition，并验证 exact anchor ancestry 后才可停止。恶意 LOG 即使带着可解析的
segment，也不能改写 prior snapshot/base；该不变量必须通过读取 exact previous
manifest 证明。generation 不逐代递减、previous 不匹配、parent tuple 不等、cycle、
重复 key、gap/overlap、window 计数不等或先遇到 null 都 fail closed。

`segment_tip` 与 `previous_segment` 使用同一 union：kind=SEGMENT 时
key/size/SHA/sequence 必填而 snapshot_id/cursor 必须为 null；
`SNAPSHOT_ROOT` 时 key/size/SHA/sequence 必须为 null，snapshot_id/cursor 必填且
等于创建该 root 的 snapshot。segment sequence 在 stream 内严格递增且相邻值为
`+1`，不能只靠
file/pos 连续性跳过对象。恢复达到当前 snapshot cursor 后仍按上述规则走到
snapshot-introducing transition 和 exact anchor；不要求 HEAD 当前 snapshot 等于
更早的 historical root snapshot。BOOTSTRAP root 后的第一个 LOG segment sequence
固定为 1；后续 SNAPSHOT 不重置 sequence。

同一 binlog 文件的相邻 segment 必须满足
`next.start_pos == previous.end_pos`。跨文件只允许上一段以有效 Rotate_event
结束且指向下一文件，下一段 `start_pos=0`，其 body 包含新文件的 magic、FDE 和
后续 event。segment 不得跨源 binlog 文件，start/end 必须是完整 event 和完整事务
边界。

segment body 没有 v2 wrapper，也不压缩或重新编码；它逐字节等于源 binlog 文件的
半开区间 `[start_pos,end_pos)`，所以 `size == end_pos - start_pos`，manifest SHA
也只计算这段原始字节。同文件恢复把 body 写到 start_pos；跨 rotate 的
`start_pos=0` body 新建下一文件，绝不能追加到旧文件。恢复随后用 native reader
解析 event、CRC32、GTID 和 XID；解析得到的 canonical GTID set、事务数和排序后
XID digest 必须与 segment manifest 完全相同。

`gtid_set.canonical` 和 `gtid_executed.canonical` 使用同一确定格式。producer 和 reader
都先解析为 `(SID, inclusive GNO intervals)`：SID 必须是 16-byte UUID，GNO 只接受
`1..INT64_MAX`；每个 SID 的重叠或相邻 interval 合并后按起点递增，SID 按 16-byte
unsigned lexical order 排序。SID 编码为 lowercase `8-4-4-4-12` UUID，singleton 写
`n`、range 写 `n-m`，同一 SID 的 interval 用 `:` 连接，不同 SID 用 `,` 连接，不含
空格；空集合固定为空字符串。`sha256` 的 preimage 不是裸 UTF-8，而是 sibling
`canonical` 字符串作为一个完整 JSON string value 的 RFC 8785 JCS bytes，digest 写
lowercase hex。例如空集合 preimage 是两个字节 `""`。

`xids` 只描述该 segment 原生 `Xid_log_event` 的 unsigned 64-bit `my_xid` multiset，
不是外部 XA 的 formatID/gtrid/bqual。每个值编码为无前导零的十进制 JSON string
（零只能是 `"0"`），按解析后的 uint64 数值递增排序并保留重复值；`count` 必须等于
数组长度。`xids.sha256` 的 preimage 是该完整字符串数组的 RFC 8785 JCS bytes，digest
写 lowercase hex。例如输入 10、2、2 的 preimage 固定为 `["2","2","10"]`。
使用 JSON string 是为了覆盖 `0..UINT64_MAX`，不能把 XID 塞进受 safe-integer 上限
约束的 JSON number。以上 GTID/XID 正规化、preimage bytes 和 digest 都必须有 golden
vector。XID digest 只做 segment-local 字节完整性校验，不是跨进程事务身份或
prepared-decision authority；恢复顺序和去重以连续 cursor、完整 event 与 canonical
GTID 为准。

一个 group commit 只生成一个 LOG manifest 和一个或少量连续 segment，不按事务
逐个 PUT。候选发布范围必须是 prior durable 之后的 all-valid contiguous prefix；
不能只挑 `commit_error == CE_NONE` 的最远 `get_trans_fixed_pos()`。该物理字节区间内
每个产生 durable event 的 THD 都必须 commit_error 为空、有 endpoint、有已分配
GTID，并可生成绑定 intended HEAD 的 authorization。中间任一 THD 失败、缺 endpoint、
匿名或未授权时，整个候选组在 HEAD 前 fail-stop；不得越过它发布后面的成功 THD。
真正只读且没有 binlog bytes/持久副作用的 THD 可以忽略。

remote commit 模式下，只有经过服务端证明为只读、没有任何持久状态变化的组可以
没有 binlog endpoint 并绕过协议；只要存在 durable mutation 却没有 endpoint，
进程必须在 engine commit 前 fail-stop。用户临时表、系统表、replica applier 和
插件不能自行声明这个例外；唯一内部旁路是带专用 `RECOVERING` capability 的
bounded replay THD，且写 admission 尚未开放。

### 3.4 snapshot manifest

snapshot manifest 至少使用以下 schema；省略号不代表可以增加未知 critical 字段：

```json
{
  "format": "wesql.remote_commit.snapshot",
  "version": 2,
  "stream_id": "r=repo-id/b=branch-id",
  "snapshot_id": "128-bit-random-hex",
  "writer": {"id": "...", "epoch": 37},
  "cursor": {"file": "binlog.000120", "pos": 789},
  "log_anchor": {
    "kind": "EMPTY_BASE|MANIFEST_BOUNDARY",
    "generation": 40,
    "manifest": {"key": "...", "size": 123, "sha256": "..."},
    "cursor": {"file": "binlog.000120", "pos": 789}
  },
  "server_identity": {"server_uuid": "canonical-uuid"},
  "deployment_fingerprints": {
    "startup_config_sha256": "...",
    "server_build": "...",
    "plugin_component_set_sha256": "...",
    "keyring_config_sha256": "...",
    "tls_config_sha256": "..."
  },
  "gtid_executed": {
    "canonical": "uuid:intervals",
    "sha256": "sha256-of-jcs-canonical-string"
  },
  "binlog_seed": {
    "file": "binlog.000120",
    "cursor": {"file": "binlog.000120", "pos": 789},
    "key": "...",
    "size": 789,
    "sha256": "...",
    "compression": "none",
    "format": "native-mysql-binlog-prefix-v1",
    "checksum": "CRC32"
  },
  "objects": [
    {
      "component": "innodb|mysql-dd|smartengine-meta|smartengine-wal",
      "ordinal": 0,
      "relative_path": "...",
      "key": "...",
      "size": 123,
      "sha256": "...",
      "compression": "none",
      "format": "..."
    }
  ],
  "smartengine_extents": [
    {
      "ordinal": 0,
      "writer_epoch": 37,
      "allocation_seq": "18446744073709551615",
      "database_name_hex": "74657374",
      "index_id": "42",
      "object_id": "123456",
      "key": "...",
      "size": 4096,
      "sha256": "...",
      "format": "smartengine-object-extent-v2"
    }
  ]
}
```

HEAD 和 transition manifest 中逐字段相同的 `snapshot` ref 必须绑定 fetched snapshot
body，不能只验证 ref 自身：ref `id == body.snapshot_id`，ref `cursor == body.cursor ==
body.binlog_seed.cursor`，`body.binlog_seed.file == body.cursor.file`，且 HEAD/transition
`base_cursor == ref.cursor`。ref 的 `manifest_size`/`manifest_sha256` 必须等于 exact GET
原始 JCS body 的 size/SHA，`manifest_key` 必须按第 3 节规则由同一 body 的
`snapshot_id` 和 SHA 重算；body `stream_id` 必须等于 HEAD/transition 的 canonical
stream。`binlog_seed.key` 也必须由 body snapshot ID、cursor file sequence/position 和
raw seed SHA 按第 3 节重算，exact GET bytes 必须匹配其 size/SHA。任何 cursor、file、
ID、key、size、SHA 或 stream 不等都在下载其他数据和 replay 前拒绝。

非 bootstrap snapshot 的 `MANIFEST_BOUNDARY` 必须精确引用制作 snapshot 时读取的
prior HEAD manifest/generation 和 exact cursor；该 anchor 必须是发布 SNAPSHOT 后
HEAD manifest chain 的 ancestor。`EMPTY_BASE` 只允许首次 bootstrap：写 admission
始终关闭，source 必须是 freshly initialized EMPTY_SOURCE；mutation-free scan 证明
internal prepared 与 external XA 均为空，没有旧 TC/binlog authority、用户数据或
legacy live extent，且 snapshot/base/durable 三个 cursor 完全相等。snapshot 流程
必须先请求在 cursor seal，不能让 cursor 落在 segment 中间；
恢复固定回放 `(snapshot cursor, HEAD durable cursor]`。

`EMPTY_BASE` 下 `generation` 和 `manifest` 必须为 null，cursor 仍必填；
`MANIFEST_BOUNDARY` 下二者都必填。anchor cursor 必须等于 snapshot cursor，不能
引用“覆盖了它”的更晚 segment 或从 segment 中间截取。

snapshot manifest 的 writer 必须逐字段等于引用它的 BOOTSTRAP/SNAPSHOT manifest
和 target HEAD writer。snapshot ID 每次制作随机生成且不得跨 writer/epoch 或失败
attempt 复用；writer 已失权后完成的后台 snapshot 只能成为孤儿，后继不得代为发布。
非 BOOTSTRAP snapshot 的 `server_identity` 和全部 deployment fingerprints 必须与
prior HEAD snapshot 逐字段相同；P0 不允许借 snapshot 改 UUID、binary/plugin ABI、
keyring、TLS 或声明式配置。

snapshot 的 `objects` 必须包含 InnoDB Clone、MySQL DD/system tables、SmartEngine
checkpoint/meta/WAL 的 materialized payload；每项记录 component、ordinal、规范化
relative path、P key、size、SHA-256、compression 和 format。`smartengine_extents` 不复制
extent body 到 P，而是引用所有 live immutable S object；每项记录足以按 5.3 重算 full
key 的 epoch/allocation/database/index/object 字段及 exact key/size/SHA。server UUID 和
canonical `gtid_executed` 是 snapshot 数据状态的一部分，不能由新节点猜测。
deployment fingerprints 只散列去除凭证值后的 canonical 启动配置、binary build ID、
plugin/component 清单、keyring/TLS 配置；恢复节点必须逐项匹配，不把 token、密码或
私钥写入 manifest。

materialized objects 先按 `(component ASCII bytes, canonical relative_path UTF-8 bytes)`
排序；同一 component/path 在赋 ordinal 前就必须唯一。随后在每个 component 内从 0
连续分配 ordinal，再按第 3 节从 ordinal 和 raw SHA 导出 P key；最终 array 按
`(component,ordinal)` 排列，key 和完整 tuple 也不得重复。parser 必须重做同一过程，
对非 canonical order、ordinal gap 或派生 key 不等立即拒绝。SmartEngine extent 里的三个 uint64
字段使用无前导零十进制 JSON string；`writer_epoch` 仍是受 safe-integer 限制的 number，
且 `1 <= writer_epoch <= snapshot.writer.epoch`。array 先按
`(writer_epoch, allocation_seq numeric, database_name_hex bytes, index_id numeric,
object_id numeric, key)` 排序，再从 0 分配全局连续 ordinal；database hex 必须 lowercase、
偶数长度并解码为有效 UTF-8，完整 tuple 和 key 都不得重复。每个 full key 必须按 5.3
由这些字段、canonical S 和 stream digest 重算，并 exact GET 校验 size/SHA。

P0 不定义单独的 object-ref 或 live-set-root digest；两个完整排序 array 都直接位于
snapshot JCS body 中，外层 ref 的 `manifest_sha256 == SHA256(JCS(snapshot body))` 是
唯一 commitment。恢复后的 materialized component tuple 和 SmartEngine extent tuple
必须分别与两个 array exact-match，不能用另一个实现自定的路径拼接 hash 代替。

`binlog_seed` 也是每个 snapshot 的必填不可变对象。它保存 snapshot cursor 所在
active binlog 从文件 magic 开始到 cursor（不含 cursor 之后字节）的原始前缀，
因此 size 必须精确等于 cursor.pos，内容必须包含有效 magic、FDE、CRC32 event 和
完整事务边界。只保存 FDE 或从 cursor 开始的 tail 都不够：删掉 datadir 后，native
binlog reader 不能凭 event `log_pos` 合成缺失的 `[0,cursor)` 字节。恢复用 seed 加
严格连续的 `(snapshot cursor, HEAD durable cursor]` segment 重建原生 binlog 和
index；即使 bootstrap HEAD 没有任何 LOG segment，也有一个可直接打开的 binlog。
源文件必须持有 purge/file pin 直到 seed 已复制进本地 temp；之后才能允许 purge。
源文件 rotate/close 可能改写 FDE 的 `LOG_EVENT_BINLOG_IN_USE_F`，这是前缀唯一允许的
变化，FDE checksum 按上游规则不覆盖该 bit。对象 SHA 仍校验实际捕获的原始字节；
恢复在全部对象校验后按下文统一规范化该 bit。

relative path 禁止绝对路径、`..`、重复项和平台分隔符歧义。tar 可以作为 P0
容器格式，但 tar 对象也必须使用唯一 snapshot ID、条件创建和整体 SHA-256；
不能先删除固定远端目录再上传同名对象。提取器拒绝 symlink、hardlink、device、
FIFO、sparse/overlap entry 和重复 normalized path，并用 dirfd-relative `openat` 加
`O_NOFOLLOW` 落盘，不能让 archive 跳出 fresh temp root。

## 4. writer 获取、首次 bootstrap 和接管

本地状态机为：

```text
STARTING -> INITIALIZING | RECOVERING -> RUNNING
                                     \-> FENCED
```

`FENCED` 不可在进程内恢复为 RUNNING。

`WRITER_EPOCH` 只承担 writer admission/fencing，不是事务决定点或恢复上界；HEAD
是唯一事务决定与恢复权威。任何以精确旧 ETag 成功 CAS 的 HEAD 永久有效，即使
epoch 随后已推进。读取历史状态时合法关系是
`1 <= HEAD.writer.epoch <= WRITER_EPOCH.epoch`；低 epoch HEAD 是 takeover/crash
race 的正常结果。只有 HEAD epoch 高于当前 epoch、同 epoch 对应不同 writer_id、
epoch 回退或字段损坏才是 corruption。本进程成功发布 BOOTSTRAP/SNAPSHOT 后才要求
`HEAD.writer == WRITER_EPOCH.writer == local_writer`；这是 RUNNING/ACK 条件，
不是验证历史 HEAD 的条件。

### 4.1 provider 与配置预检

启动先验证 provider 支持强一致 exact GET、`If-None-Match`、精确 ETag
`If-Match` 和 404/409/412 可区分状态。当前 `local` provider 没有条件接口，
remote commit 模式必须拒绝启动；MinIO/AWS/R2 走 S3 条件接口。

条件写接口必须让上层能够解歧义一次 logical attempt。现有 SDK 内部重试若在
首次请求已应用后收到 409/412，上层仍需看到该结果并执行 GET；每次重试还必须
重新创建或 rewind request body，不能复用已读到 EOF 的 stream。

exact GET 的对象内容仍受调用方 `max_bytes` 限制；内存对象 body 和下载目标文件
都不得保留超限内容，成功响应还必须满足 Content-Length 与实际长度一致。S3 SDK
用同一响应流读取错误 XML，因此另外保留最多 64 KiB 的可读响应前缀。该前缀不属于
对象内容额度，不能用来接受更大的成功对象。只有完整错误体落入该前缀、没有本地写入
失败且 SDK 返回 HTTP 错误时，才优先采用错误分类：NoSuchKey 为缺失，NoSuchBucket
和权限错误为永久错误，暂时不可用仍按原规则处理。超出前缀上限的 HTTP 错误体不能
认定为缺失；文件接口的失败结果必须截空目标文件。每次 SDK 重试重置前缀、计数与
错误标记，并重新截空响应文件，不能把前次错误体带入成功对象。

P0 同时要求：

- `log_bin=ON`；
- `binlog_format=ROW`、`binlog_row_image=FULL`；
- `binlog_transaction_compression=OFF`、`binlog_checksum=CRC32`、
  `binlog_row_value_options=''`、`binlog_encryption=OFF`；
- `max_binlog_cache_size` 和 `max_binlog_stmt_cache_size` 均不超过
  `min(1 GiB, remote_commit_max_segment_bytes - 16 MiB)`；
- `binlog_order_commits=ON`；
- `binlog_error_action=ABORT_SERVER`；
- `gtid_mode=ON`、`enforce_gtid_consistency=ON`，禁止匿名 GTID；
- `innodb_flush_log_at_trx_commit=1`；
- SmartEngine 启用时 `smartengine_flush_log_at_trx_commit=1`；
- SmartEngine 启用时所有用户、系统和 recovery THD 的
  `smartengine_write_disable_wal=OFF`；
- SmartEngine 启用时 `smartengine_persistent_cache_size=0`；
- `tc_heuristic_recover=OFF`，COMMIT 和 ROLLBACK 两种 heuristic policy 都拒绝；
- `snapshot_archive=ON`、`snapshot_archive_on_objectstore=ON`；
- SmartEngine 数据必须使用可从远端闭合的 object-store extent 模式。

这些值在 remote commit 运行期间不得动态降级；sysvar check hook 必须拒绝修改，
关键提交路径还要每组 fail closed 复核。`sync_binlog` 只控制本地 cache fsync，
不控制远端决定，允许 0、1 和 N；三种值都必须通过相同远端协议测试。

remote mode 还必须在代码中令 SmartEngine `commit_in_the_middle()` 恒为 false，
禁止 `do_bulk_commit()` 调用 `flush_batch()`。不能只把 `smartengine_bulk_load_size`
调大，因为任意更大的语句仍会越界；该 THDVAR 以及未来可能恢复的
`commit_in_the_middle` knob 都不得在 remote mode 重新打开中途提交。除正常
server prepare/commit 和下文受权 recovery 外，直接 `commit_no_binlog()` 的路径
一律 fail-stop。

P0 只允许 InnoDB 和采用本设计 immutable extent v2 的 SmartEngine 承载持久用户
表。启动扫描 DD，发现 MyISAM、CSV、插件引擎或其他持久用户表即拒绝 remote
commit；DDL 也拒绝新建或 ALTER 到这些引擎。外部 XA 在 P0 禁止，XA START、
END、PREPARE、COMMIT 和 ROLLBACK 均在执行前拒绝，恢复只允许只读
`XA RECOVER`。首次 BOOTSTRAP、已有流的 fresh temp restore 完成后以及进入 RUNNING
前都必须确认 internal prepared 和 external XA 集合为空；非空时 fail closed，不能发布 HEAD
或开放写入。

任何用户/session/plugin 路径都不得用 `sql_log_bin=0` 写持久状态；remote commit
期间直接拒绝用户修改 `sql_log_bin`。P0 不接收任何外部 replication channel 或
Group Replication：启动时 repository 中已有 channel、配置了 Group Replication 或
applier 状态非空都拒绝；运行期在产生持久副作用前拒绝 `CHANGE REPLICATION
SOURCE TO`、`START/STOP/RESET REPLICA` 和 `START/STOP GROUP_REPLICATION`。
binlog source 的只读 dump 客户端不在此限制内。

不能由 HEAD tail 在 fresh 节点确定性重放的本地持久操作也在执行前拒绝，包括
`INSTALL/UNINSTALL PLUGIN`、`INSTALL/UNINSTALL COMPONENT`、`CREATE/ALTER/DROP
SERVER`、`CHANGE REPLICATION FILTER`、`CREATE/DROP FUNCTION ... SONAME`、
`CLONE INSTANCE`、全部 `ALTER INSTANCE`、`RESET BINARY LOGS [AND GTIDS]` 的所有
`TO` 变体、运行期 `SET GLOBAL gtid_purged`，以及 `SET PERSIST`、`SET PERSIST_ONLY`、
`RESET PERSIST`。

实现不能把上段当作手写 blacklist。SQL dispatch 对每个 `enum_sql_command` 都必须
先归入且只归入 `NO_DURABLE_MUTATION`、`REMOTE_REPLAYABLE`、
`LOCAL_MUTATING_REJECT` 或 `RECOVERING_ONLY`，table size 与 enum sentinel 做
compile-time 校验，启动自检不允许任何 `UNCLASSIFIED`。`SET`、`RESET` 等复合命令
还要在子类型解析后、执行前完成同样分类；新增 enum/subtype 未分类时编译或启动
失败。event scheduler、DDL、账号/权限和系统表变更只有归为 REMOTE_REPLAYABLE、
生成 endpoint 并通过统一提交 guard 时才允许。内部 fresh replay 使用不可由 SQL
用户伪造的 `RECOVERING` capability，固定 replay 上界为 HEAD，写 admission 打开前
销毁该 capability。

单个事务不得拆成多个 segment。P0 在 engine prepare 前检查本事务两类 binlog cache
的实际 encoded bytes，加 16 MiB 固定 envelope 预算后必须小于等于
`remote_commit_max_segment_bytes`；超限返回明确 statement error，尚未 prepare、
未写 segment/HEAD。上述两个 cache 上限运行期不得提高，恰好等于和超过边界都要
按真实 encoded size 测试，不能等 archive seal 后才发现装不下。

### 4.2 首次创建

v2 remote 模式不启动或等待 SmartEngine 的旧独占 lease；暂停的旧 writer 不能靠
续租阻止新 epoch 接管。不可变 extent、精确 epoch/HEAD 复核和 HEAD CAS 共同控制
发布，不能把 lease 当作 IO fence。非 remote 模式仍按原 timeout 使用旧 lease。task #34 的
P0 产品入口只接受 freshly initialized `EMPTY_SOURCE`：没有用户数据、legacy live
extent、旧 TC/binlog decision、外部 replication repository、internal prepared XID 或
external XA。检查必须在创建第一个远端对象前以 mutation-free 方式完成；任一集合
非空时不得 commit/rollback、不得创建 epoch/HEAD，直接拒绝启动。pre-v2/local 数据
迁移需要另一个有部署围栏和独立 format version 的设计，不属于本轮。

首次创建顺序固定为：

1. SQL write admission 始终关闭。先用 exact GET 只读探测 HEAD 与 WRITER_EPOCH；HEAD
   已存在时立即转 4.3，不能先用 EMPTY_TARGET gate 检查正常 managed target；HEAD
   存在而 epoch 缺失为 corruption。只有 HEAD absent 才继续首次创建：先证明 target
   不存在或真正为空；任一非空 target 都拒绝，不能把旧 marker 当作迁移凭据。再在
   freshly initialized root 上完成 4.1 全部配置、engine/DD、repository、legacy extent
   与 mutation-free prepared-set 检查；该 root 必须是与 target 同一文件系统的唯一
   temp root，不能直接在 target 初始化。同时固定 server UUID 和去除凭证值后的
   deployment fingerprints；
2. HEAD 与 WRITER_EPOCH 都不存在时，以 `If-None-Match:*` 创建 epoch 1；HEAD 不存在但
   epoch 已存在表示允许的“epoch 后、首个 HEAD 前”崩溃窗口，新进程用 exact ETag CAS
   到更高 epoch。transport-unknown 只按 6.1 exact GET 解歧义：读回 intended 表示成功，
   读回 exact prior 才能用原 body 和原条件重试。explicit conflict 或任一第三种合法
   epoch/HEAD body 都使本进程进入 FENCED；不得采用读回的新 ETag、不得在进程内转 4.3。
   新进程重新从第 1 步探测，才能按已有 HEAD 路由；
3. 当前 epoch owner 再次证明 source 仍为 EMPTY_SOURCE 且两类 prepared set 均为空，
   严格按 7.1 固定同一 committed cut，上传并 exact GET 校验完整 snapshot、binlog seed
   和 SmartEngine exact live set；初始化期间没有并发 SQL，
   `snapshot.cursor == base_cursor == durable_cursor`；
4. 创建 generation 1 BOOTSTRAP manifest：`head_parent=null`、`previous=null`、window
   恰好包含自身、segment tip 为 snapshot root。manifest create-only PUT/read-back
   完成后，以 `If-None-Match:*` 创建 generation 1 READY HEAD；
5. timeout-after-apply 按 6.1 exact GET 解歧义；explicit conflict 按 6.2 只在读回 body
   完全等于 intended HEAD 时视为同一 logical attempt 已成功，任何其他 body 都进入
   FENCED，不能在进程内转 4.3、覆盖或用 LIST 拼接。HEAD 创建成功后读回 HEAD/epoch，
   必须仍属于本 writer；再复核两类 prepared set 为空和所有固定配置，把 intended HEAD
   写入 temp marker，并按 7.3 原子安装到 target，最后才能再次读回 HEAD/epoch。安装后
   的复核仍完全匹配本 writer 才能开放写入；失权时保持 admission closed，由新进程从
   权威 HEAD 重建。

epoch 后、HEAD 前崩溃只留下单调 epoch 和不可变孤儿；后继取更高 epoch、重新验证
EMPTY_SOURCE 并使用新的 snapshot ID 重做。HEAD CAS 成功后的崩溃已有完整 READY
恢复点；temp 尚未安装也不影响后继按 4.3 从远端重建。P0 不存在中间态 HEAD，也不
把任何旧本地 decision 带进 v2。

### 4.3 已有流接管

1. 保持 SQL write admission closed，GET HEAD body+ETag 与
   WRITER_EPOCH；先校验两个固定对象的 size/schema/stream 和 epoch 关系，不把本地
   datadir 作为恢复输入。按 HEAD ref exact GET 当前 snapshot manifest，仅用其已验证
   server UUID/fingerprints 和 stream 执行 7.3 target 分类；FOREIGN_OR_CORRUPT 在
   epoch CAS 前退出；
2. 用 epoch 的精确旧 ETag CAS 到本进程的新 epoch。旧 writer 最多再成功发布一个
   已使用旧 HEAD ETag 的在途 LOG/SNAPSHOT；若是 LOG，它在 post-CAS epoch check
   失败后不得 engine commit/ACK。接管者重新 GET HEAD，把读到的 body+ETag 固定为
   candidate；
3. 完整校验 candidate 的 bounded immutable chain、snapshot 和 segments，只在同一
   文件系统的唯一 fresh temp root 重建。现有 datadir 不作为恢复输入、不得先打开
   engine；完整步骤见 7.4。重建后 direct internal
   prepared 与 external XA 集合都必须为空；
4. 在没有普通 SQL 并发的 temp root 上制作 cursor 恰好等于 candidate durable cursor
   的新 snapshot，上传并 exact GET 全部对象。其 `MANIFEST_BOUNDARY` anchor 精确引用
   candidate manifest，因此新 window 只保留 SNAPSHOT manifest 和该 anchor，replay
   segment count 为 0；
5. 再 GET HEAD/epoch；epoch 必须仍属于本进程。HEAD 若已变化则 snapshot 为孤儿，
   丢弃 temp 并从新 candidate 重走第 3-4 步；HEAD 未变则创建 SNAPSHOT manifest，
   用 candidate ETag 一次 CAS 同时重锚 window、把 HEAD.writer 改为本进程；
6. 按超时规则解歧义并读回 HEAD/epoch。二者完全匹配后，才以 7.3 的 crash-safe
   install protocol 原子安装 temp root；最终状态、GTID、DD 和 prepared set 复核通过
   后进入 RUNNING。

epoch CAS 成功但 SNAPSHOT HEAD 未成功只会跳过一个 epoch，不能改变 HEAD 或事务
结果。SNAPSHOT CAS 前崩溃不增长 chain；CAS 后崩溃留下已经重锚的完整 READY HEAD，
后继仍走同一流程。因此每次失败接管都不依赖有限空 transition reserve。恢复、CAS 和
安装完成前不得接受写入、提交普通 engine transaction 或 ACK。

两个 writer 即使都误拿 lease，也只能有一个以精确 ETag 推进 HEAD。旧 writer
可以留下不可变孤儿，但它的旧 ETag 不能覆盖新 HEAD，也不能向客户端 ACK。

## 5. 提交组协议

### 5.1 prepared WAL durable barrier

MySQL 当前在写 binlog 前调用 `ha_flush_logs(true)`，位置正确，但返回值被丢弃。
P0 必须把该返回值带回 flush leader。任一参与 engine flush 失败时，不得写
binlog、segment、manifest 或 HEAD，也不得返回成功；为避免部分 prepared 状态
继续运行，remote commit 模式直接 fail-stop。后继不打开旧 root，只从未变化的
HEAD fresh restore；该未被 HEAD 覆盖的事务因此不会出现。

只重复调用 `ha_flush_logs(true)` 不足以保证 durable，必须同时固定 InnoDB 和
SmartEngine 的 flush 配置为 1。

### 5.2 统一 engine commit 准入

只在 ordered commit 的 final queue 检查 endpoint 不够。当前存在
`cache_mngr == nullptr` 等分支直接调用 `commit_in_engines()`，XA 在 `tc_log ==
nullptr` 时还会直接调用 `ha_commit_low()`；遗漏任一调用点，都可能先把 durable
mutation 提交到引擎，再发现没有远端决定。

P0 在 `commit_in_engines()` 保留尽早检查，并在真正最低公共入口
`ha_commit_low()` 的 handler commit loop 前做最后检查和原子消费。不能只在若干
caller 外围打补丁。每个准备提交的 durable THD 都必须持有一种不可伪造、一次性的
authorization：正常组 authorization 精确绑定
`stream_id + intended HEAD generation/body SHA + group endpoint + GTID/XID`，且 HEAD
已 CAS/读回并仍由本 writer 持有；恢复 authorization 则只绑定启动时固定的 candidate
HEAD 和其中已有的 GTID/XID/cursor。authorization 消费一次后立即失效，不能跨组、
跨 HEAD 或给另一个 THD 使用。

`ha_commit_low(all=false)` 不等于“不会 durable commit”：autocommit DML 的 statement
end 也是 all=false，但 engine 会真实提交；显式 `BEGIN` 中间的 statement end 则只
做 statement finalization。guard 必须复用 `ha_commit_trans()` 已计算的同一
`is_real_trans = all || !in_active_multi_stmt_transaction()` 判定，并结合 engine
read-write state；只有真实 durable transaction 才检查并消费 authorization。显式
事务的中间 statement 不消费，最终 COMMIT 恰好消费一次。

服务端经 handler 确认没有 engine prepare、DD/system table/change buffer/file
副作用的真正只读事务可以带 `READ_ONLY` 判定通过。其他没有 authorization 的路径，
包括 `cache_mngr == nullptr`、插件/系统 THD 和意外的 no-binlog durable mutation，
一律在首个 engine commit 前 fail-stop。final queue 和 `commit_in_engines()` 的检查
用于尽早报错，`ha_commit_low()` 才是不可绕过的最后不变量。XA 等有 pre-commit
副作用的命令还必须先通过 4.1 的 dispatch gate，不能指望最后 guard 回滚副作用。
所有确需持久化的新路径必须进入 ordered remote commit；不能靠新增 caller 白名单
绕过。

`ha_commit_low()` 只是 THD commit 的最低公共入口，不覆盖启动 recovery。当前
`sql/xa/recovery.cc` 的 internal XID 分支和 `TC_LOG` detached XA 路径会直接循环调用
各 handlerton 的 `commit_by_xid()`/`rollback_by_xid()`。P0 不实现本地 2PC decision
set；remote mode 下所有这类 direct XID mutation 都必须在进入 `plugin_foreach()` 或
首个 handler call 前拒绝。

stock `ha_recover()` 在 `total_ha_2pc <= opt_bin_log + 1` 时还会把全局
`tc_heuristic_recover` 改成 ROLLBACK。remote mode 必须绕过该赋值，并在每次 guarded
recovery 入口、每个 direct handler call 前和 recovery 返回后断言它仍为 OFF；任何
变化都在下一次 mutation 前 fail closed。不能只依赖启动 sysvar check，因为第一次
`ha_recover(commit_list)` 之后的第二次 `ha_recover(nullptr)` 会看到被污染的全局值。

fresh snapshot 是在无 in-flight commit、无 external XA 的精确 cut 制作的。恢复只
允许普通 engine redo/undo，不运行 stock Binlog 2PC decision；在任何 tail replay 前
对全部 handler 做 mutation-free enumerate scan，internal prepared 和 external XA 都
必须为空。任一非空都是 snapshot corruption，既不 commit 也不 rollback，直接 fail
closed。manifest 中的 XID 只用于 native binlog range 完整性校验，不授权 direct
`commit_by_xid()`。

HEAD tail 只由不可由 SQL 用户伪造的 RECOVERING THD 有界重放，并在正常
`ha_commit_low()` 消费绑定 fixed candidate HEAD 的一次性 recovery authorization。
`ha_recover(commit_list)`、`ha_recover(nullptr)`、XA recovery、detached `TC_LOG`、
`trx_coordinator::commit_detached_by_xid()`、
`trx_coordinator::rollback_detached_by_xid()` 及其 `tc_log == nullptr` 分支均命中
同一 reject-only guard；SQL dispatch 拒绝只是前置防线，不能替代 direct handler
入口的不变量。

### 5.3 SmartEngine immutable extent fence

remote commit 不能沿用当前 SmartEngine object extent 地址。当前 key 只由
`(table_space_id, offset)` 派生，`ObjectIOExtent::write_object()` 允许覆盖，
`ObjectExtentSpace::recycle()` 会无条件删除且 offset 可再次分配。把 PUT 改成
create-only 仍挡不住旧 writer 的延迟 delete 和逻辑 ID 复用。

P0 不新增一套宽泛的 extent ref 格式，而是复用现有已经进入 checkpoint、manifest
replay、LOB 和多版本恢复链的 `ExtentMeta.prefix_`。每次 allocation 都生成：

```text
canonical_prefix = <cluster>/smartengine/v2/extents/s<stream_sha256>/
  e<writer_epoch>/
  a<allocation_seq>/db=<lowercase_hex(UTF-8 database_name)>/
  idx=<canonical_decimal(index_id)>/data/
object_id = canonical_decimal(
  assemble_objid_by_fdfn(extent_id.file_number, extent_id.offset))
full_object_key = canonical_prefix + object_id
```

`stream_sha256` 必须按第 3 节的结构化 JSON array preimage 计算，不能散列路径拼接或从
可变配置另行取值。`writer_epoch` 取 remote-commit CAS 获得的 epoch，不取 SmartEngine lease 时间戳；
`allocation_seq` 复用进程内原子 `UniqueIdAllocator`。epoch 每个 writer 进程都不同，
所以二者组合不会在进程重启后复用。database 使用当前 `TableSchema` 持久化的 name，
不引入不存在的 database ID；hex 编码避免 `/`、Unicode normalization 和 path escape
歧义。object ID 精确沿用当前 `ObjectIOExtent` 的无符号十进制
`assemble_objid_by_fdfn()` 结果，不添加 `.extent` 后缀。分配时把 canonical prefix
传入 `ExtentIOInfo.prefix_`，并写入 normal/LOB 的 `ExtentMeta.prefix_`；读取、失败清理
和逻辑 recycle 都必须调用同一个 `extent_object_key(ExtentMeta)` 生成 full key，不能
退回 writer-wide prefix 或仅由 logical `ExtentId` 推导对象地址。

每个 extent body 只写一次：使用 `If-None-Match:*`，成功后 exact GET 并逐字节验证
intended body、size 和 SHA-256，之后才能让引用它的 SmartEngine metadata/WAL
durable。timeout 或 already-exists 时也只 GET 同一个 key；相同 body 是幂等成功，
不同 body 立即 FENCED。`ha_flush_logs(true)` 的 SmartEngine barrier 还必须确认本组
引用的所有新 extent 已完成上述验证，不能只 fsync 一个尚未闭合的 logical ID。

remote commit v2 下 recycle 只移除内存/metadata 所有权，禁止 ObjectStore physical
DELETE；snapshot release 和 legacy cleanup 也不得删除 `S/`。P0 remote GC 关闭，
因此旧 writer 的延迟 recycle 最多留下孤儿，不能删除或覆盖 live extent。P0 还
强制 `smartengine_persistent_cache_size=0`，因为当前 persistent cache key 只有
`ExtentId`，不含 prefix/epoch，逻辑 ID 复用会读到旧 generation。

制作 snapshot 时，在 snapshot ID 仍被 engine pin 住的窗口调用新的 SmartEngine
回调，从该 snapshot 的 normal/LOB extent layer 对每个 live `ExtentMeta` 调用上述
`extent_object_key()`，严格解析 prefix 与 object ID，导出 snapshot schema 定义的
`writer_epoch/allocation_seq/database_name_hex/index_id/object_id/key/size/SHA` tuple。
这些 extent 是 S 下的外部 immutable live refs，不作为 P 下的 materialized snapshot
payload 再复制一份。恢复只 exact GET 这份集合，且要求它与恢复后从全部 normal/LOB
metadata 重新计算的结构化 tuple/full-key 集合逐项完全相等；prefix set、root LIST、
只记录增量变化的 extent ID 列表都不是同一种类型，不能替代 exact live set。

EMPTY_SOURCE 首次 BOOTSTRAP 必须证明 legacy live extent 为空，直接生成只引用 v2
prefix 的 checkpoint 和 snapshot。任何 legacy live extent 都拒绝首次创建；P0 禁止
读取、复制或引用仍可被旧 writer 覆盖或删除的 live v1 key。旧 mutable
`smartengine.index`、`snapshot.index`、
snapshot status 和 lease 对象只能留在 legacy/advisory 路径，HEAD 模式的所有权、
发布和恢复都不得依赖它们。
远端不可变模式在 SmartEngine manifest replay 后不申请旧版 recovering 锁，
也不枚举或删除旧版 status 锁；释放本地 snapshot pin 时不写 releasing 锁。
本地 release 日志和引用清理仍执行，v2 extent 的物理删除仍被禁止。
以上 prefix 持久化、create-only read-back、snapshot live-set 和 no-delete 任一项
未落地时，SmartEngine remote commit 必须拒绝启动，不能降级为“靠 lease 单写”。

### 5.4 私有 flushed cursor 与外部 published cursor

现有 `binlog_end_pos` 同时服务 archive reader、dumper 和 waiter，必须拆开：

- `local_flushed_cursor`：`flush_cache_to_file()` 成功后推进，只唤醒 archive
  reader；它不是提交或外部可见水位；
- `remote_durable_cursor`：HEAD CAS、读回与 epoch 复核成功后推进；
- 外部 `binlog_end_pos`：引擎 commit 成功后从 remote durable cursor 发布并唤醒
  dumper/status/client。

archive reader 只读取到 `local_flushed_cursor`，不得依赖提前发布外部 end pos。
本地 binlog 没有 fsync 也不影响远端决定，因为 HEAD 前已经从 segment 读回并
独立校验；重启必须先按 HEAD 修复本地 cache，不能信任未 fsync 的本地尾部。

### 5.5 seal、上传和 HEAD CAS

提交组 leader 登记单调 endpoint。archive 线程按 event 读取，达到 endpoint 的
完整事务边界后立即 seal，不等待 slice size 或 period。不能在事务中间切片，
也不能读过本组 endpoint 后把后续未决定组一起发布。

单个进程内只有一个 HEAD publisher。每组执行：

1. 生成 deterministic segment body、size、SHA 和 key；
2. `PUT segment If-None-Match:*`，再 exact GET 校验 size/SHA；
3. 生成 canonical LOG manifest，条件 PUT 并读回校验；
4. 以缓存 HEAD body+ETag 构造 generation+1 HEAD；紧邻 CAS 前 GET 两个固定对象：
   epoch body+ETag 必须仍属于本 writer，prior HEAD 必须仍是缓存的 exact
   body+ETag，否则不发 CAS 并 FENCED；
5. `PUT HEAD If-Match:<exact-old-etag>`；
6. 解歧义后按顺序先 GET HEAD、再 GET epoch；HEAD 必须完全等于 intended body，
   epoch 必须仍属于本 writer。若 HEAD intended 但 epoch 已提高，远端决定仍有效，但本进程
   不得 engine COMMIT/ACK，直接 fail-stop，由接管者按 HEAD 完成；
7. 推进内存 remote durable cursor，此时才可删除 slice retry 状态；
8. 进入 engine COMMIT；组内任一 commit error 均 fail-stop；
9. engine commit 后、外部 end pos 和 `signal_done()` 前第三次按顺序先 GET HEAD、
   再 GET epoch；HEAD 必须仍是本组 intended HEAD，epoch 必须仍属于本 writer，否则断开连接为
   unknown 并 fail-stop，不得发普通 SQL error 或 OK；
10. 发布外部 binlog end pos，再 `signal_done(final_queue)`。

第 9 步成功是 ACK ownership linearization point。若 takeover 在该检查后、实际
socket write 前发生，逻辑顺序视为旧 writer 已先获得 ACK 许可；新的 takeover
SNAPSHOT 必须以
该 LOG HEAD 为 parent，恢复仍得到一次。若 takeover 在第三次检查前发生，旧
writer 不得发 OK。任一 HEAD 成功后发生 epoch 丢失、engine error 或进程崩溃都
不能撤销 HEAD；结果只能是客户端 unknown，恢复按 HEAD commit。

P0 在 SYNC 与 COMMIT stage 之间执行远端决定。现有 stage transition 会在取得
`LOCK_commit` 前释放 `LOCK_sync`，所以不能声称靠 `LOCK_sync` 保序；否则 B 组可在
A 组 engine commit 前发布 HEAD，A 的 final exact-HEAD check 也会被同 writer 的
B 组破坏。

实现增加单独的 FIFO `remote_commit_order_token`：A 在仍持有 `LOCK_sync` 时取得
token，完成 segment/manifest/HEAD 后带着 token 进入 COMMIT，直到 engine commit、
final ownership check、外部 end-pos 和 `signal_done(A)` 全部完成才释放；B 可以做
local flush，但必须在发布自己的 LOG manifest/HEAD 前取得同一 token。任何路径都
禁止持有 token 后重新获取 `LOCK_sync`，archive reader/publisher 也不得获取
`LOCK_sync`、`LOCK_log` 或 `LOCK_commit`。MinIO 故障因此阻塞全局提交，这是明确
的 P0 可用性代价。后续性能版可以设计允许 descendant HEAD 的流水线和独立 stage，
但不能在 P0 暗中依赖现有 stage mutex。

一组事务只等待最远 endpoint。更远水位覆盖全部较早 waiter；条件变量广播后
每个 waiter 重新比较 file/pos。binlog rotate 时，旧文件尾部必须完整发布，
新文件才能成为后续 HEAD；不能只按文件名变化判定旧目标成功。

从 LOG manifest/HEAD 发布开始到 ACK 完成，order token 禁止同一 writer 的下一
LOG 或 SNAPSHOT 插入。takeover writer 只能以当时最新 HEAD 为 parent；
任何更晚 HEAD 都不能被旧 cached ETag 覆盖。

snapshot 与 commit admission 使用一个独立 `commit_admission_mu`、状态
`OPEN|CLOSED`、`in_flight` 计数和 cond。每个可能 durable 的事务必须在 engine
prepare 和 binlog-cache flush 前持该 mutex 检查 OPEN 并原子 `++in_flight`，再释放
mutex；看到 CLOSED 就等待，不能先读 OPEN、释放后再登记。事务只在 engine commit 和 public cursor
完成后，或确定尚未 prepare/HEAD 的失败路径，重新持 mutex `--in_flight` 并广播。
quiesce 在同一 mutex 下把 OPEN 改为 CLOSED，再等待计数为 0；cond wait 期间释放
mutex。三个 snapshot source handle 固定后才在同一 mutex 下重开 OPEN 并广播。
任何 HEAD 已决定但无法正常 decrement 的路径都 fail-stop，不允许 snapshot 越过它。

P0 锁顺序固定如下，箭头表示允许的嵌套获取：

```text
登记/等待 endpoint:
  LOCK_sync -> remote_commit_order_token -> endpoint_mu
  （cond wait 释放 endpoint_mu）

进入 COMMIT stage 后:
  remote_commit_order_token -> LOCK_commit

COMMIT stage 释放后:
  remote_commit_order_token -> LOCK_binlog_end_pos
  -> publish/signal -> release token

snapshot admission（不与上述锁嵌套）:
  commit entry: commit_admission_mu -> ++in_flight -> release
  quiesce: commit_admission_mu -> CLOSED -> cond_wait(in_flight == 0)
  commit exit: commit_admission_mu -> --in_flight/broadcast -> release
```

stage transition 必须先释放 `LOCK_sync`，持有 token 后永不重新获取它。publisher
只短暂获取 `endpoint_mu` 复制/dequeue 状态，随后释放；archive seal、HEAD 状态和
任何 ObjectStore I/O 都在无 `endpoint_mu` 时执行，publisher 永不获取
`LOCK_log`、`LOCK_sync` 或 `LOCK_commit`。archive rotate/slice lock、HEAD publisher
ownership、lifecycle/fence mutex、purge 的 `LOCK_log`/index lock、snapshot engine
lock、`commit_admission_mu` 和 public cursor lock 之间默认不嵌套：锁内只复制状态，
释放后再 enqueue、等待或做网络 I/O。

shutdown/fence 在 lifecycle/endpoint mutex 内只设置状态并 broadcast，释放全部锁后
才 join；不得持锁等待 token 或 join publisher。purge/snapshot 同样不得持 binlog/
engine lock 等 HEAD 或访问网络。DEBUG_SYNC 至少覆盖：publisher dequeue 后暂停再
并发 COMMIT+shutdown；A 持 token、进 COMMIT 前暂停而 B 到达 SYNC；A 在
post-HEAD/pre-engine 与 post-engine/pre-public 两点暂停并被 takeover；snapshot
upload/publication 与 rotate+purge+shutdown 竞态。每例都要证明无锁反转、FENCED
waiter 全部唤醒、没有提前 public cursor/OK，且 B 的 HEAD 不越过 A 的 token。

### 5.6 机读状态与组证据

测试和运维不能从 SQL 尾部位置猜远端提交状态。P0 提供一个原子状态快照：

```sql
SHOW GLOBAL STATUS LIKE 'Remote_commit_status';
```

`SHOW GLOBAL STATUS` 的 value buffer 只有 1024 bytes，所以该值使用小于 768 bytes
的 versioned canonical JSON，只包含 lifecycle state、stream ID SHA、writer
id/epoch、HEAD generation/ETag SHA/body SHA 和 durable file/pos。manifest 与 segment
ref 从固定 `P/HEAD` exact GET 获取，不能塞进 status 后被静默截断。所有字段在同一个
remote-state mutex 下复制后再序列化，不能用多条 status variable 拼出一个撕裂快照。
OFF、INITIALIZING、RECOVERING、RUNNING、BLOCKED 和 FENCED 都有明确 state；当前
不适用的字段为 null，不沿用上一状态旧值。单元测试用每个字段最大值证明没有
P_S/SHOW_VAR 截断。

每个组还在第 9 步最终 HEAD+epoch 复核成功之后、外部 cursor 和 SQL OK 之前，通过
server error-log API 写一条固定前缀 `REMOTE_COMMIT_ACK_READY` 加 canonical JSON：

- schema format/version、stream ID、writer id/epoch；
- group endpoint、transaction count、canonical GTID set SHA 和 sorted XID SHA；
- HEAD generation/ETag/body SHA；
- manifest key/size/SHA；
- segment count、canonical segment-ref array SHA，以及最后一个 segment 的
  sequence/key/size/SHA/source start/end。

验收固定显式 `--log-error=<run_root>/mysqld.log`。先从 HEAD chain exact GET
manifests，找到 canonical GTID set 包含 session-tracked GTID 的唯一 LOG manifest；
再用 manifest key/SHA 和 HEAD generation 匹配 ACK 事件，取得完整 segment 列表并
逐个 GET segments/HEAD，最后核对 status 快照。单个 session GTID 不能自行推导并发
组的 GTID-set SHA，禁止直接拿二者比较。事件只提供关联证据，不是新的事务决定点；
客户端没有 OK 时不能仅凭该事件声称事务成功。MySQL error-log buffer 只有
`LOG_BUFF_MAX=8192` bytes，因此事件不
内嵌最多 4096 个 segment ref；全部字段取格式上限时，prefix+JSON 的静态最大值必须
小于 7168 bytes，并用单元测试证明 formatter 输出不含截断省略号。

## 6. 超时、幂等和 fail-stop

### 6.1 transport-unknown

timeout、断连或 SDK retry exhausted 后，请求 body、key、old ETag 和 intended
generation 全部冻结，先 exact GET 解歧义：

| GET 结果 | 判定与动作 |
| --- | --- |
| body 完全等于 intended，size/SHA/schema 正确 | 请求已应用；成功，并采用本次 GET 的 ETag |
| 固定对象 body+ETag 完全等于 prior | 尚未应用；只重试同 body 和原 `If-Match` |
| first-create 的 prior=ABSENT 且 GET=404 | 只重试同 body 和 `If-None-Match:*` |
| 第三种合法 body、更高 generation/epoch、不同 writer 或 divergent parent | FENCED；不得取新 ETag 追写 |
| prior 原本存在但 GET=404，或 schema/hash 损坏 | provider/corruption，FENCED |
| GET timeout/5xx/不可达 | BLOCKED；只继续 GET，不重试 PUT、不生成新 body、不返回 SQL 结果 |

不可变 segment/manifest/snapshot/extent 没有 prior body：GET intended bytes 的
size/SHA 相同即成功；404 才重试原 `If-None-Match:*`；不同 bytes 立即 FENCED。

### 6.2 explicit conflict 和 success read-back

HTTP 409/412 是明确 conflict，不适用“GET prior 后重试”规则：

| GET 结果 | 判定与动作 |
| --- | --- |
| body 完全等于 intended | SDK 首次请求已成功、内部重复请求才冲突；视为成功 |
| prior body、其他合法 body或更高 generation/epoch | FENCED；不得再次 PUT或拿新 ETag追写 |
| 404 | 强一致条件写合约破坏，FENCED |
| GET 暂不可用 | BLOCKED，直到能按本表分类；期间不得 PUT或返回 SQL 结果 |

PUT API 返回 success 后仍必须 exact GET read-back。read-back 暂不可用时只继续 GET，
不能重写；success 后 GET 404、不同 bytes/hash 或 schema 错都 FENCED。以上规则同样
适用于 WRITER_EPOCH 和 HEAD CAS；只有 transport-unknown 且仍读到 exact prior
时，才允许用原条件重试同一次 CAS。

### 6.3 SQL 与进程错误语义

远端请求不确定时，已进入协议的组持续阻塞并重试，且服务器停止接收更多写入。
客户端主动断开、KILL 或 shutdown 不取消组决定；它只使客户端结果变为 unknown。

不能用会先向客户端发送明确 SQL error 的 fatal helper 处理 timeout-after-apply。
确定的不变量破坏、旧 writer、hash mismatch、HEAD 后 engine error 走专用无
statement-result fail-stop：停止 listener、断开连接并终止进程。不得设置普通
`CE_COMMIT_ERROR` 后继续写，也不得用 `binlog_error_action=IGNORE_ERROR` 绕过。

## 7. snapshot、恢复闭包与恢复算法

### 7.1 snapshot 发布

首个 READY HEAD 前必须已有完整 snapshot anchor。后续 snapshot 在后台上传时
不持有 HEAD publisher；所有数据对象和 snapshot manifest 都校验完成后，才把
一次 SNAPSHOT manifest 排入 publisher。发布时必须基于最新 HEAD，保持最新
durable cursor，且 snapshot cursor 已被该 HEAD 的日志链覆盖。

takeover 是同一格式的离线特例：RECOVERING temp root 没有普通 SQL 并发，tail replay
完成后 cursor 恰好等于 candidate durable cursor。它仍要导出完整 InnoDB/MySQL DD、
SmartEngine checkpoint/meta/WAL/exact live set 和 binlog seed，并做相同 hash/GTID/
fingerprint 校验；snapshot anchor 必须是 candidate manifest。对象全部验证后才允许
用 SNAPSHOT CAS 同时接管 writer，不能把半成品 temp root 当作 snapshot。

snapshot 不能只拿 backup lock 后先后抓两个 engine；backup lock 只挡 DDL，期间
DML 仍可提交，会得到 split image。stock InnoDB Clone 的 `clone_begin()` 也不是 cut：
它只创建 handle，FILE/PAGE copy 后进入 REDO_COPY 时的 `init_redo_copy()` 才同步
binlog/GTID 并停止 redo archiving。coordinator 必须按实际 cut 点执行：

1. 先让 InnoDB Clone 用本地 staging sink 完成 FILE/PAGE copy；这时 commit 可以继续，
   不能记录 snapshot cursor、pin SmartEngine 或上传任何候选；
2. Clone 即将进入 REDO_COPY 时，严格按 5.5 在同一个 `commit_admission_mu` 内把
   OPEN 原子改为 CLOSED，再等待 `in_flight==0`；这保证不存在“已看到 OPEN、尚未
   登记”的 group。已登记事务必须走完 order token、engine commit 和 public cursor；
   尚未提交的普通事务可以继续执行，但其 commit entry 等待；
3. 在没有 in-flight commit 时，短暂持 binlog file-registry/`LOCK_log`，原子取得
   engine-committed public cursor `C` 对应文件的 purge/file ref，记录 C 与 canonical
   `gtid_executed` 后立即释放锁；不能取可能领先于 engine commit 的 remote durable
   cursor。该 file ref 保留到 seed local copy 完成；
4. 不持 order token 或 publisher/binlog lock，让 Clone 执行实际 REDO_COPY transition；
   task #34 增加 `Remote_clone_cut` 结果，因为 stock API 只返回 int。它必须在
   `synchronize_binlog_gtid` 与 redo-stop 的同一临界区原子捕获 exact binlog file/pos、
   canonical GTID+digest、redo start/end locator+digest 和 clone handle ID，并存入该
   handle 的 immutable lifetime state；coordinator 在 transition success 后读取它，
   cursor/GTID 必须逐项等于 C，redo range/handle identity 必须完整，否则取消本次
   snapshot；
5. 仍保持 commit quiesce，pin SmartEngine 在 C 的 checkpoint/meta/WAL 和 exact live
   full-key set，并把 active binlog `[0,C)` 原始字节复制、fsync 到唯一 local staging；
   期间只做本地 bounded IO，不做对象存储网络 IO；
6. Clone redo range、SmartEngine snapshot 和 binlog seed 三个 source handle 都已固定后
   才开放 commit admission。随后在保持各自 pin/handle 的前提下完成本地 materialize、
   create-only 上传和 exact GET；全部校验成功前不能发布 SNAPSHOT。

两个 engine image、cursor 和 GTID 必须代表同一个 committed cut：不得漏掉任何
`<= cursor` 的已提交事务，也不得在任一 engine image 中出现 `> cursor` 的已提交
事务。HEAD 只负责证明 cursor 之后到最新 durable cursor 的 replay 日志完整。
任一 transition、cursor/GTID 比较或 pin 失败就一起释放并取消本次 snapshot，不得
发布一个半完成的 cut。不能把 `clone_begin()` handle 当作已固定的 InnoDB image，
也不能在 Clone 实际 redo-stop 前 pin SmartEngine 后开放 commit。

固定 cursor 后还必须在同一 source-handle 生命周期内把 active binlog 的
`[0,cursor)` 原始字节保存为 immutable `binlog_seed`，并从 pinned SmartEngine
snapshot 导出 exact live full keys。三份 image 的 local staging、清单和所有远端对象
校验完成前不能释放对应 handle/pin 或发布 SNAPSHOT。后台可以继续追加 cursor 之后的
binlog；source file pin 至少保留到 seed local copy 完成。除 rotate/close 修改 FDE
in-use bit 外，`[0,cursor)` 其他字节不得变化。

snapshot 发布失败只留下孤儿。旧 writer 不得删除或覆盖新 writer 的 snapshot
目录。HEAD 未引用的 snapshot 不能用于自动恢复。

### 7.2 删除 datadir 后仍需闭合的状态

对象存储内必须包含：

- HEAD、WRITER_EPOCH、从 snapshot cursor 到 durable cursor 的 manifest/segment；
- 被 HEAD 引用的 snapshot manifest 和 active binlog 的 immutable seed；
- InnoDB Clone，包括 MySQL DD/system tables 和恢复所需 redo seed；
- SmartEngine checkpoint/meta/WAL，以及 object-store extents；若存在 local-only SST，
  本次验收直接失败；
- 已确认事务的 GTID/XID 与完整 binlog event 边界。
- snapshot manifest 中的原 server UUID、canonical `gtid_executed` 和 digest；fresh
  restore 必须据此原子重建 `auto.cnf`，不能生成 replacement identity。

以下是显式外部必需状态，不宣称从 datadir 自动恢复：

| 外部状态 | P0 规则 |
| --- | --- |
| 对象存储配置 | provider、region、endpoint、bucket、repo/branch、HTTPS 与 engine path 必须由部署系统提供 |
| 凭证 | 通过环境/provider chain 提供，绝不写入 HEAD/snapshot |
| keyring/encryption | key 与 component/keyring 加载配置外部提供；缺失或错误 fail closed |
| 启动配置 | P0 禁止 `SET PERSIST`/`RESET PERSIST`；`mysqld-auto.cnf` 不进入合同，所有 sysvar 由声明式启动配置提供并记录 digest |
| binaries/plugins/TLS | 版本、ABI、plugin/component 与 TLS 配置由部署系统提供并记录 fingerprint |

users、grants、DD、GTID table、server UUID 和 replication repository 必须由
snapshot 加 HEAD 限界 replay 恢复；不能另行手工补数据。P0 的 repository 恢复
结果必须为空，不能借恢复重新启用外部 channel。这里的“只依赖 MinIO 恢复”指
数据库状态闭包；访问 MinIO 必需的凭证、声明式启动配置、keyring 和匹配版本
二进制仍由部署系统注入，不把秘密反写到对象存储。

### 7.3 fresh root 与 crash-safe install

旧 datadir 永远不作为事务结果、prepared decision 或 replay 起点。P0 只在同一文件
系统的唯一 temp root 完成首次 EMPTY_SOURCE 初始化，或从已有 HEAD 完整重建；本地
marker 只用于防止误覆盖 foreign 目录，不进入恢复正确性证明：

```json
{
  "format": "wesql.remote_commit.local_install",
  "version": 1,
  "stream_id": "r=repo-id/b=branch-id",
  "server_uuid": "canonical-uuid",
  "installed_head": {
    "generation": 42,
    "body_sha256": "...",
    "snapshot_id": "...",
    "snapshot_manifest_sha256": "...",
    "snapshot_cursor": {"file": "binlog.000120", "pos": 789}
  },
  "config_digest": "...",
  "binary_fingerprint": "..."
}
```

temp root 的 marker 在远端 BOOTSTRAP/SNAPSHOT HEAD CAS 成功后、install 前写入；要求 temp
write、file fsync、rename、所有 engine/binlog file fsync 和 temp-root directory fsync。
marker 不含凭证，不授权复用任何本地数据，HEAD 仍是唯一事务权威。

安装前只按下表从上到下分类，命中后不得降级：

| 本地状态 | 动作 |
| --- | --- |
| target 不存在或真正为空 | `EMPTY_TARGET`：原子 rename temp root 到 target |
| target 非空，marker 可完整解析且 stream/UUID/config/binary 与本次部署相同 | `MANAGED_REPLACE`：不打开旧 engine，原子 exchange 后把旧 root 保留到唯一 quarantine path |
| marker 缺失、损坏、foreign 或 fingerprint 不匹配 | `FOREIGN_OR_CORRUPT`：fail closed，不打开、不覆盖现场 |

`MANAGED_REPLACE` 只适用于已有合法 HEAD 的恢复；HEAD absent 的首次创建只接受
`EMPTY_TARGET`，必须在创建 epoch 前完成该只读分类。

`EMPTY_TARGET` 要求 temp 与 target parent 同一 mount，atomic rename 后立即 fsync
parent；崩溃后 target 只能是 absent 或完整新 root。
`MANAGED_REPLACE` 只在 Linux 同一 mount 上使用 `renameat2(RENAME_EXCHANGE)`；不支持
该原语、跨设备、任一父目录 fsync 失败都拒绝启动。exchange 前 temp root 和 marker
必须全部 durable；exchange 后 fsync parent，旧 root 留在原 temp 名对应的 quarantine
位置，P0 不自动删除。崩溃后 target 不是旧完整 root 就是新完整 root；下一次启动仍
重新从 HEAD 构建，不信任任一方的数据内容。这样既支持普通重启，也不需要无法证明
的 same-datadir stock 2PC。

### 7.4 启动恢复顺序

1. 加载并校验外部声明式配置、凭证和 keyring，保持 write
   admission closed；GET epoch 与 HEAD body+ETag，验证
   `HEAD.writer.epoch <= epoch`。exact GET 当前 snapshot manifest 并按 7.3 对 target
   做只读分类，FOREIGN_OR_CORRUPT 立即退出；之后才用 epoch 的 exact ETag CAS 到
   fresh writer；
2. 重新 GET HEAD 并固定为 candidate。校验其 immutable manifest chain、snapshot
   manifest/data、binlog seed、segment/extent objects、size/SHA、资源上限、路径、
   cursor、event checksum、GTID/XID 和完整事务边界。遍历不能在 cursor coverage
   完成时提前停止，必须验证引入当前 snapshot 的 transition、exact anchor ancestry，
   并按新 stop boundary 复算 `recovery_window` 三项；下载只进入唯一 fresh temp root；
3. 用 snapshot `binlog_seed` 的 `[0,cursor)` 原始字节，加严格连续的
   `(snapshot cursor, candidate durable cursor]` segment 重建 native binlog 文件和
   index。同文件 body 按 start_pos 写入；跨文件必须由有效 Rotate_event 衔接，使用
   `start_pos=0` body 新建下一文件并保留 magic/FDE。结果只到 exact durable
   file:end。LIST 中孤儿、remote tail、本地 tail 和 legacy index 全部忽略，构造
   完成后先按远端原始字节逐对象校验；随后只规范化 FDE in-use bit：HEAD durable
   file 之前的所有文件清零，最后一个文件置位。FDE checksum 不含该 bit，除此之外
   不得改变任何字节；最后逐文件 fsync；
4. 根据 snapshot manifest 在 temp root 安装 engine snapshot 并重建原 `auto.cnf`；
   native binlog/index 必须在第一次 `MYSQL_BIN_LOG::open()` 和 `tc_log->open()` 前完成。
   打开 engine 时允许普通 redo/undo，但禁止 stock Binlog 2PC、`ha_recover()` 或任何
   direct XID mutation；
5. 在 tail replay 前对全部 handler 做 mutation-free scan；internal prepared 和 external
   XA 都必须为空。随后由专用 RECOVERING THD 严格回放
   `(snapshot cursor, candidate durable cursor]`，source 不得越过 candidate，
   authorization 逐事务绑定 manifest GTID/XID。串行 RLI 也必须先初始化
   `Mts_submode_logical_clock`，因为 Query event 的临时表 attach/detach 会调用它；
   不创建并行 worker。submode 归非 fake RLI 所有，清理时先销毁 RLI，再将 THD
   还原为 BACKGROUND 并销毁 Auto_THD，随后恢复调用方 THD 及线程观测关联。
   DDL 的 GTID 收尾仅在当前恢复 THD、
   禁写 binlog 的会话标志、owned GTID 与逐事务授权 digest、候选 HEAD 和
   CLOSED/RECOVERING 状态全部匹配时更新内存；不生成额外的空 binlog 事务，
   也不补写派生 GTID 表。此路径不签发或消费 engine commit 授权，原有提交守卫
   和回放结束时的单次消费核对仍然必需。temp replay 失败或进程崩溃就丢弃
   temp root 并从 snapshot 重来，不在旧/半成品 root 上补 decision；
6. replay 后再次要求两类 prepared set 为空，并验证 recovered file:pos、server UUID、
   canonical GTID set+digest、DD、users/grants、空 replication repository 和 SmartEngine
   exact live extent set。业务 row/token 等价性属于验收 harness，不是 server-side
   recovery oracle；
7. 在 temp root 制作 cursor 等于 candidate durable cursor 的完整 snapshot。
   用户 schema 内 MySQL 生成的 `table-prefix_ID.sdi` 作为 `mysql-dd-v1` 持久文件
   进入快照；启动与运行时快照共享同一分类规则，未知文件和符号链接仍拒绝。再 GET
   HEAD/epoch。先验证 epoch 仍属于本 writer；失权时 snapshot/temp 作为孤儿并进入
   不可逆 FENCED，不得在本进程重试。仍持有 epoch 但 HEAD 不再等于 candidate
   body+ETag，表示旧 writer 的合法在途 CAS 已落地；此时丢弃 snapshot/temp，固定新
   HEAD 为 candidate 并从第 2 步重来；
8. 创建以 candidate manifest 为 exact previous/anchor 的 SNAPSHOT manifest，按 6 节
   解歧义后用 candidate ETag CAS HEAD。读回 HEAD/epoch 必须精确等于 intended SNAPSHOT/
   本 writer；该 CAS 同时完成 takeover 和 recovery-window rebase；
9. 将新 HEAD 和 snapshot 写入 temp marker，按 7.3 分类 target 并完成 atomic install。
   install 后，协调器再次 GET HEAD/epoch，复核 marker、file:pos、UUID、GTID、DD、
   repository、extent live set 和两类 prepared set。协调器返回 READY_FOR_ADMISSION
   只表示这一层验证完成；适配器须重新核对配置的根身份，把完整证据和精确
   HEAD body/ETag、cursor、GTID/digest 交给 Server 的
   `verify_installed_root_post_engine()`。该入口重查已采用的 HEAD/epoch 与原生
   预恢复状态，成功后才转为 ROOT_VERIFIED，此时普通写入仍关闭。随后清理启动
   控制目录，启动运行期快照服务并确认就绪，最后调用 `open_commit_admission()`
   进入 RUNNING 并允许网络初始化。顺序必须为“协调器验证 → Server ROOT_VERIFIED
   → 服务就绪 → 开放准入”；任一验证失败都在启动服务和开放准入之前返回，服务
   初始化失败也不得开放准入。

任何对象缺失、hash 错、gap/overlap、截断事务、未知版本、错误 epoch 或非法路径都
fail closed。恢复期间 HEAD 变化只允许按第 7 步在仍持有 epoch 时丢弃全部临时结果并
从新 candidate 重建；其他 HEAD/epoch 竞争都进入 FENCED。自动启动不得静默回退到旧
snapshot、本地 cache 或 legacy index。

## 8. purge 和 GC

远端模式不再维护 `mysql.gtid_executed` 派生表：GTID 集合由认证的 snapshot
binlog seed（包含 Previous-GTIDs 及截至 cut 的原始事件）和已提交 segment 重建，
并与 snapshot 的 canonical GTID/digest 核对。`Gtid_table_persistor::save(Gtid_set)`
与 `compress()` 在此模式下不创建表事务，覆盖后台落表、启动补写、rotation 和
关闭时补写；内存 GTID 集合和正常 binlog 事件仍照常更新。单个 THD 的 GTID 保存
入口及 engine commit 授权检查不因此获得例外。普通模式继续原有派生表持久化。

安装后的 re-exec 重开认证的末 binlog，并未创建空文件。因此仅在精确
INSTALLED_REEXEC_PRE_RECOVERY 授权且准入 CLOSED/drained 时，启动 GTID 扫描保留
索引末项，随后仍与发布快照的 GTID/digest 比较。普通启动沿用跳过新建空末文件
的规则；takeover worker 不借此把尚待回放的尾部事务提前标成 executed。

takeover worker 在开始 bounded replay 前，从当前精确 HEAD 引用的 snapshot
manifest 恢复内存 GTID 基线。必须重新核对 HEAD body/ETag、快照字节数/SHA、
解析结果、snapshot ID 和 cursor，且仍持有采用的 epoch、准入 CLOSED/drained、
没有事务授权或 owned GTID。一次性恢复替换 stock startup 可能从多个 binlog
文件预读的 executed 集合，并清空派生的内存缓存；它不调用会写表的
`Gtid_state::clear()`。后续逐事务回放只增加 `(snapshot, HEAD]` 中已实际提交
的 GTID。即使回放区间为空也必须恢复基线；重复调用、失权、普通运行或
installed re-exec 均拒绝。安装后仍独立从 binlog 重建并校验完整集合。

P0 禁止 `P/` 下的远端自动 GC。旧 purge 命令不得删除 HEAD、epoch、manifest、
segment、v2 snapshot 或 immutable SmartEngine extent；孤儿和历史对象暂时保留。
恢复 reader pin、保留代数和
grace period 尚无完整协议，不能边开发提交路径边猜测删除。

本地 binlog 只有在 READY HEAD 连续 durable cursor 覆盖整个文件后才能删除。
同文件部分覆盖、对象已 enqueue、单个 slice 存在或派生 index 出现都不能触发
整文件 purge。

后续远端 GC 必须另行设计 immutable GC plan、HEAD generation pin、live-set
遍历和删除前二次 HEAD 校验，不属于本 P0。

## 9. 崩溃和歧义矩阵

| 点 | 权威结果 | 客户端与恢复 |
| --- | --- | --- |
| engine WAL barrier 前 | HEAD 不变 | 不得 OK；fresh restore 中事务 absent |
| SmartEngine 写超过 bulk threshold、HEAD 前 | 中途提交被代码禁用 | 强杀后 fresh restore 不得留下 row；直接 flush 路径 fail-stop |
| WAL durable 后、完整 binlog 前 | HEAD 不变 | 不得 OK；旧 root 隔离，fresh restore 中事务 absent |
| local binlog flush 后、segment 前 | 本地尾部非权威 | 不得 OK；恢复忽略尾部 |
| segment 成功、manifest 前 | segment 是孤儿 | 不得 OK；恢复忽略 |
| manifest 成功、HEAD 前 | manifest/segment 是孤儿 | 不得 OK；恢复忽略 |
| segment/manifest 响应丢失 | exact GET 解歧义 | 相同继续，缺失重试，不同 fence |
| HEAD CAS 已应用、响应丢失 | GET intended 即 committed | 完成 engine commit；崩溃则恢复必须出现一次 |
| HEAD 后、engine commit 前 | 远端已决定 commit | 不得 rollback；后继只以 fresh replay 完成 |
| engine commit 后、外部 end pos/OK 前 | committed，响应歧义 | 恢复出现一次，retry token 不得重复 |
| 客户端收到 OK 后 | 全部 ACK 前置成立 | 立即 kill+删 datadir 仍出现一次 |
| 旧 writer 上传 segment | 最多产生孤儿 | 新 HEAD 不变，旧 writer 不得 ACK |
| 旧 writer CAS HEAD | 旧 ETag/epoch 失败 | 旧 writer fail-stop |
| timeout 长期无法解歧义 | BLOCKED/人工 kill | 不得 OK，也不得明确 rollback error |
| snapshot 上传中崩溃 | HEAD 仍引用旧 snapshot | 新对象为孤儿，恢复只用旧 anchor+HEAD chain |
| binlog seed 已上传、SNAPSHOT HEAD 前崩溃 | seed 是孤儿 | 不得用 LIST 采用；仍由旧 snapshot+tail 恢复 |
| Clone FILE/PAGE copy 或 REDO_COPY cut 中崩溃 | HEAD 仍引用旧 snapshot | staging/pin 丢弃；不得把 clone_begin handle 发布为 cut |
| fresh restore scan 发现 internal prepared 或 external XA | 尚未执行任何 XID decision | 整体 fail closed；两类都不 commit/rollback |
| B 已 CAS epoch，A 用旧 HEAD ETag 成功发布 LOG，A post-CAS 发现失权 | A 的 LOG HEAD 永久有效，epoch 可高于 HEAD epoch | A 不得 engine commit/ACK并 fail-stop；B 或后继从该 HEAD 恢复一次 |
| A post-CAS 通过，B 在 A engine commit 前取得 epoch | A 的 LOG HEAD 有效，B 尚未发布 SNAPSHOT | A final ACK check 失权并断连；B 从含该 LOG 的 candidate fresh replay |
| A engine commit 后、final ACK check 前 B takeover | A 的 LOG HEAD 有效，本地 engine 可能已 commit | A 不得 OK；B 丢弃旧 root并 fresh replay，结果恰好一次 |
| A final ACK check 后、socket write 前 B takeover | A 的 ACK 许可在线性化上先于 B | A 可以 OK；B 的 SNAPSHOT 包含 A 的 LOG 结果 |
| epoch CAS 成功、takeover SNAPSHOT 前崩溃 | HEAD 不变，只跳过 epoch | 新 writer 取更高 epoch，从旧 HEAD fresh restore |
| 首次创建前 target/source/prepared 检查非空 | 尚无 v2 对象 | 不创建 epoch/HEAD，不覆盖 target、不做 XID mutation |
| 首次 epoch 已创建、BOOTSTRAP HEAD 前崩溃 | HEAD 不存在，epoch 单调保留 | 新 writer 取更高 epoch，重新验证 EMPTY_SOURCE并重做 snapshot |
| BOOTSTRAP/SNAPSHOT manifest PUT 成功、HEAD CAS 失败 | manifest/data 为孤儿，prior HEAD 或 absent HEAD 权威 | 不得 LIST 采用；从权威 HEAD 重做 |
| recovery snapshot 完成、CAS 前 HEAD 改变 | 新 HEAD 权威，旧 candidate 失效 | 仍持有 epoch 才能丢弃 temp/snapshot 并从新 candidate fresh restore；epoch 失权则 FENCED |
| BOOTSTRAP 或 takeover SNAPSHOT HEAD 成功、local install 前崩溃 | 新 READY HEAD 权威 | 后继从该 HEAD fresh restore；不依赖旧 temp/root |
| 旧 datadir 非空 | 本地不是 authority | 不打开 engine；匹配 marker 才保留后 exchange，否则 fail closed |
| legacy source 非空 | P0 不支持迁移 | 在首个远端对象前拒绝，不读取 live v1 补救 |
| transport-unknown 且 GET 仍为 prior | 尚未决定 | 同 old ETag/body 重试；客户端 BLOCKED |
| 明确 409/412 且 GET 仍为 prior | ownership 竞争或 provider 矛盾 | 不重试；当前 writer FENCED |
| 旧 writer 延迟写/回收 SmartEngine extent | create-only 或 no-delete，只能留下孤儿 | live v2 extent 不被覆盖/删除；旧 writer 不得 ACK |

全局不变量：ACK 事务恰好一次；未 ACK 事务零次或一次，且与 HEAD 一致。

## 10. 实现边界

task #34 的产品改动至少包含：

1. `ObjectStore` 条件 create/CAS 与 GET body+ETag 的结构化接口；file upload 也要
   支持条件创建和 size/SHA 校验；
2. 独立 remote commit sysvar、启动配置校验和运行期降级拒绝；
3. writer epoch/HEAD/manifest 的 RFC 8785 JCS 序列化、strict parser、recovery-window
   prospective/rebase counters 和单 publisher；
4. `ordered_commit()` 检查 engine flush error、拆分 private/public cursor、
   在 engine commit 前等待 HEAD、在 `signal_done` 前检查 post-decision error，并在
   `commit_in_engines()` 早检、`ha_commit_low()` handler loop 前消费一次性
   authorization；另在 `ha_recover()`、XA recovery 和 `TC_LOG` 所有 direct
   `commit_by_xid()`/`rollback_by_xid()` caller 前执行 reject-only guard；
5. archive 的 exact endpoint seal、不可变 PUT、read-back、状态保留和条件广播；
6. snapshot 唯一对象、binlog seed、InnoDB Clone 实际 REDO_COPY cut 与
   `Remote_clone_cut` API、SmartEngine exact live full-key set 与 HEAD 原子引用；
7. recovery 只认 HEAD chain，必须在首次 binlog-index open 和 `tc_log->open()`/
   stock `Binlog_recovery` 前用 seed+segments 按 exact end 在 fresh temp root 重建 native
   binlog/index；禁止本地 2PC，使用 crash-safe atomic install；
8. local purge gate、P0 远端 GC 禁用；
9. 全量 SQL command/subtype 正向分类、durable mutation/binlog coverage gate、
   GTID/ROW/binlog encoding/transaction-size/engine/XA 配置约束，外部 replication
   和不可重放 admin mutation 的提前拒绝；
10. SmartEngine canonical stream/epoch/allocation `ExtentMeta.prefix_` 与 full-key serializer、
    create-only read-back、persistent cache 禁用、WAL 强制开启、commit-in-middle 禁用、
    exact snapshot live set、fresh EMPTY_SOURCE gate 和
    P0 no-delete；
11. 明确 endpoint mutex/cond、archive read+seal、order token、HEAD publisher、
    lifecycle/fence/shutdown、purge/snapshot 和 public-cursor wakeup 的锁图，并用
    DEBUG_SYNC 覆盖反向获取、shutdown 与 takeover；
12. deterministic fault hooks、`Remote_commit_status`、
    `REMOTE_COMMIT_ACK_READY` 和 MTR/MinIO 测试。

不在本轮：跨 AZ 双桶、S3 Express 性能结论、远端 GC、外键、控制面、独立高性能
remote commit stage，以及 pre-v2/local 数据迁移。未来迁移必须另立带部署围栏和新
format version 的设计；task #34 产品入口只接受 EMPTY_SOURCE。legacy SmartEngine extent layout、inline physical recycle
和 snapshot cleanup 不得进入 v2 live set；不能仅引用 lease 作为通过依据。

## 11. 验收门槛

设计修订先做 owner/subagent 只读协议与可读性检查，并更新既有 infracreate 页面；
不设置中途人工等待闸门，也不把任何未完成 Server diff 当候选。task #34 随后
自主实现和验证，task #36 只在唯一完整 candidate SHA 上做一次终态审查。task #34
至少完成：

1. Oracle `mysql-9.7.2@008e09c2834b98143a8c067d4d225c90953050cf`
   forward/apply/reverse/零残留检查；
2. fresh Linux/aarch64 source/build/install，GCC 13.3、CMake 3.28.3、
   Ninja 1.11.1、8 GiB 环境 `ninja -j1`；验证 mysqld、clients、
   `mysql_clone.so`、`component_reference_cache.so`；
3. 大 slice/长 period 下单笔事务仍主动 seal，SQL OK 晚于 HEAD 覆盖和对象校验；
4. 并发组提交证明一组产生一个或少量连续 segment、一个 manifest 和一次 HEAD
   CAS，而不是一事务一次；每个 segment body 逐字节等于声明的 native file range；
   DEBUG_SYNC 构造“失败的中间 THD + 后续成功 THD”，HEAD 不得越过中间事务；
5. `sync_binlog=0/1/N` 都遵守相同远端决定；unsafe engine flush、非 CRC32 checksum、
   transaction compression、binlog encryption、partial row value、SmartEngine
   persistent cache、`smartengine_write_disable_wal=ON`、可重新启用 commit-in-middle 的
   knob、非 OFF `tc_heuristic_recover` 和 `binlog_error_action=IGNORE_ERROR` 在启动或
   动态 SET 时被拒绝；
   两类 binlog cache 在上限恰好等于/超过 segment payload 边界时分别通过/在 prepare
   前报错；
6. 已有 external replica channel、Group Replication、internal prepared 或 external XA
   都阻止首次 BOOTSTRAP；fresh restore 的任一 prepared set 非空也阻止 takeover/
   RUNNING；运行期所有 replica/GR 管理语句与 XA mutation 在副作用前拒绝；
7. `sql_log_bin=0` 持久写、匿名 GTID、非 ROW/FULL、非 InnoDB/SmartEngine 持久表、
   plugin/component/UDF mutation、`CREATE/ALTER/DROP SERVER`、replication filter、
   Clone、`ALTER INSTANCE`、所有 RESET BINARY LOGS/GTIDS 变体、`gtid_purged` 和三种
   PERSIST 操作均在变更前拒绝；每个 SQL enum/subtype 都有非 UNCLASSIFIED 结果；
   真正只读无 endpoint 组可通过，debug 路径分别绕过 `cache_mngr` 和
   `commit_in_engines()` 时仍在 `ha_commit_low()` 前 fail-stop；autocommit DML 的
   all=false 路径必须消费 authorization，显式 BEGIN 的中间 statement 不消费、最终
   COMMIT 恰好消费一次；SmartEngine 分别执行超过 10000 行 UPDATE/DELETE 后 ROLLBACK
   和 pre-HEAD crash，不能出现 `flush_batch()`/`commit_no_binlog()` 提前持久化；
8. segment、manifest、snapshot/seed、extent、HEAD、epoch timeout-after-apply 均按
   exact GET 解歧义，并分别覆盖 transport-unknown、明确 conflict、
   success/read-back 超时；
9. 两 writer pause/takeover 覆盖 pre-CAS、post-CAS、post-engine 三个窗口，证明旧
   ETag/epoch 不能 ACK；旧 writer 延迟 extent write/recycle 不能改坏 live data；同一
   bucket/cluster prefix 下两个 branch 使用相同 epoch/allocation sequence 时 full extent
   key 仍不同，cross-stream metadata 必须拒绝；
10. HEAD 后、engine commit 前强杀并丢弃原 datadir，fresh replay 得到一次且
    `XA RECOVER` 为空；在 temp replay 中途强杀后必须丢弃半成品并从 snapshot 重做，
    结果仍恰好一次。DEBUG_SYNC 分别从 `ha_recover(commit_list)`、
    `ha_recover(nullptr)`、XA recovery、detached `TC_LOG` 和 `tc_log == nullptr` 分支
    直达 guard，全部在 `plugin_foreach()`/首个 handler mutation 前拒绝；注入 internal
    prepared、external XA 或混合集合都只允许 enumerate 后 fail closed，不能 commit
    或 rollback。heuristic COMMIT/ROLLBACK 启动均拒绝，guard 前后全局仍为 OFF；
11. snapshot cursor 取非平凡的文件中间位置；分别在 HEAD 尚等于 snapshot、随后在
    同文件提交、随后跨 rotate 提交三种状态删除全部本地 binlog/index，证明 seed 加
    exact native-range segments 能在首次 `tc_log->open()` 前重建文件并打开到
    exact HEAD；另在 seed copy 时并发 rotate，证明 file pin 有效、旧文件 in-use bit
    清零且仅 HEAD 最后文件置位；
12. SQL OK 后立即 kill，删除容器、整个 datadir、local binlog/archive/snapshot/
   recovery tmp/socket/pid，只保留 MinIO 和已声明外部状态，fresh 恢复后 token、
   原 server UUID、GTID、DD、users/grants 恰好一次，再重启并提交下一 token；
13. 在 Clone FILE/PAGE copy、实际 REDO_COPY transition 和 SmartEngine pin 前后分别
    暂停一个同时修改 InnoDB/SmartEngine 的事务；验证 `clone_begin()` 不记录 cut，
    quiesce 中 `Remote_clone_cut` 的 cursor/GTID/redo-range/handle ID、public cursor 和
    SmartEngine pin 完全相等；缺失或撕裂字段取消 snapshot；
    恢复后该事务只能在两个 engine 都 absent 或都 present；
14. DEBUG_SYNC 锁图用例覆盖 publisher dequeue、双组 token、两次 takeover、snapshot
    与 rotate/purge/shutdown，以及 commit entry 在原子 admission 登记前/后的暂停；
    CLOSED 后不能新增未计数 prepared group，所有 waiter 可退出且无提前
    status/event/cursor/OK；
15. status JSON 与每个 SQL OK 的 `REMOTE_COMMIT_ACK_READY` 事件能经“session GTID
    -> containing manifest -> manifest key/SHA+HEAD generation -> event”关联，并通过
    MinIO exact GET 逐字段复核 HEAD/manifest/segment；并发多 session 同组也通过；
    status 小于 768 bytes、最大字段事件小于 7168 bytes，二者均无 truncation；
16. empty target 可原子安装；带精确同 stream/UUID/fingerprint marker 的旧 datadir
    不打开 engine，`RENAME_EXCHANGE` 后完整保留到 quarantine；无 marker、marker
    损坏、foreign、跨设备和半删除目录全部 fail closed 且不覆盖。分别在 exchange 前后
    强杀，重启只能看到旧完整 root 或新完整 root，且两种情况都重新 fresh restore；
17. missing object、bad hash、gap/overlap、cycle、超限 chain、truncated transaction、
    divergent manifest previous、HEAD.parent 与 manifest.head_parent 不等、恶意 LOG
    改写 snapshot/base、cursor coverage 后缺 snapshot-introducing transition/anchor、
    wrong epoch、wrong keyring、legacy/local-only
    extent、seed 缺失/截断和 unresolved timeout 全部 fail closed；full-key serializer
    与恢复 metadata 不一致、snapshot body 中 checkpoint/meta/WAL/cursor/live-set
    array 与恢复结果不一致也必须拒绝；non-empty target、legacy source、用户数据、旧
    TC/binlog authority 和首次
    internal/external prepared set 都必须在创建 epoch/HEAD 前拒绝。覆盖 epoch 后、首个 HEAD
    前强杀，BOOTSTRAP/SNAPSHOT object 后、HEAD CAS 前强杀，以及 BOOTSTRAP/takeover
    SNAPSHOT CAS 后、local install 前强杀；restored direct-prepared 非空必须在 replay
    前拒绝；
18. 为所有 JSON body/digest 提供 RFC 8785 golden vectors；用 4096-segment LOG 连续发布
    验证 segment cap 在远早于 80,000 manifest 时触发 snapshot，分别在 generation、
    manifest bytes 和 segment count 的 operational edge 强杀。SNAPSHOT 的三项 counter
    必须从新 anchor retained suffix 重算并与 reader 完全相等；连续多次 writer 在
    takeover SNAPSHOT CAS 前/后崩溃，下一次仍能以单次 SNAPSHOT 重锚到 hard limit 内；
19. 只发布一个完整 candidate SHA 给 task #35/#36/#37，历史 8.0、旧 v11、
    local provider 和开发自测不能替代独立验收。

P0 每个 group commit 至少产生 segment PUT、small manifest PUT 和 HEAD CAS，费用
模型必须按三次条件写及相应 GET 计费，不能继续沿用“一组一次 S3 请求”的数字。
后续可把 manifest header 合并进 segment 降低请求数，但必须保持同一 HEAD/CAS
和恢复校验语义。

## 启动配置检查的时机（2026-09-05）

远端启动发生在命令行解析完成之后、MySQL 初始化 GTID 子系统之前。
此时检查 `gtid_mode` 和 `enforce_gtid_consistency`，必须读取已经解析的配置值。
不能读取运行时 GTID 缓存，也不能调用要求 GTID 锁已存在的接口。
例如，命令行明确设置 `--gtid-mode=ON` 时，运行时缓存暂时仍为 OFF，
不能据此拒绝启动。配置本身为 OFF 或任一 PERMISSIVE 模式时仍须拒绝。

这项修正不提前创建 GTID 子系统，也不放宽返回成功或恢复的条件。
回归验证需覆盖“配置 ON、运行时缓存 OFF、GTID 锁尚未创建”的启动状态，
再用新候选执行首次启动、写入和空目录接管。

内部 BOOTSTRAP_PREFLIGHT 子进程只初始化空目录并提交检查结果，不提供客户端
服务、不取得写入权。MySQL 在 `--initialize-insecure` 阶段主动关闭 binlog，
因此该子进程的配置检查允许这一项临时状态，其余持久性检查继续执行。
这项例外由启动适配器在已解析为 BOOTSTRAP_PREFLIGHT 且确实处于初始化时传入。
普通父进程、快照子进程、接管进程和最终服务进程仍须开启 binlog；
子进程还必须证明 HEAD 不存在、准入关闭且事务已排空，才可以完成空目录检查。

同一隔离初始化阶段允许 MySQL 编译进二进制的数据字典与系统表初始化语句
修改尚未发布的临时目录。这仅限 `SYSTEM_THREAD_DD_INITIALIZE` 和
`SYSTEM_THREAD_SERVER_INITIALIZE`，并同时验证初始化选项、内部 preflight 模式、
HEAD 不存在、准入关闭、未取得 epoch 等状态；客户端、init-file、DD 重启与升级
线程均不在例外内。该能力贯穿 SQL 准入与最终 engine commit 检查，不打开普通
写入准入，也不生成远端事务成功声明；初始化结果仍须通过 EMPTY_SOURCE 检查
及首次快照发布，才能成为正式数据源。

初始化阶段的 TC_LOG_DUMMY 仅在同一空根能力有效时跳过普通 `ha_recover`；
这一条件不改变普通启动、按 XID 提交或回滚、binlog 恢复的拒绝规则。
此子进程暂不加载 SmartEngine，系统表使用 InnoDB，防止在没有 epoch 时
为 SmartEngine 内部元数据分配远端区间。后续快照子进程使用原配置，
在采用父进程 epoch 后首次打开 SmartEngine。

该 bootstrap 快照子进程重新打开初始化目录时，MySQL 的编译内 DD 重启流程
同样使用 `SYSTEM_THREAD_DD_INITIALIZE`，并执行不写 binlog 的系统表语句。
只有这个线程可以在精确采用父 epoch、HEAD 仍不存在、生命周期仍为
INITIALIZING、普通准入关闭且排空时完成 DD 工作；其结果由首次完整快照覆盖。
这不向客户端、init-file、系统升级线程或接管/已安装根授予同一例外。
进入普通准入、关闭或其他启动角色后，能力立即失效。

InnoDB 编译内 PFS 建表调用单独创建 `Auto_THD`，其原始类型为 BACKGROUND。
仅在上述 bootstrap 快照能力成立时，这一次调用的作用域临时将其标为
DD_INITIALIZE，每次提交仍重新检查生命周期。作用域退出（包括错误返回）
后恢复 BACKGROUND，再由 Auto_THD 销毁；其他后台线程不获得此授权。

接管 worker 与完成预恢复验证的 installed re-exec 另有只用于 SQL 检查的 DD
缓存重建例外。它要求已采用的精确 epoch/HEAD 仍有效、准入关闭且排空，线程是
DD_INITIALIZE，未启用 initialize，DD 和 Server 版本完全相同，且阶段恰为
FETCHED_PROPERTIES。只允许单个已注册 DD 表的非临时 CREATE TABLE，以及该建表
循环的单项会话 `SET FOREIGN_KEY_CHECKS=0/1` 字面量设置；全局/持久 SET、其他变量、
表达式、用户表、升级/降级、其他 SQL 或其他阶段均不适用。此时上游 `mysql_create_table_no_lock`
对注册 DD 表设置 `no_ha_table`，`Storage_adapter::store` 在 CREATED_TABLES 前
仅执行 `core_store`，因此重建的是缓存。该能力不接入 begin/check/consume 提交
入口，不授予持久提交；已有 published-root 提交拒绝规则继续生效。

同一版本重启在 SYNCED 阶段另允许不带表名、附加刷新标志或 NO_WRITE_TO_BINLOG
的 `FLUSH TABLES`，只供 DD 同步后重新打开缓存。该阶段的字符集重填改为只读验证：
读取完整 character_sets/collations 集合，按当前编译定义比对主键、名称、默认排序
规则、字符最大长度、注释、字符集关联、compiled、sort_length 和 pad_attribute。
缺行、多行、重复主键或字段不同均拒绝启动；匹配后进入 POPULATED 并结束只读事务。
验证前后均重查已采用的启动授权，不对已有快照执行 INSERT/UPDATE/DELETE，也不
扩大任何提交入口。bootstrap 初始构建仍走上游正常填充。

InnoDB 重启恢复在字符集验证之前还会无条件更新 innodb_temporary 的 DD 文件名。
对具有同一 SYNCED 启动授权的已发布根，此调用改用只读 acquire，要求表空间名、
InnoDB 引擎、单文件集合及文件名字节均与当前配置一致；缺项、多项或差异均失败。
匹配时不调用 acquire_for_modification/update，避免把冗余更新留给后续 DD 事务
提交。初始化与非远端路径保持原有更新行为。

资源组加载发生在同版本 DD 完成后的 FINISHED 阶段。已发布根仅恢复内存资源组，
核对两个默认组的精确名称、类型、启用状态、空 CPU 掩码及零优先级，不执行默认
组的冗余 DD 更新。默认组缺失、重复或内容不同均失败；自定义组仍走原反序列化
和控制参数验证，但参数无效时拒绝启动，不把禁用状态写回已发布快照。读事务
结束前重查同一启动授权。bootstrap 与普通启动保留原创建和更新路径。
WESQL 的自定义组 CPU 掩码转换保留最高位为 1 的尾段，避免原转换遗漏末段后使
控制参数验证漏看 CPU 绑定；空掩码、每个单点、连续尾段、分离区间和全掩码均
按 DD 掩码重新编码核对，验证前不丢失任何位。

已发布根的 InnoDB redo PFS 注册同样只读恢复。FINISHED 阶段在编译内置调用点
建立线程局部作用域，绑定 performance_schema.innodb_redo_log_files 的原始编译
定义，不改变 BACKGROUND 线程类型或提交权限。此作用域只省去持久 DROP/CREATE，
仍由原 PFS 服务注册内存表及其回调。服务先比对注册定义与作用域绑定，再用原
SQL 解析器、mysql_prepare_create_table 和 DD 构建器生成内存描述；不执行 DDL。
该描述与只读取得的已有表使用原 DD SDI 序列化器完整比对，仅忽略对象 ID 和
创建/修改时间，并额外拒绝触发器。缺表、定义不同、授权失效或注册失败都会
停止启动。验证前后重查启动授权；作用域退出后恢复正常服务路径。

恢复后的本地 binlog 必须复用已有日志流，并显式把底层写位置与内存位置同时
定位到已认证 cursor。普通新建日志的 open() 即使文件已存在也从位置 0 开始，
不能用于恢复续写。安装后的末条 index 文件名和物理文件长度先与认证边界严格
比对；不一致即拒绝，不截断尾部、不增加格式事件或 index 项。仅更新原有
in-use 标记并同步，再定位末尾，最后公开日志状态和末尾位置。

初始化预检不要求尚不存在的 binlog cursor，也不申请 SmartEngine 快照。
它在全局读锁下前后两次检查 DD、初始账号及权限、复制仓库、prepared 和空
GTID 集；同时要求 SmartEngine 未加载、对应目录不存在、TC 与 binlog 文件
不存在，且授权和两个样本均保持不变。取得 epoch 后的快照阶段仍执行完整
EMPTY_SOURCE、binlog cut 和 SmartEngine 精确 live-set 校验。

首次 bootstrap 安装后的最终校验仍执行同一 EMPTY_SOURCE 双采样，但权限按
实际安装阶段选择：安装前要求精确 bootstrap snapshot worker，安装后要求
已绑定 epoch、HEAD 和 install marker 的 INSTALLED_ROOT 预恢复授权。两者都
要求 BACKGROUND 内部线程、同版本 DD 的 FINISHED 阶段和关闭且排空的准入。
全局读锁下的前后样本、prepared 排空、SmartEngine 快照以及全部一致性比较
继续执行；采样结束重新核验权限，失权即拒绝。此只读能力不连接提交入口。

运行期快照服务只创建全新的绝对路径目录。文件系统明确返回 not_found 且
错误为空或 ENOENT 才表示可创建；已有文件、目录、包括悬空链接在内的链接，
以及其他 I/O 错误仍拒绝。父路径必须为真实目录，创建后同步父目录并使用
0700 权限；这些检查不因正常的目标不存在状态而被跳过。

复制清单保留原始通道数，并单独识别 MySQL 自动创建、尚未配置的默认通道。
只有默认名称、无 source 配置、MI/RLI 未初始化、无线程和 worker，且三个
持久仓库都零行时才视为无复制状态；其他通道仍拒绝。检查期间保持通道映射
与默认通道运行锁。旧 tc.log 检查只接受明确的文件不存在；现存文件、目录、
悬空链接和其他文件系统错误均拒绝。

SmartEngine Env 与 Server 各自拥有独立的对象存储客户端。远端 extent 初始化
使用已采用运行时中的客户端，绑定其 bucket、stream 和 epoch；不把两个独立
客户端的指针相等当作配置一致的证据。后续条件写入仍检查绑定指针及 bucket
与当前运行时一致，并执行原有前缀、epoch、内容和精确读取校验。
