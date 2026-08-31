# 9.7 远端提交日志设计

授权：task #13 总账与 task #34 实现包。长期分支：`9.7`，固定起点
`4e26b01e194c3b7617c3491b9bc1ac7f94e39716`。本设计更新 task #13
原有的 S3 Express One Zone 评估，把“可作为低延迟热层”收敛为可执行的
提交协议；本轮先用 MinIO 验证正确性，不把 MinIO 延迟当作 S3 Express
性能结论。

## 1. 目标和承诺

当 `serverless=ON`、`binlog_archive=ON` 时，事务的成功响应满足：

> 客户端收到成功前，覆盖该事务结束位置的 binlog slice 已写入对象存储，
> 且远端 `binlog-index.index` 已发布该 slice。删除计算节点、容器和整个本地
> 数据目录后，恢复流程只依赖对象存储，也能看到这笔已确认事务。

这里的“远端耐久水位”是源 MySQL binlog 的 `(file, end_pos)`，不是本地
archive 文件偏移，也不是“PUT 请求已经发出”。只有同时满足下面两个条件，
水位才能前进：

1. slice 对象 PUT 成功；
2. 包含该 slice 条目的完整 `binlog-index.index` PUT 成功。

顺序固定为 **slice 先、index 后**。恢复只读取 index 中已发布的 slice，
因此远端 index 是提交可恢复性的发布点。任何仅有 slice、没有 index 条目的
状态都不能向客户端返回成功。

本地 binlog 只承担写入缓存和故障诊断作用。远端水位覆盖后，它可以被删除，
不能再作为已确认事务的耐久性前提。

## 2. 当前实现与缺口

9.7 当前链路如下：

1. `MYSQL_BIN_LOG::ordered_commit()` 在 FLUSH 阶段把提交组写入本地 binlog，
   在 SYNC 阶段按 `sync_binlog` 决定是否 fsync，然后进入引擎 COMMIT 阶段。
2. `Binlog_archive` 后台读取本地 binlog，只在事务边界切 slice。
3. slice worker 调用 `ObjectStore::put_object()`；index worker 等连续 slice
   都成功后，覆盖写远端 `binlog-index.index`。
4. index PUT 成功后，`m_persisted_mysql_binlog_file_name` 和
   `m_persisted_mysql_binlog_last_event_end_pos` 才前进。
5. `binlog_archive_wait_for_archive()` 已能等待这个水位，但现有提交路径不调用
   它；空闲 slice 默认还可能等 `binlog_archive_period` 才切出。

因此当前“后台归档成功”不等于“事务提交时已经远端耐久”。P0 需要补两个窄
接点：提交组等待远端水位，以及提交需求触发事务边界立即封片。

## 3. 提交协议

### 3.1 提交组端点

一个 binlog group commit 只做一次远端等待，不按事务逐个 PUT。

SYNC 阶段完成本地 flush/sync、且最终队列已经确定后，leader 遍历
`final_queue`。对 `commit_error == CE_NONE` 的 THD 读取
`get_trans_fixed_pos()`，取队列中最远的 `(file, pos)` 作为组端点。没有
binlog 端点的组不进入远端等待。

本轮不改变 binlog group 的组成，也不创建“一事务一对象”。归档线程可以把
多个事务、甚至相邻提交组合并进同一个不可变 slice；只要发布水位覆盖当前组
端点即可唤醒该组。

### 3.2 封片请求

leader 向 `Binlog_archive` 登记单调递增的目标 `(file, pos)`，并再次唤醒
binlog 更新条件。归档线程仍负责读取和封片，提交线程不直接操作归档线程的
slice cache。

归档线程满足以下条件时立即封片，不再等待大小或周期阈值：

- 已读到目标文件和不小于目标的位置；
- 当前事件是完整事务边界；
- `m_rotate_forbidden == false`。

若归档仍落后于目标文件/位置，它继续顺序读取；不能在事务中间切片，也不能
发布跳过中间 slice 的 index。

### 3.3 等待与返回

leader 等待持久化水位覆盖组端点。index worker 每次成功发布 index 并推进
水位后立即广播条件变量；等待者被虚假唤醒时重新检查 `(file, pos)`。

P0 把等待放在 SYNC 阶段之后、引擎 COMMIT 阶段之前，并保留 `LOCK_sync`：

```text
FLUSH local binlog
  -> optional local fsync
  -> publish local binlog end position
  -> request/seal/upload slice
  -> publish remote index
  -> persisted watermark >= group endpoint
  -> engine COMMIT
  -> signal group clients
```

保留 `LOCK_sync` 会把远端等待纳入 group commit 的串行段，但有两个 P0 优点：
保持提交组和引擎提交顺序，不引入新的上游 commit stage，也不会让后续组越过
当前组。归档线程和两个上传 worker 不获取 `LOCK_sync`；等待只获取归档自己的
run/rotate/index 锁，且不会持有 `LOCK_log`，所以现有锁序中没有反向依赖。
后续性能版本可以增加独立 REMOTE_DURABILITY stage，但必须先证明跨组顺序、
GTID 顺序和 Clone commit order 不变。

## 4. 超时、重试和围栏

对象存储 PUT 超时是未知结果：服务端可能已经落盘。因此本协议不把超时转换成
普通事务失败，也不生成新 key 或新 body。

- slice key 由归档文件和源 binlog 结束位置确定；重试使用相同 key 和相同
  immutable body。
- index 使用固定 key，由单个 index worker 串行覆盖发布。一次 index PUT
  结果不明时，重新读取远端 index 水位或重新覆盖同一完整内容；不能先推进
  内存水位。
- 429、5xx、连接断开和超时均留在阻塞重试状态。客户端既不收到成功，也不
  收到“事务失败”。网络恢复后继续同一目标。
- 只有明确的进程级不可继续状态才进入 `FENCED`：归档主线程/worker 非预期
  退出、本地 binlog 无法继续读取、队列或 index 顺序不变量破坏。`FENCED`
  会唤醒等待者并终止服务进程，停止接受新写入；已写本地 binlog 的事务只表现
  为连接中断、结果未知，不返回可重试的普通事务失败。
- 正常 shutdown 先停止接收连接，再让已进入提交协议的组完成远端发布；若
  外部强杀，尚未收到成功的事务归入“未确认”，不能计入 0 丢失承诺。

这一定义刻意不提供“超时后返回 unknown 状态”的新 SQL 协议。MySQL 客户端
普遍不会正确处理新状态；阻塞重试和进程围栏更容易保证不会把可能已提交的事务
误报为普通失败。

## 5. 崩溃边界

| 崩溃点 | 客户端结果 | 远端状态 | 恢复要求 |
| --- | --- | --- | --- |
| 本地 binlog flush 前 | 可返回失败 | 无要求 | 事务不得出现 |
| 本地 flush 后、slice PUT 前 | 不得成功；连接中断为未知 | 可能无 slice | 未确认事务单列 |
| slice PUT 后、index PUT 前 | 不得成功；连接中断为未知 | 孤立 slice 可存在 | index 不得引用缺失对象 |
| index PUT 成功、水位未被线程观察 | 仍在等待 | 已可恢复 | 重试/重启读回 index 后可判定覆盖 |
| 水位覆盖、引擎 COMMIT 前 | 尚未成功 | 已可恢复 | binlog 作为 TC，恢复必须提交/重放 |
| 引擎 COMMIT 后、响应前 | 连接中断可未知 | 已可恢复 | 恢复后事务恰好一次可见 |
| 客户端收到成功后 | 成功 | slice + index 均可见 | 删除全部本地目录后仍必须可见 |

恢复端继续遵守现有顺序：先下载远端 index，再下载 index 引用的全部 slice，
重建本地 binlog，随后恢复引擎并回放。不得扫描并猜测未写入 index 的孤立
slice，也不得把本地 archive cache 作为恢复输入。

## 6. 并发和文件切换

- 目标水位按 binlog 文件序号和文件内位置单调前进；同文件取最大位置，后续
  文件覆盖前一文件目标。
- 一个等待者看到更远水位时，所有较早目标同时满足；广播后各自重新检查。
- binlog rotate 期间，旧文件剩余事务先封片并发布，再处理新文件。不能仅凭
  “持久化文件名不同”判断旧目标成功；旧文件必须已经存在于远端 index。
- slice worker 可以并行 PUT 数据对象；index worker 必须按
  `(file_seq, slice_seq)` 连续顺序发布。前序失败时禁止越过发布后序 slice。
- `RESET BINARY LOGS`、purge 和本地文件删除继续受已持久化水位保护。

## 7. 配置与兼容边界

- 远端提交保证只在 `serverless=ON` 且 `binlog_archive=ON` 时生效；显式关闭
  该模式就是普通 MySQL 本地提交语义，不能宣传为 diskless。
- MinIO 使用现有 `objectstore_provider=minio` 路径，只验证 S3 API 兼容层上
  的正确性、重试和恢复。S3 Express 目录桶 endpoint、CreateSession、真实
  延迟、费用和单 AZ 故障仍是 task #13 后续独立验收项。
- 本轮不实现跨 AZ 双桶，不修改 SmartEngine 外键/DDL 兼容，不做控制面。
- 单写互斥继续依赖现有 ObjectStore lease。两个节点抢写必须由 task #35
  验证只有一个节点能进入写路径；本提交协议不替代 owner/epoch 围栏。

## 8. 验收门槛

产品候选 SHA 交给 task #35/#37 前，task #34 至少完成：

1. patch 在 Oracle `mysql-9.7.2` / `008e09c2834b98143a8c067d4d225c90953050cf`
   上 forward check、apply、reverse check 和零残留；
2. 干净 configure/build/install，记录完整命令、exit code、版本和 SHA；
3. 把 `binlog_archive_period` 和 slice size 调大，单笔事务仍能主动封片并在
   客户端成功前发布远端 index；
4. 并发事务证明每个提交组只等待最远端点，远端水位单调覆盖全部已确认序号；
5. slice PUT 故障、index PUT 故障和超时期间客户端不得收到成功或普通失败，
   恢复服务后同 key 重试并完成；
6. 成功响应后强杀计算节点，删除容器和整个本地数据目录，用全新节点只从
   MinIO 恢复，已确认事务可见；
7. 归档线程终止/顺序不变量破坏触发进程围栏，没有继续接收写入的窗口；
8. 明确标记一个且只有一个候选 SHA，后续 100 轮黑盒只认该 SHA。

task #35 负责独立故障脚本和两个节点抢写；task #36 负责只读核对状态清单与
崩溃矩阵；task #37 在唯一候选上执行连续 100 轮删本地目录恢复。三者的证据
不能用历史 8.0、旧 9.7 v11 或本次开发自测替代。
