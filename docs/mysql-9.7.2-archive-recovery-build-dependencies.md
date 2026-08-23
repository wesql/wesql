# MySQL 9.7.2 归档恢复源码与链接依赖清单

> 任务：task #26。
>
> 本文只给 task #27 提供 `source -> target -> link dependency` 输入。所有
> CMake 文件和最终 target graph 仍由 task #27 独占，task #26 不提交 CMake
> 改动。

## 1. ObjectStore

| 源文件 | 目标 target | 直接链接依赖 |
| --- | --- | --- |
| `mysys/objstore/local.cc` | `myobjstore` | C++23 标准库；`std::filesystem` 不再链接 `stdc++fs` |
| `mysys/objstore/objstore.cc` | `myobjstore` | local/S3/OSS provider factory；C++23 标准库 |
| `mysys/objstore/objstore_lock.cc` | `myobjstore` | `myobjstore` 内部接口 |
| `mysys/objstore/s3.cc` | `myobjstore` | AWS SDK `s3`、`core`、CRT 及 SDK 自身的平台依赖 |
| `mysys/objstore/aliyun_oss.cc` | `myobjstore` | `alibabacloud-oss-cpp-sdk`、curl、OpenSSL 及 SDK 自身的平台依赖 |
| `mysys/objstore/local_test.cc` | 独立测试 executable，建议名 `myobjstore_local_test` | `myobjstore`；`SKIP_INSTALL`，不得进入 `myobjstore` 生产 source list |

公开头文件是 `include/objstore.h`；provider 私有头文件是
`mysys/objstore/{local,s3,aliyun_oss}.h`。

构建约束：

- AWS/OSS include directory 应按第三方 `SYSTEM` include 处理；
- SDK 自身的兼容 warning flag 只能落在 SDK target，不能覆盖上述 WeSQL
  source；
- `myobjstore` 和 `myobjstore_objlib` 使用同一组 provider link dependency；
- OSS 解包/生成 target 必须成为 `myobjstore` 的显式 build dependency；
- MySQL 9.7.2 使用 C++23，不保留 8.0.35 的 `stdc++fs` 链接。

## 2. Binlog archive 与 replay

| 源文件 | 建议归属 target | 必需符号/链接边 |
| --- | --- | --- |
| `sql/binlog_archive.cc` | `binlog` | `myobjstore`、`mysql_binlog_event`、binlog reader/stream、`sql_main`、`rpl`/`rpl_source` server symbols |
| `sql/binlog_archive_command.cc` | `sql_main` | archive public API、UDF service implementation |
| `sql/binlog_archive_replica.cc` | `rpl_replica` | 同 target 的 `queue_event()`、MI/RLI/channel/start-stop APIs；另需 `binlog`、`myobjstore`、`sql_main` |

对应头文件为 `sql/binlog_archive.h`、
`sql/binlog_archive_command.h`、`sql/binlog_archive_replica.h`。

`binlog_archive_replica.cc` 必须进入 `RPL_REPLICA_SRCS`，不能复制
`queue_event()` 到第二个 target。这样对象存储 relay worker 直接使用 9.7.2
的 tagged GTID、FDE/checksum、transaction parser、metrics 和 relay-log 持久化
实现。

### 2.1 ObjectStore provider 所有权

三个后台组件只拥有各自的 `ObjectStore` client，不拥有 provider SDK 运行时：

| client caller | provider 运行时所有者 | client 销毁点 | provider 最终关闭点 |
| --- | --- | --- | --- |
| `binlog_archive.cc` | SmartEngine `Env::InitObjectStore()` | archive 线程创建失败，或 archive stop/join 后 | 所有 snapshot/archive client 销毁后，由 `Env::DestroyObjectStore()` 的 `cleanup_object_store()` 关闭 |
| `consistent_archive.cc` | SmartEngine `Env::InitObjectStore()` | snapshot stop/join 后 | 同上，且必须晚于 snapshot/archive client 销毁 |
| `binlog_archive_replica.cc` | `Consistent_recovery` 初始化的 source provider | relay worker、download worker、SQL applier 全部停止后 | replay client 销毁后，由 recovery owner 的幂等 cleanup 关闭 |

这些 caller 继续使用仅 `delete client` 的 `destroy_object_store()`；禁止改成
`cleanup_object_store()`，否则会在其他 client 或 SmartEngine 仍使用 SDK 时提前
shutdown。provider owner 的成功、失败和 `clean_up()` 路径必须调用同一幂等最终
关闭门面；task #26 在 recovery/lifecycle 切片落地该门面，task #27 保证它早于
engine shutdown、晚于所有借用 client 的 stop/join。

### 2.2 Archive 消费的 Server 声明

task #27 的 Server patch 需要为 archive 提供下列声明；task #26 不在自有源码中
重复定义：

- bool：`opt_binlog_archive`、`opt_binlog_archive_expire_auto_purge`、
  `opt_consistent_snapshot_archive`、
  `opt_consistent_snapshot_persistent_on_objstore`、
  `opt_objstore_use_https`、`opt_serverless`；
- string：`opt_binlog_archive_dir`、`opt_branch_objstore_id`、
  `opt_objstore_bucket`、`opt_objstore_endpoint`、`opt_objstore_provider`、
  `opt_objstore_region`、`opt_repo_objstore_id`；
- integer：`opt_binlog_archive_expire_seconds`、
  `opt_binlog_archive_parallel_workers`、`opt_binlog_archive_period`、
  `opt_binlog_archive_slice_max_size`；
- `ER_BINLOG_ARCHIVE_*` 的错误消息编号和消息模板。

`opt_bin_log`、`opt_source_verify_checksum`、`Log_info`、
`compare_log_name()`、`my_micro_time()` 和 binlog event API 直接消费 9.7.2
上游定义。task #26 的独立语法门禁只用仓库外 compile-only preinclude 补齐上述
未落地的 Server 声明；该 shim 不进入提交、生产 include path 或任何 target。

### 2.3 File/position-only 格式不变量

- binlog archive 只使用稳定的 `binlog-index.index`；slice key 固定为
  `{binlog}.{mysql_end_pos}`，其中 `mysql_end_pos` 是源 MySQL binlog 的物理
  event end position；
- snapshot、InnoDB 和 SmartEngine 各自只使用稳定的 `snapshot.index`、
  `innodb.index` 和 `smartengine.index`；
- 源 MySQL position 与本地/对象存储中重建后的归档字节长度是两种不同量。
  STOP_EVENT 的进度取 reader 的物理源位置；merge/recovery 不得用重建文件大小
  与源 position 比较；
- 不读取或生成 term/index/previous-index 版本化对象，不提供旧 bucket、旧 archive
  或 downgrade 兼容分支。

### 2.4 Object-store replay 的 9.7.2 运行契约

- 内部 channel 的 network source 必须为空，只启动 `REPLICA_SQL`；下载和 relay
  worker 代替网络 receiver，禁止隐式启动 `REPLICA_IO`；
- 内部 channel 使用持久化的 replication repository。首次创建时才从 recovery
  选项写入初始 source file/position 并 flush；同 datadir 重启时必须校验已有
  channel 的 host 为空且 receiver 未运行，然后直接复用 repository 中已推进的
  file/position，禁止删除 repository、重建 channel 或退回 snapshot 初始位置；
- `Master_info`/`Relay_log_info` repository、relay log、FDE 和 checksum 状态初始化
  完成后，relay worker 才能写事件。worker 的 THD 按上游锁顺序同时持有
  `mi->run_lock` 和 `mi->info_thd_lock` 后绑定到 `mi->info_thd`，析构 THD 前按
  同一规则解绑；
- fake Rotate 和修改后的 FDE 使用各自的可写 `std::string` 缓冲区。下载事件在
  读取 type/length 前校验完整 header，并验证 event length 和 type；
- 首个 FDE 写入 relay log 后必须推进本地 `reader_size`，避免重复读取同一事件。
  `LOG_POS_OFFSET` 可能是 archive core 重建后的本地位置，不能与源
  `start_read_pos` 比较；replay 保持 task #20 的完整 slice 排队语义；
- relay/download worker 即使启动后提前退出也保留 joinable 状态。停止顺序固定为
  relay worker -> download workers -> SQL applier，随后才销毁 replay client；主
  replay 线程只有在 channel 和两类 worker 全部启动后才进入 running 状态，启动
  失败与重复 stop 走同一清理路径。

## 3. Consistent snapshot 与 recovery

| 源文件 | 建议归属 target | 必需符号/链接边 |
| --- | --- | --- |
| `sql/consistent_archive.cc` | `sql_main` | `binlog`/archive API、`myobjstore`、`sql/clone_handler`、backup lock、task #27 SmartEngine handlerton 合同 |
| `sql/consistent_recovery.cc` | `sql_main` | `binlog`/archive API、`rpl_replica`、`myobjstore`、plugin/handler 恢复合同 |
| `sql/consistent_snapshot_force_command.cc` | `sql_main` | consistent archive API、UDF service |
| `sql/consistent_snapshot_purge_command.cc` | `sql_main` | consistent archive API、UDF service |

对应头文件为 `sql/consistent_archive.h`、`sql/consistent_recovery.h`、
`sql/consistent_snapshot_force_command.h` 和
`sql/consistent_snapshot_purge_command.h`。

Clone 的编译边只到 `sql/clone_handler.{h,cc}`。`mysql_clone.so` 保持
`MODULE_ONLY`，Server 不静态链接该 module；启用 snapshot 的运行配置用
`--plugin-load-add=clone=mysql_clone.so` 装载，并在 snapshot 启动前验证
`clone_plugin_lock()` 成功。

### 3.1 Snapshot 消费的 Server 声明

除 2.2 节的共用 ObjectStore、Serverless 和 snapshot 开关外，task #27 还需为
`consistent_archive.cc` 提供下列声明；task #26 不在自有源码中重复定义：

- bool：`opt_consistent_snapshot_expire_auto_purge`、
  `opt_consistent_snapshot_smartengine_backup_checkpoint`；
- string：`opt_consistent_snapshot_archive_dir`；
- integer：`opt_consistent_snapshot_archive_period`、
  `opt_consistent_snapshot_expire_seconds`、
  `opt_consistent_snapshot_innodb_tar_mode`、
  `opt_consistent_snapshot_se_tar_mode`；
- `ER_CONSISTENT_SNAPSHOT_LOG`、
  `ER_CONSISTENT_SNAPSHOT_ARCHIVE_THREAD_LOG`、
  `ER_CONSISTENT_SNAPSHOT_ARCHIVE_BINLOG_LOG`、
  `ER_CONSISTENT_SNAPSHOT_ARCHIVE_INNODB_LOG`、
  `ER_CONSISTENT_SNAPSHOT_ARCHIVE_SMARTENGINE_LOG`、
  `ER_CONSISTENT_SNAPSHOT_PURGE_LOG` 的错误消息编号和消息模板。

task #27 的 `handlerton` 合同还需提供并由 SmartEngine 注册以下回调：

```cpp
int (*checkpoint)(THD *thd);
int (*create_backup_snapshot)(THD *thd, uint64_t *backup_snapshot_id,
                              std::string &binlog_file,
                              uint64_t *binlog_file_offset);
int (*cleanup_tmp_backup_dir)(THD *thd);
int (*release_backup_snapshot)(THD *thd, uint64_t backup_snapshot_id);
int (*list_backup_snapshots)(THD *thd,
                             std::vector<uint64_t> &backup_snapshot_ids);
```

Snapshot 只借用由 SmartEngine 初始化的 provider SDK。线程创建失败、已停止和
正常 join 三条路径都调用同一幂等 client 释放函数：先清空本组件指针，再只析构
client；不得在这里 shutdown provider。最终 provider 关闭点仍按 2.1 节执行。

`compare_log_name()`、`Clone_handler`、`clone_plugin_lock()` /
`clone_plugin_unlock()`、`acquire_exclusive_backup_lock()` 和
`release_backup_lock()` 直接消费 9.7.2 上游稳定接口，不引用 Clone module 私有头。

### 3.2 Recovery 消费的 Server 声明与生命周期

`consistent_recovery.cc` 直接消费 9.7.2 上游的
`MYSQL_BIN_LOG::truncate_update_log_file()`、`compare_log_name()`、权限/系统变量
tracker 和 THD API。task #27 还需提供以下 WeSQL Server 声明；task #26 不在
自有源码中重复定义：

- bool：`opt_initialize_from_source_objectstore`、
  `opt_recovery_consistent_snapshot_only`、`opt_recovery_from_objstore`、
  `opt_source_objectstore_smartengine_data`、
  `opt_source_objectstore_use_https`；
- string：`opt_recovery_consistent_snapshot_timestamp`、
  `opt_recovery_consistent_snapshot_tmpdir`、
  `opt_source_objectstore_branch_id`、`opt_source_objectstore_bucket`、
  `opt_source_objectstore_endpoint`、`opt_source_objectstore_provider`、
  `opt_source_objectstore_region`、`opt_source_objectstore_repo_id`；
- 恢复状态：`consistent_recovery_archive_recovery`、
  `consistent_recovery_snapshot_end_binlog_position`、
  `consistent_recovery_apply_stop_timestamp`、
  `consistent_recovery_truncated_end_binlog` 和
  `consistent_recovery_truncated_end_position`；
- `ER_CONSISTENT_RECOVERY_LOG` 的错误消息编号和模板。

Recovery 是 source provider 的运行时所有者。初始化失败会销毁已创建的
source/destination client，再按各自成功的 init 次数关闭 provider；这些路径和
`cleanup_objstore()` 都是幂等的。正常退出时 task #27 必须先 stop/join object-store
replay 的 relay/download/SQL 三类 borrower，再调用
`consistent_recovery.cleanup_objstore()`，最后才允许 SmartEngine 关闭自身 provider。
`clean_up()`/`unireg_abort()` 也必须走相同顺序，不能只覆盖正常
`close_connections()` 路径。

## 4. 组合 target graph

建议的无重复 source 归属如下：

```text
AWS S3 / Aliyun OSS SDK
        -> myobjstore
        -> binlog (+ binlog_archive.cc)
        -> rpl_replica (+ binlog_archive_replica.cc)
        -> sql_main (+ consistent snapshot/recovery and command sources)
        -> mysqld
```

9.7.2 原生 `sql_main`、`binlog`、`rpl_replica` 已有相互引用，task #27
应沿用上游静态库的 link multiplicity/order，不另建一份 archive source 副本来
绕开链接顺序。

组合 worktree 必须验证：

1. 每个 task #26 `.cc` 只属于一个生产 target；测试 `.cc` 不进入生产库；
2. `myobjstore` 的 object/static 两种 target 都能解析 S3/OSS symbols；
3. `binlog_archive_replica.cc` 与上游 `queue_event()` 同属 `rpl_replica`；
4. `sql_main` 能解析 archive、replay、Clone handler 和 task #27 handlerton
   symbols，且 `mysql_clone.so` 仍为 module；
5. `mysqld --verbose --help` 在 snapshot 关闭时不要求 Clone module；
6. snapshot 开启但未装载 Clone 时启动明确失败；装载后完整链接、安装和启动
   通过。
