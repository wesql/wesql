# MySQL 9.7.2 Server 逐文件 API 差异清单

## 1. 可复现基线

- Oracle tag：`mysql-9.7.2`
- Oracle commit：`008e09c2834b98143a8c067d4d225c90953050cf`
- WeSQL 历史接点基线：`7c6d385931f2ef6c56d449bd30825b9981573cd5`
- 检查脚本：`tools/mysql_9_7_2_patch_path_probe.sh`
- 脚本 SHA256：
  `ce5bb4a57af6b2000086f68e36486dc80ac5e252f06c1e38327d2b90bfc91a8e`
- 原始 TSV：`docs/mysql-9.7.2-server-patch-probe.tsv`
- TSV SHA256：
  `b9fb380605b552e65880fd4fa7a08847ba9c5722cab5a4c75b74a0da0f09398d`
- 完整命令：

```bash
tools/mysql_9_7_2_patch_path_probe.sh \
  ../wesql \
  ../mysql-9.7.2-task27-clean \
  7c6d385931f2ef6c56d449bd30825b9981573cd5 \
  patches/mysql-server-8.0.35.patch
```

脚本固定校验两端 40 位 SHA、两个 clean worktree、patch 存在、路径总数 65
和路径唯一性。它先把完整历史 patch 和每个单文件 section 写入 `mktemp -d`
目录，校验 section 非空及首行路径，再单独执行 `git apply --check` 并记录
该命令的原始退出码。不存在的 ref 会 exit 4，不再产生 `total=0` 假成功。

机器结果：`total=65 applies=33 conflicts=32`。退出码 `0` 仅表示历史文本
能在当前文件上下文中套用；不表示接口语义、产品边界或运行行为正确。
退出码 `1` 表示上下文/路径冲突；`128` 表示历史 section 自身不合法。

## 2. 65 个路径

“语义判断”全部基于 9.7.2 clean tree 和当前产品边界；没有任何一行因
`apply_exit=0` 自动获得移植批准。

| # | 文件 | apply exit / 文本 | 9.7.2 语义判断 | 处理分类 |
| ---: | --- | --- | --- | --- |
| 1 | `CMakeLists.txt` | `1` / 冲突 | 根构建布局、依赖探测和打包位置已变；9.7.2 使用 C++23 | **必须重写** WeSQL 选项与依赖入口 |
| 2 | `MYSQL_VERSION` | `1` / 冲突 | `STABILITY` 已改为 `MYSQL_VERSION_MATURITY` | **必须重写** 版本字段，不改 Oracle 9.7.2 身份 |
| 3 | `cmake/mysql_version.cmake` | `0` / 可套用 | 文本可插入，但需按新 maturity/version 逻辑验证 `WESQL_VERSION` | **可适配**，不得直接认定通过 |
| 4 | `include/m_ctype.h` | `1` / 冲突 | 文件已删除，接口迁到 `include/mysql/strings/m_ctype.h` | **必须重写** 头文件接点 |
| 5 | `include/m_string.h` | `1` / 冲突 | 字符串/整数转换声明和常量布局已变 | **必须重写**，优先使用 9.7.2 公开接口 |
| 6 | `include/my_base.h` | `0` / 可套用 | 错误号文本可插入，但 `HA_ERR_LAST` 和编号碰撞仍需重算 | **必须校验后适配** |
| 7 | `include/my_sqlcommand.h` | `1` / 冲突 | 旧：在 `SQLCOM_DROP_SRS -> SQLCOM_END` 间插入两个 proc command；新：9.7.2 在 `SQLCOM_END` 前新增 library/masking-policy commands（行 200-215） | **必须重写** 并同步 `sql/command_mapping.cc` 的名称数组/断言 |
| 8 | `include/mysql/plugin_audit.h.pp` | `1` / 冲突 | 旧：手工镜像 8.0.35 command enum；新：必须镜像第 7 行的 9.7.2 完整 enum | **必须由预处理流程生成/同步**，不手工套旧 hunk |
| 9 | `include/mysql_version.h.in` | `0` / 可套用 | WeSQL 宏位置可用，但必须与新 CMake 取值一致 | **可适配** |
| 10 | `mysql-test/CMakeLists.txt` | `0` / 可套用 | 安装列表上下文可用，9.7.2 测试资产集合已变 | **可适配**，只加真实用例 |
| 11 | `mysql-test/include/default_mysqld.cnf` | `0` / 可套用 | 配置文本可插入，但不能恢复三节点/旧兼容参数 | **可适配** 最小单机配置 |
| 12 | `mysql-test/include/wait_condition.inc` | `0` / 可套用 | 仅历史等待行为变化，无 9.7.2 API 必要性 | **不移植**，除非新测试证明需要 |
| 13 | `mysql-test/mysql-test-run.pl` | `1` / 冲突 | runner 已大幅变化；历史 diff 混有已删除的 WeSQL 变体逻辑 | **必须重写** 最小 runner 接点 |
| 14 | `mysql-test/r/mysqld--help-notwin.result` | `0` / 可套用 | 文本可插入但结果必须由 9.7.2 新二进制生成 | **不直接移植**，构建后重录 |
| 15 | `mysql-test/suite/funcs_1/t/is_engines.test` | `0` / 可套用 | 历史删除可命中，SmartEngine 枚举行为仍需新门禁 | **可适配** 测试预期 |
| 16 | `mysql-test/suite/rpl/t/rpl_drop_scheduled_event.test` | `0` / 可套用 | 复制事件行为不属于文本兼容结论 | **暂不移植**，由保留能力决定 |
| 17 | `mysys/CMakeLists.txt` | `1` / 冲突 | convenience/object library 和链接布局已变 | **必须重写** `myobjstore` target 接线 |
| 18 | `mysys/my_handler_errors.h` | `0` / 可套用 | 数组文本可插入，但依赖第 6 行错误号重算 | **必须联动校验** |
| 19 | `mysys/print_version.cc` | `0` / 可套用 | 版本输出 hook 可定位，格式需绑定 9.7.2 与 WeSQL SHA | **可适配** |
| 20 | `plugin/clone/CMakeLists.txt` | `1` / 冲突 | 旧：删除 `MODULE_ONLY/MODULE_OUTPUT_NAME` 以改成内建；新：`MYSQL_ADD_PLUGIN(clone ...)` 在 31-43，仍为 `MODULE_ONLY`；动态 plugin 在 `mysqld.cc:8598` 初始化，早于 snapshot 启动，早期空目录恢复只复制已有 Clone 文件 | **不移植旧 CMake hunk**；固定 `--plugin-load-add=clone=mysql_clone.so`，snapshot 开启但 Clone 缺失必须明确失败，snapshot 关闭不强制 Clone |
| 21 | `plugin/clone/src/clone_plugin.cc` | `0` / 可套用 | 旧：为内建 target 改 `MYSQL_DYNAMIC_PLUGIN` include 和 plugin 声明；新：第 20 行已固定原生动态装载，声明在 654 | **不移植旧宏/声明 hunk**，保持 9.7.2 原生 plugin ABI |
| 22 | `scripts/CMakeLists.txt` | `1` / 冲突 | bootstrap SQL 生成 target/依赖列表已变 | **必须重写** WeSQL SQL 生成入口 |
| 23 | `scripts/mysql_system_tables.sql` | `1` / 冲突 | 9.7.2 replication metadata 表定义已变 | **不移植旧表改写**；只接入 WeSQL 自有 SQL |
| 24 | `share/messages_to_clients.txt` | `1` / 冲突 | 错误段边界和生成输入已变 | **必须重分配/生成** 客户端错误 |
| 25 | `share/messages_to_error_log.txt` | `1` / 冲突 | 日志编号段和生成输入已变 | **必须重分配/生成** 日志错误 |
| 26 | `sql/CMakeLists.txt` | `1` / 冲突 | Server/BINLOG source list 已重排 | **必须重写** 自有模块 source 接线 |
| 27 | `sql/basic_istream.h` | `0` / 可套用 | 历史 hunk 仅空行漂移，无行为 | **不移植** |
| 28 | `sql/binlog.cc` | `1` / 冲突 | 旧 hook：`MYSQL_BIN_LOG::purge_logs()` 约 5935、`truncate_update_log_file()` 后；新位置：同签名 purge 在 5329，`Binlog_ofile::open_existing()` 在 392，truncate 在 6013；`normalize_binlog_name()` 已移到 `sql/binlog_index.cc:77` | **必须在新位置重写** purge guard；flag helper 可复用 9.7.2 `Binlog_ofile::update()`，但不复制旧上下文 |
| 29 | `sql/binlog.h` | `128` / 非法 section | 旧：损坏 hunk 把 helper 放在 `normalize_binlog_name()` 后；新：该声明已在 `sql/binlog_index.h:64`，`binlog.h` 尾部是 purge/show API，无直接替代接点 | **必须新写** helper 声明位置，不修旧 hunk |
| 30 | `sql/binlog_reader.cc` | `128` / 非法 section | 历史 section 只有上下文，无实际改动 | **不移植** |
| 31 | `sql/dd/impl/system_registry.cc` | `0` / 可套用 | 历史内容只是空 `#ifdef` | **不移植** |
| 32 | `sql/dd/impl/upgrade/server.cc` | `1` / 冲突 | 旧：在 `I_S_upgrade_required()` 后新增 `initialize_wesql_schemas()`；新：`I_S_upgrade_required()` 在 1453，旧新增函数无同位置替代；通用回调是 `sql/bootstrap.h:38` 的 `bootstrap::run_bootstrap_thread()` | **必须按 9.7.2 bootstrap 回调签名重写** |
| 33 | `sql/dd/upgrade/server.h` | `0` / 可套用 | 声明可插入，但需与第 32 行新签名一致 | **必须联动适配** |
| 34 | `sql/dd_table_share.cc` | `0` / 可套用 | SmartEngine table-share 接点可命中，DD 生命周期仍需核对 | **可适配** 窄 hook |
| 35 | `sql/event_parse_data.cc` | `1` / 冲突 | 事件 originator/status 逻辑已变，旧复制语义不再成立 | **不直接移植**，按单机边界重判 |
| 36 | `sql/handler.cc` | `0` / 可套用 | 旧：新增 `post_engine_recover_handlerton()/ha_post_engine_recover()`；新：已有 `post_recover_handlerton()/ha_post_recover()`（9000），但它在 mysqld 8857 调用，早于 `after_engine_recovery` 9992，不能等价替换旧晚期 hook | **必须联动重写**；晚期 hook 无直接替代，需保留窄回调或改为 server-state observer |
| 37 | `sql/handler.h` | `1` / 冲突 | 旧：在 `post_ddl/post_recover` 后插入 `post_engine_recover` 与 6 个 backup callback；新：同区在 3002-3006，已有 `post_recover` 后直接接 `Clone_interface_t`，没有 checkpoint/backup 标准 ABI | **必须重写** 最小自有 ABI；不重复已有 `post_recover_t` |
| 38 | `sql/log.cc` | `1` / 冲突 | WeSQL 日志接点周边实现已变 | **可适配** 最小日志桥接 |
| 39 | `sql/log.h` | `0` / 可套用 | 声明文本可命中，需与第 38 行实现一致 | **必须联动适配** |
| 40 | `sql/log_event.h` | `128` / 非法 section | 历史 section 无有效行为且 hunk 损坏 | **不移植** |
| 41 | `sql/mysqld.cc` | `1` / 冲突 | 旧：binlog restore 放 `init_server_components()` 首次开 index 前，engine restore 在 core plugin load 后，线程停止在 `close_connections()`；新启动锚点：core SE load=`plugin_register_builtin_and_init_core_se()` 8499，首次 TC/binlog open=`tc_log->open()` 8849，现有 `ha_post_recover()` 8857，晚期 `after_engine_recovery` 9992；新关闭锚点：`before_server_shutdown` 2432 早于 `end_slave()` 2464，`after_server_shutdown` 2498 只可断言，`ha_pre_dd_shutdown()` 2788 已太晚；`unireg_abort()` 直接走 `clean_up()`，不走 `close_connections()` | **必须按这些 9.7.2 锚点重写**：正常路径在 2432 stop/join、2498 断言；异常启动路径用 started-state + 同一个幂等 stop facade 收口；禁止把正常首次 stop 放在 2788，也禁止把 restore 放到 8892 的第二次 `open_binlog()` 之后 |
| 42 | `sql/mysqld.h` | `1` / 冲突 | 旧：在 `opt_initialize` 等全局声明区加入 50 余配置变量；新：对应声明区已移动且部分类型/选项新增，无统一注册替代 | **必须只声明新模块实际引用的配置 ABI**，与第 59 行一一对应 |
| 43 | `sql/parse_tree_nodes.cc` | `1` / 冲突 | 旧：`PT_create_table_engine_option::contextualize()` 在 1996，调 `resolve_engine(..., false, &db_type)` 前强改 engine；新：接口改名为 `PT_create_table_engine_option::do_contextualize()`（2476），`resolve_engine()` 还新增 `strict` 参数 | **必须在新的 `do_contextualize` hook 重写**，不复制旧签名/告警文本 |
| 44 | `sql/range_optimizer/range_optimizer.cc` | `0` / 可套用 | SmartEngine optimizer 文本可命中，但成本/范围语义未验证 | **可适配**，需引擎专项测试 |
| 45 | `sql/rpl_commit_stage_manager.cc` | `0` / 可套用 | commit-stage 文本可命中，事务语义仍需与 9.7.2 对齐 | **可适配**，需事务门禁 |
| 46 | `sql/rpl_context.h` | `0` / 可套用 | 新 channel enum 文本可命中，归档 replica 是否保留未定 | **暂缓**，等待 task #26 能力边界 |
| 47 | `sql/rpl_gtid_state.cc` | `0` / 可套用 | GTID 特判可命中，但与旧 archive replica 绑定 | **暂缓**，不能由文本通过 |
| 48 | `sql/rpl_replica.cc` | `0` / 可套用 | 约 385 行 archive replica 侵入可命中，但默认关闭且从未验收 | **不纳入首轮骨架**；保留需独立门禁 |
| 49 | `sql/rpl_replica.h` | `0` / 可套用 | replica 状态扩展可命中，依赖第 48 行产品决定 | **暂缓** |
| 50 | `sql/sp.cc` | `1` / 冲突 | 旧：在 `check_routine_already_exists()` 特判 native proc；新：函数和 DD 查重流程已移动，无服务层替代 | **必须在 9.7.2 同名查重函数重写** 或取消 SQL-level native proc |
| 51 | `sql/sql_class.cc` | `0` / 可套用 | binlog/THD 特判可命中，需核对 9.7.2 statement lifecycle | **可适配** |
| 52 | `sql/sql_class.h` | `0` / 可套用 | 声明/include 可命中，需与 package 接口一致 | **可适配** |
| 53 | `sql/sql_db.cc` | `0` / 可套用 | DB DDL binlog 接点可命中，file/position 语义需新恢复门禁 | **可适配**，运行由 task #26 验收 |
| 54 | `sql/sql_parse.cc` | `1` / 冲突 | 旧锚点：`init_sql_command_flags()`、`dispatch_command()`、`mysql_execute_command()`；新仍分别在 515、1752、3031，但 command 集合和 dispatch 主体已变 | **必须按新符号逐段重写** native proc flag/执行分支；SmartEngine trace 单独归类 |
| 55 | `sql/sql_prepare.cc` | `0` / 可套用 | 旧：在 `Prepared_statement::prepare_query()` switch 增加两个 proc command；新同函数在 1215，文本命中但依赖第 7/54 行 enum/execute | **必须联动适配**，单独 exit 0 不成立 |
| 56 | `sql/sql_show.cc` | `1` / 冲突 | 旧：把 10 张表直接塞进 `schema_tables[]` 和 core fill 函数；新：core 数组在 5694，但已有 `MYSQL_INFORMATION_SCHEMA_PLUGIN` 查找路径 `find_schema_table_in_plugin()` 5108 与 `initialize_schema_table()` 5722 | **优先改为 I_S plugin**；无必要继续移植 568 行 core 侵入 |
| 57 | `sql/sql_table.cc` | `0` / 可套用 | 默认引擎/charset 文本可命中，9.7.2 DDL 语义未验证 | **可适配**，需 DDL 门禁 |
| 58 | `sql/sql_yacc.yy` | `1` / 冲突 | 旧：在 `call_stmt` 用 `PT_call($2,$3)` 前拦截 native proc；新同规则在 3967，构造器已是 `PT_call(@$, $2, $3)` | **必须按新构造器签名重写** 最小 grammar 入口 |
| 59 | `sql/sys_vars.cc` | `1` / 冲突 | 旧：在当时文件尾 `Sys_explain_format` 后追加约 268 行；新：该变量在 7601，后续还有多组变量；7660–7728 只是外键变量的匿名 namespace，7731 又开始另一个匿名 namespace，内建命令行 global var 没有直接外部 registrar | **优先移到自有 translation unit**；若必须保留在本文件，只放稳定的顶层区，不进入无关 namespace，也不依赖 EOF |
| 60 | `sql/sys_vars.h` | `0` / 可套用 | 旧：扩展 `Sys_var_plugin`；新文本可命中但 class 周边接口已扩展，且必须与第 59 行注册方式一致 | **必须联动适配**，优先不改公共 class |
| 61 | `sql/udf_service_impl.cc` | `0` / 可套用 | 三个 UDF 注册文本可命中，service 生命周期仍需核对 | **可适配**，保持集中注册 |
| 62 | `strings/ctype-simple.cc` | `1` / 冲突 | strxfrm 参数/实现上下文已变 | **必须重写** SmartEngine 所需字符集适配 |
| 63 | `strings/ctype-uca.cc` | `0` / 可套用 | 文本可命中，但 `CHARSET_INFO` 已迁到新公共头 | **可适配**，需排序规则测试 |
| 64 | `strings/ctype-utf8.cc` | `0` / 可套用 | 两处文本可命中，但接口所有权和调用方已变 | **可适配**，需 UTF-8 排序测试 |
| 65 | `strings/int2str.cc` | `1` / 冲突 | 整数转换实现和声明布局已变 | **必须重写或删除**，优先复用 9.7.2 API |

## 3. 汇总边界

- **必须重写的核心链**：root/mysys/sql/scripts CMake 和 Clone 装载配置，SQL command/audit，
  handler ABI，binlog purge/flag，mysqld 生命周期，DD bootstrap，parser，sysvar，
  I_S，错误生成和字符集头文件迁移。
- **文本可套用但语义未通过**：33 个文件。它们只能作为定位线索，每个仍需
  在新 patch 中重新审查，不能整体 `git apply`。
- **明确不移植**：`sql/basic_istream.h`、`sql/binlog_reader.cc`、
  `sql/dd/impl/system_registry.cc`、`sql/log_event.h` 的空/格式 hunk；旧
  replication metadata 表改写；无新证据支持的 MTR 等待改动。
- **暂缓能力**：archive replica 的 `rpl_*` 大块侵入。task #27 首轮只提供
  可编译窄接口，不用“文本可套用”替代产品决定和运行验收。

本清单完成的是接口和风险归类，不是移植批准。必须等本清单 review 通过后，
才建立 writable 9.7.2 port tree 并开始新补丁实现。
