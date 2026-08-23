# MySQL 9.7.2 Server 逐文件 API 差异清单

## 1. 可复现基线

- Oracle tag：`mysql-9.7.2`
- Oracle commit：`008e09c2834b98143a8c067d4d225c90953050cf`
- WeSQL 历史接点基线：`7c6d385931f2ef6c56d449bd30825b9981573cd5`
- 检查脚本：`tools/mysql_9_7_2_patch_path_probe.sh`
- 脚本 SHA256：
  `8e5956c3172af32027e7af0a24fa89f2c6c254185270ade197af3a07be4d79e6`
- 完整命令：

```bash
tools/mysql_9_7_2_patch_path_probe.sh \
  . \
  ../mysql-9.7.2-task27-clean \
  HEAD \
  patches/mysql-server-8.0.35.patch
```

脚本逐个抽取历史 patch 的 diff section，并在干净 9.7.2 tree 中独立执行
`git apply --check`。脚本完整消费 patch 流，避免 SIGPIPE 污染退出码。

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
| 7 | `include/my_sqlcommand.h` | `1` / 冲突 | SQL command 枚举已有新成员，所有数组边界受影响 | **必须重写** 并同步映射 |
| 8 | `include/mysql/plugin_audit.h.pp` | `1` / 冲突 | audit 预处理输出必须与新 command 枚举一致 | **必须生成/同步**，不手工套旧 hunk |
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
| 20 | `plugin/clone/CMakeLists.txt` | `1` / 冲突 | Clone plugin target 参数和静态/动态接线已变 | **必须重写** 静态 Clone target |
| 21 | `plugin/clone/src/clone_plugin.cc` | `0` / 可套用 | 宏文本可插入，但注册语义必须与第 20 行一致 | **必须联动适配** |
| 22 | `scripts/CMakeLists.txt` | `1` / 冲突 | bootstrap SQL 生成 target/依赖列表已变 | **必须重写** WeSQL SQL 生成入口 |
| 23 | `scripts/mysql_system_tables.sql` | `1` / 冲突 | 9.7.2 replication metadata 表定义已变 | **不移植旧表改写**；只接入 WeSQL 自有 SQL |
| 24 | `share/messages_to_clients.txt` | `1` / 冲突 | 错误段边界和生成输入已变 | **必须重分配/生成** 客户端错误 |
| 25 | `share/messages_to_error_log.txt` | `1` / 冲突 | 日志编号段和生成输入已变 | **必须重分配/生成** 日志错误 |
| 26 | `sql/CMakeLists.txt` | `1` / 冲突 | Server/BINLOG source list 已重排 | **必须重写** 自有模块 source 接线 |
| 27 | `sql/basic_istream.h` | `0` / 可套用 | 历史 hunk 仅空行漂移，无行为 | **不移植** |
| 28 | `sql/binlog.cc` | `1` / 冲突 | purge/ofile 上下文和 binlog 内部布局已变 | **必须重写** 未归档禁止 purge 与 flag 更新 |
| 29 | `sql/binlog.h` | `128` / 非法 section | 历史 hunk 计数损坏；9.7.2 声明位置也已变 | **必须新写** 窄接口，不修旧 hunk |
| 30 | `sql/binlog_reader.cc` | `128` / 非法 section | 历史 section 只有上下文，无实际改动 | **不移植** |
| 31 | `sql/dd/impl/system_registry.cc` | `0` / 可套用 | 历史内容只是空 `#ifdef` | **不移植** |
| 32 | `sql/dd/impl/upgrade/server.cc` | `1` / 冲突 | DD upgrade/bootstrap 回调位置和依赖已变 | **必须重写** WeSQL schema 初始化入口 |
| 33 | `sql/dd/upgrade/server.h` | `0` / 可套用 | 声明可插入，但需与第 32 行新签名一致 | **必须联动适配** |
| 34 | `sql/dd_table_share.cc` | `0` / 可套用 | SmartEngine table-share 接点可命中，DD 生命周期仍需核对 | **可适配** 窄 hook |
| 35 | `sql/event_parse_data.cc` | `1` / 冲突 | 事件 originator/status 逻辑已变，旧复制语义不再成立 | **不直接移植**，按单机边界重判 |
| 36 | `sql/handler.cc` | `0` / 可套用 | 回调调用点可命中，但依赖全新的 handlerton ABI | **必须联动重写**，不能只套文本 |
| 37 | `sql/handler.h` | `1` / 冲突 | handlerton/handler 结构和成员位置已变 | **必须重写** checkpoint/backup/recovery ABI |
| 38 | `sql/log.cc` | `1` / 冲突 | WeSQL 日志接点周边实现已变 | **可适配** 最小日志桥接 |
| 39 | `sql/log.h` | `0` / 可套用 | 声明文本可命中，需与第 38 行实现一致 | **必须联动适配** |
| 40 | `sql/log_event.h` | `128` / 非法 section | 历史 section 无有效行为且 hunk 损坏 | **不移植** |
| 41 | `sql/mysqld.cc` | `1` / 冲突 | 启动、plugin load、binlog open、shutdown 阶段均已移动 | **必须重写** 生命周期 hook |
| 42 | `sql/mysqld.h` | `1` / 冲突 | 全局声明区域和若干类型已变 | **必须重写** 仅保留实际配置 ABI |
| 43 | `sql/parse_tree_nodes.cc` | `1` / 冲突 | engine contextualize API/调用上下文已变 | **必须重写** SmartEngine 约束 hook |
| 44 | `sql/range_optimizer/range_optimizer.cc` | `0` / 可套用 | SmartEngine optimizer 文本可命中，但成本/范围语义未验证 | **可适配**，需引擎专项测试 |
| 45 | `sql/rpl_commit_stage_manager.cc` | `0` / 可套用 | commit-stage 文本可命中，事务语义仍需与 9.7.2 对齐 | **可适配**，需事务门禁 |
| 46 | `sql/rpl_context.h` | `0` / 可套用 | 新 channel enum 文本可命中，归档 replica 是否保留未定 | **暂缓**，等待 task #26 能力边界 |
| 47 | `sql/rpl_gtid_state.cc` | `0` / 可套用 | GTID 特判可命中，但与旧 archive replica 绑定 | **暂缓**，不能由文本通过 |
| 48 | `sql/rpl_replica.cc` | `0` / 可套用 | 约 385 行 archive replica 侵入可命中，但默认关闭且从未验收 | **不纳入首轮骨架**；保留需独立门禁 |
| 49 | `sql/rpl_replica.h` | `0` / 可套用 | replica 状态扩展可命中，依赖第 48 行产品决定 | **暂缓** |
| 50 | `sql/sp.cc` | `1` / 冲突 | stored-program/native procedure 查重路径已变 | **必须重写** native procedure 接点 |
| 51 | `sql/sql_class.cc` | `0` / 可套用 | binlog/THD 特判可命中，需核对 9.7.2 statement lifecycle | **可适配** |
| 52 | `sql/sql_class.h` | `0` / 可套用 | 声明/include 可命中，需与 package 接口一致 | **可适配** |
| 53 | `sql/sql_db.cc` | `0` / 可套用 | DB DDL binlog 接点可命中，file/position 语义需新恢复门禁 | **可适配**，运行由 task #26 验收 |
| 54 | `sql/sql_parse.cc` | `1` / 冲突 | command flag、dispatch 和 execute 分支位置均已变 | **必须重写** native procedure 路由 |
| 55 | `sql/sql_prepare.cc` | `0` / 可套用 | prepare 例外可命中，需与新 command enum 联动 | **必须联动适配** |
| 56 | `sql/sql_show.cc` | `1` / 冲突 | I_S 实现/注册布局已变，历史主体约 568 行 | **必须重写/收窄** I_S 注册接口 |
| 57 | `sql/sql_table.cc` | `0` / 可套用 | 默认引擎/charset 文本可命中，9.7.2 DDL 语义未验证 | **可适配**，需 DDL 门禁 |
| 58 | `sql/sql_yacc.yy` | `1` / 冲突 | grammar include 与 stored-program 规则已变 | **必须重写** 最小 grammar 入口 |
| 59 | `sql/sys_vars.cc` | `1` / 冲突 | sysvar registry 尾部和校验 API 已变 | **必须重写/模块化** WeSQL 变量注册 |
| 60 | `sql/sys_vars.h` | `0` / 可套用 | plugin sysvar 扩展可命中，需与第 59 行实现一致 | **必须联动适配** |
| 61 | `sql/udf_service_impl.cc` | `0` / 可套用 | 三个 UDF 注册文本可命中，service 生命周期仍需核对 | **可适配**，保持集中注册 |
| 62 | `strings/ctype-simple.cc` | `1` / 冲突 | strxfrm 参数/实现上下文已变 | **必须重写** SmartEngine 所需字符集适配 |
| 63 | `strings/ctype-uca.cc` | `0` / 可套用 | 文本可命中，但 `CHARSET_INFO` 已迁到新公共头 | **可适配**，需排序规则测试 |
| 64 | `strings/ctype-utf8.cc` | `0` / 可套用 | 两处文本可命中，但接口所有权和调用方已变 | **可适配**，需 UTF-8 排序测试 |
| 65 | `strings/int2str.cc` | `1` / 冲突 | 整数转换实现和声明布局已变 | **必须重写或删除**，优先复用 9.7.2 API |

## 3. 汇总边界

- **必须重写的核心链**：root/mysys/sql/scripts/Clone CMake，SQL command/audit，
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
