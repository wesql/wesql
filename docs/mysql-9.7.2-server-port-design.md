# WeSQL MySQL 9.7.2 Server 补丁与构建适配设计

> 设计首版提交：`1a844220d3bff5aa42085b57858c59ea8e991212`
>
> 远端工作分支：`origin/port/mysql-9.7.2-server-build`
>
> Oracle 基线：`mysql-9.7.2` / `008e09c2834b98143a8c067d4d225c90953050cf`

## 1. 基线与边界

- 集成主线：`port/mysql-9.7.2`，固定起点
  `7c6d385931f2ef6c56d449bd30825b9981573cd5`，不直接开发。
- task #27 工作分支：`port/mysql-9.7.2-server-build`。
- Oracle 基线：tag `mysql-9.7.2`，commit
  `008e09c2834b98143a8c067d4d225c90953050cf`。
- task #27 只负责 MySQL Server 接点、公共接口和构建安装适配。
- task #26 负责 ObjectStore、binlog 归档、快照、恢复实现及其运行验收；
  task #27 不修改这些模块的状态机和数据格式。
- 共享构建文件只归 task #27：顶层 `CMakeLists.txt`、`cmake/*.cmake`、
  `mysys/CMakeLists.txt`、`sql/CMakeLists.txt`、`plugin/clone/CMakeLists.txt`、
  `scripts/CMakeLists.txt` 和 `mysql-test/CMakeLists.txt`。task #27 负责 SDK
  探测、`myobjstore` target、task #26 提供的 archive/recovery/replay 源列表、
  Clone `MODULE_ONLY` 和最终链接图；task #26 只提交模块自有 `.cc/.h` 与完整
  “源文件 -> target -> link dependency”清单，不修改这些共享文件。若需新增
  模块私有 `CMakeLists.txt`，必须先由两条任务线程联合 review 文件归属。
- `storage/smartengine/**` 引擎本体不在 task #27 内；Server 侧 handlerton、
  编译和生命周期接线属于 task #27。
- task #20 的后续 8.0.35 最终提交不会直接混入本分支。task #20 完成后，
  应以独立提交重新移植并重新构建，不继承其测试结论。

独立现场：

| 用途 | 路径 |
| --- | --- |
| Oracle 干净源码 | `wesql-compat/source/mysql-9.7.2-task27-clean` |
| WeSQL task #27 工作树 | `wesql-compat/source/wesql-9.7.2-task27` |
| task #27 构建目录 | `wesql-compat/work/mysql-9.7.2-task27-build` |
| task #27 安装目录 | `wesql-compat/work/mysql-9.7.2-task27-install` |
| task #27 日志目录 | `wesql-compat/work/mysql-9.7.2-task27-logs` |

这些目录不得放入 8.0.35 的对象文件、安装文件、datadir、bucket 或证据。

原生基线固定使用隔离 Linux 容器 `wesql-builder-ready:latest`：Ubuntu
24.04.4、CMake 3.28.3、GCC 13.3.0、Ninja 1.11.1、linux/arm64。
Docker 运行时上限为 4 vCPU、4 GiB，编译并发固定为 2，避免资源 OOM
污染源码结论。
Boost 1.87.0 下载到独立目录
`wesql-compat/work/boost_1_87_0-task27`。日志固定保存到
`wesql-compat/work/mysql-9.7.2-task27-logs`。

原生 configure 命令的容器内主体为：

```bash
cmake -S /workspace/wesql-compat/source/mysql-9.7.2-task27-clean \
  -B /workspace/wesql-compat/work/mysql-9.7.2-task27-build \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_INSTALL_PREFIX=/workspace/wesql-compat/work/mysql-9.7.2-task27-install \
  -DDOWNLOAD_BOOST=1 \
  -DWITH_BOOST=/workspace/wesql-compat/work/boost_1_87_0-task27 \
  -DWITH_UNIT_TESTS=OFF \
  -DWITH_ROUTER=OFF \
  -DWITH_NDB=OFF
```

原生 build/install 命令为：

```bash
cmake --build /workspace/wesql-compat/work/mysql-9.7.2-task27-build \
  --parallel 2
cmake --install /workspace/wesql-compat/work/mysql-9.7.2-task27-build
```

三条命令分别记录到 `native-configure.log`、`native-build.log` 和
`native-install.log`；同时记录源码 SHA、容器 image ID、工具链版本和目录清单。

原生基线已在 Oracle clean tree
`008e09c2834b98143a8c067d4d225c90953050cf` 上完成：configure、
`3983/3983` build 和 install 均 exit 0。容器 image ID 为
`sha256:23dcfa059c3fedd50daff72e60d1a4390cacf56ce8f6eec707599fe5968a09cc`；
Docker VM 原始资源是 4 vCPU、`4109398016` bytes，build 并发为 2。

| 原始日志 | 行数 | SHA256 |
| --- | ---: | --- |
| `native-configure.log` | 847 | `e388387f95ec404f2f1036fbac2a634720425ec0234ef8aad698de584f08002c` |
| `native-build.log` | 5009 | `32636900091f496e782f3c6cd1503220b8d6e5c2972ad65aebe91b2355279ffa` |
| `native-install.log` | 25833 | `acd4274c7b204ddb19cecda701db0f4994031e5db5a19694fea170b1f671119e` |
| `native-version.log` | 1 | `37e93a1e176e9ac2eefacae066b16cdb7bafa8ed0f4fb0b29378b1bce12677a9` |
| `native-help.log` | 3252 | `db120d370a2f382d180bdcbed165f05b978a2d5cedb5026a8aec3165a38bb0df` |

安装后二进制在同一 Linux 容器内执行：

```text
mysqld  Ver 9.7.2 for Linux on aarch64 (Source distribution)
```

`mysqld --no-defaults --verbose --help` 同样 exit 0。安装树有 25,234 个文件、
1.3 GiB，包含 `lib/plugin/mysql_clone.so`。

本次命令中的 `-DWITH_NDB=OFF` 没有关闭 NDB storage target：最终
`CMakeCache.txt` 原始值仍是 `WITH_NDBCLUSTER_STORAGE_ENGINE=ON` 和
`WITH_NDBMTD=ON`，因此原生日志确实包含 NDB 构建。这个事实不影响 Oracle
原生基线通过，但后续 task #27 port build 必须另加
`-DWITH_NDBCLUSTER_STORAGE_ENGINE=OFF -DWITH_NDBMTD=OFF`，不得把本次行为
误写成 NDB 已禁用。

## 2. 初始差异事实

8.0.35 的历史补丁仅作为功能和接点清单，不作为 9.7.2 的输入补丁。
对其 65 个声明路径逐文件在 9.7.2 干净源码上执行只读 `git apply
--check`，结果如下：

- 65 个路径中有 33 个文件的整段历史 diff 可直接定位；32 个文件冲突。
  计数脚本必须完整消费 patch 流；若在目标 diff 后提前退出，`pipefail` 会把
  `git show` 的 SIGPIPE 错判成该文件冲突。
- 65 个上游文件相对 8.0.35 均已变化，没有字节相同文件。
- `include/m_ctype.h` 已删除，公共字符集接口移动到
  `include/mysql/strings/m_ctype.h`。
- `sql/binlog_index.cc` 已承接 `normalize_binlog_name()` 等索引逻辑，
  不能按 8.0.35 的文件位置机械移植。
- 9.7.2 的 `MYSQL_VERSION` 使用 `MYSQL_VERSION_MATURITY="LTS"`；
  版本和 CMake 探测必须重新实现。
- Clone、Server CMake、bootstrap SQL 生成、SQL command/audit 映射、
  mysqld 生命周期、handler、binlog、parser、sysvar 和 I_S 注册均有上游变化。

因此 9.7.2 必须从干净 tag 重新形成
`patches/mysql-server-9.7.2.patch`，不能修补或重命名 8.0.35 patch。

## 3. 必须改

以下是首个“可配置、可编译、可安装”骨架的硬门槛。

1. **构建身份与依赖**
   - 只在 9.7.2 上增加 WeSQL 版本字段和 CMake 选项。
   - 重新接入 ObjectStore SDK、SmartEngine、Server 自有模块和 RPM 入口。
   - 先验证原生 9.7.2 空配置，再逐组启用 WeSQL 目标；不把第三方 SDK 类型
     扩散到 MySQL 公共头文件。
2. **Server/引擎公共接口**
   - 在 9.7.2 handlerton 结构上重新定义 checkpoint、快照、备份、恢复和
     post-recovery 回调；保持回调集中，避免把实现下沉到 `handler.cc`。
   - 重新接入 SmartEngine 类型、错误码和字符串/字符集 API；适配新的
     `mysql/strings` 头文件布局。
3. **binlog 安全接点**
   - 在 9.7.2 purge/index/ofile API 上重新实现“未归档不得 purge”。
   - 重新实现恢复后 binlog header in-use flag 更新；不得依赖 8.0.35
     私有类布局或文件位置。
4. **启动、恢复和关闭顺序**
   - 首次打开 binlog index 前保留 binlog 恢复 hook。
   - 内置插件加载后、普通恢复完成前保留 InnoDB/SmartEngine 恢复 hook。
   - `ha_pre_dd_shutdown()` 卸载引擎和 Clone 前停止后台线程；退出阶段再释放
     pthread 对象。
   - task #27 只提供稳定 hook 和编译接口，具体归档/恢复状态机由 task #26
     实现。
5. **SQL、管理面与初始化**
   - 重新对齐 SQL command 枚举、audit 映射、lexer/parser、prepare/execute、
     native procedure 和状态计数。
   - 重新接入 WeSQL bootstrap SQL、DD upgrade 入口、sysvar、I_S 表、UDF
     command、客户端错误和错误日志编号。
   - 任何已删除的 term/index/RAFT 字段不得借移植重新出现。
6. **可复现补丁**
   - 新补丁必须在 `008e09c...` 上通过 apply check、实际 apply、reverse check
     和零残留检查。
   - 补丁只描述 9.7.2 上真实需要的改动；空 hunk、格式漂移和 8.0.35
     上下文均不进入新文件。

## 4. 可适配

这些能力保留，但优先通过 WeSQL 自有模块或窄接点适配，避免继续扩大上游
patch 面。

- `sql/package/**`、native procedure、三个管理 UDF：短期对齐 9.7.2
  parser/service API；后续评估迁到 component/service。
- `sql/sys_vars.cc` 与 `sql/sql_show.cc`：短期保留注册入口，表实现和校验逻辑
  尽量移到 WeSQL 自有文件。
- Clone：保留 9.7.2 原生 `MODULE_ONLY`，不移植 8.0.35 的静态接线 hunk。
  动态 plugin 在 `mysqld.cc:8598` 初始化，早于 snapshot 启动；更早的空目录
  恢复只复制已生成的 Clone 文件，不调用 Clone plugin。部署固定
  `--plugin-load-add=clone=mysql_clone.so`；snapshot 开启但 Clone 缺失必须
  明确启动失败，snapshot 关闭时普通启动不强制 Clone。
- MTR、help result 和 CI：只更新 9.7.2 构建或行为确实改变的内容；不得恢复
  已删除的三节点测试变体。
- `binlog_archive_replica`：若 task #26 决定保留，task #27 只保证 Server
  接点可编译；必须另设运行门禁，不能借用单机恢复证据。

## 5. 暂不支持

首轮 9.7.2 交付不承诺以下兼容性：

- 直接复用 8.0.35 datadir 做原地升级；
- 读取 8.0.35 bucket、archive、snapshot、term/index 或旧锁文件；
- 从 9.7.2 降级回 8.0.35；
- 复用 8.0.35 编译产物、补丁、运行现场或验收结论；
- 由 task #27 单独宣称 ObjectStore、归档、快照和空目录恢复运行通过。

首轮迁移路径仅定义为：9.7.2 全新安装、全新 datadir、全新 repo/bucket，
运行验收由 task #26 与独立复核共同完成。

## 6. 实施顺序

1. 固化 9.7.2 tag、task 分支和独立目录证据。
2. 在干净 tag 上完成原生 configure，记录工具链与依赖要求。
3. 建立 9.7.2 writable port tree；按构建、公共接口、生命周期、SQL 管理面
   四组逐步实现，每组都保持可审查 diff。
4. 生成新的 `mysql-server-9.7.2.patch`，执行 apply/reverse/零残留门禁。
5. 在全新 build/install 目录执行 configure、compile、install；记录完整版本、
   Oracle SHA、WeSQL SHA、目录和日志摘要。
6. 把 Server 接口交给 task #26 对接；两边 review 通过后才合入集成主线。
7. task #20 完成后，再以单独提交移植其最终单机清理，不覆盖本轮证据。

## 7. task #27 验收定义

task #27 进入 review 前至少提供：

- 设计文档和按文件列出的 API 冲突清单；
- 新 9.7.2 patch 的 apply/reverse/零残留证据；
- 全新 configure、compile、install 成功证据；
- `mysqld --version`、`mysqld --verbose --help` 的版本和参数证据；
- 精确 Oracle commit、WeSQL commit、分支、源码/build/install/log 路径；
- 未复用 8.0.35 产物、目录和结论的目录清单；
- 明确列出仍由 task #26 或后续运行任务负责的未验收能力。
