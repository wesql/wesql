# 9.7 剩余移植设计

授权：工作帖 `#b8d7dfa4`。长期分支：`9.7`，头
`afca8833491e1228a88d55bf271d3be5697b952e`。
相对 `8.0` ahead 24 / behind 0。`8.0` 仍是
`7c6d385931f2ef6c56d449bd30825b9981573cd5`。

对象存储归档恢复已经正式验收：`#5bff7c69`（依据 `#39975502`）。
这次不重跑 unique-run，不开 objstore28。

## 1. 现在是什么

GitHub `9.7` 还是 WeSQL 8.0 那套仓库结构。根目录没有 Oracle 的
`MYSQL_VERSION`。多出来的是 24 个移植提交，加上一份很薄的
`patches/mysql-server-9.7.2.patch`（大约 185 行）。

真正按 MySQL 9.7.2 编过、跑过对象存储恢复的，是本机独立的 Oracle
9.7.2 overlay，不是 GitHub 上这一棵 8.0 源码树自己变成了 9.7.2。

所以：长期分支名字已经对了，代码也合进去了；但“在 9.7 上把剩余移植做完”
还没发生。剩下的不是再开一条对象存储闸门，而是把已经在 9.7.2 overlay
上做过的 Server 适配，变成 `9.7` 上能 PR、能自己编、能自己测的代码。

## 2. 相对 8.0 WeSQL 还缺什么

8.0 头 `7c6d3859` 是单机归档的 WeSQL 8.0.35。`9.7` 在它上面多了这些
已经合入的东西：

- 版本门和 CLI 里的 WeSQL 字样
- ObjectStore CMake / SDK / 安全修复
- 归档恢复源码移植 `950cde79e`
- SmartEngine 在 9.7.2 头文件、bit 运算、`Log_info`、`NullS`、ODR、sys_vars
  接线

还缺的是：

1. GitHub `9.7` 还不是一份可独立编译的 MySQL 9.7.2 Server 树。
2. 历史 65 个 8.0 补丁路径里，大约 32 个在 9.7.2 上必须重写，不能拿旧
   diff 硬套。清单在 `docs/mysql-9.7.2-server-api-diff.md`。首要缺口是：
   根/mysys/sql/scripts 的 CMake、mysqld 启停锚点、错误号生成、handler
   晚期 hook、DD bootstrap、parser / sysvar 自有翻译单元。
3. 33 个“文本能套上”的文件只是定位线索，不能当成已经移植成功。
4. 建表兼容（字符集改写、外键剥离）按授权属于第 3 步，现在不做。

## 3. 这次做哪些

同一人从头做到自己验证完。先合这篇设计，再改代码。

1. 把本机已经编过的 MySQL 9.7.2 overlay 适配，按可审查的提交合进 `9.7`。
   目标：在 `9.7` 上能配置、编译、安装，`mysqld --version` 能看到
   MySQL 9.7.2 和 WeSQL 0.1.0。
2. 只补还缺的 Server 接点：CMake、生命周期、错误、handler、bootstrap、
   sysvar。已经验收的对象存储/归档恢复源码能复用就复用，不再改状态机。
3. 自己编、自己测：干净配置/编译/安装、启动、SmartEngine 基本读写、
   同配置重启。不重跑对象存储 unique-run。
4. PR 合进 `9.7`，不合进 `8.0`。临时开发分支从 `9.7` 开，合完再删这条
   临时分支。其他旧临时分支不动。

## 4. 这次不做什么

- 不开工建表兼容
- 不合进 `8.0`
- 不清 `port/mysql-9.7.2-archive-recovery`、
  `port/mysql-9.7.2-server-build` 等其他临时分支
- 不重跑 unique-run，不开 objstore28，不 retry `16d21c27`
- 不改本机 KEEP 运行现场（objstore27/26/25 的 src/rec/region、hook、
  PID `76687`）
- 不把旧三节点 / Raft / 旧 event 兼容加回来
- 不把 archive replica 那大段 `rpl_*` 侵入当成本轮必须交付
- 不飞书重复，不另立总负责人

## 5. 完成怎么算

- 这篇设计在 `9.7` 上
- 后续代码 PR 合进 `9.7`，头能指出提交号
- 本席自己的编译、安装、`--version`、启动、读写、重启有命令输出摘要
- 没有往 `8.0` 合，没有新的对象存储 unique-run 包

对象存储三条验收结论继续以 `#5bff7c69` / `#39975502` 为准，不在本步重开。
