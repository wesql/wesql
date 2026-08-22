# 第二补丁如何应用

在 MySQL 8.0.35 上先覆盖 WeSQL 目录，再按顺序打补丁：

```
patch -p1 < patches/mysql-server-8.0.35.patch
patch -p1 < patches/mysql-server-8.0.35-orm-ddl-rewrite.patch
```

或 `scripts/apply-wesql-patches.sh`。

CI / RPM / get-version 也必须走这个脚本，不能只 `git apply` 第一张补丁。

第二补丁包含：

- `wesql_orm_ddl_rewrite` 开关（默认关）
- 客户端错误 7516/7517/7518
- CREATE/ALTER 钩子和 binlog buffer（不 `set_query`）
- 每条 CREATE/ALTER 入口复位 binlog 状态；prepare 前快照本次新增索引
- CMake 加入 `wesql_orm_ddl_rewrite.cc`

自有代码在 overlay：`sql/wesql_orm_ddl_rewrite.*`。
引擎里已去掉原始 SQL 的 FOREIGN KEY 扫描。
