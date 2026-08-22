# WeSQL SmartEngine ORM 建表兼容改写（第四稿）

作者：白小纯  
日期：2026-08-22  
对应：task #9  
第三轮：方向能解 Prisma 20/20，但去 FK 自动索引、落地 SQL 生成、no-op binlog 未过。本稿钉死这三项。  
仍不要开始 Server 实现。

已通过可保留：开关默认关、只改 SmartEngine、不改原始 SQL 文本、其他引擎不动、CREATE 落在 `create_table_impl`、ALTER 分步、客户端码 7516/7517、现有证据附件。

## 1. 问题与证据

| 项 | 值 |
| --- | --- |
| 镜像 | `apecloud/wesql-server:8.0.35-0.1.0_beta5.40` |
| digest | `sha256:90d4c9e328e23785a26263b998835bbf8ca92bfa3449ceda70d513b375ead4fc` |
| 脚本 / 20 条 SQL / 原始结果 | 已作为附件发在 #wesql:b91dc2ae |

20 条里 11 条失败。附录 A。Prisma 的第 2 条是先建无索引 `email`，再单独 `CREATE UNIQUE INDEX`。

## 2. 开关

`wesql_orm_ddl_rewrite`：GLOBAL + SESSION，默认 **OFF**。Compose 试用显式 ON。  
文档、warning、日志都写开关值。OFF 时行为与现在相同（1235 / 1105）。

## 3. 字符序

只映射：`utf8mb4_unicode_ci` → `utf8mb4_general_ci`。  
不改字符集，不改 latin1/gbk/utf8mb3，不改成 0900。跟现有补丁的 0900→general_ci 对齐。

CREATE 继承表默认值时，`Create_field::charset` 在 `mysql_prepare_create_table()` 前可能为空。必须用 `get_sql_field_charset()`。被改的索引列显式写成 general_ci。

**已有列再加索引（开关 ON）：**  
整列改成 `utf8mb4_general_ci`，warning 7516 写明比较规则变了、表可能重建。  
这是 Prisma `CREATE UNIQUE INDEX` 能过的唯一办法。OFF 仍 1105。

## 4. 落点

### 4.1 CREATE

`create_table_impl()`：`set_table_default_charset()` 之后、`mysql_prepare_create_table()` 之前。

去外键时必须成对处理解析器生成项。  
`PT_foreign_key_definition::do_contextualize()` 会连续塞入：

1. `Foreign_key_spec`（`KEYTYPE_FOREIGN`）
2. `generated=true` 的 `KEYTYPE_MULTIPLE`（自动支撑索引）

ON：两项一起删。用户显式写的索引（`generated=false`）保留。  
OFF：SQL 层 1235。  
然后删掉 `ha_smartengine::create()` 里对原始 SQL 的 `FOREIGN KEY` 扫描。引擎收不到 `Alter_info`。

### 4.2 ALTER 分两步

1. 引擎已确定之后，**早于** `check_fk_parent_table_access()` 和 FK MDL：  
   成对删除 `KEYTYPE_FOREIGN` + 对应 `generated=true` 的 `KEYTYPE_MULTIPLE`，并清 `Alter_info::ADD_FOREIGN_KEY`。
2. `mysql_prepare_alter_table()` 之后：处理本条 ADD/MODIFY/CHANGE 的列，以及已有 unicode_ci 列加索引（整列改 general_ci）。

门禁表：附件 `06_rewrite_cases.sql` 的 `a_child`。  
主从 `SHOW CREATE TABLE` 必须一致，且都不能多出自动索引。

## 5. binlog

原则：

- **不**调用 `thd->set_query()`。原始查询留在 THD，审计/诊断仍看用户语句。
- 落地 SQL 放在单独 buffer 里，**直接**传给 inplace 和 copy 两处的 `ha_binlog_log_query()` 与 `write_bin_log()`。
- GTID 开着时，成功必须写一条可重放事件。

### 5.1 CREATE

成功后打开实表，`store_create_info()` 得到落地 `CREATE`，把这段字符串传给写盘接口。不改 `thd->query()`。

### 5.2 已有列加索引（Prisma `CREATE UNIQUE INDEX`）

走 `mysql_alter_table()`，原句没有 `MODIFY COLUMN`。  
落地 SQL 必须是完整的：

```sql
ALTER TABLE `User`
  MODIFY COLUMN `email` <完整列定义> COLLATE utf8mb4_general_ci,
  ADD UNIQUE INDEX `User_email_key` (`email`);
```

完整列定义从哪里来：复用/抽取 `store_create_info()` 的结构化列打印，不能手拼。  
至少保留：类型、NULL/NOT NULL、DEFAULT、COMMENT、前缀索引、复合索引、反引号名称。  
来源是 `mysql_prepare_alter_table()` 之后的新表 `Create_field` / `Field`。

### 5.3 只去外键（去掉 FK 和自动支撑索引之后）

先确认 Alter_info 里**真的没有元数据变化**（没有剩余用户索引/列变更）。  
不要再 `ALTER TABLE ... COMMENT=原注释`：仍会进 ALTER/引擎，可能改元数据或重建。

改为写一条真实 no-op 的 Query event，例如：

```sql
DO 0 /* wesql_orm_ddl_rewrite stripped fk */
```

必须验证：GTID 连续、主 ON / 从 OFF 能重放、崩溃恢复不丢号、从库表结构不变。

### 5.4 加外键又夹着其它子句

整句拒绝（7518）。请调用方拆开。不写伪造 ALTER。

### 5.5 不需要改写

仍把原始 `thd->query()` 传给现有写盘。不 `set_query()`。

## 6. 错误码

`messages_to_clients.txt` 保留区 7500–7999，本任务：

| 码 | 名字 |
| --- | --- |
| 7516 | `ER_WESQL_ORM_COLLATION_REWRITTEN` |
| 7517 | `ER_WESQL_ORM_FK_STRIPPED` |
| 7518 | `ER_WESQL_ORM_ALTER_NOT_REWRITABLE`（夹杂子句无法安全改写） |

`LogErr` 另在 `messages_to_error_log.txt` 的 25000–29999 分配，不复用 7516–7518。

不用 8091/8092。

## 7. 自动测试

- 开关 OFF/ON
- 未命名 / 多个 / 复合外键
- `a_child`：去 FK 后主从 `SHOW CREATE` 一致，无自动支撑索引
- Prisma 独立 `CREATE UNIQUE INDEX`：列类型、NULL、DEFAULT、COMMENT 不丢，collation 为 general_ci
- 已有 unicode_ci 列加索引：ON 整列改写 + 7516；OFF 1105
- 加 FK 又夹其它子句：7518
- InnoDB 外键保留；非索引 unicode_ci 列不变
- 20 条：ON 全过；OFF 与 `compat-results.json` 一致
- binlog：主 ON / 从 OFF 的 CREATE、`DO 0` 占位、Prisma UNIQUE INDEX 落地 SQL；GTID 连续；崩溃恢复

## 8. 验收

第四稿通过后再改 Server 代码。
