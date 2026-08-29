# 9.7 建表兼容设计

授权：工作帖 `#274acc7e`。长期分支：`9.7`，头
`32789959bf16d9f104004016708112a620d853e0`。`8.0` 仍是
`7c6d385931f2ef6c56d449bd30825b9981573cd5`。不合进 `8.0`。

对象存储归档恢复已经正式验收：`#5bff7c69`（依据 `#39975502`）。
「9.7 剩余移植」已经正式验收：`#5b2e6da4`。这次不重跑 unique-run，
不开 objstore28。不要把 GitHub PR #85 当成 9.7 的交付。

## 1. 要解决什么

SmartEngine 当默认引擎时，常见 ORM 的默认 `CREATE TABLE` 会硬失败。
2026-08-22 在 WeSQL 8.0.35 镜像上测过 Prisma / Django / Rails /
Laravel / Drizzle 官方默认建表：20 条语句，11 条硬失败，每个 ORM
至少失败 1 条。证据：`wesql-compat/results/REPORT.md`、
`compat-results.json`。

两类会挡默认脚手架的失败：

1. **索引列排序规则不在白名单。** 例如 Prisma / Laravel 默认
   `utf8mb4_unicode_ci`，唯一索引会报
   `1105 Unsupported collation on string indexed column`。
   白名单包括 `utf8mb4_0900_ai_ci`、`utf8mb4_general_ci`、
   `utf8mb4_bin` 等，**不包括** `utf8mb4_unicode_ci`。
2. **外键。** SmartEngine 报
   `1235 SE currently doesn't support foreign key constraints`。
   Rails / Django / Drizzle / Prisma 的默认关联都会撞上。

FULLTEXT、GEOMETRY、生成列也会失败，但五个 ORM 的默认脚手架
不依赖它们。第一版不改这些。

v1 **不在 SmartEngine 里实现外键**。垫层把语句改到能建表，
并明确告诉调用方：库不再执行 ON DELETE CASCADE。

## 2. 改写范围

只处理 **SmartEngine 表**。InnoDB 或其他能做外键的引擎，原样执行。

时机：CREATE / ALTER 已经解析完、还没交给存储引擎。不是改 SQL
文本，也不是只写注释。

改两件事：

1. **排序规则。** 索引列（含主键、唯一键）上不支持的 collation，
   改成白名单里最接近的：`utf8mb4_unicode_ci` /
   `utf8mb4_unicode_520_ci` → `utf8mb4_0900_ai_ci`。表默认
   collation 如果会落到索引列上，一并改。
2. **外键。** 从表定义里拿掉 `FOREIGN KEY` / `REFERENCES`。
   列本身留下。`ON DELETE CASCADE` 不会在引擎里执行。

配套必须同时有：

- 客户端 `SHOW WARNINGS` 能看见：旧/新 collation；被剥掉的外键名；
  以及“没有父表级联，应用自己处理”。
- 服务器错误日志同样记一条。
- `SHOW CREATE TABLE` 显示落地后的 collation，不再出现被剥掉的外键。
- MCP / 方言说明仍然要写：SmartEngine 不强制外键；Prisma 可走
  `relationMode=prisma`。

## 3. 例子

### 3.1 会失败的 Prisma 用户表（排序规则）

```sql
CREATE TABLE `User` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `email` VARCHAR(191) NOT NULL,
    `name` VARCHAR(191) NULL,
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ENGINE=SMARTENGINE;
```

UNIQUE 建在 `email` 上时，当前会 1105。改写后索引列 collation
变成 `utf8mb4_0900_ai_ci`，建表成功，并警告原来是
`utf8mb4_unicode_ci`。

### 3.2 会失败的 Rails 帖子表（外键 + CASCADE）

```sql
CREATE TABLE posts (
  id bigint NOT NULL AUTO_INCREMENT,
  user_id bigint NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT fk_rails_posts_user
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=SMARTENGINE;
```

当前会 1235。改写后：`user_id` 列还在，外键没了，建表成功。
警告里写清：剥掉了 `fk_rails_posts_user`，删除用户时数据库
**不会**自动删帖子，应用自己删。

### 3.3 不改的 InnoDB

```sql
CREATE TABLE t (
  id INT PRIMARY KEY,
  parent_id INT,
  FOREIGN KEY (parent_id) REFERENCES t(id)
) ENGINE=InnoDB;
```

InnoDB 原样执行，不剥外键，不改 collation。

### 3.4 已经在白名单里的 collation

```sql
CREATE TABLE t (
  id INT NOT NULL PRIMARY KEY,
  email VARCHAR(191) NOT NULL,
  UNIQUE KEY email (email)
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

不改写，无警告。

## 4. 这次做哪些

同一人从头做到自己验证完。先合这篇设计，再改代码。

1. 在 GitHub `wesql/wesql` 的 `9.7` 上落地垫层：SmartEngine 的
   collation 改写 + 外键剥离 + 警告。
2. 自己测：上面四类例子；五个 ORM 默认脚手架能建表；InnoDB 外键
   不变；重启后 `SHOW CREATE TABLE` 仍是改写后的样子。
3. PR 合进 `9.7`，不合进 `8.0`。临时分支从 `9.7` 开，合完再删。

## 5. 这次不做什么

- 不在 SmartEngine 里实现外键
- 不合进 `8.0`
- 不把 PR #85（`weicao/orm-ddl-rewrite`）cherry-pick 或标成 9.7 交付
- 不重跑 unique-run，不开 objstore28，不 retry `16d21c27`
- 不改本机 KEEP 运行现场，不 `docker rm wesql-task27-v11`
- 不改 FULLTEXT / GEOMETRY / 生成列
- 不飞书重复

## 6. 完成怎么算

- 这篇设计在 `9.7` 上
- 后续代码 PR 合进 `9.7`，头能指出提交号
- 本席自己的建表、警告、`SHOW CREATE TABLE`、写入、读取、重启
  有命令输出摘要
- 没有往 `8.0` 合，没有新的对象存储 unique-run 包

对象存储仍以 `#5bff7c69` 为准。剩余移植仍以 `#5b2e6da4` 为准。
本线只收 SmartEngine 建表兼容。
