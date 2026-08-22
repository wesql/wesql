# WeSQL ORM 建表复跑

镜像：`apecloud/wesql-server:8.0.35-0.1.0_beta5.40`  
digest：`sha256:90d4c9e328e23785a26263b998835bbf8ca92bfa3449ceda70d513b375ead4fc`

## 启动（本地对象存储，不改 Server 代码）

见当时用过的 `objectstore_provider=local` 单容器。端口 3307。容器名默认 `wesql-compat`。

## 跑

```
python3 run_compat.py
```

读 `sql/*.sql`，逐条执行，写 `results/compat-results.json`。

- `01`–`05`：五个 ORM 默认建表（20 条）
- `00_probes.sql`：定性探针，不计入 20 条
- `06_rewrite_cases.sql`：评审要求的未命名/多外键/复合外键/ALTER/字符集不变/InnoDB

当前镜像没有 `wesql_orm_ddl_rewrite` 开关。脚本记录的是引擎现状。开关落地后，同一脚本应在 ON 下让 `01`–`05` 全过，OFF 下 11 条失败与 `results/compat-results.json` 一致。
