# WeSQL ORM 建表复跑

镜像：`apecloud/wesql-server:8.0.35-0.1.0_beta5.40`  
digest：`sha256:90d4c9e328e23785a26263b998835bbf8ca92bfa3449ceda70d513b375ead4fc`

## 启动（本地对象存储，不改 Server 代码）

见当时用过的 `objectstore_provider=local` 单容器。端口 3307。容器名默认 `wesql-compat`。

## 跑

只采集、不验收：

```
python3 run_compat.py
```

读 `sql/01_*.sql`–`05_*.sql`，逐条执行，写 `results/compat-results.json`。

门禁（会失败退出）：

```
# OFF：20 条里必须仍是那 11 条失败，分类与 results/expected-20.json 一致
python3 run_compat.py --set-rewrite off --check-baseline --expect-fails 11

# ON：20 条全过
python3 run_compat.py --set-rewrite on --check-baseline --expect-fails 0
```

- `01`–`05`：五个 ORM 默认建表（20 条）
- `00_probes.sql`：定性探针，不计入 20 条
- `06_rewrite_cases.sql`：评审要求的未命名/多外键/复合外键/ALTER/字符集不变/InnoDB
- `results/expected-20.json`：逐条 OFF/ON 预期（fail + class）
- `results/compat-results.json`：旧镜像采集基线，不是门禁
