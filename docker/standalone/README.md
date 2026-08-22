# WeSQL standalone trial (Docker Compose + MinIO)

This is the Phase 1 single-node developer path.

- Starts MinIO and one WeSQL data node together.
- Does not require AWS.
- Does not start a 3-node cluster.
- Uses a pinned WeSQL image. See [IMAGE.md](IMAGE.md).

Need Docker Compose v2. Port 3306, 9000, and 9001 should be free, or override them in `.env`.

## 10-minute loop

```bash
cd docker/standalone
docker compose up -d
./scripts/wait-ready.sh
./scripts/smoke.sh
./scripts/restart-verify.sh
./scripts/logs.sh
# optional
./scripts/cleanup.sh
```

Or run the same loop in one command:

```bash
cd docker/standalone
./scripts/try.sh
```

## Connect

```bash
docker compose exec wesql mysql -uroot -ppasswd
```

From the host, if you have a MySQL client:

```bash
mysql -h127.0.0.1 -P3306 -uroot -ppasswd
```

MinIO console: <http://127.0.0.1:9001>  
User / password: `wesqlminio` / `wesqlminio123`

## Cleanup

```bash
./scripts/cleanup.sh
```

This removes containers and volumes.

## Known limits

- This path is a local trial, not a production cluster.
- SmartEngine tables do not support foreign keys.
- Some default ORM collations cannot be used for indexes. That rewrite is a separate Phase 1 task.
- Raft / logger nodes are not enabled here.

## Image pin

Only this WeSQL tag is used here:

`apecloud/wesql-server:8.0.35-0.1.0_beta5.40`
