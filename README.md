# WeSQL

WeSQL is a MySQL distribution with a compute-storage separation architecture.
Its SmartEngine storage engine stores persistent data in S3 or an S3-compatible
object store, while local disks act as cache.

## Try WeSQL locally

The standalone developer path starts one WeSQL server and MinIO with Docker
Compose. It does not require an AWS account.

Prerequisites:

- Git
- Docker Engine or Docker Desktop with Docker Compose v2
- Free local ports `3306`, `9000`, and `9001`

```bash
git clone --branch 8.0 https://github.com/wesql/wesql.git
cd wesql/docker/standalone
./scripts/try.sh
```

The script waits for WeSQL to become ready, creates a SmartEngine table,
restarts WeSQL, verifies that the row is still present, and prints recent logs.
It uses the pinned image
`apecloud/wesql-server:8.0.35-0.1.0_beta5.40`.

Connect from inside the container:

```bash
docker compose exec wesql mysql -uroot -ppasswd
```

Remove the trial containers and volumes:

```bash
./scripts/cleanup.sh
```

See the [standalone trial guide](docker/standalone/README.md) for configuration
and the [full quick start](https://wesql.io/docs/tutorial/standalone) for
troubleshooting.

> **Local trial only:** this Compose setup runs one data node without logger
> nodes or high availability. It is not a production deployment. SmartEngine
> also has documented MySQL feature differences, including foreign keys and
> some indexed collations. Review the
> [compatibility limits](https://wesql.io/docs/usage/compatibility) before
> migrating an application.

## Learn more

- [Introduction](https://wesql.io/docs/introduction)
- [Architecture](https://wesql.io/docs/architecture)
- [Tutorials](https://wesql.io/docs/tutorial)
- [Build from source](https://wesql.io/docs/tutorial/binary/install)

## Community

Join our [Discord](https://discord.com/channels/1308609231498510427/1308609231498510430) to discuss features, get help, and connect with other users.

## Licensing

Portions Copyright (c) 2024, ApeCloud Inc Holding Limited. WeSQL is specifically available only under version 2 of the GNU General Public License (GPLv2). (I.e. Without the "any later version" clause.) This is inherited from MySQL. Please see the README file in the MySQL distribution for more information.
