# WeSQL

WeSQL is a MySQL distribution with a compute-storage separation architecture.
Its SmartEngine storage engine stores persistent data in S3 or an S3-compatible
object store, while local disks act as cache.

## Try WeSQL locally

The current developer path builds one writable WeSQL server from the verified
source revision. No container image is recommended for this revision yet.

Prerequisites:

- Git
- A supported Linux build environment and the MySQL 8.0.35 build dependencies
- CMake 3, a C++ compiler, Make, and Perl

The verified WeSQL source revision is
[`04538e63b0cab42ce3a9bb8bc06a19a62d3b2346`](https://github.com/wesql/wesql/commit/04538e63b0cab42ce3a9bb8bc06a19a62d3b2346),
merged into `8.0` as
[`7c6d385931f2ef6c56d449bd30825b9981573cd5`](https://github.com/wesql/wesql/commit/7c6d385931f2ef6c56d449bd30825b9981573cd5).

```bash
git clone --branch mysql-8.0.35 --single-branch \
  https://github.com/mysql/mysql-server.git
git clone https://github.com/wesql/wesql.git wesql-overlay
git -C wesql-overlay checkout 04538e63b0cab42ce3a9bb8bc06a19a62d3b2346
git -C wesql-overlay archive HEAD | tar -x -C mysql-server

cd mysql-server
git apply --check patches/mysql-server-8.0.35.patch
git apply patches/mysql-server-8.0.35.patch

cmake3 -S . -B build \
  -DCMAKE_INSTALL_PREFIX="$PWD/install" \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DWITH_SSL=system \
  -DWITH_ROUTER=OFF \
  -DWITH_CLONE=ON \
  -DWITH_WESQL=ON \
  -DWITH_SMARTENGINE=ON \
  -DDOWNLOAD_BOOST=1 \
  -DWITH_BOOST="$PWD/boost"
cmake3 --build build --parallel 2
cmake3 --install build
```

See the [full source-build quick start](https://wesql.io/docs/tutorial/standalone)
for OS packages, single-server initialization, connection, restart, and
troubleshooting.

> **Development and evaluation only:** the current build is a single writable
> server and does not provide built-in high availability. Object-store binlog
> archiving is asynchronous, so recovery from object storage is not a zero-loss
> guarantee. SmartEngine also has documented MySQL feature differences,
> including foreign keys and some indexed collations. Review the
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
