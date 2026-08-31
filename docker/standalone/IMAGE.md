# Pinned images for the standalone trial

| Service | Image | Why this tag |
| --- | --- | --- |
| WeSQL | `apecloud/wesql-server:8.0.35-0.1.0_beta5.40` | Latest official beta on Docker Hub (2025-01-21). Do not use `gotest*` tags. Older docs used `beta4.38`. |
| MinIO | `minio/minio:RELEASE.2024-12-18T13-15-44Z` | Fixed community release. Avoid `latest` (license/product change). |
| MinIO client | `quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z` | Docker Hub has no `mc` tag for 2024-12-18. This quay tag exists; WeSQL only auto-creates the bucket when `objectstore_provider=local`. |

Change these tags only after a new image is verified end-to-end with the scripts in this directory.
