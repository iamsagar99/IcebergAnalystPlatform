# 0006. pgsty/silo instead of minio/minio

Date: 2026-10-26. Status: accepted.

## Context

The requirements name MinIO as the local S3-compatible store. The `minio/minio` images were withdrawn from Docker
Hub and the upstream repository was archived in early 2026; `docker manifest inspect minio/minio:latest` fails.

## Decision

Use `pgsty/silo`, the maintained fork of the same code base, pinned by release tag. The S3 API, `mc` client and
metrics endpoints are unchanged; the server binary is `silo`.

## Consequences

* Nothing in Spark, Trino, Kyuubi or Lakekeeper is aware of the difference.
* Production storage is HDFS anyway; this only affects the local POC.
* Revisit if the client wants on-premises object storage: Apache Ozone or Ceph RGW are the long-term options.
