# 0004. The catalog never vends storage credentials

Date: 2026-10-26. Status: accepted.

## Context

Iceberg REST catalogs can return scoped storage credentials to any client that can load a table. In the POC the
catalog API is unauthenticated on the Docker network, so vending would let anyone who can reach port 8181 read
data files directly.

## Decision

Create the warehouse with `sts-enabled=false` and `remote-signing-enabled=false`. Each engine (Spark, Trino,
Kyuubi engines) carries its own storage identity from its container environment.

## Consequences

* `test_catalog_does_not_vend_storage_credentials` proves a client asking for delegated access gets metadata only.
* The model maps one to one onto Kerberos keytabs and HDFS ACLs in production: analysts never hold a storage identity.
* Lakekeeper's remote-signing advantage for MinIO-class stores is unused; that is acceptable for a read path served
  by engines rather than by end users.
