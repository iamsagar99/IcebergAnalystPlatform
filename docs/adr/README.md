# Architecture decision records

Short records of the decisions that shaped the POC. Each one links to the evidence in the main documents.

| ADR | Decision | Status |
|---|---|---|
| [0001](0001-lakekeeper-as-rest-catalog.md) | Lakekeeper as the Iceberg REST catalog | accepted |
| [0002](0002-trino-primary-kyuubi-secondary.md) | Trino primary, Kyuubi secondary, both shipped | accepted |
| [0003](0003-trino-read-only-connector.md) | Trino's Iceberg connector is read-only | accepted |
| [0004](0004-no-credential-vending.md) | The catalog never vends storage credentials | accepted |
| [0005](0005-ranger-admin-stub.md) | Real Ranger plugin, simulated Ranger Admin | accepted (local only) |
| [0006](0006-minio-fork.md) | pgsty/silo instead of minio/minio | accepted |
