# Trino vs. Spark SQL (Kyuubi) dialect notes for this warehouse

`sql/sample_queries.sql` and `sql/validation_queries.sql` are written in the common subset and run unchanged on both
engines (verified by `scripts/validate.sh --engine both`). Differences that matter when analysts (or the ETL team)
go beyond that subset:

| Topic | Trino | Spark SQL (Kyuubi) |
|---|---|---|
| String type name | `VARCHAR` | `STRING` (Spark accepts `VARCHAR(n)` as well) |
| Time travel (admin only, P4) | `SELECT ... FROM t FOR VERSION AS OF <id>` / `FOR TIMESTAMP AS OF TIMESTAMP '...'` | `SELECT ... FROM t VERSION AS OF <id>` / `TIMESTAMP AS OF '...'` |
| Metadata tables | `"t$snapshots"`, `"t$history"`, `"t$files"` | `t.snapshots`, `t.history`, `t.files` |
| Approximate distinct | `approx_distinct(x)` | `approx_count_distinct(x)` |
| Integer division | integer `/` integer truncates | integer `/` integer returns double |
| Date arithmetic | `date_add('day', 7, d)` | `date_add(d, 7)` |
| Current date | `current_date` | `current_date()` / `current_date` |
| Listing | `SHOW TABLES FROM lakehouse.gold` | `SHOW TABLES IN lakehouse.gold` |
| Maintenance | `ALTER TABLE t EXECUTE expire_snapshots(...)` (disabled for analysts) | `CALL lakehouse.system.expire_snapshots(...)` (disabled for analysts) |
| Session props | `SET SESSION query_max_memory = '1GB'` | `SET spark.sql.shuffle.partitions = 8` (most keys restricted by Kyuubi) |
| Existing ETL code | must be ported (Spark SQL/DataFrame API is not Trino SQL) | runs as-is: same Spark SQL dialect as the ETL |

The last row is the main dialect argument for Kyuubi in P1; the other rows are the day-to-day friction for analysts
when the primary engine is Trino. `sql/kyuubi/admin_metadata_and_time_travel.sql` shows the Spark-dialect versions
of the admin-only metadata / time-travel statements.
