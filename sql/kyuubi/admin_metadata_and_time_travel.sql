-- Spark SQL dialect (Kyuubi) statements that only ADMINS can run (P4). Analysts get an authorization error
-- for the metadata tables and have no path to a snapshot id. Run as admin via kyuubi-beeline.

-- name: k_snapshots
SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added_records
FROM lakehouse.silver.customers.snapshots ORDER BY committed_at;

-- name: k_history
SELECT * FROM lakehouse.silver.customers.history ORDER BY made_current_at;

-- name: k_files
SELECT file_path, record_count, file_size_in_bytes FROM lakehouse.silver.transactions.files LIMIT 5;

-- name: k_time_travel_version
-- replace <snapshot_id> with a value from k_snapshots: the customer 97 still exists in the pre-CDC snapshot
SELECT customer_id, segment FROM lakehouse.silver.customers VERSION AS OF <snapshot_id> WHERE customer_id = 97;
