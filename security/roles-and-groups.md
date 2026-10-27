# Roles, groups and users (one model, two enforcement points)

The authorization model is defined once (this file) and enforced twice: by Trino's system access control
(`infrastructure/trino/rules.json`) and by the Kyuubi Spark AuthZ plugin with Ranger policies
(`infrastructure/kyuubi/authz/policies.json`). Group membership is resolved by the same source on both sides:
locally a static file (`trino/generated/groups.txt`, `hadoop/core-site.xml`), in production AD/LDAP.

## Groups (= roles)

| Group | Purpose | lakehouse access | PII table `silver.customer_pii` | Workload group (P8) |
|---|---|---|---|---|
| `admins` | platform team | everything, incl. metadata tables, maintenance (Spark only), kill queries | raw | `admin` |
| `etl` | ETL service account (Spark jobs, Airflow) | read/write all layers | raw | `etl` (YARN queue `etl`) |
| `analysts` | ordinary analysts | SELECT on `gold.*` and the non-PII `silver` tables | **denied** | `interactive` |
| `restricted_analysts` | analysts cleared for customer contact data | as analysts + SELECT on `customer_pii` with masks and row filter | masked + `marketing_opt_in = true` only; `street_address` never | `interactive` |
| `monitors` | Prometheus | Trino system information read only | none | `admin` (metrics scrape) |

## Demo users (local; passwords in `infrastructure/.env`)

| User | Groups | Exists on |
|---|---|---|
| `admin` | admins | Trino password file, Kyuubi `auth_users` |
| `etl` | etl | both (Spark jobs run as the container identity, see security-model) |
| `analyst1`, `analyst2` | analysts | both (two users to demonstrate GROUP engine sharing) |
| `restricted1` | restricted_analysts | both |
| `bench` | analysts | both (benchmark identity: own Trino resource group, own Kyuubi engine pool) |
| `monitor` | monitors | Trino only |

## PII policy

* Column masks (restricted analysts): `email` partially masked, `phone` and `national_id` show last 4 / hashed,
  `date_of_birth` reduced to the year, `street_address` blocked outright.
* Row filter (restricted analysts): only customers with `marketing_opt_in = true`.
* Ordinary analysts: table denied (Trino) / column reads denied and a `1 = 0` row filter (Kyuubi, see `docs/security-model.md`).
* Admin access to PII is audited (Trino query log / Kyuubi operation audit log; production: Ranger audit).

## Periodic access review

Quarterly: export group membership from AD, diff against the previous export, re-approve `restricted_analysts`
members with the data-protection officer, and re-run `scripts/validate.sh --engine both` so the policy tests
prove the model still holds after any policy change.
