#!/bin/bash
# Runs once on first Postgres start (docker-entrypoint-initdb.d). Creates:
#   - the Lakekeeper metadata database
#   - the kyuubi_auth database with the auth_users table used by Kyuubi JDBC authentication
# Passwords come from the container environment (infrastructure/.env); nothing is hard-coded here.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-SQL
  CREATE DATABASE ${LAKEKEEPER_DB};
  CREATE DATABASE ${KYUUBI_AUTH_DB};
  CREATE ROLE ${KYUUBI_AUTH_DB_USER} LOGIN PASSWORD '${KYUUBI_AUTH_DB_PASSWORD}';
SQL
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$KYUUBI_AUTH_DB" <<-SQL
  CREATE TABLE auth_users (
    username      text PRIMARY KEY,
    password_hash text NOT NULL,          -- hex(sha256(password)) ; PROD: LDAP/Kerberos, this table disappears
    enabled       boolean NOT NULL DEFAULT true,
    created_at    timestamptz NOT NULL DEFAULT now()
  );
  INSERT INTO auth_users (username, password_hash) VALUES
    ('admin',       encode(sha256(convert_to('${ADMIN_PASSWORD}',      'UTF8')), 'hex')),
    ('etl',         encode(sha256(convert_to('${ETL_PASSWORD}',        'UTF8')), 'hex')),
    ('analyst1',    encode(sha256(convert_to('${ANALYST_PASSWORD}',    'UTF8')), 'hex')),
    ('analyst2',    encode(sha256(convert_to('${ANALYST_PASSWORD}',    'UTF8')), 'hex')),
    ('restricted1', encode(sha256(convert_to('${RESTRICTED_PASSWORD}', 'UTF8')), 'hex')),
    ('bench',       encode(sha256(convert_to('${BENCH_PASSWORD}',      'UTF8')), 'hex'));
  GRANT CONNECT ON DATABASE ${KYUUBI_AUTH_DB} TO ${KYUUBI_AUTH_DB_USER};
  GRANT SELECT ON auth_users TO ${KYUUBI_AUTH_DB_USER};
SQL
echo "init-postgres: databases ${LAKEKEEPER_DB}, ${KYUUBI_AUTH_DB} and auth_users created"
