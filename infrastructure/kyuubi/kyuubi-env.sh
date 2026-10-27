#!/usr/bin/env bash
# Sourced by bin/kyuubi. Secrets are read from the container environment (set from infrastructure/.env).
export JAVA_HOME="${JAVA_HOME:-/opt/java/openjdk}"
export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export SPARK_CONF_DIR="${SPARK_HOME}/conf"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/opt/hadoop/conf}"
# [LOCAL] small server heap; PROD: 4-8g (server holds session + operation state for all analysts)
export KYUUBI_JAVA_OPTS="-Xmx512m -XX:+UseG1GC -Dkyuubi.authentication.jdbc.user=${KYUUBI_AUTH_DB_USER} -Dkyuubi.authentication.jdbc.password=${KYUUBI_AUTH_DB_PASSWORD}"
