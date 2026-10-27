# syntax=docker/dockerfile:1
# One multi-stage build produces the two JVM images the POC needs:
#   target spark-base : Spark 3.5.8 (Java 17) + Iceberg 1.11.0 runtime + AWS bundle + JupyterLab
#                       -> used for ETL/seeding (spark-submit) and the analyst notebook (Jupyter)
#   target kyuubi     : spark-base + Apache Kyuubi 1.12.0 server + Kyuubi Spark AuthZ (Ranger) plugin
#                       -> the Kyuubi server AND the Spark engines it spawns use this image/binary
# The same Spark + Iceberg binaries therefore back ETL, Jupyter and Kyuubi engines.

ARG SPARK_IMAGE=apache/spark:3.5.8-scala2.12-java17-python3-ubuntu
ARG KYUUBI_IMAGE=apache/kyuubi:1.12.0

FROM ${KYUUBI_IMAGE} AS kyuubi-dist

# ---------------------------------------------------------------- spark-base
FROM ${SPARK_IMAGE} AS spark-base
ARG ICEBERG_VERSION=1.11.0
ARG MAVEN=https://repo1.maven.org/maven2
USER root
RUN set -eux; \
    cd /opt/spark/jars; \
    for art in \
      org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
      org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar ; do \
        f=$(basename "$art"); \
        curl -fsSL -o "$f" "${MAVEN}/${art}"; \
        echo "$(curl -fsSL "${MAVEN}/${art}.sha1")  $f" | sha1sum -c -; \
    done
# JupyterLab for the manual-verification notebook (spark-base only; Kyuubi engines never start it)
RUN pip3 install --no-cache-dir "jupyterlab>=4.3,<5" "pandas>=2.2,<3" "pyarrow>=17,<22"
RUN mkdir -p /home/spark/notebooks /opt/hadoop/conf /tmp/spark-local /opt/spark/work-dir /opt/spark/conf \
    && chown -R spark:spark /home/spark /tmp/spark-local /opt/spark/work-dir /opt/spark/conf
ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    HADOOP_CONF_DIR=/opt/hadoop/conf \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    HOME=/home/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:/home/spark/.local/bin:$PATH
USER spark
WORKDIR /home/spark

# ---------------------------------------------------------------- kyuubi
FROM spark-base AS kyuubi
ARG KYUUBI_VERSION=1.12.0
ARG MAVEN=https://repo1.maven.org/maven2
USER root
COPY --from=kyuubi-dist /opt/kyuubi /opt/kyuubi
RUN set -eux; \
    # Kyuubi Spark AuthZ plugin (bundles the Ranger plugin classes) -> Spark engine classpath
    art=org/apache/kyuubi/kyuubi-spark-authz-shaded_2.12/${KYUUBI_VERSION}/kyuubi-spark-authz-shaded_2.12-${KYUUBI_VERSION}.jar; \
    curl -fsSL -o /opt/spark/jars/$(basename $art) "${MAVEN}/${art}"; \
    echo "$(curl -fsSL "${MAVEN}/${art}.sha1")  /opt/spark/jars/$(basename $art)" | sha1sum -c -; \
    # PostgreSQL JDBC driver for JDBC authentication: already shipped in /opt/kyuubi/jars (postgresql-42.7.11.jar)
    mkdir -p /opt/kyuubi/logs /opt/kyuubi/pid /opt/kyuubi/work /opt/kyuubi/policycache /opt/kyuubi/conf; \
    chown -R spark:spark /opt/kyuubi
ENV KYUUBI_HOME=/opt/kyuubi \
    KYUUBI_CONF_DIR=/opt/kyuubi/conf \
    KYUUBI_LOG_DIR=/opt/kyuubi/logs \
    KYUUBI_PID_DIR=/opt/kyuubi/pid \
    KYUUBI_WORK_DIR_ROOT=/opt/kyuubi/work \
    PATH=/opt/kyuubi/bin:$PATH
USER spark
WORKDIR /opt/kyuubi
CMD ["/opt/kyuubi/bin/kyuubi", "run"]
