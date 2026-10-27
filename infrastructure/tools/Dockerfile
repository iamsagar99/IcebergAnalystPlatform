# Client/test toolbox: Trino python client, Hive-thrift client for Kyuubi, pytest, htpasswd, psql.
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      apache2-utils curl jq postgresql-client ca-certificates openssl \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /workspace
ENV PYTHONUNBUFFERED=1
CMD ["bash"]
