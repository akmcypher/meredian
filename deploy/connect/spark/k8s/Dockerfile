# Spark ingestion on Kubernetes (S5b, deploy/connect/README.md): the Spark image with Meridian's
# Spark modules baked in, because executor pods have no host mounts. Build context: deploy/connect.
#   docker build -f deploy/connect/spark/k8s/Dockerfile -t meridian-spark-ingest-k8s:local deploy/connect
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu@sha256:a1f2c9dcecb36c0f5c342be340b368883cfaa1d7ab23e6e222c8a93157b73a74
COPY spark/requirements.txt /tmp/meridian-requirements.txt
USER root
RUN pip install --no-cache-dir --timeout 120 --retries 5 -r /tmp/meridian-requirements.txt
COPY sdk.py bronze_parquet.py source_postgres.py source_mysql.py source_files.py source_kafka.py spark_ingest.py spark-submit.sh /opt/meridian/connect/
USER 185
ENV PYTHONPATH=/opt/meridian/connect HOME=/tmp
