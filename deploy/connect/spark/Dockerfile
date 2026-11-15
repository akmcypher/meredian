# Spark for Community ingestion (deploy/connect/README.md, "Spark ingestion"): the pinned Spark 3.5.3
# Java 17 / Python 3.10 image plus the source drivers the executors use (requirements.txt here).
# Meridian's Python modules (deploy/connect) are mounted, not baked in. Runs as the base image's
# unprivileged `spark` user.
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu@sha256:a1f2c9dcecb36c0f5c342be340b368883cfaa1d7ab23e6e222c8a93157b73a74
COPY requirements.txt /tmp/meridian-requirements.txt
USER root
RUN pip install --no-cache-dir --timeout 120 --retries 5 -r /tmp/meridian-requirements.txt
USER spark
