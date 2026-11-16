# Meridian Community release notes

## Unreleased (next preview)

Demonstrated locally (Docker Desktop and a local kind cluster); not hosted or certified.

- **Secured Spark:** Spark RPC is authenticated and encrypted. The driver checks
  `spark.authenticate`, `spark.network.crypto.enabled` and `spark.io.encryption.enabled` before any
  task (with source credentials and the lease token) leaves it, and fails the run with a
  `spark_insecure` log line otherwise. `allow_insecure_spark` allows it with a warning, for
  throwaway local clusters only. The local cluster now creates and uses a shared secret.
- **Spark on Kubernetes:** run Spark ingestion on Kubernetes from a driver pod
  (`deploy/connect/spark/k8s/`), with per-run executor limits, a namespace quota and limit range,
  the restricted Pod Security profile, no API token in executors, a network policy, and the Spark
  secret passed by reference.
- **Change data capture:** `postgres-cdc-v1` (logical replication) and `mysql-cdc-v1` (binlog)
  copy a table once, then land every insert, update and delete in commit order, each once. They
  fail closed on column changes, `TRUNCATE` and a lost position, report lag in bytes, and `check`
  names any missing source setting. Migration 0105 adds sync mode `cdc`.
- **Connect in the web app:** a connection list with health and lag, a detail view (overview,
  streams, runs, settings) with guidance for every error code, and a new-connection wizard for
  PostgreSQL, MySQL (cursor or CDC) and Kafka.
- The agent image now includes the files and CDC connectors.
- Known limitations: Spark on Kubernetes is tested on kind only and uses client mode (no Spark
  Operator); Bronze parts are still NDJSON; CDC is tested on local PostgreSQL and MySQL only, and a
  paused PostgreSQL CDC stream holds WAL until its slot moves or is dropped.

## Meridian Community Preview 0.2

Preview 0.2 is pre-release software for evaluation and feedback. It is not production-certified.
Everything below was demonstrated on local stacks (Windows, Docker Desktop); none of it is hosted
or certified yet.

### What is new

- **Parallel runs (Connect tasks):** each stream of a run is a leased task, so several agents work
  on one run at once. Tasks are fenced and retried on their own; agents from 0.1 keep working.
- **Kafka at scale:** one task per partition with a stream-wide commit sequence, per-partition lag
  in the stream view, and backpressure with bounded runs. See [Kafka](KAFKA.md).
- **Secured and managed Kafka:** SASL/SCRAM and SASL/PLAIN over TLS, mutual TLS, and profiles for
  Confluent Cloud, Azure Event Hubs, Amazon MSK (SCRAM and IAM), Redpanda Cloud and Aiven.
- **Object-store files:** a `files-v1` connector reads CSV, TSV, JSON Lines and Parquet under a
  prefix of an S3-compatible bucket (AWS S3, GCS interoperability, MinIO, R2). Credentials stay on
  the agent host.
- **Spark for ingestion:** `spark-postgres-v1`, `spark-mysql-v1`, `spark-files-v1` and
  `spark-kafka-v1` run loads on a Spark cluster. Executors write Bronze parts straight to storage
  through short-lived signed URLs; the control plane verifies every part before a fenced commit, so
  Bronze readers see ordinary landing batches.
  - PostgreSQL and MySQL: full refresh in hash partitions of the key (PostgreSQL in one exported
    snapshot) and incremental streams in a fixed (cursor, key) window.
  - Files: record-aligned byte ranges (CSV/TSV/JSONL) and row-group ranges (Parquet); a file
    replaced during a run fails it as `source_changed`.
  - Kafka: offset chunks per partition with the actor's identity, position and gap checks.
  - Every Spark run is tested against the single-worker actor for the same rows and positions, and
    for resuming a failed attempt without loss or duplicates. A local cluster is in
    `deploy/connect/spark/`.
- **Installers:** launchers and packages for macOS and Linux next to the Windows installer.

### Known limitations

- PostgreSQL and MySQL actors are incremental cursor extractors, not CDC: deletes and updates that
  do not advance the cursor are not captured.
- Spark runs: Spark RPC authentication and encryption are not configured (database credentials and
  the run's lease token reach executors over Spark RPC inside the cluster), Kubernetes is not
  supported yet, PKCS#12 Kafka keystores are refused (use PEM files), and Bronze parts are NDJSON.
- Files: full refresh only, at most 4,096 ranges per run; no Avro files and no ADLS yet. CSV with
  quoted newlines is read one range per file.
- Managed Kafka profiles are checked offline and against a local secured broker, not against the
  hosted services.
- Raw upsert/delete envelopes are retained as-is; no warehouse merge is applied.
- REST/Push batches and native uploads are not exposed through the Bronze reader.
- Storage reconciliation is report-only; destructive cleanup is disabled.
- Team invitations are not available yet.
- Container images are not signed. Release archives are signed with Sigstore and carry SLSA
  build provenance.

### Upgrading from Preview 0.1

Apply migrations 0094 to 0097 in order, with the updated application. They add leased tasks per
stream (0094), Kafka partition tasks (0095), partition lag (0096) and a claim fix for tasks whose
stream already completed (0097). Runs already queued keep working; agents from 0.1 use the
run-level lease until they are updated.

## Meridian Community Preview 0.1

Preview 0.1 is pre-release software for evaluation and feedback. It is not production-certified.

### What is included

- **Sources:** manual REST pulls, a Push API, native file upload (CSV, TSV, Parquet, Avro, JSON,
  JSONL, NDJSON), and Connect actors for PostgreSQL, MySQL and Kafka.
- **Recoverable runs:** raw NDJSON commits are immutable, idempotent and fenced by run leases. A
  failed run resumes from its last committed checkpoint.
- **Kafka:** explicit partition assignment, durable per-partition offsets, Confluent wire-format
  Avro with a schema registry, tombstones, dead letters and replay. Positions lost to retention
  stop the run until the owner approves the exact gap.
- **Bronze consumption:** registered owner-scoped engines read immutable Bronze snapshots through
  bounded manifests and short-lived signed URLs. See [Bronze consumption](BRONZE_CONSUMPTION.md).
- **Accounts:** email signup with confirmation, a private workspace per new account, onboarding,
  profile editing and password recovery. See [authentication](AUTHENTICATION.md).
- **Security:** organization-scoped row-level security, server-only secrets, hashed per-source
  tokens, request limits, an append-only hash-chained admin audit and least-privilege database
  grants for anonymous and signed-in clients.

### Known limitations

- PostgreSQL and MySQL actors are incremental cursor extractors, not CDC: deletes and updates that
  do not advance the cursor are not captured.
- Raw upsert/delete envelopes are retained as-is; no warehouse merge is applied.
- REST/Push batches and native uploads are not exposed through the Bronze reader.
- Kafka consumer-group scaling, rebalancing and SASL/TLS clusters are not supported yet.
- Connector actors share one agent process. Run one installation per trust boundary.
- Storage reconciliation is report-only; destructive cleanup is disabled.
- Team invitations are not available yet.
- Container images and release archives are not signed.

### Upgrading an earlier preview database

Apply migrations in order and never edit applied files. Before upgrading past migration 0071,
cancel outstanding replay runs and requeue still-open dead letters afterwards. Migration 0073 and
the updated application must be deployed together: the old Bronze read function is removed and old
callers fail closed. Migrations 0074 and 0075 tighten client grants and must be applied before use.
