# Meridian Community release notes

## Unreleased (next preview)

Demonstrated on a local stack (Spark items with a two-worker Spark cluster); not hosted or certified.

- **Parquet Bronze parts from Spark:** with `CONNECT_SPARK_OPTIONS` `{"bronze_format": "parquet"}`,
  Spark runs land their Bronze parts as Parquet (layout `meridian-bronze-parquet/1`) instead of
  NDJSON. Each data field is its own column, typed from the stream schema, so every part of a stream
  has the same columns. The parts hold exactly the records an NDJSON part would; the server decodes
  and checks every record before registering a part. A part that can't be stored without changing a
  value (for example a `bigint` past 2^53) stays NDJSON. The default is unchanged, and batches the
  agent pushes itself are always NDJSON.
- **Readers accept both formats:** `read_bronze.py` downloads and checks Parquet parts and counts
  over both formats with `--query-count`; the dataset registry records each part's format.
- **Worker pools:** a pool is a named group of agents in one workspace with its own token, shown
  once. Its agents share the work of every connection assigned to it, least recently served
  connection first, and skip connections another agent is claiming from. Tested with three agents
  of four slots each draining sync runs of 50 connections: every run landed each row once.
- **Parallel task limit:** each connection can cap how many of its tasks run at once on all agents,
  to protect its source. The default is no limit.
- **Pool boundaries:** a pool only serves connections of its own workspace, and no two of its
  connections may share a credential file. Disabling a pool or issuing a new token cuts off its
  agents at once; each connection's own token keeps working.
- **Connect UI:** a Worker pools panel (create, enable or disable, new token, agents online and
  slots) and a pool choice and limit in each connection's settings.
- **Incremental object-store files:** `files-v1` and `spark-files-v1` streams can run
  incrementally. Each object is identified by its content (SHA-256), so unchanged objects are
  skipped, new keys are read whatever their sort order, and an interrupted object resumes after its
  last committed row. A changed object at a known key fails with `source_changed` instead of
  overwriting Bronze; deleted objects keep their history.
- **Avro files and ADLS:** `format: avro` reads Avro container files, and `provider: adls` reads
  Azure Data Lake Storage with an account key, SAS token or service principal kept on the agent.
  ADLS is tested with local files only; live Azure access is unverified.
- **Kafka consumer groups:** `consumer_group: true` with a `group_id` reads a whole topic as one
  group member. With `CONNECT_KAFKA_SHARED=1`, several agents share one connection's partitions
  through broker assignments; revoked partitions are fenced and handed over without duplicates.
  Positions always come from landed Bronze checkpoints; broker offsets are never committed.
- **Avro schema evolution:** Kafka and Spark Kafka resolve each record's writer schema, including
  referenced schemas, and can normalize to a chosen reader subject. Changing only the reader keeps
  committed offsets. Incompatible records become dead letters that can be replayed after a fix.
- **Retire streams:** a stream can be retired, which fences future ingestion and keeps its Bronze
  history. For PostgreSQL CDC, the agent drops the replication slot so it stops holding WAL.
- **Durable schedules:** scheduled runs are created by `POST /api/public/connect-tick` (bearer
  `CRON_SECRET`) from your own cron, not by agent polling. Missed intervals are coalesced into one
  run, and connections with an active run are skipped. Manual Sync works without a cron.
- **Upgrading:** apply migrations 0106, 0108, 0109 (stream retirement), 0110 (scheduler), 0111
  (shared Kafka), 0112 (Avro reader state), 0114 (connection lock order) and 0115 (finish lock
  order), update the agents and the Spark image, and set up a cron for `/api/public/connect-tick`
  if you use schedules. Without these options, existing connections behave as before.

## Meridian Community Preview 0.3

Preview 0.3 is pre-release software for evaluation and feedback. It is not production-certified.
Everything below was demonstrated on local stacks (Windows, Docker Desktop, and a local kind
cluster for Kubernetes); none of it is hosted or certified yet.

### What is new

- **Change data capture:** `postgres-cdc-v1` (logical replication) and `mysql-cdc-v1` (binlog)
  copy a table once, then land every insert, update and delete in commit order, each once, with
  the same values as the copy. A changed key lands as a delete and an upsert. They fail closed on
  column changes, `TRUNCATE` and a lost position, report lag in bytes of log, and `check` names any
  missing source setting.
- **Connect in the web app:** a connection list with health and lag, a detail view (overview,
  streams, runs, settings) with guidance for every error code, and a new-connection wizard for
  PostgreSQL, MySQL (cursor or CDC) and Kafka (managed profiles or self-managed security).
- **Secured Spark:** Spark RPC is authenticated and encrypted. The driver checks
  `spark.authenticate`, `spark.network.crypto.enabled` and `spark.io.encryption.enabled` before any
  task (with source credentials and the lease token) leaves it, and fails the run with a
  `spark_insecure` log line otherwise. `allow_insecure_spark` allows it with a warning, for
  throwaway local clusters only. The local cluster creates and uses a shared secret.
- **Spark on Kubernetes:** run Spark ingestion on Kubernetes from a driver pod
  (`deploy/connect/spark/k8s/`), with per-run executor limits, a namespace quota and limit range,
  the restricted Pod Security profile, no API token in executors, a network policy, and the Spark
  secret passed by reference.
- The Connect agent image now includes the files and CDC connectors.

### Known limitations

- CDC is tested on local PostgreSQL and MySQL only, not on managed cloud databases. A paused
  PostgreSQL CDC stream keeps its slot and holds WAL on the source until the slot moves or is
  dropped (watch the lag). MySQL positions are binlog file and offset (no GTID yet).
- The cursor connectors (`postgres-v1`, `mysql-v1`) still miss deletes; use CDC for those tables.
- Spark on Kubernetes is tested on kind only and uses client mode (no Spark Operator). Bronze parts
  are NDJSON. PKCS#12 Kafka keystores are refused on Spark (use PEM files).
- Files: full refresh only, at most 4,096 ranges per run; no Avro files and no ADLS yet.
- Managed Kafka profiles are checked offline and against a local secured broker, not against the
  hosted services.
- Raw upsert/delete envelopes are retained as-is; no warehouse merge is applied.
- Streams and connections cannot be deleted in the app yet.
- Container images are not signed. Release archives are signed with Sigstore and carry SLSA
  build provenance.

### Upgrading from Preview 0.2

Apply migration 0105 with the updated application. It adds sync mode `cdc` and the trigger that
keeps `cdc` streams on CDC connections; existing streams are unchanged. Update Connect agents to
the new image (or `deploy/connect` files and `requirements.txt`) before creating CDC connections.
Spark clusters started from `deploy/connect/spark/compose.yml` must be recreated to pick up the
shared secret; until then the driver refuses to run on them.

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
