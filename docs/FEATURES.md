# Meridian Community features

Meridian Community is open-source data ingestion with a web control plane: it brings data in from
APIs, files, databases, object stores and streams, and keeps it as immutable, verifiable raw
(Bronze) batches that other engines can read. This page lists everything in the current release,
**Community Preview 0.3**.

Status: pre-release. Everything here is demonstrated on local stacks and covered by automated
tests; none of it is hosted or production-certified. See [known limitations](#known-limitations).

## Sources

| Source                                                                            | How it runs                                                       | What it reads                                                                                            |
| --------------------------------------------------------------------------------- | ----------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| REST API                                                                          | Manual pull from the web app                                      | JSON responses from an HTTP endpoint                                                                     |
| Push API                                                                          | Your system posts to `/api/public/ingest` with a per-source token | JSON records                                                                                             |
| File upload                                                                       | Browser upload                                                    | CSV, TSV, Parquet, Avro, JSON, JSON Lines, NDJSON                                                        |
| PostgreSQL (`postgres-v1`)                                                        | Connect agent                                                     | Tables, full refresh or incremental (cursor + primary key)                                               |
| MySQL (`mysql-v1`)                                                                | Connect agent                                                     | Tables, full refresh or incremental (cursor + primary key)                                               |
| PostgreSQL CDC (`postgres-cdc-v1`)                                                | Connect agent, logical replication                                | Every insert, update and delete, in commit order, after a first full copy                                |
| MySQL CDC (`mysql-cdc-v1`)                                                        | Connect agent, binlog                                             | Every insert, update and delete, in commit order, after a first full copy                                |
| Kafka (`kafka-v1`)                                                                | Connect agent, one task per partition                             | JSON, Debezium JSON, Confluent wire-format Avro                                                          |
| Object-store files (`files-v1`)                                                   | Connect agent                                                     | CSV, TSV, JSON Lines and Parquet under an S3-compatible prefix (AWS S3, GCS interoperability, MinIO, R2) |
| Spark (`spark-postgres-v1`, `spark-mysql-v1`, `spark-files-v1`, `spark-kafka-v1`) | Spark cluster driven by the Connect agent                         | The same sources, read in parallel across executors                                                      |

Database, Kafka and object-store credentials stay on the agent host; the control plane never
opens connections into your network.

## Change data capture

- PostgreSQL through logical replication (`pgoutput` read with core SQL functions, so no extension
  and no replication connection), MySQL through the binlog as a replica.
- The first run copies the table; later runs land each change once, in commit order: inserts and
  updates as upserts, deletes as deletes, a changed key as a delete and an upsert. Changed rows land
  with the same values as the copy.
- Fails closed instead of guessing: column changes, `TRUNCATE`, a lost replication position, and
  (PostgreSQL) an unchanged large value without `REPLICA IDENTITY FULL`. A reset copies the table
  again.
- `check` names any missing source setting (`wal_level`, replication privilege, publication; binlog
  format, row image and row metadata).
- Lag in bytes of log behind the source, and the PostgreSQL slot name, in the stream view.

## Connect in the web app

- Connection list with health, last success and lag; a detail view with overview, streams, runs and
  settings tabs; plain-language guidance for every error code.
- A new-connection wizard for PostgreSQL, MySQL (cursor or CDC) and Kafka (managed profiles or
  self-managed security), which writes the agent's credential file template with placeholders for
  secrets.

## Spark for ingestion

- PostgreSQL and MySQL full refresh in hash partitions of the primary key (PostgreSQL from one
  exported snapshot; MySQL with a consistent snapshot per partition).
- Incremental streams in a fixed (cursor, key) window, so a resumed attempt reads the same window
  and later changes wait for the next run.
- Files split into record-aligned byte ranges (CSV, TSV, JSON Lines) or Parquet row-group ranges.
  A file replaced during a run fails it as `source_changed` instead of mixing versions.
- Kafka read in offset chunks per partition, with the same cluster identity, position and
  retention-gap checks as the single-worker actor.
- Executors write Bronze parts straight to storage through short-lived signed URLs. The control
  plane re-verifies every part (size, checksum, rows, envelopes) before a fenced commit, so Bronze
  readers see ordinary batches.
- Every Spark connector is tested against its single-worker counterpart: same rows, same
  committed positions, and failed attempts resume without loss or duplicates.
- Secured Spark: the driver refuses to run unless Spark RPC authentication, network encryption and
  spill-file encryption are all on, before any credentials leave it. The local cluster
  (`deploy/connect/spark/`, master and two workers) runs with a shared secret only the Spark user
  can read.
- Spark on Kubernetes (`deploy/connect/spark/k8s/`): executor pods with CPU and memory limits, a
  namespace quota, the restricted Pod Security profile, no Kubernetes API access for executors, a
  network policy that admits only Spark pods, and the Spark secret passed by reference (never as a
  value in a pod spec). A local kind cluster setup is included.
- Parquet Bronze parts (opt-in, `CONNECT_SPARK_OPTIONS` `{"bronze_format": "parquet"}`): Spark writes
  each part as Parquet, with one column per field typed from the stream schema, so every part of a
  stream has the same columns. The parts hold exactly the records an NDJSON part would, and the server
  decodes and checks them the same way. A part that can't be stored without changing a value stays
  NDJSON. `read_bronze.py` reads both formats.

## Worker pools

- A pool is a named group of agents in one workspace with its own token (`mp_...`, shown once and
  stored only as a SHA-256 hash). Agents started with it claim tasks of every connection assigned to
  the pool, least recently served connection first, so one large backfill can't starve other sources.
- An optional per-connection parallel task limit (1 to 64; empty means no limit) counts that
  connection's running tasks on every agent, through a pool or its own token.
- A pool serves connections of its own workspace only, and no two of its connections may share a
  credential file. Disabling a pool or issuing a new token stops its agents' calls at once; each
  connection's own token keeps working.
- Agents report their id, slot count and version; the Connect page lists pools with agents online,
  their slots and assigned connections, and each connection's settings choose its pool and limit.

## Kafka

- Topic discovery; one leased task per partition so several agents read a topic in parallel,
  with one stream-wide commit sequence.
- Durable per-partition offsets and per-partition lag (committed position and end offset) in the
  stream view.
- Backpressure: records stream straight to storage, checkpoints by record count or bytes, a capped
  prefetch queue, and bounded runs that continue from their position next time.
- Secured clusters: SASL/SCRAM-SHA-256/512, SASL/PLAIN over TLS and mutual TLS. Bad credentials,
  an untrusted CA or a hostname mismatch fail as `auth_failed` before reading.
- Managed-service profiles: Confluent Cloud, Azure Event Hubs, Amazon MSK (SCRAM and IAM),
  Redpanda Cloud and Aiven.
- Confluent schema registry with Avro, including pinned reader-schema evolution.
- Tombstones and delete events kept in raw ingestion; dead letters with replay.
- Retention-gap detection: positions lost to retention stop the run until the owner approves the
  exact gap, recorded in commit history.
- Broker failure recovery, tested with one of three brokers stopped.

See [Kafka](KAFKA.md).

## Reliable runs

- Every stream of a run is a leased, fenced task; several agents can work on one run, and a task
  is retried on its own.
- Each commit is durable, idempotent and stores its source checkpoint, so a failed run resumes
  from the last commit without loss or duplicate positions.
- Schedules: Connect actors run every N minutes through the agent, or from your own scheduler.
- Large inputs go through partitioned runs with atomic publication.

## Bronze output

- Append-only raw NDJSON batches with manifests and checksums.
- Registered, owner-scoped engines read completed Bronze snapshots through bounded manifests and
  short-lived signed URLs, and verify what they read. See [Bronze consumption](BRONZE_CONSUMPTION.md).

## Build your own connector

The Connect SDK and protocol (`meridian-connect/1`) with a conformance kit for testing a new
connector. See the [connector developer guide](CONNECTOR_DEVELOPER_GUIDE.md).

## Accounts and security

- Email signup with confirmation, a private workspace per account, onboarding, profile editing and
  password recovery. See [authentication](AUTHENTICATION.md).
- Organization-scoped row-level security in the database.
- Secrets stay server-side and never reach the browser; per-source tokens are stored only as
  hashes.
- Request limits, least-privilege database grants and an append-only, hash-chained admin audit
  trail.

## Install

- Windows 11 (x64 and ARM64), macOS (Apple silicon and Intel) and Ubuntu/Debian (x64 and ARM64)
  launchers that fetch pinned, hash-verified Node, Bun and Supabase CLI builds.
- Release archives are signed with Sigstore (keyless cosign) and carry SLSA build provenance.
- Start with the [quickstart](../QUICKSTART.md) and the [user guide](USER_GUIDE.md).

## Known limitations

- The cursor connectors (`postgres-v1`, `mysql-v1`) miss deletes and updates that don't advance
  the cursor; use the CDC connectors for those tables.
- CDC: a paused PostgreSQL stream keeps its slot and holds WAL on the source (watch the lag; drop
  the slot to retire a stream). No GTID-based MySQL positions yet.
- Spark: Bronze parts are NDJSON unless Parquet is turned on. Spark on Kubernetes is tested on a local kind cluster only, in
  client mode (no Spark Operator).
- Object-store files: full refresh only; no Avro files or ADLS yet.
- Managed Kafka profiles are checked offline and against a local secured broker, not against the
  hosted services.
- Raw upsert/delete envelopes are kept as-is; no warehouse merge is applied.
- REST/Push batches and file uploads are not exposed through the Bronze reader.
- Connectors share one agent process; run one installation per trust boundary.
- Team invitations are not available yet; container images are not signed.

Full details are in the [release notes](RELEASE_NOTES.md).
