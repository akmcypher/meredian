# Meridian Community features

Meridian Community is open-source data ingestion with a web control plane: it brings data in from
APIs, files, databases, object stores and streams, and keeps it as immutable, verifiable raw
(Bronze) batches that other engines can read. This page lists everything in the current release,
**Community Preview 0.2**.

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
| Kafka (`kafka-v1`)                                                                | Connect agent, one task per partition                             | JSON, Debezium JSON, Confluent wire-format Avro                                                          |
| Object-store files (`files-v1`)                                                   | Connect agent                                                     | CSV, TSV, JSON Lines and Parquet under an S3-compatible prefix (AWS S3, GCS interoperability, MinIO, R2) |
| Spark (`spark-postgres-v1`, `spark-mysql-v1`, `spark-files-v1`, `spark-kafka-v1`) | Spark cluster driven by the Connect agent                         | The same sources, read in parallel across executors                                                      |

Database, Kafka and object-store credentials stay on the agent host; the control plane never
opens connections into your network.

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

- PostgreSQL and MySQL connectors are incremental cursor extractors, not change data capture:
  deletes and updates that do not advance the cursor are not captured.
- Spark: Bronze parts are NDJSON. Spark on Kubernetes is tested on a local kind cluster only, in
  client mode (no Spark Operator).
- Object-store files: full refresh only; no Avro files or ADLS yet.
- Managed Kafka profiles are checked offline and against a local secured broker, not against the
  hosted services.
- Raw upsert/delete envelopes are kept as-is; no warehouse merge is applied.
- REST/Push batches and file uploads are not exposed through the Bronze reader.
- Connectors share one agent process; run one installation per trust boundary.
- Team invitations are not available yet; container images are not signed.

Full details are in the [release notes](RELEASE_NOTES.md).
