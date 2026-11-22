# Meridian Connect v1 — PostgreSQL, MySQL and Kafka preview

Real PostgreSQL, MySQL and Kafka sources and a bronze destination, not Airbyte/Fivetran parity. One protocol (`meridian-connect/1`), one agent, one actor per source kind (`agent.py` picks it from the connection's `source_version`). Certification evidence lives in `src/lib/connect-certification.ts`; the connector catalogue is derived from it.

## Connector foundation

- **Protocol additions (all optional, v1 actors stay valid):** composite `primary_key` lists (`connect_streams.key_columns`, migration 0053), RECORD `operation` (`upsert`/`delete`), STATE `positions` (Kafka next offset per partition), `DEAD_LETTER` messages, safe code `dead_letter_budget`. Values are lossless JSON (`sdk.to_json`: integers past 2^53, decimals, bytes, timestamps).
- **Conformance kit** `conformance.py`: drives an actor over STDIO against a live database (spec, check, auth failure, discover with composite keys, full refresh, resume without loss/duplicates, incremental, reset, type round-trip, schema drift). `test_certify_postgres.py` and `test_certify_mysql.py` subclass it.
- **MySQL** `source_mysql.py` (PyMySQL): same contract as PostgreSQL; `tinyint(1)` is boolean, JSON parsed, TIMESTAMP in UTC.
- **Kafka** `source_kafka.py` (confluent-kafka): explicit partition assignment from Postgres-stored positions (the consumer group is never committed), runs bounded by the high watermarks at start, tombstones and Debezium `op=d` as deletes, Confluent wire-format Avro through any Confluent-API registry, optional pinned reader schema (`x-avro-reader-schema`).
- **Dead letters and replay** (migration 0054): undecodable records are stored (payload in `ingest-files/<owner>/dead-letters/`, metadata in `connect_dead_letters`) in the same fenced sequence as the commit that moves past them. Over `dead_letter_budget` per run the run fails and nothing advances. Owners replay chosen dead letters (`connect_enqueue_replay`, never moves the stream position), discard them, or rewind a stream's checkpoint (`connect_rewind_stream`, evented).
- **Offsets below retention fail closed** (migration 0059): when a saved Kafka position is below the partition's low watermark, the run fails with `offset_out_of_range`, reads nothing and moves no checkpoint; the agent reports partition, saved offset, low watermark and gap size (`connect_events.detail`). Retrying does not help (the code is terminal). The owner decides with `connect_accept_offset_gap` (audited event with the gap); the next run then resumes at the low watermark and its commits keep the accepted ranges in `checkpoint.gaps`.
- **Schema registry** (migration 0055, `src/lib/schema-registry.server.ts`): Meridian subjects/versions; compatibility checked on the server, service-only registration fenced on the checked version.

Live suites: `docker compose -p meridian-certify -f deploy/connect/certify.compose.yml up -d --wait` (3 KRaft brokers, RF 3, min ISR 2, Karapace; localhost only) and `deploy/ingest/test-sources.compose.yml` (MySQL), then `bun run test:python:local connect` and `bun run test:e2e:local connect-live connect-kafka-live schema-registry`.

**Secured clusters (Phase 3 K1).** The Kafka actor accepts `security_protocol` (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`), `sasl_mechanism` (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`) with `sasl_username`/`sasl_password`, and TLS material as files or inline PEM: `ssl_ca_location`/`ssl_ca_pem`, client `ssl_certificate_*` + `ssl_key_*` (+ `ssl_key_password`) or a PKCS#12 `ssl_keystore_location` + `ssl_keystore_password`. File paths may be relative to the config file, so certificates live next to it in the private secrets directory. Hostname verification is on by default; `ssl_endpoint_identification_algorithm: "none"` turns it off and logs `tls_hostname_verification_disabled`. `SASL_PLAINTEXT` with `PLAIN` sends the password in clear text and is refused unless `allow_cleartext_password: true`. Only these settings reach the Kafka client. The schema registry takes `schema_registry_ca_location` for a private CA, and Basic credentials are refused over plain HTTP. Wrong credentials, unknown users, untrusted broker certificates and hostname mismatches end the run as `auth_failed` (terminal). A broker that drops a client without a trusted certificate looks like a network fault and is retried as `source_unavailable`. Certified against `certify-secure.compose.yml` (`test_kafka_security.py`; start it with `docker compose -p meridian-certify-secure -f deploy/connect/certify-secure.compose.yml up -d --wait`).

**Partition lag (Phase 3 K2, migration 0096).** After each committed checkpoint the Kafka actor reports, per partition, the committed position and the end offset (`partition_lag` LOG, positions only); the final report of a run re-reads end offsets, so records produced during the run count. The agent forwards it with the `lag` action under the task's lease, and `connect_record_lag` keeps the latest observation per partition in `connect_partition_lag` (lag = end offset − position, never negative). A partition task may report only its own partition. Owners read lag under RLS; the Connectors page shows total and worst-partition lag per stream. Lag is a metric, not a checkpoint: it is never part of a commit (a retried commit must reproduce its checkpoint exactly), and a failed report never fails a run. It is as fresh as the last run: no consumer polls between runs.

**Backpressure (Phase 3 K3).** The Kafka actor streams records straight to the destination and holds none of them; it takes a checkpoint every `max_batch_records` records (default 500) or `max_batch_bytes` of record payload (default 4 MiB), whichever comes first. librdkafka's prefetch queue is capped at `max_prefetch_kbytes` (default 16 MiB, for the whole consumer). When the destination is slow, the agent stops reading the actor's output, the actor's pipe fills and it stops polling, so nothing piles up in memory. `max_run_records` (default unlimited) and `max_run_seconds` (default 600) end a run at the position it reached, logged as `run_limit_reached`; the next run continues from there, so a large pending records drains over several runs exactly once (lag shows what is left). Every limit has a floor and a ceiling, so a config cannot switch them off.

**Managed Kafka profiles (Phase 3 K4).** `profile` fills in what a managed service needs and refuses settings that contradict or weaken it (TLS with hostname verification is always on):

- `confluent_cloud`: `api_key` + `api_secret` (SASL_SSL PLAIN).
- `azure_event_hubs`: `connection_string` (SASL_SSL PLAIN with the user name `$ConnectionString`; port 9093).
- `aws_msk_scram`: SCRAM-SHA-512 user and password.
- `aws_msk_iam`: `aws_region`, plus nothing (AWS default credential chain: environment, IRSA/web identity, container or instance role), `aws_role_arn` (assumed), or `aws_access_key_id` + `aws_secret_access_key` (+ `aws_session_token`) from the private config. Tokens are short-lived SigV4 signatures over SASL OAUTHBEARER from AWS's `aws-msk-iam-sasl-signer-python`; OAUTHBEARER cannot be selected without this profile.
- `redpanda_cloud`: SCRAM-SHA-256 (default) or -512.
- `aiven`: client certificates (SSL) or SCRAM over SASL_SSL.

Without a CA setting, TLS uses the system trust store (`probe` on Linux, the Windows store on Windows), which covers the public CAs of these services. Endpoints that do not implement DescribeTopics (Event Hubs) read without a topic id, as for principals without Describe. The SCRAM, PLAIN and mTLS profiles are certified against the local secured broker (`test_kafka_profiles.py`); the MSK IAM token is checked offline (a SigV4 presigned `kafka-cluster:Connect` request). No run against the managed services themselves has been made.

Not yet certified: runs against Confluent Cloud, MSK (IAM or SCRAM), Event Hubs, Redpanda Cloud or Aiven, broker ACL authorization, CDC for PostgreSQL/MySQL, MySQL through the control-plane e2e, registry mode changes, a production Kafka deployment.

## Protocol decision / license boundary

Implement an original STDIO subset shaped like Airbyte SPEC, CONNECTION_STATUS/CHECK, CATALOG/DISCOVER, RECORD, STATE, LOG and TRACE; destination WRITE acknowledges STATE only after commit. `sdk.py` and `src/lib/connect-protocol.ts` are authoritative for Meridian v1, not a claim of full Airbyte runtime/socket/state compatibility. No Airbyte CDK/connector code or package copied. Airbyte's current protocol is MIT; current broader code defaults to ELv2, whose managed-service restrictions require version-specific legal review before adopting implementations:

- https://github.com/airbytehq/airbyte/blob/master/docs/community/licenses/README.md
- https://github.com/airbytehq/airbyte/blob/master/docs/platform/understanding-airbyte/airbyte-protocol.md
  Dependencies: Python image, psycopg 3.2.10 (LGPL library; binary package includes libpq/OpenSSL notices); retain release notices and scan transitive artifacts. Image build/signing/license certification remains unexecuted.

## Reproduce the slice

1. In existing Ingestion create a PostgreSQL source; retain its one-time source token outside Git.
2. Connectors → Meridian Connect → select that source → register name and credential reference `postgres`.
3. Create a private directory with `ingest_token` containing the source token and `postgres.json` containing actual psycopg connection settings (`host`, `port`, `dbname`, `user`, `password`, `sslmode`). Use TLS for remote databases. Directory/files must be readable by container UID 65532, not world-readable. Never paste these into ordinary connection configuration or logs.
4. Supply `MERIDIAN_URL`, `CONNECT_SECRET_DIR`, `POSTGRES_PASSWORD_FILE`. Run `docker compose -f deploy/connect/compose.yml --profile replicate up --build`. The development agent executes source children inside the isolated agent container. Source and destination Dockerfiles are separately runnable through `run-isolated.sh`; production per-actor pod dispatcher is still missing.
5. Queue Check credentials, then Discover streams. Agent polls outbound every 10 seconds; no inbound VPC port required. Select a discovered table with a primary key, non-null monotonic cursor, mode, Save stream, Sync now.
6. Observe run rows/bytes/attempts and committed batches/checkpoints. Incremental next sync starts from durable `(cursor, primary key)`; full refresh/reset reads a new snapshot from beginning, retaining raw history (not overwrite).
7. Restart the agent during sync. A 90-second lease expires, next claim has new attempt token and resumes last committed checkpoint. Destination retries exact batch sequence; old token cannot commit/finish. Three attempts then terminal failed; retry delay exponential+jitter. UI Cancel fences writes immediately, source process may finish current blocking query before stopping.
8. Compatible customer fields (`id`, `source`, `name`, `email`, `phone`, `city`) can use committed batch ID in downstream Source systems. Envelopes unwrap only after Connect commit exists; downstream consumers still apply their own approval. No automatic mapping.

Local source-only harness: `python3 -m pip install -r deploy/connect/requirements.txt`; against isolated PostgreSQL on 55439 run `PYTHONPATH=deploy/connect python3 deploy/connect/test_postgres.py` (it creates/drops `connect_test`, never use production). Destination contract harness `PYTHONPATH=deploy/connect python3 deploy/connect/test_destination.py` mocks only transport loss, not database execution.
Application live harness: `bunx vitest run src/test/connect-live.e2e.test.ts` requires running dev app, backend test environment and isolated PostgreSQL fixture `public.connect_acceptance` with 1200 rows; fixture schema in the test evidence. Live suite retains immutable bronze evidence and deletes temporary identities.

## Parallel streams (tasks)

The agent claims work per **task**: one stream of a run at a time, each with its own 90-second lease (migration 0094). Several agents, or one agent with `CONNECT_SLOTS=N` (default 1, up to 64), work on the streams of one run in parallel; every commit, heartbeat and finish is fenced by the task's own lease, so a task token can never act on another stream. A retryable failure retries only that task, from its last checkpoint and with backoff; a terminal one fails the run and fences its other tasks. `CONNECT_WORKER_ID` names the agent in task records (default: host name and process id). `CONNECT_TASKS=0` makes the agent claim whole runs, as agents did before 0094. An older control plane without task claims answers 400, and the agent falls back to whole runs on its own. A run is leased either whole or per task, decided at its first claim, and check/discover runs are always leased whole. A source token serves its one connection; a worker pool token serves many (below).

**Kafka partitions (migration 0095).** A Kafka stream in a task-leased sync, reset or backfill is split into one task per topic partition. The first task of the stream plans: the agent runs the actor's `plan` command, which lists the topic's partitions, and calls `split`; the control plane replaces that task with one queued task per partition, each starting from the stream checkpoint restricted to its own partition (identity and any accepted gap approval kept). Partition tasks are claimed, leased, retried and failed like other tasks. Each commit may carry only its own partition's offset and merges it into the stream state without touching the other partitions. Commits keep one stream-wide sequence (assigned by the control plane), so Bronze readers page a split stream as before; a task's own sequence is `shard_sequence`. A stream is never split once its planning task has committed, replay runs are never split, and run-leased (pre-task) agents read the whole topic as before. Partitions added to a topic after the split join at the next run.

## Worker pools (M2)

A **worker pool** (migration 0108) lets several agents share the work of many connections in one workspace. Create a pool in Connect (Worker pools); its token (`mp_…`) is shown once and stored only as a SHA-256 hash. Assign connections to the pool in each connection's Settings, then start any number of agents with `INGEST_TOKEN_FILE` holding the pool token and `CONNECT_SLOTS` set to the tasks each may run at once.

- **Claiming:** a pool agent's claim tries the pool's connections least recently served first, so one large backfill cannot starve the others. A connection another agent is claiming from at that moment is skipped (`FOR UPDATE SKIP LOCKED`), so agents never wait on one another. Each try is the connection's own task claim, with every check it makes; the claim names its connection, and every later call (heartbeat, commit, finish) must be for a run of a connection still in the pool.
- **Workspace boundary:** a pool serves connections of its own workspace only: the same organization, or, for personal connections, the same owner. The database refuses any other assignment, and a pool token never claims, heartbeats or commits for another workspace. Disabling a pool or issuing a new token stops agents holding the old token at once.
- **Credentials:** pool agents read `CONNECT_SECRET_DIR/<credential_ref>.json` like any agent, so every host in a pool needs the credential files of all the pool's connections, and `credential_ref` must be unique within a pool (enforced). The assignment is the trust boundary: assign a connection only to a pool whose hosts may hold its credentials.
- **Limits:** a connection's _parallel tasks limit_ (`max_parallel_tasks`, 1–64; empty = no limit) caps its running tasks across all agents, pool or source token, to protect a source database.
- **Workers:** agents report their id, slot count and version with each claim; the pool list shows agents seen in the last two minutes and their slots.
- A connection's own source token keeps working whether or not the connection is in a pool.

## Change data capture (CDC-1 PostgreSQL, CDC-2 MySQL)

A `postgres-cdc-v1` connection reads PostgreSQL logical replication (`source_postgres_cdc.py`). Its streams use sync mode `cdc` (migration 0105; a trigger keeps `cdc` streams on CDC connections and the reverse).

- **Source setup:** `wal_level=logical`, a role with `REPLICATION` (or `rds_replication`), and a publication you create that lists the tables: `CREATE PUBLICATION meridian FOR TABLE public.orders, ...;`. The credential file names it as `publication` (default `meridian`). `check` names whichever is missing (`cdc_wal_level`, `cdc_replication_privilege`, `cdc_publication_missing`).
- **First run:** the actor creates the stream's slot (`meridian_<hash of the stream id>`), then copies the table by primary key as `upsert` records. Changes made during the copy are read from the slot afterwards; replaying them in commit order converges on the same rows.
- **Later runs:** the actor moves the slot to the checkpoint (the end LSN of the last landed transaction), then reads `pgoutput` with `pg_logical_slot_peek_binary_changes` up to the WAL position at the start of the run (at most 100,000 changes per run). Inserts and updates land as `upsert` with the whole row, deletes as `delete` with the key (the whole old row with `REPLICA IDENTITY FULL`), and a changed key as a delete of the old key and an upsert of the new one. Values are decoded with psycopg's own type loaders, so a changed row lands with the same `data` as the copy.
- **Exactly-once landing:** the slot is only peeked, never consumed, and moves only to a landed checkpoint, so an attempt that dies before committing loses nothing. Checkpoints fall on transaction boundaries about every 500 records; a crash inside a larger transaction re-lands part of it (at-least-once, like segmented commits elsewhere).
- **Fails closed:** a column change (`schema_drift`), an update that sends an unchanged TOAST value when the table lacks `REPLICA IDENTITY FULL` (`schema_drift`, log `cdc_unchanged_value` with the columns), `TRUNCATE` (`source_changed`, log `cdc_truncate`) and a missing slot (`offset_out_of_range`, log `cdc_slot_missing`). A reset drops the slot, starts a new one and copies the table again.
- **Source safety:** a slot keeps WAL until it moves, so a paused or stopped stream holds WAL on the source. After each run the actor reports how many bytes of log the landed position is behind (`connect_partition_lag`, partition 0), and the stream view shows it with the slot name. Set `max_slot_wal_keep_size` as a backstop. Use Retire on the stream to fence future ingestion and queue worker-local slot cleanup; keep its connection and worker enabled until cleanup reports done. Retirement preserves Bronze history. A reset drops and recreates the slot.
- **In the app:** the new-connection wizard offers "Read changes from the database log (CDC)" for PostgreSQL (with the publication name) and MySQL; streams of a CDC connection are saved in mode `cdc`, labelled "Change data capture", and their lag is shown in bytes.

**MySQL (`mysql-cdc-v1`, `source_mysql_cdc.py`)** reads the binlog as a replica with `python-mysql-replication` (Apache-2.0).

- **Source setup:** `log_bin=ON`, `binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_metadata=FULL` (column names in the binlog; MySQL 8.0.14+), no `PARTIAL_JSON` in `binlog_row_value_options`, and `REPLICATION SLAVE` plus `REPLICATION CLIENT`. `check` names the settings that differ (`cdc_binlog_settings`).
- **Positions:** the first run takes the binlog file and offset, then copies the table; later runs read from the binlog file and offset after the last landed transaction, up to the log end captured at the start (never stopping inside a transaction). Each stream reads as its own replica `server_id` (derived from the stream, 2^30 and up). MySQL keeps no state for it, so binlog retention decides how long a stream may pause.
- **Records:** inserts and updates as `upsert`, deletes as `delete` with the whole old row, a changed key as a delete and an upsert. Values go through the MySQL actor's rules (`tinyint(1)` boolean, JSON, UTC `TIMESTAMP`), so a changed row lands with the same `data` as the copy.
- **Fails closed:** `TRUNCATE` (`source_changed`, `cdc_truncate`), `ALTER`/`DROP`/`RENAME` of the table or a column mismatch (`schema_drift`), and a binlog file purged before it was read (`offset_out_of_range`, `cdc_binlog_purged`). A reset copies the table again from a new position.

Live tests: `src/test/connect-cdc-postgres.e2e.test.ts` (needs `wal_level=logical` on the fixture database) and `src/test/connect-cdc-mysql.e2e.test.ts` (the MySQL test source; the test switches `binlog_row_metadata` and restores it). Offline: `deploy/connect/test_cdc.py`.

## Spark ingestion (S1–S6, direct writes)

A connection with `source_version: spark-postgres-v1`, `spark-mysql-v1`, `spark-files-v1` or `spark-kafka-v1` checks and discovers with that source's actor, and runs sync, reset and backfill of a full-refresh or incremental stream as a Spark application:

- The agent claims the task and keeps its lease, then starts `CONNECT_SPARK_SUBMIT`, for example `sh /opt/meridian/connect/spark-submit.sh --master spark://host:7077`. The claim, the source config and the ingest token go on stdin; `spark-submit.sh` hands them to the driver as a private file that the driver deletes on read. A lost lease or the run's deadline kills the application.
- The driver exports one PostgreSQL snapshot and runs one Spark job per hash partition of the primary key (`CONNECT_SPARK_OPTIONS` `{"partitions": N}`, default 8). Executors read their partition in that snapshot, encode rows exactly as the PostgreSQL actor does, and upload Bronze parts (at most 8 MiB / 10,000 rows) **straight to storage** through signed upload URLs (`upload_url`): one per part, never overwriting, bound to the attempt's lease token, and never issued for a part that is already registered.
- The driver commits each part with `commit_file`. The control plane re-downloads the part and checks its size, SHA-256, line count and every envelope (protocol, connection, run, stream, schema, extraction time, operation) before registering it and making the usual fenced `connect_commit`. The checkpoint keeps, per partition, the last committed key: a new attempt resumes each partition by key (`consistency: "mixed"`, since the first snapshot is gone), and the first failure stops all further commits.
- **Incremental streams** read a fixed window: after the stream's committed `(cursor, key)` position, up to the highest `(cursor, key)` when the run starts. The window is stored in the checkpoint, so a resumed attempt reads the same window, and rows changed after it was fixed wait for the next run. Completion moves the stream to the window's end: the position a single-worker run reading the same rows reaches.
- **MySQL** cannot share a snapshot across connections: each partition reads in its own consistent snapshot (`consistency: "partition"`), which is still more consistent than the single-worker actor's keyset batches. PostgreSQL partitions share the driver's exported snapshot (`"single"`).
- **Files** (`files-v1` actor, `spark-files-v1` on Spark): one prefix of an S3-compatible bucket (credential file: `endpoint`, `region`, `bucket`, `prefix`, `format` csv/tsv/jsonl/parquet, `access_key_id`, `secret_access_key`, optional `session_token`, `csv_quoted_newlines`, `range_bytes`). Spark reads one task per record-aligned byte range (CSV/TSV/JSONL) or row-group range (Parquet); key `(_file, _position)`, cursor `_modified`. The plan is kept in the checkpoint, and a range whose object changed fails the run with `source_changed`.
- **Kafka** (`spark-kafka-v1`): one Spark application reads every partition from its committed position up to the high watermarks at the start of the run, in offset chunks (`CONNECT_SPARK_OPTIONS` `chunk_records`, default 100,000). Identity, positions and retention gaps are checked by the actor's own code; positions advance only across contiguous finished chunks; dead letters ride in `commit_file`. Certificate files travel inline as PEM (no PKCS#12 keystores on Spark).
- Manifests record `produced_by: {engine: "spark", part, executor}`.
- **Secured Spark (S5).** Source credentials, the ingest token and the lease token travel to executors in task closures over Spark RPC, and record data comes back the same way. Before any task leaves it, the driver checks that `spark.authenticate`, `spark.network.crypto.enabled` and `spark.io.encryption.enabled` are all `true`; otherwise it logs `{"level":"ERROR","code":"spark_insecure","settings":[...]}` (the agent passes it on) and the run fails as `connector_failed` without reading anything. `CONNECT_SPARK_OPTIONS` `{"allow_insecure_spark": true}` turns this into a `spark_insecure_allowed` warning, for throwaway local clusters only. A standalone cluster has one shared secret: set it on every node and for every application in a `spark-defaults.conf` readable only by the Spark user (`spark.authenticate.secret`), never on a command line. `deploy/connect/spark/secure-conf.sh` does this from a secret file.

Local cluster: `docker compose -p meridian-spark -f deploy/connect/spark/compose.yml up -d --build --wait` (master + two single-core workers, pinned Spark 3.5.3 Java 17 / Python 3.10 image plus psycopg, PyMySQL, pyarrow 25, confluent-kafka and fastavro, `deploy/connect` mounted). Live tests: `src/test/connect-spark.e2e.test.ts` (S1) and `src/test/connect-spark-s2.e2e.test.ts` (S2, also needs `deploy/ingest/test-sources.compose.yml`) `src/test/connect-spark-s3.e2e.test.ts` (S3, the local Storage S3 protocol) and `src/test/connect-spark-s4.e2e.test.ts` (S4, `deploy/connect/certify.compose.yml`; the Spark containers join its network and the submit command pins `spark.driver.host`); `deploy/connect/test_files.py` covers range splitting offline. Containers reach the host's app, storage and database through Docker Desktop's host gateway; `CONNECT_SPARK_MERIDIAN_URL`, `CONNECT_SPARK_CONFIG_OVERRIDES` and the `rewrite` option exist only for that local setup.

The local cluster is secured: a one-shot `spark-secret` service creates a random secret in a private volume, and every node starts through `secure-conf.sh`. An application without the secret is refused at registration (`src/test/connect-spark-s5.e2e.test.ts`).

**Spark on Kubernetes (S5b).** `deploy/connect/spark/k8s` runs Spark in client mode from a long-running driver pod; the agent's submit command is `kubectl exec -i deploy/spark-driver -n meridian-spark -- sh /opt/meridian/connect/spark-submit.sh`, and each run's driver creates its own executor pods (owned by the driver pod, removed when the run ends). The driver pod's `spark-defaults.conf` (a ConfigMap, no secrets) sets the master, image and limits.

- **Resource limits:** per run, 2 executors of 1 CPU (500m requested) and 1 GiB (640m heap + 384m overhead). The namespace caps all applications together (ResourceQuota: 8 pods, 4 CPU / 6 GiB requested, 6 CPU / 6 GiB limits), and a LimitRange gives every container defaults and a 2 CPU / 2 GiB maximum.
- **Isolation:** the namespace enforces the `restricted` Pod Security profile (non-root, no privilege escalation, all capabilities dropped, RuntimeDefault seccomp). Executors also have a read-only root filesystem and no Kubernetes API token; the driver's service account may only manage pods, config maps and services in its namespace. A NetworkPolicy lets only Spark pods reach Spark pods, and blocks egress to the cloud metadata address. The Spark UI is off.
- **Secret:** `up.sh` creates the `spark-auth` Secret once from random bytes. The driver reads it from a mounted file; executors get its base64 form (what Spark uses) through a `secretKeyRef` on `_SPARK_AUTH_SECRET`. Spark's own Kubernetes default would put the secret's value in the executor pod spec, readable by anyone who can list pods; the `spark.authenticate.secret.executor.file` setting stops that.
- **Local cluster:** `kind create cluster --config deploy/connect/spark/k8s/kind.yaml` then `sh deploy/connect/spark/k8s/up.sh` (builds the image with Meridian's Spark modules baked in, since executor pods have no host mounts). Live tests: set `CONNECT_SPARK_K8S=kind-meridian-spark` and run the Spark suites; `src/test/connect-spark-k8s.e2e.test.ts` checks the executor pod specs during a run, the namespace's admission rules and the network policy. S4 (Kafka) stays standalone-only, because its fixture joins Docker networks.
- **Limits of the local evidence:** kind on one machine, images loaded with `kind load`. A hosted cluster brings its own registry, node pools and CNI; the NetworkPolicy needs a CNI that enforces it (kindnet does).

**Parquet Bronze parts (S6).** `CONNECT_SPARK_OPTIONS` `{"bronze_format": "parquet"}` makes a Spark run write its Bronze parts as Parquet instead of NDJSON (default `ndjson`; agent-pushed batches stay NDJSON). A part holds the same `meridian-connect/1` envelopes in layout `meridian-bronze-parquet/1` (`bronze_parquet.py`):

- The fields shared by the whole part (protocol, connection, run, stream, schema, extraction time, source namespace and stream) are stored once, in the file's `meridian.bronze` key-value metadata, with the kind of every data column.
- `_meridian_operation`, `_meridian_primary_key` and `_meridian_cursor` carry each record's envelope fields; every `data` field is its own column.
- Column types come from the stream schema's `source_type`, so every part of a stream has the same Parquet schema: booleans (`boolean`, `tinyint(1)`), integers that always fit a JSON number (`smallint`, `integer`, MySQL `int` and smaller) and types the actors always send as text (strings, decimals, dates and times, UUIDs, bytes) are typed columns. Everything else (JSON, arrays, `bigint`, floats, unknown types) is JSON text, so no value changes.
- A part that can't be laid out this way falls back to NDJSON for that part. That happens when records have different fields, a field is named `_meridian_*`, or a value doesn't fit its column (a `bigint` past 2^53 travels as text).
- Before registering a part, the server decodes it and checks every envelope exactly as for NDJSON (`src/lib/bronze-parquet.ts`). The object key ends in `landing.parquet`, and the dataset version records format `parquet` (migration 0106).
- Readers accept both formats: `read_bronze.py` (downloads `.parquet` files, checks their run and stream, and counts over both formats with `--query-count`) and the test harness. Parts are still at most 8 MiB and 10,000 records, measured on the NDJSON size.

**Avro and ADLS files.** `format: avro` reads Avro object-container files using the
existing fastavro dependency, split by container blocks with stable global row
positions. It shares standalone full-refresh/incremental parsing and Spark
full-refresh/incremental execution. For ADLS use `provider: adls`, `account_name`, and `bucket`
as the container, with worker-local `account_key`, `sas_token`, or service-principal
`tenant_id`, `client_id`, `client_secret`. Credentials never enter the checkpoint.
Saved Spark plans now bind source location/provider/account before reuse. Legacy
ADLS plans without a binding require reset. Live Azure authentication/list/read remains unverified without a test account.
Distributed Avro and incremental S3 checks passed locally in `connect-spark-s3.e2e.test.ts`;
offline ADLS tests use local files and controlled SDK wiring.

**Kafka group membership.** Set worker-local `consumer_group: true` and `group_id`
on `kafka-v1`. The agent uses one unsplit whole-topic task with real subscription,
assigning from authoritative Postgres offsets; broker offset storage/commits stay
disabled. Only a complete initial topic assignment is supported. Revoke/lost,
partial or repeated assignment stops the attempt before another checkpoint; retry
loads committed positions. This first mode supports one full-topic member, not
multi-member partition sharing. Changing mode on an already-split run requires
cancelling it and starting a new run. Spark group mode is refused. Local three-broker tests verify assignment loss, partial assignment, replay and
broker-offset override. `connect-kafka-groups.e2e.test.ts` exercises actual Bronze
commits and restart. Slow-destination/max-poll and managed-broker checks remain pending.

**Retire streams.** Streams now offer retirement and retry source cleanup. Stop
active runs first and wait 90 seconds after the last worker run for its watchdog
to stop source work. The stream is disabled and generation-fenced, preserving all
Bronze history. PostgreSQL CDC queues worker-local cleanup of its deterministic
pgoutput slot; missing slots are success, busy slots retry. Updated CDC actors use
source-side advisory exclusion for reads/cleanup. Upgrade all workers before using
retirement. Keep the connection/source/worker enabled until cleanup reports done.
MySQL has no persistent replication slot, so retirement needs no source cleanup.
Retired stream names cannot be reconfigured or resumed; use a new connection if
you need to ingest the same name afresh. Migration 0109 adds the owner RPC and guard.

**Durable schedules.** Migration 0110 moves due-run creation out of agent polling.
Configure a trusted external cron to POST `/api/public/connect-tick` with bearer
`CRON_SECRET`. Scheduling
works without online workers, coalesces missed intervals, snapshots enabled streams
and skips connections with active runs. Workers claim persisted runs as before.
An installation without a configured cron can still use manual Sync; agent polling
alone no longer triggers schedules. Local database concurrency/workspace checks passed, including concurrent ticks,
manual enqueue races and suspended workspace suppression. These are local checks;
external cron deployment remains an installation step.

Not yet: the Spark Operator (S5b uses plain client mode). None of the new offline evidence is hosted
certification.

### Shared Kafka groups across agents

Set `CONNECT_KAFKA_SHARED=1` on each agent for one `kafka-v1` connection. Its local
credential file must set `consumer_group: true` and the same `group_id` on every
member. Use a dedicated group for that connection; unrelated consumers can hold
partitions that Meridian then waits for. Connection tokens select their own
connection. Pool tokens additionally require `CONNECT_GROUP_CONNECTION_ID` naming
a connection assigned to the pool. `CONNECT_SLOTS` bounds active partition actors
per agent; existing pool quotas and worker heartbeats still apply.

One membership consumer joins all configured topics, immediately pauses assigned
partitions and polls on its own thread while actors read and land data. Only its
assigned partitions can be leased. Each actor reads explicit Postgres positions
with broker membership disabled. Neither consumer commits broker offsets, and
preexisting broker commits do not replace landed Bronze checkpoints. Kafka must
permit group membership and topic metadata/read access. The assignment selector
is capped at 1024 partitions per stream and 48 KiB serialized JSON.

Revocation cancels affected actors and fences/requeues their task leases. A commit
that wins before fencing remains authoritative; a later stale commit is rejected.
Rebalance handoffs preserve the source-failure retry budget. After process death,
the survivor waits for broker reassignment and the existing 90-second database
lease to expire before continuing from landed positions. `CONNECT_ONCE=1` waits
for the whole active run, including work held by peers, before closing membership.

Start sharing on a new run. Cancel an in-flight legacy whole-topic group run
before switching modes; it cannot be converted after it has started reading.
All agents serving this connection must use sharing consistently. Spark group
sharing is unsupported. Existing single-agent whole-topic group mode continues
when `CONNECT_KAFKA_SHARED` is unset. Worker tests are in `test_kafka_shared.py`,
scoping/fencing tests in `connect-shared-kafka-*.test.ts`, and actual two-agent
tests in `connect-kafka-shared.e2e.test.ts`; see the dated plan for executed results.

### Avro registry schema evolution on Kafka and Spark

Both `kafka-v1` (including shared groups) and `spark-kafka-v1` use the same Avro
decoder. Each Confluent-wire record identifies its immutable writer schema ID.
Automatic mode reads each writer's shape, including new fields as producers evolve
schemas. Referenced Avro records/enums/fixed types resolve through the registry's
exact subject/version links, including nested references. Cycles, mismatched names
and conflicting definitions fail safely instead of silently changing decoded data.
The registry must implement Confluent-compatible Avro reference endpoints; the
local Karapace 5 fixture rejects Avro references, while the optional Apicurio fixture
in `certify-registry.compose.yml` exercises that path.

In Connect's stream picker, select a registered Avro reader subject to normalize
older/newer writers to one shape. Saving pins the latest version visible to the
caller; it does not silently follow later subject changes. Reader defaults,
aliases and Avro-compatible type promotions apply to both execution paths.
Changing only the reader preserves committed offsets and does not re-read old
records. Stop active runs before changing a reader; source/key/cursor/mode changes
retain their existing checkpoint-reset behavior.
Automatic writer schemas remain the default. The reader subject is Meridian's
caller-scoped registered schema; external writer schemas and credentials stay
on the worker. Producer registration/compatibility policy stays with the external
registry and producer, and ingestion never auto-approves schema changes.

Incompatible records become `schema_incompatible` dead letters under the existing
budget, fenced checkpoint and replay rules. Missing writer/reference schemas are
replayable `schema_not_found` records. Registry outages, authentication failures
and TLS failures stop extraction instead of acknowledging records as poison.
Registry redirects are refused, and credentials require HTTPS.
After correcting the reader, selected dead letters can be replayed without moving
live partition positions. Spark Kafka reuses the bounded Kafka actor for replay
and the same fenced commit protocol; replay does not submit a Spark application.

Reference resolution is bounded to depth 32, 128 referenced documents and 4 MiB of
schema text per writer graph. HTTP responses are capped at 4 MiB; the writer cache
holds at most 64 IDs/8 MiB of source schema text. Eviction refetches immutable IDs.
Offline tests: `test_kafka_registry.py`. Actual multi-executor evolution, reader
pinning, retry and incompatible-record checks: `connect-spark-avro.e2e.test.ts`.
Locally verified on 2026-11-22: real reference-aware registry actor/chunk test and
all four two-executor Spark/Bronze tests passed, including replay after a reader
fix without moving live offsets. Managed registries and production HA remain
outside this local evidence.

### Incremental object-store files (single worker)

Configure a `files-v1` stream with sync mode `incremental`, discovered key
`[_file, _position]` and cursor `_modified`. `spark-files-v1` also supports incremental
streams with the distributed range ledger described below. Incremental files use content identity rather than a modification-time
watermark, so a new object whose key sorts before earlier objects is still read.

- The worker copies each listed object to a private temporary disk file while
  computing SHA-256 in bounded chunks, then parses that snapshot. Size and mtime
  are checked against the listing after the copy. Parsed bytes match the digest;
  this is not a provider-atomic snapshot of a concurrently changing object.
  Producers must publish immutable objects.
- A versioned `files` checkpoint stores a hash of the source location, parsing
  settings and schema, plus each key's digest, size, original modification time,
  last committed row position and completion flag. Source credentials stay in
  the worker's credential file and are excluded from the binding and checkpoint.
- An unchanged completed object emits no new Bronze rows. An unfinished object
  with the same digest resumes after its last committed row. Each incremental
  RECORD is immediately followed by a STATE so it lands atomically with its
  position, even at the destination's byte/count boundaries. Existing identical
  sequence retries handle response loss, and existing leases fence stale workers.
- Different bytes at a known key fail with `source_changed`, including same-size
  replacements with preserved mtimes. Replacements do not overwrite Bronze or
  emit implicit deletes. Missing completed objects keep their ledger entries and
  Bronze history; missing unfinished objects fail closed. Reappearing identical
  objects are skipped; reappearing replacements fail.
- Reset/backfill explicitly re-reads all currently present objects and builds a
  fresh ledger; it does not remove historical Bronze. Reset is also required when
  switching from a legacy full-refresh checkpoint or changing the source binding.
- The ledger is capped at 48 KiB serialized JSON. Exceeding it fails with
  `record_too_large` before rows for the unrepresentable object are emitted;
  a checkpoint never silently drops identities. This first slice is for small
  object sets: it hashes all listed objects on every run, needs temporary disk
  space for the largest object and commits once per row. A scalable object ledger
  and batched row checkpoints remain follow-up work.

Offline verification lives in `deploy/connect/test_files.py`, `test_destination.py`
and `test_agent.py`, plus the control-plane protocol tests. Live S3 and database recovery checks are in `connect-spark-s3.e2e.test.ts`,
`connect-retirement-agent.e2e.test.ts` and the task-lease regression suites.
See the dated implementation plan for executed results and remaining gaps.

### Incremental object-store files on Spark

Use `spark-files-v1` with sync mode `incremental`. New immutable objects are split
through the same CSV/TSV/JSONL byte, Parquet row-group and Avro block planner as
full refresh. Quoted-newline CSV/TSV remains one range per file. The driver hashes
bounded disk snapshots, and each executor verifies the expected digest against its
own disk snapshot before parsing or uploading. Source credentials remain local to
the worker/secured Spark task transport; they never enter checkpoints.

Version-2 `files` state stores completed object identities and only unfinished
ranges with their committed row offsets. Every accepted Bronze part advances that
ledger under the existing task lease. Retried attempts and new runs after a partial
cancel retain exact unfinished boundaries, including ranges committed out of order.
Unchanged objects produce no rows; late keys are read. Missing completed objects
are remembered; missing unfinished objects and changed known bytes fail closed.
Reset/backfill rebuild the configured mode. Entering Spark incremental from a
standalone version-1 ledger or a legacy full-refresh checkpoint requires an
explicit reset. Configured full refresh continues to reload current objects each
run, including the first sync after reset/backfill.

This first implementation hashes every listed object each sync and snapshots a
whole object on each range's executor. Temporary disk must fit that object; memory
is bounded by copy/parse/upload budgets. Entire serialized incremental state is
capped at 48 KiB, with at most 4096 unfinished ranges; exceeding either limit fails
rather than forgetting identities. A scalable external object ledger and shared
executor snapshots remain follow-up work. This is append-only Bronze ingestion,
not file replacement/upsert/delete application.

Offline recovery tests: `test_spark_files_incremental.py`; concurrent publication
failure regression: `test_spark_commit.py`. The secured two-worker S3 demonstration
is `src/test/connect-spark-incremental.e2e.test.ts`. Dated executed evidence is in
the implementation plan; runnable or skipped tests are not live certification.

## Guarantees / gaps

At-least-once extraction; idempotent committed run/stream/sequence. Canonical extraction timestamp currently equals run start for deterministic retry bytes. Raw operations labelled upsert describe source rows, not applied upserts: destination is append-only bronze. No CDC/deletes, late cursor regressions, snapshot consistency across restarts or mutable full-refresh exactly-once guarantee. Requires a monotonic non-null scalar cursor and a primary key (composite allowed). Schema drift fails, no quarantine/auto-evolve. Large values remain raw (bigint/decimal/time representations documented by source encoder). Source can change between checkpoint/commit and retry; conflicting bytes fail closed. Native Iceberg/Delta materialization, query-engine governance bridge, retention/deletion, alerts, searchable payload-free log drilldown, metrics/lag and tenant-level pools remain unfinished.

## Release/runtime isolation

Compose caps CPU/memory, drops caps, read-only FS and secrets mounts; no Docker daemon access. OCI source/destination images are not built/digest-pinned here. `release-check.sh` refuses mutable tags and requires cosign, trivy and syft; `image-lock.example.json` deliberately has null digests. `worker-job.yaml` is a non-deployable UNBUILT example requiring real digest, secrets and network policies. Namespace/pod-per-tenant scheduling, actual NetworkPolicy allowlists, signing/SBOM verification, CI certification and upgrade/rollback tests are release blockers, not delivered safety claims.

Roadmap: isolated per-actor dispatcher and immutable image certification → CDC (logical replication, binlog) applied in Silver → consumer-group scaling and backpressure → S3/ADLS/GCS file manifests → SQL Server/Oracle/MongoDB/REST, each with separate evidence.
