# Partitioned ingestion — self-host kit

The in-app planner (Partitioned ingest page) runs the demo source end to end inside Meridian.
For your own systems, two runners use the **same partition predicates** (`src/lib/partitioning.ts`):

| Runner | Sources | Writes to |
|---|---|---|
| `partitioned_worker.mjs` (Node 20) | PostgreSQL, MySQL | Meridian (`/api/public/ingest-partition`) |
| `partitioned_ingest.py` (Spark 3.5) | PostgreSQL, MySQL (JDBC), Kafka, S3, ADLS, REST | Apache Iceberg or Delta Lake (MERGE, atomic commit) |

## Guarantees and how they are met
- **No full-table loads**: each worker reads one partition with keyset pagination (`WHERE pk > :last ORDER BY pk LIMIT :batch`), never OFFSET.
- **Consistent snapshot**: Postgres workers share one exported snapshot (`pg_export_snapshot()` + `SET TRANSACTION SNAPSHOT`); MySQL uses `START TRANSACTION WITH CONSISTENT SNAPSHOT` per worker (consistent per partition only).
- **After a restart (contract, not point-in-time)**: the exported snapshot does not survive the worker. A restarted worker exports a new one and each partition resumes by key after its checkpoint, so rows at or before a checkpoint keep the earlier values (later updates are not re-read), rows after it come from the new snapshot, and deletes are never captured. The run's `snapshot` is then marked `"consistency": "mixed"`. For an exact copy, cancel the run and plan a new one; change capture is not part of this worker.
- **Connection limits**: `--max-conn` caps the pool; workers back off (AIMD) on errors or slow batches.
- **Idempotent retries**: checkpoints are stored per partition; rows upsert on `(run_id, pk)` (Meridian) or `MERGE ... ON pk` (Iceberg/Delta).
- **Atomic publish**: Meridian swaps the table pointer only when every partition is `done`; Iceberg/Delta commit one snapshot per run.
- **Kafka**: logical partitions = topic partitions; offsets are the checkpoint (`startingOffsets`/`endingOffsets` JSON per partition). Records that are not JSON objects (malformed JSON, null values, JSON scalars/arrays) follow `--poison`: `quarantine` (default) stores them in Meridian's dead-letter store (`ingest_dead_letters`, raw bytes in `ingest-files`) before the checkpoint moves; `skip` logs and counts them; `fail` fails the partition without moving its checkpoint. A bad record never blocks a partition.
- **Files (S3/ADLS)**: files are bin-packed by size into partitions. JSON-lines objects are read with byte ranges (`--s3-chunk-bytes`, default 4 MiB; a longer record doubles the range), never downloaded whole; the checkpoint is `{ file, byte, line }` (older `{ file, row }` checkpoints resume by skipping lines). Run creation in the app does not accept file partitions yet, so S3 runs are not plannable from the UI.

## Push workers (Ingestion page)
The Ingestion page generates `worker.mjs` for PostgreSQL, MySQL and Kafka/Debezium (`workerScript` in
`src/lib/ingest.ts`). It pushes batches to `/api/public/ingest` with the source token. Delivery is
at-least-once: the database cursor (saved to `STATE_FILE`) and Kafka offsets advance only after a batch
lands, and each batch keeps one id across retries. `ONCE=1` runs a single database sync; `IDLE_EXIT_MS`
stops the Kafka worker once idle. The Kafka worker skips only tombstones and Debezium truncate, message
and schema-change records; malformed JSON, unknown operations and change events without a row are dead
letters stored before the offset is committed (`DEAD_LETTER_POLICY=store`, the default), or stop the
worker without committing (`DEAD_LETTER_POLICY=fail`).

Live-tested 2026-11-01 against MySQL 8.4 and Kafka 3.9.1 (`src/test/ingest-workers-live.e2e.test.ts`):
```bash
docker compose -f deploy/ingest/test-sources.compose.yml up -d --wait
bun run test:e2e:local ingest-workers
```
The PostgreSQL push worker shares the MySQL code path but was not run against PostgreSQL here.

`partitioned_worker.mjs` is live-tested (`src/test/partitioned-worker-live.e2e.test.ts`, 2026-11-03):
Kafka poison records under all three policies, S3 ranged reads against the local Supabase storage S3
endpoint, and the Postgres restart contract (worker killed mid-run, rows changed behind and ahead of
the checkpoint, result and `mixed` marker checked).

## Not verified here
### Token-fenced protocol upgrade
Deploy the updated worker after migrations 0020/0021. Claims return a fresh `lease_token` and `claim_version`; every heartbeat/write/fail sends that token. A reused worker name cannot authorize an old attempt. Old running claims expire and are reclaimed with their saved checkpoints. Name-only requests fail validation; do not restore deprecated RPC grants. Live database fencing/recovery tests passed; the updated kit was syntax checked, not rerun against a real source in this increment.

`partitioned_ingest.py` was not executed against a Spark cluster; ADLS and AWS S3 themselves were not used (only an S3-compatible endpoint). Run `node partitioned_worker.mjs --help` and `spark-submit partitioned_ingest.py --help` on your servers first.
