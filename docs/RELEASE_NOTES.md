# Meridian Community Preview 0.1

Preview 0.1 is pre-release software for evaluation and feedback. It is not production-certified.

## What is included

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

## Known limitations

- PostgreSQL and MySQL actors are incremental cursor extractors, not CDC: deletes and updates that
  do not advance the cursor are not captured.
- Raw upsert/delete envelopes are retained as-is; no warehouse merge is applied.
- REST/Push batches and native uploads are not exposed through the Bronze reader.
- Kafka consumer-group scaling, rebalancing and SASL/TLS clusters are not supported yet.
- Connector actors share one agent process. Run one installation per trust boundary.
- Storage reconciliation is report-only; destructive cleanup is disabled.
- Team invitations are not available yet.
- Container images and release archives are not signed.

## Upgrading an earlier preview database

Apply migrations in order and never edit applied files. Before upgrading past migration 0071,
cancel outstanding replay runs and requeue still-open dead letters afterwards. Migration 0073 and
the updated application must be deployed together: the old Bronze read function is removed and old
callers fail closed. Migrations 0074 and 0075 tighten client grants and must be applied before use.
