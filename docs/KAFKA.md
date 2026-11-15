# Kafka connector

Status: available (preview).

Capabilities:

- Kafka topic discovery and multi-partition ingestion.
- One task per partition: every partition is leased, committed and retried on its own, so several
  agents read one topic in parallel. Commits keep one stream-wide sequence for Bronze readers.
- Durable per-partition offset checkpoints and per-partition lag (committed position and end
  offset) shown for each stream.
- Crash recovery without loss or duplicate positions.
- Backpressure: records stream straight to the destination; checkpoints every `max_batch_records`
  records or `max_batch_bytes` bytes, a capped prefetch queue, and bounded runs (`max_run_records`,
  `max_run_seconds`) that continue from their position in the next run.
- Secured clusters: SASL/SCRAM-SHA-256/512 and SASL/PLAIN over TLS, and mutual TLS. Wrong
  credentials, an untrusted CA or a hostname mismatch fail as `auth_failed` without reading. PLAIN
  without TLS is refused unless explicitly allowed; turning hostname verification off is logged.
- Managed services through a `profile`: `confluent_cloud`, `azure_event_hubs`, `aws_msk_scram`,
  `aws_msk_iam` (IAM over SASL/OAUTHBEARER with the AWS default credential chain, an optional role
  or explicit keys), `redpanda_cloud` and `aiven`. A profile fills in the security settings the
  service needs and refuses contradicting ones.
- JSON and Debezium JSON event ingestion.
- Confluent wire-format Avro, including pinned reader-schema evolution.
- Tombstones and delete events retained in raw ingestion.
- Dead-letter handling and replay, including recovery after a registry fix.
- Retention-gap detection and explicit owner approval recorded in commit history.
- Broker failure recovery: reads and acknowledgements with one broker stopped.
- On Spark (`spark-kafka-v1`): one Spark application reads every partition in offset chunks, with
  the same identity, position and gap checks; see the Spark section of `deploy/connect/README.md`.

Tests run against three Kafka 3.9.1 KRaft brokers (replication factor 3, minimum in-sync replicas 2) and Karapace 5.0.3 (`deploy/connect/test_kafka.py` and the live `connect-kafka*` end-to-end tests), a SASL_SSL/mTLS broker (`deploy/connect/test_kafka_security.py`) and offline profile checks (`deploy/connect/test_kafka_profiles.py`).

## Limitations

Partitions are assigned explicitly by the control plane; Kafka consumer-group rebalancing is not used. Managed-service profiles are checked offline and against a local secured broker, not against the hosted services themselves. Production actor isolation and high availability are not certified. Raw upsert/delete envelopes are retained; the connector does not apply warehouse merges.
