# Kafka connector

Status: available (preview).

Capabilities:

- Kafka topic discovery and multi-partition ingestion.
- Durable per-partition offset checkpoints.
- Crash recovery without loss or duplicate positions.
- JSON and Debezium JSON event ingestion.
- Confluent wire-format Avro, including pinned reader-schema evolution.
- Tombstones and delete events retained in raw ingestion.
- Dead-letter handling and replay, including recovery after a registry fix.
- Retention-gap detection and explicit owner approval recorded in commit history.
- Broker failure recovery: reads and acknowledgements with one broker stopped.

Tests run against three Kafka 3.9.1 KRaft brokers (replication factor 3, minimum in-sync replicas 2) and Karapace 5.0.3: `deploy/connect/test_kafka.py` and the live `connect-kafka*` end-to-end tests.

## Limitations

The actor uses explicit partition assignment and one actor per connection. Consumer-group scaling, rebalancing and backpressure are not certified. SASL/TLS clusters have not been tested. Production actor isolation and high availability are not certified. Raw upsert/delete envelopes are retained; the connector does not apply warehouse merges.
