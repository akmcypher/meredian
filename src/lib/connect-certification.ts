// Certified Meridian Connect actors (meridian-connect/1). connector-registry.ts derives the
// PostgreSQL, MySQL and Kafka catalogue entries from this list, so the catalogue never claims more
// than the suites below prove. Add an actor here only with a passing live suite, and keep the
// date and environment to the run that produced the evidence.
export interface CertifiedActor {
  actor: string;
  modes: ("batch" | "incremental" | "cdc" | "streaming")[];
  deletes: boolean;
  formats: string[];
  suites: string[];
  environment: string;
  certifiedOn: string;
  proves: string;
  missing: string;
}
const kit =
  "conformance kit (deploy/connect/conformance.py): spec, check, auth_failed, discover with single and composite keys, full refresh, checkpoint resume without loss or duplicates, incremental, reset, lossless types, schema drift";
export const CERTIFIED: Record<"postgres" | "mysql" | "kafka", CertifiedActor> = {
  postgres: {
    actor: "postgres-v1",
    modes: ["batch", "incremental"],
    deletes: false,
    formats: ["Rows as JSON (bigint, numeric, bytea, timestamps lossless)"],
    suites: ["deploy/connect/test_certify_postgres.py", "src/test/connect-live.e2e.test.ts"],
    environment: "local Supabase Postgres, Windows 11 ARM64",
    certifiedOn: "2026-11-02",
    proves: `${kit}; agent → control plane → bronze with lease fencing`,
    missing:
      "No deletes/CDC (logical replication), no managed scheduling; bronze is raw NDJSON, not a native table.",
  },
  mysql: {
    actor: "mysql-v1",
    modes: ["batch", "incremental"],
    deletes: false,
    formats: ["Rows as JSON (tinyint(1) boolean, JSON, UTC TIMESTAMP)"],
    suites: ["deploy/connect/test_certify_mysql.py"],
    environment: "MySQL 8.4 (deploy/ingest/test-sources.compose.yml)",
    certifiedOn: "2026-11-02",
    proves: kit,
    missing:
      "No deletes/CDC (binlog), no managed scheduling; not yet run through the control-plane e2e suite.",
  },
  kafka: {
    actor: "kafka-v1",
    modes: ["streaming"],
    deletes: true,
    formats: ["JSON", "Debezium JSON envelope", "Confluent wire-format Avro"],
    suites: [
      "deploy/connect/test_kafka.py",
      "src/test/connect-kafka-live.e2e.test.ts",
      "src/test/connect-kafka-gap.e2e.test.ts",
      "src/test/connect-kafka-batching.e2e.test.ts",
    ],
    environment: "3-broker Kafka 3.9.1 KRaft (RF 3, min ISR 2) + Karapace 5.0.3",
    certifiedOn: "2026-11-04",
    proves:
      "topic discovery and multi-partition ingestion; JSON, Debezium JSON and Confluent wire-format Avro; durable per-partition offset checkpoints stored with each landed commit; crash recovery without loss or duplicate positions; reads and acks=all writes with one broker stopped; tombstones and delete events; dead-letter handling and replay within a budget; retention-gap detection fails closed until the owner approves the exact gap, retained in commit history; replay after a registry fix; Avro evolution with a pinned reader schema",
    missing:
      "Explicit partition assignment, one actor per connection: no consumer-group scaling, rebalance or backpressure certification; no SASL/TLS cluster tested. Production actor isolation and HA are not certified. Raw delete/upsert envelopes are retained, not applied as warehouse merges.",
  },
};
