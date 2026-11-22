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
      "src/test/connect-kafka-partitions.e2e.test.ts",
      "deploy/connect/test_kafka_security.py",
      "deploy/connect/test_kafka_profiles.py",
      "deploy/connect/test_kafka_registry.py",
      "deploy/connect/test_kafka_registry_live.py",
      "src/test/connect-kafka-shared.e2e.test.ts",
      "src/test/connect-spark-avro.e2e.test.ts",
    ],
    environment:
      "3-broker Kafka 3.9.1 KRaft (RF 3, min ISR 2) + Karapace 5.0.3; secured Kafka 3.9.1 (SASL_SSL SCRAM-SHA-512/PLAIN, mTLS listener, test PKI), Windows 11 ARM64; Avro evolution on Apicurio 3.3.5 with secured two-worker Spark 3.5.3 (2026-11-22); secured-broker/profile evidence from 2026-11-15",
    certifiedOn: "2026-11-22",
    proves:
      "topic discovery and multi-partition ingestion; JSON, Debezium JSON and Confluent wire-format Avro; durable per-partition offset checkpoints stored with each landed commit; crash recovery without loss or duplicate positions; reads and acks=all writes with one broker stopped; tombstones and delete events; dead-letter handling and replay within a budget; retention-gap detection fails closed until the owner approves the exact gap, retained in commit history; replay after a registry fix; Avro evolution with a pinned reader schema; per-partition tasks read one topic in parallel with per-partition offsets; SASL/SCRAM-SHA-512 and SASL/PLAIN over TLS and mutual TLS read end to end; wrong credentials, unknown users, untrusted broker CAs and hostname mismatches fail as auth_failed without reading; cleartext PLAIN and unknown mechanisms are refused; registry credentials never go over plain HTTP; per-partition lag (position vs end offset) recorded under the task lease after each commit and readable only by the owner; backpressure: records stream without buffering, checkpoints bounded by records and payload bytes, capped prefetch, and per-run record/time limits drain pending records across runs without loss or duplicates; managed-service profiles (Confluent Cloud, Event Hubs, MSK SCRAM and IAM, Redpanda Cloud, Aiven) expand to TLS-only settings and refuse weakening, the SCRAM/PLAIN/mTLS profiles connect to a secured broker, and MSK IAM produces SigV4 OAUTHBEARER tokens; locally demonstrated shared consumer groups with fenced handoff/death recovery; Avro schema references resolved by exact subject/version, pinned reader defaults and type promotions through standalone Kafka and secured two-executor Spark; reader changes retain committed offsets, Spark retries do not duplicate records, and incompatible records replay after a reader fix without moving live positions",
    missing:
      "Shared groups are supported for kafka-v1; Spark Kafka uses planned offset chunks. Lag is as fresh as the last run (no polling between runs); no managed-service runs (profiles are tested against a local secured broker and an offline MSK IAM token, not against Confluent Cloud, MSK, Event Hubs, Redpanda Cloud or Aiven) or broker ACL authorization tests; a broker that drops clients without a trusted certificate is retried as source_unavailable. Production actor isolation and HA are not certified. Raw delete/upsert envelopes are retained, not applied as warehouse merges.",
  },
};
