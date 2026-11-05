import { CERTIFIED } from "./connect-certification";
export type ConnectorStatus =
  | "Preview / limited"
  | "Available — Certified Preview"
  | "Available"
  | "Configuration required"
  | "Planned"
  | "Unsupported";
export type SyncMode = "batch" | "incremental" | "cdc" | "streaming";
export interface ConnectorDefinition {
  id: string;
  name: string;
  category: string;
  status: ConnectorStatus;
  adapter: "native" | "independent-worker" | "external-managed" | "none";
  modes: SyncMode[];
  formats: string[];
  auth: string[];
  discovery: string;
  capabilities?: string;
  certification?: string;
  limitations: string;
  /** App path of the existing workflow; shown only when that page is installed. */
  destination?: string;
  version: 1;
}
export interface ConnectorCheckpoint {
  version: 1;
  cursor?: string;
  object?: string;
  offset?: string;
}
export interface ConnectorAdapter {
  definition: ConnectorDefinition;
  validate(config: Record<string, unknown>): void;
  test(): Promise<{ healthy: boolean; checkedAt: string }>;
  discover(): Promise<
    { name: string; kind: "table" | "topic" | "file" | "endpoint"; columns?: string[] }[]
  >;
  extract(checkpoint?: ConnectorCheckpoint): AsyncIterable<{
    rows: Record<string, unknown>[];
    checkpoint?: ConnectorCheckpoint;
    batchId: string;
  }>;
}
const entry = (
  id: string,
  name: string,
  category: string,
  options: Partial<ConnectorDefinition> = {},
): ConnectorDefinition => ({
  id,
  name,
  category,
  status: "Planned",
  adapter: "none",
  modes: [],
  formats: [],
  auth: [],
  discovery: "Not implemented",
  limitations:
    "No Meridian extraction adapter. Registration and successful connectivity are not available.",
  version: 1,
  ...options,
});
const worker = {
  status: "Configuration required" as const,
  adapter: "independent-worker" as const,
  modes: ["batch", "incremental"] as SyncMode[],
  formats: ["JSON rows"],
  auth: ["Worker-local credentials", "Per-source bearer"],
  destination: "/ingest" as const,
  limitations:
    "Requires independently running worker. Checkpoint/recovery scope differs by worker; no hosted schema discovery or managed scheduling. Raw landing is not native lakehouse publication.",
};
// Catalogue text for certified Connect actors comes from their certification record.
const certified = (id: keyof typeof CERTIFIED, name: string, category: string) => {
  const c = CERTIFIED[id];
  return entry(id, name, category, {
    status: id === "kafka" ? "Available — Certified Preview" : "Preview / limited",
    adapter: "independent-worker",
    modes: c.modes,
    formats: c.formats,
    auth: ["Agent-local credentials", "Per-source agent token"],
    discovery: `Meridian Connect ${c.actor} (meridian-connect/1) check/discover`,
    destination: "/ingest",
    capabilities: c.proves,
    certification: `Certified ${c.certifiedOn} on ${c.environment} (${c.suites.join(", ")}).`,
    limitations: c.missing,
  });
};
export const CONNECTORS: ConnectorDefinition[] = [
  certified("postgres", "PostgreSQL", "Relational databases"),
  certified("mysql", "MySQL", "Relational databases"),
  ...[
    ["mariadb", "MariaDB"],
    ["sqlserver", "Microsoft SQL Server"],
    ["oracle", "Oracle"],
    ["jdbc", "Generic JDBC"],
  ].map(([id, name]) => entry(id ?? "", name ?? "", "Relational databases")),
  ...["MongoDB", "Cassandra", "DynamoDB", "Elasticsearch", "OpenSearch"].map((name) =>
    entry(name.toLowerCase(), name, "NoSQL / operational stores"),
  ),
  ...[
    "Snowflake",
    "BigQuery",
    "Redshift",
    "Databricks",
    "Trino",
    "Iceberg-compatible catalogs",
  ].map((name) =>
    entry(name.toLowerCase(), name, "Warehouses and lakehouses", {
      limitations:
        "Some existing Connections offer query/catalog operations; these are not ingestion/sync adapters.",
      destination: "/connections",
    }),
  ),
  entry("s3", "Amazon S3", "Object / file storage", {
    status: "Planned",
    adapter: "none",
    modes: ["batch"],
    formats: ["CSV", "JSON", "Parquet", "Avro"],
    auth: ["Per-connection credentials (pending)"],
    discovery: "Object listing once a per-connection transport exists",
    destination: "/lake",
    limitations:
      "Unavailable: a shared company-wide credential is not supported and per-connection credentials are not built yet. Listing/reading code is ready for that transport.",
  }),
  ...["Azure Blob / ADLS Gen2", "Google Cloud Storage", "SFTP", "FTP"].map((name) =>
    entry(name.toLowerCase(), name, "Object / file storage"),
  ),
  entry("local", "Local file upload", "Object / file storage", {
    status: "Available",
    adapter: "native",
    modes: ["batch"],
    formats: ["CSV", "Parquet", "Avro"],
    auth: ["Signed-in account"],
    discovery: "File headers / footer / schema",
    destination: "/partitions",
    limitations:
      "Live file import/publish/query evidence exists. Upload queues require open browser; nested Parquet, Avro snappy/zstd, ORC/Excel unsupported.",
  }),
  certified("kafka", "Apache Kafka", "Streaming / CDC"),
  entry("debezium", "Debezium CDC", "Streaming / CDC", {
    ...worker,
    modes: ["cdc"],
    formats: ["Debezium JSON envelope"],
    limitations:
      "Debezium envelopes are read by the Kafka worker (tested with Debezium-shaped messages, not a running Debezium connector). Deletes retained as events, not applied to a native master/lakehouse table.",
  }),
  ...["Confluent Kafka", "Amazon MSK", "Apache Pulsar", "Redpanda", "Database log CDC"].map(
    (name) => entry(name.toLowerCase(), name, "Streaming / CDC"),
  ),
  ...["Salesforce", "SAP", "Shopify", "HubSpot", "ServiceNow", "Workday"].map((name) =>
    entry(name.toLowerCase(), name, "SaaS / business applications"),
  ),
  entry("rest", "REST / generic HTTP API", "APIs / events", {
    status: "Available",
    adapter: "native",
    modes: ["batch"],
    formats: ["JSON"],
    auth: ["Server-only HTTP headers"],
    discovery: "Bounded response field discovery; no remote object enumeration",
    destination: "/ingest",
    limitations:
      "Manual pull only; public HTTPS destination required. No automatic pagination, rate-limit backoff, schedules, drift reconciliation or native table commit.",
  }),
  ...["GraphQL", "Webhooks", "OpenAPI endpoints"].map((name) =>
    entry(name.toLowerCase(), name, "APIs / events"),
  ),
  entry("orc", "ORC files", "Object / file storage", { status: "Unsupported" }),
  entry("excel", "Excel workbooks", "Object / file storage", { status: "Unsupported" }),
];
export function filterConnectors(search: string, category = "all", status = "all") {
  return CONNECTORS.filter(
    (c) =>
      (category === "all" || c.category === category) &&
      (status === "all" ||
        (status === "Available" ? c.status.startsWith("Available") : c.status === status)) &&
      `${c.name} ${c.category} ${c.formats.join(" ")}`.toLowerCase().includes(search.toLowerCase()),
  );
}
