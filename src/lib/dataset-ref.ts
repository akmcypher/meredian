// DatasetRef v1: the identity and version of a published
// dataset, shared by producers (Transform today) and readers (Serve today; catalog and Visual next).
// Backed by core.datasets / core.dataset_versions; provider objects never appear here.
import { z } from "zod";

export const DATASET_REF_VERSION = "meridian-dataset-ref/1";
export const datasetName = z.string().regex(/^[A-Za-z_][A-Za-z0-9_]{0,62}$/);

export const datasetRef = z
  .object({
    contract: z.literal(DATASET_REF_VERSION),
    datasetId: z.string().uuid(),
    namespace: z.enum(["transform", "connect"]),
    name: datasetName,
    layer: z.enum(["bronze", "silver", "gold", "golden", "external"]),
    versionId: z.string().uuid(),
    format: z.enum(["ndjson", "parquet", "iceberg"]),
    location: z.object({
      sha256: z.string().regex(/^[a-f0-9]{64}$/),
      bytes: z.number().int().nonnegative(),
      // Short-lived read URL; present only in Serve manifests, never stored.
      url: z.string().url().optional(),
    }),
    rows: z.number().int().nonnegative(),
    schema: z.unknown(),
    producedBy: z.object({
      product: z.enum(["transform", "ingest"]),
      runId: z.string().uuid(),
      taskId: z.string().uuid(),
      attempt: z.number().int().positive().nullable(),
    }),
    // Transform quality and lineage are reported by the worker that produced them.
    evidenceClass: z.enum(["worker_attested", "server_verified"]),
    createdAt: z.string(),
  })
  .refine(
    (ref) =>
      ref.namespace === "connect"
        ? ref.layer === "bronze" && ref.format === "ndjson" && ref.producedBy.product === "ingest"
        : ref.producedBy.product === "transform" && ref.producedBy.attempt !== null,
    "Dataset producer, layer and format disagree",
  );
export type DatasetRef = z.infer<typeof datasetRef>;

// Policy decision v1 as returned by dataset_policy_decision. Structured only:
// engines compile it themselves, so no policy text ever becomes SQL.
export const policyDecision = z.discriminatedUnion("allow", [
  z.object({ allow: z.literal(false) }),
  z.object({
    allow: z.literal(true),
    rows: z.enum(["all", "filtered", "none"]),
    // A row is kept when any filter passes; values compare as text, ordering ops as numbers.
    rowFilters: z.array(
      z.object({
        column: z.string(),
        op: z.enum(["=", "!=", "in", ">", "<", ">=", "<="]),
        value: z.string(),
      }),
    ),
    masks: z.array(z.object({ column: z.string(), mode: z.enum(["full", "partial"]) })),
  }),
]);
export type PolicyDecision = z.infer<typeof policyDecision>;

export const serveManifestRequest = z.object({
  // Identifiers the query mentions; the service never sees datasets it was not asked for.
  names: z.array(z.string().max(63)).max(256).default([]),
  bronze: z
    .object({
      streamId: z.string().uuid(),
      runId: z.string().uuid(),
      afterSequence: z.number().int().min(-1).max(2147483647).default(-1),
    })
    .optional(),
});

type ManifestRow = {
  namespace?: string;
  format?: string;
  dataset_id: string;
  name: string;
  layer: string;
  version_id: string;
  sha256: string;
  byte_count: number;
  row_count: number;
  schema: unknown;
  produced_by: unknown;
  evidence_class: string;
  created_at: string;
};

export function toDatasetRef(row: ManifestRow, url?: string): DatasetRef {
  const produced = row.produced_by as {
    product: string;
    run_id: string;
    task_id: string;
    attempt: number;
  };
  return datasetRef.parse({
    contract: DATASET_REF_VERSION,
    datasetId: row.dataset_id,
    namespace: row.namespace ?? "transform",
    name: row.name,
    layer: row.layer,
    versionId: row.version_id,
    format: row.format ?? "parquet",
    location: { sha256: row.sha256, bytes: row.byte_count, url },
    rows: row.row_count,
    schema: row.schema,
    producedBy: {
      product: produced.product,
      runId: produced.run_id,
      taskId: produced.task_id,
      attempt: produced.attempt,
    },
    evidenceClass: row.evidence_class,
    createdAt: row.created_at,
  });
}

// Only identifiers that could name a registered dataset are forwarded, lowercased like DataFusion's
// unquoted identifiers.
export function manifestNames(names: string[]) {
  return [
    ...new Set(names.map((n) => n.toLowerCase()).filter((n) => datasetName.safeParse(n).success)),
  ];
}
