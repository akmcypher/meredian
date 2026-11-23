import { z } from "zod";
// A Spark part: <partition>-<part number within the partition>.
export const SPARK_PART = /^[0-9]{1,6}-[0-9]{1,6}$/;
export const CONNECT_PROTOCOL = "meridian-connect/1";
export const identifier = z.string().regex(/^[a-zA-Z_][a-zA-Z0-9_]{0,62}$/);
export const primaryKeySpec = z.union([identifier, z.array(identifier).min(1).max(16)]);
export const streamSchema = z.object({
  name: identifier,
  namespace: identifier,
  json_schema: z.record(z.string(), z.unknown()),
  // A single column (the original v1 shape) or a composite key; both are meridian-connect/1.
  primary_key: primaryKeySpec,
});
export const recordMessage = z.object({
  type: z.literal("RECORD"),
  record: z.object({
    stream: identifier,
    namespace: identifier,
    data: z.record(z.string(), z.unknown()),
    emitted_at: z.number().int(),
    cursor: z.unknown().optional(),
    primary_key: z.unknown(),
    // Absent means upsert. Deletes come from Kafka tombstones and CDC envelopes.
    operation: z.enum(["upsert", "delete"]).optional(),
  }),
});
// Offsets are decimal strings: JSON numbers would be rounded past 2^53 when parsed here. Plain
// integers from checkpoints written before this rule are still read.
export const offset = z.union([z.string().regex(/^[0-9]{1,20}$/), z.number().int().nonnegative()]);
const offsetRange = z.object({
  partition: z.number().int().nonnegative(),
  from: offset,
  to: offset,
});
const fileDigest = z.string().regex(/^[0-9a-f]{64}$/);
const filePosition = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);
const fileIdentity = z.object({
  sha256: fileDigest,
  size: filePosition,
  modified: z.string().nullable(),
  done: z.boolean(),
});
const fileRange = z
  .tuple([
    z.string().min(1),
    filePosition,
    z.string().nullable(),
    z.enum(["bytes", "records", "rowgroups", "avroblocks"]),
    filePosition,
    filePosition,
    filePosition,
  ])
  .refine((r) => r[4] <= r[5]);
const standaloneFiles = z.object({
  version: z.literal(1),
  source: fileDigest,
  objects: z.record(z.string().min(1), fileIdentity.extend({ after: filePosition.nullable() })),
});
const distributedFiles = z
  .object({
    version: z.literal(2),
    source: fileDigest,
    objects: z.record(
      z.string().min(1),
      fileIdentity
        .extend({
          ranges: z
            .array(z.object({ range: fileRange, after: filePosition.nullable() }).strict())
            .max(4096)
            .optional(),
        })
        .strict()
        .refine((o) => (o.done ? !o.ranges?.length : !!o.ranges?.length)),
    ),
  })
  .strict()
  .refine((files) =>
    Object.entries(files.objects).every(([key, object]) =>
      (object.ranges ?? []).every(
        ({ range }) => range[0] === key && range[1] === object.size && range[2] === object.modified,
      ),
    ),
  );
const incrementalSparkState = z
  .object({
    mode: z.literal("incremental"),
    run: z.string().uuid(),
    source: fileDigest,
    partitions: z.number().int().min(1).max(4096),
    consistency: z.literal("single"),
  })
  .strict();

export const stateMessage = z.object({
  type: z.literal("STATE"),
  state: z
    .object({
      version: z.literal(1),
      cursor: z.unknown().optional(),
      primary_key: z.unknown().optional(),
      completed: z.boolean().optional(),
      schema_hash: z.string().optional(),
      retirement: z.object({ status: z.enum(["pending", "done"]) }).optional(),
      // Version 1 is the standalone row ledger; version 2 holds unfinished Spark ranges.
      files: z
        .union([standaloneFiles, distributedFiles])
        .refine((value) => new TextEncoder().encode(JSON.stringify(value)).length <= 48 * 1024)
        .optional(),
      // Partitioned sources (Kafka): next offset to read per partition. It is stored in Postgres
      // with the landed commit, so this, not the consumer group, is the authoritative position.
      positions: z.record(z.string().regex(/^[0-9]{1,6}$/), offset).optional(),
      // Identity of the data the positions belong to; a mismatch fails the next run closed.
      cluster_id: z.string().max(100).optional(),
      topic_id: z.string().max(100).optional(),
      // Replay runs: the dead letters this commit resolves.
      replayed: z.array(z.string().uuid()).max(1000).optional(),
      // Offset ranges retention deleted before they were read, accepted by the owner for this run.
      gaps: z.array(offsetRange).max(1000).optional(),
      // Spark runs: per hash partition, the last committed position and whether it is done, so a
      // new attempt of the same run resumes each partition inside the same window. consistency:
      // "single" (one snapshot), "partition" (one snapshot per partition, MySQL) or "mixed" (an
      // attempt resumed after its snapshot was lost).
      spark: z
        .union([
          incrementalSparkState,
          z.object({
            mode: z.undefined().optional(),

            run: z.string().uuid(),
            source: z
              .string()
              .regex(/^[0-9a-f]{64}$/)
              .optional(),
            partitions: z.number().int().min(1).max(4096),
            consistency: z.enum(["single", "partition", "mixed"]),
            // Incremental: the (cursor, key) positions the run reads after and up to; null = open.
            window: z.object({ after: z.unknown(), until: z.unknown() }),
            // Files: the planned ranges ([key, size, modified, kind, a, b, base]), kept so a resumed
            // attempt reads the same ranges of the same object versions.
            plan: z.array(z.array(z.unknown()).max(7)).max(4096).optional(),
            progress: z.record(
              z.string().regex(/^[0-9]{1,4}$/),
              z.object({ after: z.unknown(), done: z.boolean() }),
            ),
          }),
        ])
        .optional(),
      // CDC streams (deploy/connect/README.md, "Change data capture"): the stream's replication position. "snapshot"
      // while the table is copied (primary_key is the copy's progress), then "stream". PostgreSQL:
      // the slot and the end LSN of the last landed transaction. MySQL: the binlog file and position.
      cdc: z
        .object({
          phase: z.enum(["snapshot", "stream"]),
          slot: z
            .string()
            .regex(/^meridian_[0-9a-f]{24}$/)
            .optional(),
          lsn: z
            .string()
            .regex(/^[0-9A-F]{1,8}\/[0-9A-F]{1,8}$/)
            .optional(),
          binlog: z
            .object({
              file: z.string().regex(/^[A-Za-z0-9._-]{1,200}$/),
              position: z.number().int().nonnegative(),
            })
            .optional(),
          server_id: z.number().int().min(1).max(4294967295).optional(),
        })
        .optional(),
    })
    .refine((state) => {
      if (state.files?.version !== 2) return state.spark?.mode !== "incremental";
      if (state.spark?.mode !== "incremental" || state.spark.source !== state.files.source)
        return false;
      if (state.completed && Object.values(state.files.objects).some((object) => !object.done))
        return false;
      return new TextEncoder().encode(JSON.stringify(state)).length <= 48 * 1024;
    }),
});
export const deadLetterCode = z.enum([
  "deserialization_failed",
  "schema_not_found",
  "schema_incompatible",
  "record_too_large",
  "invalid_record",
]);
// A record the actor could not turn into a RECORD. The raw bytes travel base64-encoded and are
// stored in the ingest-files bucket, never in a database row.
export const deadLetterMessage = z.object({
  type: z.literal("DEAD_LETTER"),
  dead_letter: z.object({
    stream: identifier,
    namespace: identifier,
    position: z.string().min(1).max(200),
    error_code: deadLetterCode,
    payload: z.object({
      // Base64 of the largest record the Kafka actor dead-letters whole (1.1 MB).
      key_b64: z.string().max(1_500_000).nullable().optional(),
      value_b64: z.string().max(1_500_000).nullable().optional(),
      timestamp: z.number().int().optional(),
      partition: z.number().int().nonnegative().optional(),
      offset: offset.optional(),
    }),
  }),
});
export const safeError = z.enum([
  "ok",
  "auth_failed",
  "source_unavailable",
  "schema_drift",
  "connector_failed",
  "destination_failed",
  "timeout",
  "dead_letter_budget",
  "offset_out_of_range",
  "source_changed",
  "record_too_large",
  "replay_incomplete",
]);
// Why a run stopped: named positions, identities and counts only, so a report can never carry
// record data. Unknown fields are rejected, not stripped.
const count = z.string().regex(/^[0-9]{1,20}$/);
export const reportDetail = z
  .object({
    partition: z.number().int().nonnegative(),
    offset,
    low: offset,
    high: offset,
    gap: offset,
    bytes: count,
    reason: z.enum(["cluster_id", "topic_id", "partition_missing", "offset_above_high"]),
    // Kafka cluster and topic ids are base64 (with + / =).
    saved: z.string().regex(/^[A-Za-z0-9_+/=-]{1,64}$/),
    current: z.string().regex(/^[A-Za-z0-9_+/=-]{1,64}$/),
    selected: count,
    replayed: count,
    still_failing: count,
    deferred: count,
  })
  .partial()
  .strict();
export const agentRequest = z.discriminatedUnion("action", [
  z.object({ action: z.literal("claim") }),
  z.object({ action: z.literal("group_context"), connectionId: z.string().uuid().optional() }),
  z.object({
    action: z.literal("claim_group_task"),
    connectionId: z.string().uuid().optional(),
    assignments: z
      .record(
        z.string().uuid(),
        z
          .array(z.number().int().min(0).max(999999))
          .max(1024)
          .refine((partitions) => new Set(partitions).size === partitions.length),
      )
      .refine((assignments) => Object.keys(assignments).length <= 1000)
      .refine(
        (assignments) =>
          new Set(Object.keys(assignments).map((id) => id.toLowerCase())).size ===
          Object.keys(assignments).length,
      )
      .transform((assignments) =>
        Object.fromEntries(
          Object.entries(assignments).map(([id, partitions]) => [id.toLowerCase(), partitions]),
        ),
      ),
    worker: z
      .string()
      .regex(/^[\w.:-]{1,200}$/)
      .optional(),
    agent: z
      .string()
      .regex(/^[\w.:-]{1,200}$/)
      .optional(),
    slots: z.number().int().min(1).max(64).optional(),
    version: z
      .string()
      .regex(/^[\w.+-]{1,40}$/)
      .optional(),
  }),
  z.object({
    action: z.literal("abandon_task"),
    runId: z.string().uuid(),
    token: z.string().uuid(),
  }),
  // Task-capable agents claim one stream of a run at a time; several can work on a run at once.
  z.object({
    action: z.literal("claim_task"),
    worker: z
      .string()
      .regex(/^[\w.:-]{1,200}$/)
      .optional(),
    // Pool agents (migration 0108) report themselves for the pool's worker list.
    agent: z
      .string()
      .regex(/^[\w.:-]{1,200}$/)
      .optional(),
    slots: z.number().int().min(1).max(64).optional(),
    version: z
      .string()
      .regex(/^[\w.+-]{1,40}$/)
      .optional(),
  }),
  z.object({
    action: z.literal("report"),
    runId: z.string().uuid(),
    token: z.string().uuid(),
    code: z.enum([
      "offset_out_of_range",
      "source_changed",
      "record_too_large",
      "replay_incomplete",
    ]),
    detail: z.array(reportDetail).min(1).max(1000),
    // The stream the report is about; approvals use only that stream's reports.
    streamId: z.string().uuid().optional(),
  }),
  z.object({ action: z.literal("heartbeat"), runId: z.string().uuid(), token: z.string().uuid() }),
  // Kafka progress: committed position and end offset per partition (offsets as decimal strings).
  z.object({
    action: z.literal("lag"),
    runId: z.string().uuid(),
    token: z.string().uuid(),
    streamId: z.string().uuid(),
    partitions: z
      .array(
        z.object({
          partition: z.string().regex(/^[0-9]{1,6}$/),
          position: z.string().regex(/^[0-9]{1,20}$/),
          end: z.string().regex(/^[0-9]{1,20}$/),
        }),
      )
      .min(1)
      .max(1024),
  }),
  // A partitioned stream's planning task replaces itself with one task per partition (Kafka).
  z.object({
    action: z.literal("split"),
    runId: z.string().uuid(),
    token: z.string().uuid(),
    shards: z
      .array(z.string().regex(/^[0-9]{1,6}$/))
      .min(1)
      .max(1024),
  }),
  z.object({
    action: z.literal("finish"),
    runId: z.string().uuid(),
    token: z.string().uuid(),
    ok: z.boolean(),
    code: safeError,
    catalog: z.array(streamSchema).max(1000).optional(),
  }),
  z.object({
    action: z.literal("commit"),
    runId: z.string().uuid(),
    token: z.string().uuid(),
    streamId: z.string().uuid(),
    sequence: z.number().int().nonnegative(),
    records: z.array(recordMessage).max(1000),
    dead_letters: z.array(deadLetterMessage).max(1000).optional(),
    state: stateMessage,
  }),
  // Spark direct writes (deploy/connect/README.md, "Spark ingestion"): a signed upload URL for one
  // Bronze part, then a fenced commit of the part the executor wrote.
  z.object({
    action: z.literal("upload_url"),
    runId: z.string().uuid(),
    token: z.string().uuid(),
    streamId: z.string().uuid(),
    part: z.string().regex(SPARK_PART),
    // S6: a part is NDJSON or Parquet (deploy/connect/bronze_parquet.py); the object key depends on it.
    format: z.enum(["ndjson", "parquet"]).default("ndjson"),
  }),
  z.object({
    action: z.literal("commit_file"),
    runId: z.string().uuid(),
    token: z.string().uuid(),
    streamId: z.string().uuid(),
    sequence: z.number().int().nonnegative(),
    part: z.string().regex(SPARK_PART),
    format: z.enum(["ndjson", "parquet"]).default("ndjson"),
    rows: z.number().int().min(0).max(10000),
    bytes: z
      .number()
      .int()
      .min(0)
      .max(8 * 1024 * 1024),
    sha256: z.string().regex(/^[a-f0-9]{64}$/),
    executor: z.string().max(200).optional(),
    // Records the executor could not decode (Kafka), committed with the part that skips them.
    dead_letters: z.array(deadLetterMessage).max(1000).optional(),
    state: stateMessage,
  }),
]);
export function canonicalEnvelopes(
  connectionId: string,
  runId: string,
  streamId: string,
  records: z.infer<typeof recordMessage>[],
  schema: unknown,
) {
  return records.map((m) => ({
    protocol: CONNECT_PROTOCOL,
    connection_id: connectionId,
    run_id: runId,
    stream_id: streamId,
    schema,
    extracted_at: m.record.emitted_at,
    cursor: m.record.cursor ?? null,
    primary_key: m.record.primary_key,
    operation: m.record.operation ?? "upsert",
    source_namespace: m.record.namespace,
    source_stream: m.record.stream,
    data: m.record.data,
  }));
}
