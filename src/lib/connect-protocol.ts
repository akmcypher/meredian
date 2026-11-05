import { z } from "zod";
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
export const stateMessage = z.object({
  type: z.literal("STATE"),
  state: z.object({
    version: z.literal(1),
    cursor: z.unknown().optional(),
    primary_key: z.unknown().optional(),
    completed: z.boolean().optional(),
    schema_hash: z.string().optional(),
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
