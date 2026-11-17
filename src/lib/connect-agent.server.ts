import { agentRequest, canonicalEnvelopes, deadLetterMessage } from "./connect-protocol";
import type { SupabaseClient } from "@supabase/supabase-js";
import type { z } from "zod";
import { readBodyCapped } from "./request-limits.server";
import { sha256 } from "./ingest";
import { sha256Hex } from "./file-ingest";
import { bronzeParquetEnvelopes, isParquet } from "./bronze-parquet";
import { landSourceBatch } from "./landing-batch.server";
type ReplayClaim = {
  run_id: string;
  lease_token: string;
  operation: string;
  streams: {
    id: string;
    dead_letters?: { id: string; position: string; object_key: string; line: number }[] | null;
    deferred?: number;
  }[];
};
const REPLAY_BUDGET = 48 * 1024 * 1024;
// Keep replay hydration in the agent boundary; payloads never become database rows.
export async function hydrateReplayClaim(
  claim: ReplayClaim,
  db: SupabaseClient,
  budget = REPLAY_BUDGET,
) {
  const objects = new Map<string, string[]>();
  const blocked = new Set<string>();
  let inlined = 0;
  let downloaded = 0;
  for (const stream of claim.streams) {
    const deferred: string[] = [];
    const kept: NonNullable<typeof stream.dead_letters> = [];
    for (const d of stream.dead_letters ?? []) {
      if (inlined >= budget || blocked.has(d.object_key)) {
        deferred.push(d.id);
        continue;
      }
      if (!objects.has(d.object_key)) {
        // Each stored batch came through the 8 MiB commit endpoint. Bound the streamed
        // read before buffering, and bound the total cached raw objects independently.
        const cap = 8 * 1024 * 1024;
        if (downloaded + cap > REPLAY_BUDGET) {
          deferred.push(d.id);
          continue;
        }
        const signed = await db.storage.from("ingest-files").createSignedUrl(d.object_key, 60);
        if (!signed.data) throw new Error("dead_letter_missing");
        const response = await fetch(signed.data.signedUrl, { redirect: "error" });
        if (!response.ok) throw new Error("dead_letter_missing");
        const bytes = await readBodyCapped(response, cap);
        downloaded += bytes.byteLength;
        objects.set(d.object_key, new TextDecoder().decode(bytes).split("\n"));
      }
      const line = objects.get(d.object_key)?.[d.line];
      const stored = deadLetterMessage.safeParse(JSON.parse(line ?? "null"));
      if (!stored.success || stored.data.dead_letter.position !== d.position)
        throw new Error("dead_letter_mismatch");
      const entry = { ...d, payload: stored.data.dead_letter.payload, object_key: undefined };
      const size = new TextEncoder().encode(JSON.stringify(entry)).byteLength + 1;
      if (inlined + size > budget) {
        deferred.push(d.id);
        blocked.add(d.object_key);
        continue;
      }
      inlined += size;
      Object.assign(d, entry);
      kept.push(d);
    }
    stream.dead_letters = kept;
    if (deferred.length) {
      const released = await db.rpc("connect_defer_replay", {
        _run: claim.run_id,
        _token: claim.lease_token,
        _stream: stream.id,
        _ids: deferred,
      });
      if (released.error) throw released.error;
      stream.deferred = released.data;
    } else stream.deferred ??= 0;
  }
}

type CommitRequest = {
  runId: string;
  token: string;
  streamId: string;
  sequence: number;
  state: { state: { completed?: boolean | undefined } };
};
// Writes the commit manifest (immutable; an identical retry is accepted) and makes the fenced
// commit. Shared by agent-pushed batches and Spark parts.
async function publishCommit(
  db: SupabaseClient,
  c: {
    ownerId: string;
    batchKey: string;
    landedId: string;
    objectKey: string;
    sha: string;
    rows: number;
    bytes: number;
    data: CommitRequest;
    shard: string;
    provenance?: Record<string, unknown>;
  },
) {
  const manifestKey = `${c.ownerId}/published/${c.batchKey}/manifest.json`;
  const manifest = {
    protocol: "meridian-connect/1",
    run_id: c.data.runId,
    stream_id: c.data.streamId,
    sequence: c.data.sequence,
    ...(c.shard ? { shard: c.shard } : {}),
    batch_id: c.landedId,
    object_key: c.objectKey,
    sha256: c.sha,
    rows: c.rows,
    bytes: c.bytes,
    checkpoint: c.data.state.state,
    ...(c.provenance ? { produced_by: c.provenance } : {}),
  };
  const blob = new Blob([JSON.stringify(manifest)]);
  const uploaded = await db.storage
    .from("ingest-files")
    .upload(manifestKey, blob, { upsert: false, contentType: "application/json" });
  if (uploaded.error) {
    const old = await db.storage.from("ingest-files").download(manifestKey);
    if (!old.data || (await old.data.text()) !== JSON.stringify(manifest))
      throw new Error("manifest_conflict");
  }
  const committed = await db.rpc("connect_commit", {
    _run: c.data.runId,
    _token: c.data.token,
    _stream: c.data.streamId,
    _sequence: c.data.sequence,
    _batch: c.landedId,
    _manifest: manifestKey,
    _sha: c.sha,
    _rows: c.rows,
    _bytes: c.bytes,
    _checkpoint: c.data.state.state as never,
    _completed: c.data.state.state.completed ?? false,
  });
  if (committed.error) throw committed.error;
  return committed.data;
}

// A Spark part is checked before it is registered: the stored object must be the bytes the
// executor reported (size, SHA-256, record count) and every record a Connect envelope of this
// connection, run and stream. Parts are ordinary landing batches (8 MiB, 10,000 rows at most),
// NDJSON or, since S6, Parquet (src/lib/bronze-parquet.ts), which decodes to the same envelopes.
async function verifySparkPart(
  db: SupabaseClient,
  key: string,
  claimed: {
    runId: string;
    streamId: string;
    rows: number;
    bytes: number;
    sha256: string;
    format: "ndjson" | "parquet";
  },
  expect: {
    connection: string;
    stream: { namespace: string; name: string };
    schema: unknown;
    extractedAt: number;
  },
) {
  const { isDeepStrictEqual } = await import("node:util");
  const stored = await db.storage.from("ingest-files").download(key);
  if (!stored.data || stored.data.size !== claimed.bytes || stored.data.size > 8 * 1024 * 1024)
    throw new Error("part_mismatch");
  const bytes = new Uint8Array(await stored.data.arrayBuffer());
  if ((await sha256Hex(bytes)) !== claimed.sha256) throw new Error("part_mismatch");
  let envelopes: Record<string, unknown>[];
  if (claimed.format === "parquet") {
    if (!isParquet(bytes)) throw new Error("part_mismatch");
    try {
      envelopes = await bronzeParquetEnvelopes(bytes.slice().buffer);
    } catch {
      throw new Error("part_mismatch");
    }
  } else {
    const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    const lines = text ? text.split("\n") : [];
    if (lines.length && lines[lines.length - 1] === "") lines.pop();
    envelopes = lines.map((line) => JSON.parse(line) as Record<string, unknown>);
  }
  if (envelopes.length !== claimed.rows) throw new Error("part_mismatch");
  for (const e of envelopes) {
    if (
      e["protocol"] !== "meridian-connect/1" ||
      e["connection_id"] !== expect.connection ||
      e["run_id"] !== claimed.runId ||
      e["stream_id"] !== claimed.streamId ||
      e["extracted_at"] !== expect.extractedAt ||
      e["source_namespace"] !== expect.stream.namespace ||
      e["source_stream"] !== expect.stream.name ||
      !["upsert", "delete"].includes(e["operation"] as string) ||
      e["primary_key"] === undefined ||
      typeof e["data"] !== "object" ||
      e["data"] === null ||
      Array.isArray(e["data"]) ||
      !isDeepStrictEqual(e["schema"], expect.schema)
    )
      throw new Error("part_mismatch");
  }
  return { sha256: claimed.sha256, rows: envelopes.length, bytes: claimed.bytes };
}

// Dead letters are stored before the commit that moves the checkpoint past them; a retried
// sequence rewrites the identical object and the RPC ignores positions it already holds. Shared
// by agent-pushed commits and Spark parts. Returns a refusal response, or null when stored.
async function storeDeadLetters(
  db: SupabaseClient,
  ownerId: string,
  data: { runId: string; token: string; streamId: string; sequence: number },
  key: string,
  dead: z.infer<typeof deadLetterMessage>[],
) {
  const digest = await sha256(key);
  const objectKey = `${ownerId}/dead-letters/${data.runId}/${digest.slice(0, 32)}.ndjson`;
  const body = dead.map((m) => JSON.stringify(m)).join("\n");
  const put = await db.storage.from("ingest-files").upload(objectKey, new Blob([body]), {
    upsert: false,
    contentType: "application/x-ndjson",
  });
  if (put.error) {
    const old = await db.storage.from("ingest-files").download(objectKey);
    if (!old.data || (await old.data.text()) !== body) throw new Error("dead_letter_conflict");
  }
  const recorded = await db.rpc("connect_record_dead_letters", {
    _run: data.runId,
    _token: data.token,
    _stream: data.streamId,
    _sequence: data.sequence,
    _object: objectKey,
    _entries: dead.map((m, line) => ({
      position: m.dead_letter.position,
      error_code: m.dead_letter.error_code,
      line,
    })) as never,
  });
  if (recorded.error?.message.includes("dead_letter_budget"))
    return Response.json({ error: "dead_letter_budget", retryable: false }, { status: 409 });
  if (recorded.error) throw recorded.error;
  return null;
}

export async function handleConnectAgent(request: Request) {
  // Bounded read: the body is capped while it is read, before it is buffered or parsed.
  // No rate limit here: the agent treats any heartbeat error as a lost lease.
  const { readBodyCapped, RequestLimitError } = await import("./request-limits.server");
  let raw: string;
  try {
    raw = new TextDecoder().decode(await readBodyCapped(request, 8 * 1024 * 1024));
  } catch (e) {
    if (e instanceof RequestLimitError)
      return Response.json({ error: "request_limit" }, { status: 413 });
    throw e;
  }
  const token = request.headers.get("x-ingest-token") ?? "";
  if (!/^mi_[a-f0-9]{64}$/.test(token))
    return Response.json({ error: "unauthorized" }, { status: 401 });
  const { supabaseAdmin: db } = await import("@/integrations/supabase/client.server");
  const src = await db
    .from("ingest_sources")
    .select("id,owner_id,organization_id,enabled")
    .eq("token_hash", await sha256(token))
    .maybeSingle();
  if (!src.data?.enabled) return Response.json({ error: "unauthorized" }, { status: 401 });
  const c = await db
    .from("connect_connections")
    .select("*")
    .eq("source_id", src.data.id)
    .maybeSingle();
  if (!c.data?.enabled) return Response.json({ error: "unavailable" }, { status: 403 });
  if (c.data.owner_id !== src.data.owner_id || c.data.organization_id !== src.data.organization_id)
    return Response.json({ error: "scope_mismatch" }, { status: 403 });
  if (c.data.organization_id) {
    const member = await db
      .from("org_members")
      .select("user_id")
      .eq("org_id", c.data.organization_id)
      .eq("user_id", c.data.owner_id)
      .maybeSingle();
    if (member.error || !member.data) return Response.json({ error: "forbidden" }, { status: 403 });
  }
  let body: unknown;
  try {
    body = JSON.parse(raw || "null");
  } catch {
    return Response.json({ error: "invalid_protocol" }, { status: 400 });
  }
  const parsed = agentRequest.safeParse(body);
  if (!parsed.success) return Response.json({ error: "invalid_protocol" }, { status: 400 });
  const data = parsed.data;
  try {
    if (data.action === "claim" || data.action === "claim_task") {
      const r =
        data.action === "claim"
          ? await db.rpc("connect_claim", { _connection: c.data.id })
          : await db.rpc("connect_claim_task", {
              _connection: c.data.id,
              ...(data.worker ? { _worker: data.worker } : {}),
            });
      if (r.error) throw r.error;
      const claim = r.data as ReplayClaim | null;
      // Replay runs re-decode stored raw payloads; the agent cannot read the bucket itself.
      // Payloads are inlined up to REPLAY_BUDGET bytes per claim. Letters beyond it are released
      // from this replay (still open, selectable again) and counted as deferred, so the run
      // reports replay_incomplete instead of holding gigabytes in one response.
      if (claim?.operation === "replay") await hydrateReplayClaim(claim, db);
      return Response.json({ claim });
    }
    const run = await db
      .from("connect_runs")
      .select("*")
      .eq("id", data.runId)
      .eq("connection_id", c.data.id)
      .maybeSingle();
    // One lease rule (connect_lease_valid): the run's lease, or the lease of the task that owns
    // the named stream. A task token never reaches another task's stream.
    const lease = await db.rpc("connect_lease_check", {
      _run: data.runId,
      _token: data.token,
      ...("streamId" in data && data.streamId ? { _stream: data.streamId } : {}),
    });
    if (!run.data || run.data.status !== "running" || lease.error || lease.data !== true)
      return Response.json({ error: "stale_attempt" }, { status: 409 });
    if (data.action === "heartbeat") {
      const r = await db.rpc("connect_heartbeat", { _run: data.runId, _token: data.token });
      if (r.error || !r.data) throw new Error("stale_attempt");
      return Response.json({ ok: true });
    }
    if (data.action === "lag") {
      const r = await db.rpc("connect_record_lag", {
        _run: data.runId,
        _token: data.token,
        _stream: data.streamId,
        _partitions: data.partitions as never,
      });
      if (r.error) throw r.error;
      return Response.json({ ok: true });
    }
    if (data.action === "split") {
      const r = await db.rpc("connect_split_task", {
        _run: data.runId,
        _token: data.token,
        _shards: data.shards,
      });
      if (r.error) throw r.error;
      return Response.json({ ok: true, tasks: r.data });
    }
    if (data.action === "report") {
      const r = await db.rpc("connect_report", {
        _run: data.runId,
        _token: data.token,
        _code: data.code,
        _detail: data.detail as never,
        ...(data.streamId ? { _stream: data.streamId } : {}),
      });
      if (r.error) throw r.error;
      return Response.json({ ok: true });
    }
    if (data.action === "finish") {
      const r = await db.rpc("connect_finish", {
        _run: data.runId,
        _token: data.token,
        _ok: data.ok,
        _code: data.code,
        ...(data.catalog ? { _catalog: data.catalog as never } : {}),
      });
      if (r.error) throw r.error;
      return Response.json({ ok: true });
    }
    // The task row: the one holding this lease (task mode; a split stream has one per partition)
    // or the stream's only row (run mode).
    const rsQuery = db
      .from("connect_run_streams")
      .select("*")
      .eq("run_id", data.runId)
      .eq("stream_id", data.streamId);
    const rs = await (
      run.data.lease_mode === "task"
        ? rsQuery.eq("lease_token", data.token)
        : rsQuery.eq("shard", "")
    ).maybeSingle();
    const s = await db
      .from("connect_streams")
      .select("*")
      .eq("id", data.streamId)
      .eq("connection_id", c.data.id)
      .maybeSingle();
    if (data.action === "upload_url" || data.action === "commit_file") {
      if (!rs.data || !s.data) throw new Error("stream_mismatch");
      // The part's object key is bound to this attempt's lease token: an executor of an earlier
      // attempt can only write keys this attempt never commits.
      const partDigest = await sha256(
        `spark:${data.runId}:${data.streamId}:${rs.data.shard}:${data.token}:${data.part}`,
      );
      const partBatch = `${partDigest.slice(0, 8)}-${partDigest.slice(8, 12)}-4${partDigest.slice(13, 16)}-a${partDigest.slice(17, 20)}-${partDigest.slice(20, 32)}`;
      // The extension names the part's format; the registry reads it from the key (migration 0106).
      const partKey = `${c.data.owner_id}/published/${partBatch}/landing.${data.format}`;
      const registered = await db
        .from("ingest_landing_batches")
        .select("id,sha256,row_count")
        .eq("source_id", c.data.source_id)
        .eq("batch_key", partBatch)
        .maybeSingle();
      if (registered.error) throw registered.error;
      if (data.action === "upload_url") {
        // Never a URL for a part that is already registered, and never one that overwrites.
        if (registered.data) throw new Error("part_registered");
        const signed = await db.storage
          .from("ingest-files")
          .createSignedUploadUrl(partKey, { upsert: false });
        if (signed.error || !signed.data) throw new Error("upload_url_failed");
        return Response.json({
          url: signed.data.signedUrl,
          connection_id: c.data.id,
          extracted_at: Date.parse(run.data.created_at),
        });
      }
      const part = await verifySparkPart(db, partKey, data, {
        connection: c.data.id,
        stream: s.data,
        schema: rs.data.schema_json,
        extractedAt: Date.parse(run.data.created_at),
      });
      const landedId = registered.data
        ? registered.data.sha256 === part.sha256 && registered.data.row_count === part.rows
          ? registered.data.id
          : null
        : (
            await db.rpc("register_ingest_landing_batch", {
              _actor: c.data.owner_id,
              _source: c.data.source_id,
              _key: partKey,
              _sha: part.sha256,
              _rows: part.rows,
              _mode: "append",
              _batch: partBatch,
            })
          ).data;
      if (!landedId) throw new Error("part_registration_failed");
      const dead = data.dead_letters ?? [];
      const target = s.data;
      if (
        dead.some(
          (m) =>
            m.dead_letter.stream !== target.name || m.dead_letter.namespace !== target.namespace,
        )
      )
        throw new Error("stream_mismatch");
      if (dead.length) {
        const refused = await storeDeadLetters(
          db,
          c.data.owner_id,
          data,
          // Bound to the attempt and part: another attempt may commit a different part at the
          // same sequence, while a retried request of this part rewrites the identical object.
          `dead:spark:${data.runId}:${data.streamId}:${rs.data.shard}:${data.token}:${data.part}:${data.sequence}`,
          dead,
        );
        if (refused) return refused;
      }
      const committed = await publishCommit(db, {
        ownerId: c.data.owner_id,
        batchKey: partBatch,
        landedId,
        objectKey: partKey,
        sha: part.sha256,
        rows: part.rows,
        bytes: part.bytes,
        data,
        shard: rs.data.shard,
        provenance: { engine: "spark", part: data.part, executor: data.executor ?? null },
      });
      return Response.json({ ok: true, commit_id: committed, sequence: data.sequence + 1 });
    }
    // Partition tasks share the stream and their own sequences, so the shard joins every
    // deterministic identity derived from the sequence. Unsplit streams keep the old identities.
    const taskKey = (prefix: string) =>
      rs.data?.shard
        ? `${prefix}${data.runId}:${data.streamId}:${rs.data.shard}:${data.sequence}`
        : `${prefix}${data.runId}:${data.streamId}:${data.sequence}`;
    const dead = data.dead_letters ?? [];
    if (
      !rs.data ||
      !s.data ||
      [...data.records.map((m) => m.record), ...dead.map((m) => m.dead_letter)].some(
        (m) => m.stream !== s.data?.name || m.namespace !== s.data.namespace,
      )
    )
      throw new Error("stream_mismatch");
    if (dead.length) {
      const refused = await storeDeadLetters(db, c.data.owner_id, data, taskKey("dead:"), dead);
      if (refused) return refused;
    }
    const emittedAt = Date.parse(run.data.created_at);
    const rows = canonicalEnvelopes(
      c.data.id,
      data.runId,
      data.streamId,
      data.records.map((m) => ({
        ...m,
        record: { ...m.record, emitted_at: emittedAt },
      })),
      rs.data.schema_json,
    );
    const digest = await sha256(taskKey(""));
    const batchId = `${digest.slice(0, 8)}-${digest.slice(8, 12)}-4${digest.slice(13, 16)}-a${digest.slice(17, 20)}-${digest.slice(20, 32)}`;
    const landed = await landSourceBatch(c.data.source_id, c.data.owner_id, {
      rows,
      batchId,
      mode: "append",
    });
    const batch = await db
      .from("ingest_landing_batches")
      .select("sha256,object_key")
      .eq("id", landed.id)
      .single();
    if (!batch.data) throw new Error("destination_failed");
    const bytes = new TextEncoder().encode(
      rows.map((row) => JSON.stringify(row) + "\n").join(""),
    ).length;
    const committed = await publishCommit(db, {
      ownerId: c.data.owner_id,
      batchKey: batchId,
      landedId: landed.id,
      objectKey: batch.data.object_key,
      sha: batch.data.sha256,
      rows: rows.length,
      bytes,
      data,
      shard: rs.data.shard,
    });
    return Response.json({ ok: true, commit_id: committed, sequence: data.sequence + 1 });
  } catch {
    return Response.json({ error: "operation_rejected", retryable: false }, { status: 409 });
  }
}
