import { agentRequest, canonicalEnvelopes, deadLetterMessage } from "./connect-protocol";
import type { SupabaseClient } from "@supabase/supabase-js";
import { readBodyCapped } from "./request-limits.server";
import { sha256 } from "./ingest";
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
    if (data.action === "claim") {
      const r = await db.rpc("connect_claim", { _connection: c.data.id });
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
    if (
      !run.data ||
      run.data.status !== "running" ||
      run.data.lease_token !== data.token ||
      !run.data.lease_until ||
      Date.parse(run.data.lease_until) <= Date.now()
    )
      return Response.json({ error: "stale_attempt" }, { status: 409 });
    if (data.action === "heartbeat") {
      const r = await db.rpc("connect_heartbeat", { _run: data.runId, _token: data.token });
      if (r.error || !r.data) throw new Error("stale_attempt");
      return Response.json({ ok: true });
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
    const rs = await db
      .from("connect_run_streams")
      .select("*")
      .eq("run_id", data.runId)
      .eq("stream_id", data.streamId)
      .maybeSingle();
    const s = await db
      .from("connect_streams")
      .select("*")
      .eq("id", data.streamId)
      .eq("connection_id", c.data.id)
      .maybeSingle();
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
      // Stored before the commit that moves the checkpoint past them; retried sequences rewrite
      // the identical object and the RPC ignores positions it already holds.
      const digest = await sha256(`dead:${data.runId}:${data.streamId}:${data.sequence}`);
      const objectKey = `${c.data.owner_id}/dead-letters/${data.runId}/${digest.slice(0, 32)}.ndjson`;
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
    const digest = await sha256(`${data.runId}:${data.streamId}:${data.sequence}`);
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
    const manifestKey = `${c.data.owner_id}/published/${batchId}/manifest.json`;
    const manifest = {
      protocol: "meridian-connect/1",
      run_id: data.runId,
      stream_id: data.streamId,
      sequence: data.sequence,
      batch_id: landed.id,
      object_key: batch.data.object_key,
      sha256: batch.data.sha256,
      rows: rows.length,
      bytes,
      checkpoint: data.state.state,
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
      _run: data.runId,
      _token: data.token,
      _stream: data.streamId,
      _sequence: data.sequence,
      _batch: landed.id,
      _manifest: manifestKey,
      _sha: batch.data.sha256,
      _rows: rows.length,
      _bytes: bytes,
      _checkpoint: data.state.state as never,
      _completed: data.state.state.completed ?? false,
    });
    if (committed.error) throw committed.error;
    return Response.json({ ok: true, commit_id: committed.data, sequence: data.sequence + 1 });
  } catch {
    return Response.json({ error: "operation_rejected", retryable: false }, { status: 409 });
  }
}
