// Reports ingestion objects in the ingest-files bucket that no
// database row references: dead-letter files, landing objects and Connect manifests left behind
// when a request failed after its upload. An object is kept when any of these hold:
//   - a row references it;
//   - it is younger than the grace period (a retry with the same id may still register it);
//   - it belongs to a Connect run that is still queued, running or waiting to retry, or its owner
//     has such a run (landing objects);
//   - it cannot be shown to be an ingestion object (other features also write under published/).
// Destructive apply is disabled until registrations and deletion share writer exclusion.
// Every candidate is written to storage_reconciliation_log; storage is never mutated.
import type { SupabaseClient } from "@supabase/supabase-js";

const BUCKET = "ingest-files";
const ACTIVE = ["queued", "running", "retry_wait"];
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
type Kind = "dead_letter" | "landing" | "manifest";
type Obj = { key: string; kind: Kind; bytes: number | null; createdAt: string | null };
export type ReconcileResult = {
  runId: string;
  dryRun: boolean;
  orphans: Obj[];
  kept: { referenced: number; young: number; active: number; foreign: number };
  deleted: number;
  failed: number;
};

async function list(db: SupabaseClient, prefix: string) {
  const out: {
    name: string;
    id: string | null;
    created_at?: string;
    metadata?: { size?: number };
  }[] = [];
  for (let offset = 0; ; offset += 1000) {
    const r = await db.storage.from(BUCKET).list(prefix, { limit: 1000, offset });
    if (r.error) throw new Error(`storage list failed for ${prefix}`);
    out.push(...(r.data as typeof out));
    if (r.data.length < 1000) return out;
  }
}
async function walk(db: SupabaseClient, prefix: string, files: Obj[], kind: Kind) {
  for (const e of await list(db, prefix)) {
    const key = `${prefix}/${e.name}`;
    if (e.id === null) await walk(db, key, files, kind);
    else
      files.push({ key, kind, bytes: e.metadata?.size ?? null, createdAt: e.created_at ?? null });
  }
}
async function referenced(db: SupabaseClient, objs: Obj[]) {
  const keys = objs.map((o) => o.key);
  const found = new Set<string>();
  const lookups: [string, string][] = [
    ["connect_dead_letters", "object_key"],
    ["ingest_dead_letters", "object_key"],
    ["ingest_landing_batches", "object_key"],
    ["connect_commits", "manifest_key"],
  ];
  for (let i = 0; i < keys.length; i += 200) {
    const chunk = keys.slice(i, i + 200);
    for (const [table, column] of lookups) {
      const r = await db.from(table).select(column).in(column, chunk);
      if (r.error) throw new Error(`reference lookup failed on ${table}`);
      for (const row of r.data as unknown as Record<string, string>[]) found.add(row[column]!);
    }
  }
  return found;
}
async function activeRun(db: SupabaseClient, runId: string) {
  const r = await db.from("connect_runs").select("status").eq("id", runId).maybeSingle();
  if (r.error) throw new Error("run lookup failed");
  return !!r.data && ACTIVE.includes(r.data.status);
}
async function ownerHasActiveRun(db: SupabaseClient, owner: string) {
  const c = await db.from("connect_connections").select("id").eq("owner_id", owner);
  if (c.error) throw new Error("connection lookup failed");
  const ids = (c.data ?? []).map((x) => x.id);
  if (!ids.length) return false;
  const r = await db
    .from("connect_runs")
    .select("id")
    .in("connection_id", ids)
    .in("status", ACTIVE)
    .limit(1);
  if (r.error) throw new Error("run lookup failed");
  return r.data.length > 0;
}

export async function reconcileIngestStorage(
  db: SupabaseClient,
  opts: { apply: boolean; minAgeHours: number; owners?: string[]; now?: Date },
): Promise<ReconcileResult> {
  if (opts.apply) throw new Error("destructive_cleanup_disabled");
  if (!(opts.minAgeHours >= 0)) throw new Error("minAgeHours must be >= 0");
  const now = opts.now ?? new Date();
  const runId = crypto.randomUUID();
  const owners =
    opts.owners ??
    (await list(db, "")).filter((e) => e.id === null && UUID.test(e.name)).map((e) => e.name);
  const kept = { referenced: 0, young: 0, active: 0, foreign: 0 };
  const orphans: Obj[] = [];
  for (const owner of owners) {
    if (!UUID.test(owner)) throw new Error("owner must be a user id");
    const objs: Obj[] = [];
    await walk(db, `${owner}/dead-letters`, objs, "dead_letter");
    for (const folder of await list(db, `${owner}/published`)) {
      if (folder.id !== null) continue;
      for (const f of await list(db, `${owner}/published/${folder.name}`)) {
        const kind: Kind | null =
          f.name === "landing.ndjson" ? "landing" : f.name === "manifest.json" ? "manifest" : null;
        if (kind && f.id !== null)
          objs.push({
            key: `${owner}/published/${folder.name}/${f.name}`,
            kind,
            bytes: f.metadata?.size ?? null,
            createdAt: f.created_at ?? null,
          });
      }
    }
    if (!objs.length) continue;
    const refs = await referenced(db, objs);
    const ownerActive = await ownerHasActiveRun(db, owner);
    for (const o of objs) {
      if (refs.has(o.key)) {
        kept.referenced++;
        continue;
      }
      const age = o.createdAt ? (now.getTime() - Date.parse(o.createdAt)) / 3_600_000 : 0;
      if (!o.createdAt || age < opts.minAgeHours) {
        kept.young++;
        continue;
      }
      // Connect dead letters live under dead-letters/<run id>/; push ones under dead-letters/ingest/.
      const runPart = o.key.split("/")[2] ?? "";
      if (o.kind === "dead_letter" && UUID.test(runPart) && (await activeRun(db, runPart))) {
        kept.active++;
        continue;
      }
      if (o.kind === "landing" && ownerActive) {
        kept.active++;
        continue;
      }
      if (o.kind === "manifest") {
        // Only Connect manifests (their content says so); anything else under published/ is
        // another feature's and is left alone.
        const file = await db.storage.from(BUCKET).download(o.key);
        let m: { protocol?: string; run_id?: string } | null = null;
        try {
          m = file.data && file.data.size < 65536 ? JSON.parse(await file.data.text()) : null;
        } catch {
          m = null;
        }
        if (m?.protocol !== "meridian-connect/1" || !UUID.test(m.run_id ?? "")) {
          kept.foreign++;
          continue;
        }
        if (await activeRun(db, m.run_id!)) {
          kept.active++;
          continue;
        }
      }
      orphans.push(o);
    }
  }
  for (let i = 0; i < orphans.length; i += 100) {
    const log = await db.from("storage_reconciliation_log").insert(
      orphans.slice(i, i + 100).map((o) => ({
        run_id: runId,
        dry_run: true,
        bucket: BUCKET,
        object_key: o.key,
        kind: o.kind,
        action: "would_delete",
        bytes: o.bytes,
        object_created_at: o.createdAt,
      })),
    );
    if (log.error) throw new Error("reconciliation audit write failed");
  }
  return { runId, dryRun: true, orphans, kept, deleted: 0, failed: 0 };
}
