// @vitest-environment node
// Ingestion storage reconciliation against the live local stack, scoped to a throwaway user so
// nothing else in the shared bucket is touched.
import { createClient } from "@supabase/supabase-js";
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { reconcileIngestStorage } from "@/lib/storage-reconcile.server";
import { sha256 } from "@/lib/ingest";
const url = process.env["SUPABASE_URL"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"];
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let db: any;
let uid = "",
  activeRun = "",
  finishedRun = "";
const k = (path: string) => `${uid}/${path}`;
const put = async (path: string, body: string) => {
  const r = await db.storage
    .from("ingest-files")
    .upload(k(path), new Blob([body]), { upsert: true });
  if (r.error) throw r.error;
};
const exists = async (path: string) => {
  const dir = k(path).split("/").slice(0, -1).join("/");
  const r = await db.storage.from("ingest-files").list(dir);
  return (r.data ?? []).some((e: { name: string }) => e.name === path.split("/").pop());
};
describe.skipIf(!url || !service)("Ingestion storage reconciliation", { timeout: 120000 }, () => {
  const ids = Array.from({ length: 6 }, () => crypto.randomUUID());
  beforeAll(async () => {
    db = createClient(url!, service!, { auth: { persistSession: false } });
    const u = await db.auth.admin.createUser({
      email: `reconcile-${crypto.randomUUID()}@example.test`,
      password: `P-${crypto.randomUUID()}`,
      email_confirm: true,
    });
    if (u.error) throw u.error;
    uid = u.data.user.id;
    const src = await db
      .from("ingest_sources")
      .insert({
        owner_id: uid,
        name: "reconcile",
        kind: "kafka",
        target_table: "reconcile_t",
        config: {},
        token_hash: await sha256(crypto.randomUUID()),
      })
      .select("id")
      .single();
    if (src.error) throw src.error;
    const conn = await db
      .from("connect_connections")
      .insert({ source_id: src.data.id, owner_id: uid, name: "reconcile", credential_ref: "x" })
      .select("id")
      .single();
    if (conn.error) throw conn.error;
    for (const status of ["running", "failed"]) {
      const r = await db
        .from("connect_runs")
        .insert({
          connection_id: conn.data.id,
          request_key: crypto.randomUUID(),
          operation: "sync",
          status,
        })
        .select("id")
        .single();
      if (r.error) throw r.error;
      if (status === "running") activeRun = r.data.id;
      else finishedRun = r.data.id;
    }
    // Referenced push dead letter, orphan push dead letter.
    await put(`dead-letters/ingest/${src.data.id}/${ids[0]}.ndjson`, '{"value":"a"}');
    const ref = await db.from("ingest_dead_letters").insert({
      source_id: src.data.id,
      owner_id: uid,
      batch_id: ids[0],
      topic: "t",
      partition: 0,
      offset: "1",
      error_code: "invalid_json",
      object_key: k(`dead-letters/ingest/${src.data.id}/${ids[0]}.ndjson`),
      line: 0,
    });
    if (ref.error) throw ref.error;
    await put(`dead-letters/ingest/${src.data.id}/${ids[1]}.ndjson`, '{"value":"orphan"}');
    // Connect dead letters of an active run (kept) and of a finished run (orphan).
    await put(`dead-letters/${activeRun}/a.ndjson`, "{}");
    await put(`dead-letters/${finishedRun}/b.ndjson`, "{}");
    // Connect manifest of a finished run without a commit (orphan); another feature's
    // manifest.json under published/ (never touched); a part file (never considered).
    await put(
      `published/${ids[2]}/manifest.json`,
      JSON.stringify({ protocol: "meridian-connect/1", run_id: finishedRun }),
    );
    await put(`published/${ids[3]}/manifest.json`, JSON.stringify({ table: "file table" }));
    await put(`published/${ids[3]}/part-00000.ndjson`, '{"id":1}');
    // An unregistered landing object while the owner has an active run (kept).
    await put(`published/${ids[4]}/landing.ndjson`, '{"id":1}');
  }, 60000);
  afterAll(async () => {
    if (!db || !uid) return;
    const all: string[] = [];
    const walk = async (p: string) => {
      for (const e of (await db.storage.from("ingest-files").list(p)).data ?? [])
        if (e.id === null) await walk(`${p}/${e.name}`);
        else all.push(`${p}/${e.name}`);
    };
    await walk(uid);
    if (all.length) await db.storage.from("ingest-files").remove(all);
    await db.auth.admin.deleteUser(uid);
  }, 60000);

  it("dry run lists only provable orphans and records them without deleting", async () => {
    const r = await reconcileIngestStorage(db, { apply: false, minAgeHours: 0, owners: [uid] });
    expect(r.orphans.map((o) => o.key).sort()).toEqual(
      [
        k(`dead-letters/${finishedRun}/b.ndjson`),
        k(
          `dead-letters/ingest/${(await db.from("ingest_sources").select("id").eq("owner_id", uid).single()).data.id}/${ids[1]}.ndjson`,
        ),
        k(`published/${ids[2]}/manifest.json`),
      ].sort(),
    );
    expect(r.kept).toEqual({ referenced: 1, young: 0, active: 2, foreign: 1 });
    expect(await exists(`dead-letters/${finishedRun}/b.ndjson`)).toBe(true);
    const log = await db
      .from("storage_reconciliation_log")
      .select("action,dry_run")
      .eq("run_id", r.runId);
    expect(log.data).toHaveLength(3);
    expect(
      log.data.every(
        (l: { action: string; dry_run: boolean }) => l.action === "would_delete" && l.dry_run,
      ),
    ).toBe(true);
  });

  it("respects the grace period", async () => {
    const r = await reconcileIngestStorage(db, { apply: false, minAgeHours: 1, owners: [uid] });
    expect(r.orphans).toEqual([]);
    // Everything not already referenced is younger than an hour.
    expect(r.kept).toMatchObject({ referenced: 1, young: 6 });
  });

  it("refuses destructive apply, preserves objects, and keeps dry-run logs append-only", async () => {
    await expect(
      reconcileIngestStorage(db, { apply: true, minAgeHours: 0, owners: [uid] }),
    ).rejects.toThrow("destructive_cleanup_disabled");
    const r = await reconcileIngestStorage(db, { apply: false, minAgeHours: 0, owners: [uid] });
    expect(r.deleted).toBe(0);
    expect(await exists(`dead-letters/${finishedRun}/b.ndjson`)).toBe(true);
    expect(await exists(`published/${ids[2]}/manifest.json`)).toBe(true);
    for (const kept of [
      `dead-letters/${activeRun}/a.ndjson`,
      `published/${ids[3]}/manifest.json`,
      `published/${ids[3]}/part-00000.ndjson`,
      `published/${ids[4]}/landing.ndjson`,
    ])
      expect(await exists(kept), kept).toBe(true);
    const log = await db
      .from("storage_reconciliation_log")
      .select("id,action")
      .eq("run_id", r.runId);
    expect(log.data.map((l: { action: string }) => l.action)).toEqual([
      "would_delete",
      "would_delete",
      "would_delete",
    ]);
    const tamper = await db
      .from("storage_reconciliation_log")
      .update({ action: "would_delete" })
      .eq("id", log.data[0].id);
    expect(tamper.error).toBeTruthy();
    // With the active run finished, the landing object and its dead letters become orphans.
    await db.from("connect_runs").update({ status: "failed" }).eq("id", activeRun);
    const second = await reconcileIngestStorage(db, {
      apply: false,
      minAgeHours: 0,
      owners: [uid],
    });
    expect(second.orphans.map((o) => o.kind).sort()).toEqual([
      "dead_letter",
      "dead_letter",
      "dead_letter",
      "landing",
      "manifest",
    ]);
  });
});
