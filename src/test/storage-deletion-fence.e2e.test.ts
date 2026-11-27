// @vitest-environment node
// Writer exclusion for ingestion storage cleanup (migration 0124), with real concurrent sessions
// on the local stack. Each interleaving of a writer registering an object key and the
// reconciliation fencing it ends with exactly one of them winning, and never with a fenced key
// that is referenced. A randomized run of concurrent writers and fencers checks the same
// invariant. Needs the local Supabase database; skips otherwise.
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { Client } from "pg";

const url = process.env["SUPABASE_URL"];
const connect = async () => {
  const c = new Client({
    host: process.env["CONNECT_TEST_HOST"] ?? "127.0.0.1",
    port: Number(process.env["CONNECT_TEST_PORT"] ?? 54322),
    user: "postgres",
    password: process.env["PGPASSWORD"] ?? "postgres",
    database: "postgres",
  });
  await c.connect();
  // The reconciliation RPCs are service-only.
  await c.query("select set_config('request.jwt.claim.role','service_role',false)");
  return c;
};

describe.skipIf(!url)("Storage deletion fence", { timeout: 300000 }, () => {
  let admin: Client, writer: Client, cleaner: Client;
  let uid = "",
    source = "",
    connection = "";
  const reconcileRun = crypto.randomUUID();
  const key = (name = crypto.randomUUID()) => `${uid}/published/${name}/landing.ndjson`;
  const register = (c: Client, k: string) =>
    c.query(
      `insert into public.ingest_landing_batches(source_id,owner_id,object_key,sha256,mode,batch_key)
       values($1,$2,$3,repeat('0',64),'append',gen_random_uuid())`,
      [source, uid, k],
    );
  const fence = async (c: Client, k: string, kind = "landing", writerRun: string | null = null) =>
    (
      await c.query("select public.storage_fence_object($1,$2,$3,$4) r", [
        k,
        kind,
        reconcileRun,
        writerRun,
      ])
    ).rows[0].r as string | null;
  const fenced = async (k: string) =>
    (await admin.query("select 1 from public.storage_deletion_fences where object_key=$1", [k]))
      .rowCount === 1;
  // Resolves once `c`'s backend is blocked on the key lock.
  const blocked = async (c: Client) => {
    const pid = (c as unknown as { processID: number }).processID;
    for (let i = 0; i < 200; i++) {
      const r = await admin.query(
        "select 1 from pg_locks where pid=$1 and locktype='advisory' and not granted",
        [pid],
      );
      if (r.rowCount) return;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    throw new Error("session never waited on the key lock");
  };

  beforeAll(async () => {
    admin = await connect();
    writer = await connect();
    cleaner = await connect();
    uid = crypto.randomUUID();
    await admin.query("insert into auth.users(id,email,email_confirmed_at) values($1,$2,now())", [
      uid,
      `${uid}@example.test`,
    ]);
    source = (
      await admin.query(
        "insert into public.ingest_sources(owner_id,name,kind,target_table,config) values($1,'fence','kafka','fence','{}') returning id",
        [uid],
      )
    ).rows[0].id;
    connection = (
      await admin.query(
        "insert into public.connect_connections(source_id,owner_id,name,credential_ref) values($1,$2,'fence','x') returning id",
        [source, uid],
      )
    ).rows[0].id;
  });
  afterAll(async () => {
    if (admin && uid) {
      await admin.query("delete from public.storage_deletion_fences where reconcile_run=$1", [
        reconcileRun,
      ]);
      await admin.query("delete from public.ingest_dead_letters where owner_id=$1", [uid]);
      await admin.query("delete from public.ingest_landing_batches where owner_id=$1", [uid]);
      await admin.query("delete from public.connect_runs where connection_id=$1", [connection]);
      await admin.query("delete from public.connect_connections where id=$1", [connection]);
      await admin.query("delete from public.ingest_sources where id=$1", [source]);
      await admin.query("delete from auth.users where id=$1", [uid]);
    }
    await Promise.all([admin, writer, cleaner].map((c) => c?.end()));
  });

  it("a registration that commits while the fence waits keeps the object", async () => {
    const k = key();
    await writer.query("begin");
    await register(writer, k);
    const decision = fence(cleaner, k);
    await blocked(cleaner);
    await writer.query("commit");
    expect(await decision).toBe("referenced");
    expect(await fenced(k)).toBe(false);
  });

  it("a registration that rolls back while the fence waits lets the fence through", async () => {
    const k = key();
    await writer.query("begin");
    await register(writer, k);
    const decision = fence(cleaner, k);
    await blocked(cleaner);
    await writer.query("rollback");
    expect(await decision).toBeNull();
    expect(await fenced(k)).toBe(true);
  });

  it("a registration that arrives while a fence commits is refused, on every table", async () => {
    const k = key();
    await cleaner.query("begin");
    expect(await fence(cleaner, k)).toBeNull();
    const attempt = register(writer, k).then(
      () => "registered",
      (e: Error) => e.message,
    );
    await blocked(writer);
    await cleaner.query("commit");
    expect(await attempt).toBe("storage_object_fenced");
    // The push dead-letter path is refused the same way.
    await expect(
      writer.query(
        `insert into public.ingest_dead_letters(source_id,owner_id,batch_id,topic,partition,"offset",error_code,object_key,line)
         values($1,$2,gen_random_uuid(),'t',0,'1','invalid_json',$3,0)`,
        [source, uid, k],
      ),
    ).rejects.toThrow("storage_object_fenced");
    // Every table that references ingest-files objects carries the fence trigger.
    const triggers = await admin.query(
      `select c.relname from pg_trigger t join pg_class c on c.oid=t.tgrelid
       where t.tgname like 'storage_fence_%' and not t.tgisinternal order by 1`,
    );
    expect(triggers.rows.map((r) => r.relname)).toEqual([
      "connect_commits",
      "connect_dead_letters",
      "ingest_dead_letters",
      "ingest_landing_batches",
    ]);
    // Once the fence is released (after the delete), the key can be registered again.
    expect(
      (await admin.query("select public.storage_release_fence($1,$2) r", [k, reconcileRun])).rows[0]
        .r,
    ).toBe(true);
    await register(writer, k);
  });

  it("keeps objects a live writer run can still register", async () => {
    const run = (
      await admin.query(
        "insert into public.connect_runs(connection_id,request_key,operation,status) values($1,gen_random_uuid(),'sync','running') returning id",
        [connection],
      )
    ).rows[0].id as string;
    // A dead letter of the running run, a landing object while the owner has a running run (a
    // Spark part is uploaded before it is registered) and a manifest naming the running run.
    expect(await fence(cleaner, `${uid}/dead-letters/${run}/x.ndjson`, "dead_letter")).toBe(
      "active",
    );
    expect(await fence(cleaner, key())).toBe("active");
    const manifest = `${uid}/published/${crypto.randomUUID()}/manifest.json`;
    expect(await fence(cleaner, manifest, "manifest", run)).toBe("active");
    // A manifest whose run is unknown is not provably a Connect object.
    expect(await fence(cleaner, manifest, "manifest", null)).toBe("foreign");
    await admin.query("update public.connect_runs set status='failed' where id=$1", [run]);
    expect(await fence(cleaner, manifest, "manifest", run)).toBeNull();
    // A fenced key reports as fenced to the dry-run check.
    expect(
      (
        await cleaner.query("select public.storage_reconcile_check($1,'manifest',$2) r", [
          manifest,
          run,
        ])
      ).rows[0].r,
    ).toBe("fenced");
  });

  it("is service-only", async () => {
    for (const fn of [
      "storage_reconcile_check(text,text,uuid)",
      "storage_fence_object(text,text,uuid,uuid)",
      "storage_release_fence(text,uuid)",
      "storage_keep_reason(text,text,uuid)",
      "storage_key_lock(text)",
    ])
      for (const role of ["anon", "authenticated"])
        expect(
          (
            await admin.query("select has_function_privilege($1,$2,'EXECUTE') p", [
              role,
              `public.${fn}`,
            ])
          ).rows[0].p,
          `${role} ${fn}`,
        ).toBe(false);
    for (const role of ["anon", "authenticated"])
      expect(
        (
          await admin.query(
            "select has_table_privilege($1,'public.storage_deletion_fences','SELECT') p",
            [role],
          )
        ).rows[0].p,
      ).toBe(false);
  });

  it("never ends with a fenced key that is referenced, under concurrent writers and fencers", async () => {
    const keys = Array.from({ length: 40 }, () => key());
    const sessions = await Promise.all(Array.from({ length: 6 }, connect));
    const pick = () => keys[Math.floor(Math.random() * keys.length)]!;
    const outcomes = { registered: 0, refused: 0, fenced: 0, kept: 0 };
    try {
      await Promise.all(
        sessions.map(async (c, i) => {
          for (let n = 0; n < 60; n++) {
            const k = pick();
            if (i % 2 === 0) {
              // Writer: an explicit transaction that holds its registration briefly.
              await c.query("begin");
              try {
                await register(c, k);
                await new Promise((resolve) => setTimeout(resolve, Math.random() * 5));
                await c.query("commit");
                outcomes.registered++;
              } catch (e) {
                await c.query("rollback");
                const message = (e as Error).message;
                if (message === "storage_object_fenced") outcomes.refused++;
                else if (!/duplicate key/.test(message)) throw e;
              }
            } else if ((await fence(c, k)) === null) outcomes.fenced++;
            else outcomes.kept++;
          }
        }),
      );
    } finally {
      await Promise.all(sessions.map((c) => c.end()));
    }
    const broken = await admin.query(
      `select f.object_key from public.storage_deletion_fences f
       join public.ingest_landing_batches b on b.object_key=f.object_key
       where f.object_key = any($1)`,
      [keys],
    );
    expect(broken.rows).toEqual([]);
    // Both sides actually won some races.
    expect(outcomes.registered).toBeGreaterThan(0);
    expect(outcomes.fenced).toBeGreaterThan(0);
  });
});
