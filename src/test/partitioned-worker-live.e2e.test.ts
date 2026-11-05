// @vitest-environment node
// Runs deploy/ingest/partitioned_worker.mjs as a separate process against real local services:
// - Kafka (deploy/ingest/test-sources.compose.yml): poison records under quarantine, skip and fail.
// - S3 (the local Supabase storage S3 endpoint): JSON lines read with byte ranges, records longer
//   than a range, and resuming an older { file, row } checkpoint.
// - Postgres (connect_source fixture database): the worker is killed mid-run, rows are changed
//   behind and ahead of its checkpoint, and the restarted worker produces exactly the documented
//   "mixed" result.
import { createClient } from "@supabase/supabase-js";
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn, type ChildProcess } from "node:child_process";
import { sha256 } from "@/lib/ingest";

const url = process.env["SUPABASE_URL"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"],
  app = process.env["APP_URL"] ?? "http://localhost:8080",
  brokers = process.env["KAFKA_TEST_BROKERS"],
  s3Key = process.env["S3_PROTOCOL_ACCESS_KEY_ID"],
  s3Secret = process.env["S3_PROTOCOL_ACCESS_KEY_SECRET"],
  s3Region = process.env["S3_PROTOCOL_REGION"] ?? "local",
  pgUrl = `postgres://postgres:${process.env["PGPASSWORD"] ?? "postgres"}@${process.env["CONNECT_TEST_HOST"] ?? "127.0.0.1"}:${process.env["CONNECT_TEST_PORT"] ?? "54322"}/${process.env["CONNECT_TEST_DB"] ?? "connect_source"}`;
const WORKER = "deploy/ingest/partitioned_worker.mjs";
const rand = () => crypto.randomUUID().replaceAll("-", "").slice(0, 10);
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let db: any;
let uid = "";

async function source(kind: string, pk: string[]) {
  const token = `mi_${crypto.randomUUID().replaceAll("-", "")}${crypto.randomUUID().replaceAll("-", "")}`;
  const s = await db
    .from("ingest_sources")
    .insert({
      owner_id: uid,
      name: `partitioned ${kind} ${rand()}`,
      kind,
      target_table: `pw_${kind}_${rand()}`,
      config: {},
      token_hash: await sha256(token),
      pk_columns: pk,
    })
    .select("id,target_table")
    .single();
  if (s.error) throw s.error;
  return { id: s.data.id as string, target: s.data.target_table as string, token };
}
async function plan(
  src: { id: string; target: string },
  predicate: Record<string, unknown>,
  pk: string[],
  batch = 100,
) {
  const r = await db.rpc("create_ingest_run_verified", {
    _actor: uid,
    _source: src.id,
    _target: src.target,
    _total: 0,
    _batch: batch,
    _concurrency: 1,
    _plan: { strategy: { kind: predicate["kind"], n: 1 } },
    _partitions: [{ idx: 0, label: "p0", predicate, estRows: 0 }],
    _pk: pk,
  });
  if (r.error) throw r.error;
  return r.data as string;
}
function start(token: string, args: string[], env: Record<string, string> = {}) {
  const p = spawn("node", [WORKER, "--threads", "1", ...args], {
    env: { ...process.env, MERIDIAN_URL: app, MERIDIAN_TOKEN: token, ...env },
  });
  let output = "";
  p.stdout.on("data", (d) => (output += d));
  p.stderr.on("data", (d) => (output += d));
  const done = new Promise<{ code: number | null; output: string }>((resolve) => {
    const timer = setTimeout(() => p.kill(), 120000);
    p.on("exit", (code) => {
      clearTimeout(timer);
      resolve({ code, output });
    });
  });
  return { p, done };
}
const partition = async (run: string) =>
  (await db.from("ingest_partitions").select("status,checkpoint").eq("run_id", run).single()).data;
async function rows(run: string) {
  const out = [];
  for (let from = 0; ; from += 1000) {
    const r = await db
      .from("ingest_rows")
      .select("data")
      .eq("run_id", run)
      .range(from, from + 999);
    if (r.error) throw r.error;
    out.push(...r.data.map((x: { data: Record<string, unknown> }) => x.data));
    if (r.data.length < 1000) return out;
  }
}

beforeAll(async () => {
  if (!url || !service) return;
  db = createClient(url, service, { auth: { persistSession: false } });
  const u = await db.auth.admin.createUser({
    email: `partitioned-${crypto.randomUUID()}@example.test`,
    password: `P-${crypto.randomUUID()}`,
    email_confirm: true,
  });
  if (u.error) throw u.error;
  uid = u.data.user.id;
}, 60000);
afterAll(async () => {
  if (uid) await db.auth.admin.deleteUser(uid).catch(() => undefined);
}, 60000);

describe.skipIf(!url || !service || !brokers)(
  "partitioned worker: Kafka poison records",
  { timeout: 240000 },
  () => {
    const topic = `pw-poison-${rand()}`;
    beforeAll(async () => {
      const { Kafka, logLevel } = await import("kafkajs");
      const kafka = new Kafka({ brokers: brokers!.split(","), logLevel: logLevel.NOTHING });
      const admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({ topics: [{ topic, numPartitions: 1 }], waitForLeaders: true });
      await admin.disconnect();
      const producer = kafka.producer();
      await producer.connect();
      await producer.send({
        topic,
        messages: [
          { value: JSON.stringify({ id: 1 }) },
          { value: "not json" },
          { value: null },
          { value: "42" },
          { value: JSON.stringify({ id: 2 }) },
        ],
      });
      await producer.disconnect();
    });
    const run = async (poison: string) => {
      const pk = ["_partition", "_offset"];
      const src = await source("kafka", pk);
      const id = await plan(
        src,
        { kind: "range", column: "partition", lo: 0, hi: 1, last: true },
        pk,
      );
      const w = start(src.token, [
        "--kind",
        "kafka",
        "--kafka-brokers",
        brokers!,
        "--topic",
        topic,
        "--poison",
        poison,
      ]);
      return { src, id, ...(await w.done) };
    };
    const letters = async (sourceId: string) =>
      (
        await db
          .from("ingest_dead_letters")
          .select("offset,error_code,object_key,line")
          .eq("source_id", sourceId)
      ).data.sort(
        (a: { offset: string }, b: { offset: string }) => Number(a.offset) - Number(b.offset),
      );

    it("quarantines malformed JSON, null values and non-object JSON, then finishes the partition", async () => {
      const r = await run("quarantine");
      expect(r.code, r.output).toBe(0);
      expect((await partition(r.id)).status).toBe("done");
      expect((await rows(r.id)).map((d) => d["id"]).sort()).toEqual([1, 2]);
      const dead = await letters(r.src.id);
      expect(
        dead.map((d: { offset: string; error_code: string }) => [d.offset, d.error_code]),
      ).toEqual([
        ["1", "invalid_json"],
        ["2", "invalid_event"],
        ["3", "schema_incompatible"],
      ]);
      const file = await db.storage.from("ingest-files").download(dead[0].object_key);
      const entry = JSON.parse((await file.data.text()).split("\n")[dead[0].line]);
      expect(Buffer.from(entry.value, "base64").toString()).toBe("not json");
    });
    it("skips them when told to, and says so", async () => {
      const r = await run("skip");
      expect(r.code, r.output).toBe(0);
      expect(r.output).toContain("skipped 3 poison record(s)");
      expect((await rows(r.id)).map((d) => d["id"]).sort()).toEqual([1, 2]);
      expect(await letters(r.src.id)).toEqual([]);
    });
    it("fails the partition at the first batch with poison and moves nothing", async () => {
      const r = await run("fail");
      expect(r.output).toContain("poison record(s) at 0:1 invalid_json");
      const p = await partition(r.id);
      expect(p.status).toBe("failed");
      expect(p.checkpoint).toBeNull();
      expect(await rows(r.id)).toEqual([]);
    });
  },
);

describe.skipIf(!url || !service || !s3Key || !s3Secret)(
  "partitioned worker: S3 ranged reads",
  { timeout: 240000 },
  () => {
    const bucket = `pw-s3-${rand()}`;
    const key = "data/part-0.jsonl";
    const long = "x".repeat(5000); // longer than the 1 KiB test range
    const lines = Array.from({ length: 1000 }, (_, i) =>
      JSON.stringify({ id: i, name: `名前-${i}`, ...(i === 500 ? { long } : {}) }),
    );
    const s3Args = () => [
      "--kind",
      "s3",
      "--bucket",
      bucket,
      "--s3-endpoint",
      `${url}/storage/v1/s3`,
      "--s3-force-path-style",
      "--s3-chunk-bytes",
      "1024",
    ];
    const s3Env = () => ({
      AWS_ACCESS_KEY_ID: s3Key!,
      AWS_SECRET_ACCESS_KEY: s3Secret!,
      AWS_REGION: s3Region,
    });
    // Run creation does not accept file predicates yet (S3 runs are not plannable in the app),
    // so the worker's file adapter is driven by setting the predicate as the service role.
    const filePlan = async (src: { id: string; target: string }) => {
      const id = await plan(src, { kind: "range", column: "id", lo: 0, hi: 1000, last: true }, [
        "id",
      ]);
      const u = await db
        .from("ingest_partitions")
        .update({ predicate: { kind: "file", files: [key] } })
        .eq("run_id", id);
      if (u.error) throw u.error;
      return id;
    };
    beforeAll(async () => {
      const b = await db.storage.createBucket(bucket, { public: false });
      if (b.error) throw b.error;
      // CRLF on some lines and a trailing line without a newline exercise record boundaries.
      const body = lines
        .map((l, i) => l + (i % 7 === 0 ? "\r\n" : "\n"))
        .join("")
        .trimEnd();
      const up = await db.storage.from(bucket).upload(key, new Blob([body]));
      if (up.error) throw up.error;
    });
    afterAll(async () => {
      await db.storage.from(bucket).remove([key]);
      await db.storage.deleteBucket(bucket);
    });

    it("reads every record through 1 KiB ranges, including one longer than a range", async () => {
      const src = await source("s3", ["id"]);
      const id = await filePlan(src);
      const r = await start(src.token, [...s3Args(), "--verbose"], s3Env()).done;
      expect(r.code, r.output).toBe(0);
      // Every request is a small byte range; the ~40 KB object is never fetched whole. The one
      // record longer than 1 KiB grows its range (doubling) until it fits.
      const spans = [...r.output.matchAll(/\[s3\] \S+ bytes=(\d+)-(\d+)/g)].map(
        (m) => Number(m[2]) - Number(m[1]) + 1,
      );
      expect(spans.length).toBeGreaterThan(20);
      expect(Math.max(...spans)).toBe(8192);
      expect(spans.filter((s) => s > 1024).length).toBeLessThanOrEqual(4);
      const got = (await rows(id)).sort((a, b) => Number(a["id"]) - Number(b["id"]));
      expect(got).toHaveLength(1000);
      expect(got.map((d) => d["id"])).toEqual(lines.map((_, i) => i));
      expect(got[999]!["name"]).toBe("名前-999");
      expect(got[500]!["long"]).toBe(long);
      expect(got[42]!["_line"]).toBe(42);
      expect((await partition(id)).checkpoint).toMatchObject({ file: 1, byte: 0, line: 0 });
    });
    it("resumes a checkpoint written by an older worker ({ file, row })", async () => {
      const src = await source("s3", ["id"]);
      const id = await filePlan(src);
      await db
        .from("ingest_partitions")
        .update({ checkpoint: { file: 0, row: 600 } })
        .eq("run_id", id);
      const r = await start(src.token, s3Args(), s3Env()).done;
      expect(r.code, r.output).toBe(0);
      const got = (await rows(id)).map((d) => Number(d["id"])).sort((a, b) => a - b);
      expect(got).toEqual(lines.slice(600).map((_, i) => 600 + i));
    });
  },
);

describe.skipIf(!url || !service)("partitioned ingest: concurrent snapshot marker", () => {
  it("marks a run mixed when a second worker process records a different snapshot", async () => {
    const src = await source("postgres", ["id"]);
    const id = await plan(src, { kind: "range", column: "id", lo: 1, hi: 10, last: true }, ["id"]);
    const call = (body: object) =>
      fetch(`${app}/api/public/ingest-partition`, {
        method: "POST",
        headers: { "content-type": "application/json", "x-ingest-token": src.token },
        body: JSON.stringify({ worker: "w", ...body }),
      });
    expect((await call({ action: "register", host: "h" })).status).toBe(200);
    const snap = (lsn: string) => ({ lsn, at: lsn, consistency: "single", note: "test" });
    expect((await call({ action: "snapshot", run_id: id, snapshot: snap("0/1") })).status).toBe(
      200,
    );
    const one = await db.from("ingest_runs").select("snapshot").eq("id", id).single();
    expect(one.data.snapshot.consistency).toBe("single");
    expect((await call({ action: "snapshot", run_id: id, snapshot: snap("0/2") })).status).toBe(
      200,
    );
    const two = await db.from("ingest_runs").select("snapshot").eq("id", id).single();
    expect(two.data.snapshot).toMatchObject({
      lsn: "0/1",
      consistency: "mixed",
      concurrent: { lsn: "0/2" },
    });
  });
});

describe.skipIf(!url || !service)(
  "partitioned worker: Postgres restart contract",
  { timeout: 300000 },
  () => {
    const table = `pw_restart_${rand()}`;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let pg: any;
    beforeAll(async () => {
      const { default: PG } = await import("pg");
      pg = new PG.Client({ connectionString: pgUrl });
      await pg.connect();
      await pg.query(`CREATE TABLE public.${table} (id int PRIMARY KEY, name text NOT NULL)`);
      await pg.query(
        `INSERT INTO public.${table} SELECT n, 'v1-' || n FROM generate_series(1, 20000) n`,
      );
    });
    afterAll(async () => {
      if (!pg) return;
      await pg.query(`DROP TABLE IF EXISTS public.${table}`);
      await pg.end();
    });

    it("after a kill, rows behind the checkpoint keep old values and rows ahead are re-read", async () => {
      const src = await source("postgres", ["id"]);
      const id = await plan(src, { kind: "range", column: "id", lo: 1, hi: 20000, last: true }, [
        "id",
      ]);
      const args = ["--kind", "postgres", "--url", pgUrl, "--table", `public.${table}`];
      const first = start(src.token, args);
      let killed: ChildProcess | null = null;
      for (let i = 0; i < 600 && !killed; i++) {
        const p = await partition(id);
        if (p?.checkpoint?.last?.[0] >= 300) {
          first.p.kill("SIGKILL");
          killed = first.p;
        } else await new Promise((r) => setTimeout(r, 50));
      }
      await first.done;
      expect(killed, "worker finished before it could be killed").not.toBeNull();
      const at = Number((await partition(id)).checkpoint.last[0]);
      expect(at).toBeLessThan(20000);
      // Behind the checkpoint, ahead of it, and a delete ahead of it.
      await pg.query(`UPDATE public.${table} SET name = 'v2' WHERE id IN (1, 19999)`);
      await pg.query(`DELETE FROM public.${table} WHERE id = 19998`);
      await pg.query(`DELETE FROM public.${table} WHERE id = 2`);
      // The killed worker's lease would expire after its lease time; expire it now.
      await db
        .from("ingest_partitions")
        .update({ lease_until: new Date(Date.now() - 1000).toISOString() })
        .eq("run_id", id);
      const second = await start(src.token, args).done;
      expect(second.code, second.output).toBe(0);
      expect((await partition(id)).status).toBe("done");
      const byId = new Map((await rows(id)).map((d) => [Number(d["id"]), d["name"]]));
      expect(byId.get(1)).toBe("v1-1"); // updated behind the checkpoint: not re-read
      expect(byId.get(2)).toBe("v1-2"); // deleted behind the checkpoint: still in the run
      expect(byId.get(19999)).toBe("v2"); // updated ahead: new snapshot
      expect(byId.has(19998)).toBe(false); // deleted ahead: never read
      expect(byId.size).toBe(19999);
      const r = await db.from("ingest_runs").select("snapshot").eq("id", id).single();
      expect(r.data.snapshot.consistency).toBe("mixed");
    });
  },
);
