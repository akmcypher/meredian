// @vitest-environment node
// Runs the generated MySQL and Kafka ingest workers (workerScript) against real local services
// and the real push endpoint, then reads the landed batches back from storage.
// Needs the dev server on 8080 and deploy/ingest/test-sources.compose.yml; scripts/e2e-local.mjs
// sets MYSQL_TEST_URL / KAFKA_TEST_BROKERS only when those services answer.
import { createClient } from "@supabase/supabase-js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { spawn } from "node:child_process";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { sha256, workerScript, type IngestKind } from "@/lib/ingest";

const url = process.env["SUPABASE_URL"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"],
  mysqlUrl = process.env["MYSQL_TEST_URL"],
  brokers = process.env["KAFKA_TEST_BROKERS"];
const ENDPOINT = `${process.env["APP_URL"] ?? "http://localhost:8080"}/api/public/ingest`;
// Worker scripts import their drivers, so they must sit inside the repo's node_modules tree.
const WORK_ROOT = join(process.cwd(), "node_modules", ".cache", "ingest-worker-tests");
const BAD_TOKEN = `mi_${"0".repeat(64)}`;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let db: any;
let uid = "";
const rand = () => crypto.randomUUID().replaceAll("-", "").slice(0, 12);

async function ensureUser() {
  if (uid) return;
  db = createClient(url!, service!, { auth: { persistSession: false } });
  const u = await db.auth.admin.createUser({
    email: `ingest-workers-${crypto.randomUUID()}@example.test`,
    password: `P-${crypto.randomUUID()}`,
    email_confirm: true,
  });
  if (u.error) throw u.error;
  uid = u.data.user.id;
}

async function createSource(kind: IngestKind, config: Record<string, unknown>) {
  const token = `mi_${crypto.randomUUID().replaceAll("-", "")}${crypto.randomUUID().replaceAll("-", "")}`;
  const r = await db.rpc("create_ingest_source_verified", {
    _actor: uid,
    _name: `${kind} live test`,
    _kind: kind,
    _target: `${kind}_live_${rand()}`,
    _config: config,
    _headers: {},
    _hash: await sha256(token),
  });
  if (r.error) throw r.error;
  return { id: r.data as string, token };
}

async function writeWorker(kind: IngestKind, cfg: Parameters<typeof workerScript>[2]) {
  await mkdir(WORK_ROOT, { recursive: true });
  const folder = await mkdtemp(join(WORK_ROOT, `${kind}-`));
  const file = join(folder, "worker.mjs");
  await writeFile(file, workerScript(kind, ENDPOINT, cfg));
  return { folder, file };
}

function runWorker(file: string, env: Record<string, string>, timeoutMs = 90000) {
  return new Promise<{ code: number | null; output: string }>((resolve) => {
    const p = spawn("node", [file], { env: { ...process.env, ...env } });
    let output = "";
    p.stdout.on("data", (d) => (output += d));
    p.stderr.on("data", (d) => (output += d));
    const timer = setTimeout(() => p.kill(), timeoutMs);
    p.on("exit", (code) => {
      clearTimeout(timer);
      resolve({ code, output });
    });
  });
}

type Batch = { object_key: string; row_count: number; mode: string };
async function batches(sourceId: string): Promise<Batch[]> {
  const r = await db
    .from("ingest_landing_batches")
    .select("object_key,row_count,mode,created_at")
    .eq("source_id", sourceId)
    .order("created_at");
  if (r.error) throw r.error;
  return r.data;
}

async function records(batch: Batch) {
  const r = await db.storage.from("ingest-files").download(batch.object_key);
  if (r.error) throw r.error;
  return (await r.data.text())
    .split("\n")
    .filter(Boolean)
    .map((line: string) => JSON.parse(line));
}

afterAll(async () => {
  if (uid) await db.auth.admin.deleteUser(uid).catch(() => undefined);
});

describe.skipIf(!url || !service || !mysqlUrl)(
  "MySQL ingest worker → push endpoint → landing",
  { timeout: 240000 },
  () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let mysql: any;
    let source = { id: "", token: "" };
    let worker = { folder: "", file: "" };
    const table = `customers_${rand()}`;
    const insert = async (from: number, to: number) => {
      for (let start = from; start <= to; start += 1000) {
        const rows = [];
        for (let id = start; id <= Math.min(to, start + 999); id++)
          rows.push([id, `customer-${id}@example.test`]);
        await mysql.query(`INSERT INTO meridian_ingest_test.${table} (id, email) VALUES ?`, [rows]);
      }
    };
    const env = () => ({
      ONCE: "1",
      DATABASE_URL: mysqlUrl!,
      INGEST_TOKEN: source.token,
      STATE_FILE: join(worker.folder, "cursor.json"),
    });

    beforeAll(async () => {
      await ensureUser();
      const driver = await import("mysql2/promise");
      mysql = await driver.createConnection(mysqlUrl!);
      await mysql.query("CREATE DATABASE IF NOT EXISTS meridian_ingest_test");
      await mysql.query(
        `CREATE TABLE meridian_ingest_test.${table} (id INT PRIMARY KEY, email VARCHAR(100) NOT NULL)`,
      );
      await insert(1, 1200);
      const cfg = { query: `select * from meridian_ingest_test.${table}`, cursor: "id", key: "id" };
      source = await createSource("mysql", cfg);
      worker = await writeWorker("mysql", cfg);
    });
    afterAll(async () => {
      if (!mysql) return;
      await mysql.query(`DROP TABLE IF EXISTS meridian_ingest_test.${table}`);
      await mysql.end();
    });

    it("lands every row of the first sync as one immutable batch", async () => {
      const run = await runWorker(worker.file, env());
      expect(run.code, run.output).toBe(0);
      const landed = await batches(source.id);
      expect(landed.map((b) => [b.row_count, b.mode])).toEqual([[1200, "upsert"]]);
      const rows = await records(landed[0]!);
      expect(rows.map((r: { id: number }) => r.id)).toEqual(
        Array.from({ length: 1200 }, (_, i) => i + 1),
      );
      expect(rows[0]).toEqual({ id: 1, email: "customer-1@example.test" });
    });

    it("resumes from the saved cursor after a restart and lands nothing when idle", async () => {
      await insert(1201, 1205);
      const run = await runWorker(worker.file, env());
      expect(run.code, run.output).toBe(0);
      const landed = await batches(source.id);
      expect(landed.map((b) => b.row_count)).toEqual([1200, 5]);
      expect((await records(landed[1]!)).map((r: { id: number }) => r.id)).toEqual([
        1201, 1202, 1203, 1204, 1205,
      ]);
      const idle = await runWorker(worker.file, env());
      expect(idle.code, idle.output).toBe(0);
      expect(await batches(source.id)).toHaveLength(2);
    });

    it("keeps the cursor when the endpoint rejects a batch, then lands it on the next run", async () => {
      await insert(1206, 1206);
      const rejected = await runWorker(worker.file, { ...env(), INGEST_TOKEN: BAD_TOKEN });
      expect(rejected.code).toBe(1);
      expect(rejected.output).toContain("batch not landed (status 401)");
      expect(await batches(source.id)).toHaveLength(2);
      const run = await runWorker(worker.file, env());
      expect(run.code, run.output).toBe(0);
      const landed = await batches(source.id);
      expect((await records(landed[2]!)).map((r: { id: number }) => r.id)).toEqual([1206]);
    });

    it("pages through more than 5000 new rows in one run", async () => {
      await insert(1207, 6209);
      const run = await runWorker(worker.file, env());
      expect(run.code, run.output).toBe(0);
      const landed = await batches(source.id);
      expect(landed.slice(3).map((b) => b.row_count)).toEqual([5000, 3]);
      const last = await records(landed[4]!);
      expect(last.map((r: { id: number }) => r.id)).toEqual([6207, 6208, 6209]);
    });
  },
);

describe.skipIf(!url || !service || !brokers)(
  "Kafka CDC ingest worker → push endpoint → landing",
  { timeout: 240000 },
  () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let kafka: any;
    let source = { id: "", token: "" };
    let worker = { folder: "", file: "" };
    const topic = `meridian-live-${rand()}`;
    const group = `meridian-live-${rand()}`;
    const envelope = (op: string, before: object | null, after: object | null) =>
      JSON.stringify({ payload: { op, before, after, source: { table: "customers" } } });
    const produce = async (values: (string | null)[]) => {
      const producer = kafka.producer();
      await producer.connect();
      await producer.send({ topic, messages: values.map((value) => ({ value })) });
      await producer.disconnect();
    };
    const env = () => ({
      KAFKA_BROKERS: brokers!,
      KAFKA_GROUP: group,
      IDLE_EXIT_MS: "8000",
      INGEST_TOKEN: source.token,
    });
    const allEvents = async () => {
      const out = [];
      for (const b of await batches(source.id)) out.push(...(await records(b)));
      return out;
    };

    beforeAll(async () => {
      await ensureUser();
      const { Kafka, logLevel } = await import("kafkajs");
      kafka = new Kafka({ brokers: brokers!.split(","), logLevel: logLevel.NOTHING });
      const admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({ topics: [{ topic, numPartitions: 1 }], waitForLeaders: true });
      await admin.disconnect();
      source = await createSource("kafka_cdc", { topic, key: "id" });
      worker = await writeWorker("kafka_cdc", { topic });
    });

    const deadLetters = async () => {
      const r = await db
        .from("ingest_dead_letters")
        .select("topic,partition,offset,error_code,object_key,line")
        .eq("source_id", source.id);
      if (r.error) throw r.error;
      // Offsets are stored as text (64-bit safe); order them as numbers.
      return r.data.sort(
        (a: { offset: string }, b: { offset: string }) => Number(a.offset) - Number(b.offset),
      );
    };
    const rawValue = async (d: { object_key: string; line: number }) => {
      const r = await db.storage.from("ingest-files").download(d.object_key);
      const entry = JSON.parse((await r.data.text()).split("\n")[d.line]);
      return Buffer.from(entry.value, "base64").toString();
    };

    it("lands Debezium create/update/delete events, skips tombstones and control records, dead-letters junk", async () => {
      await produce([
        envelope("c", null, { id: 1, email: "a@example.test" }),
        envelope("c", null, { id: 2, email: "b@example.test" }),
        envelope("u", { id: 1, email: "a@example.test" }, { id: 1, email: "a2@example.test" }),
        envelope("d", { id: 2, email: "b@example.test" }, null),
        null, // tombstone
        "not json",
        envelope("t", null, null), // truncate: not a row change
        envelope("x", null, { id: 9 }), // unknown operation
        envelope("c", null, null), // create without a row
      ]);
      const run = await runWorker(worker.file, env());
      expect(run.code, run.output).toBe(0);
      const landed = await batches(source.id);
      expect(landed.every((b) => b.mode === "cdc")).toBe(true);
      const events = await allEvents();
      expect(events.map((e: { op: string }) => e.op)).toEqual(["c", "c", "u", "d"]);
      expect(events[2].after).toEqual({ id: 1, email: "a2@example.test" });
      expect(events[3].before).toEqual({ id: 2, email: "b@example.test" });
      // Malformed records are kept with their position and raw bytes, not skipped silently.
      const dead = await deadLetters();
      expect(
        dead.map((d: { offset: string; error_code: string }) => [d.offset, d.error_code]),
      ).toEqual([
        ["5", "invalid_json"],
        ["7", "unrecognized_event"],
        ["8", "invalid_event"],
      ]);
      expect(
        dead.every(
          (d: { topic: string; partition: number }) => d.topic === topic && d.partition === 0,
        ),
      ).toBe(true);
      expect(await rawValue(dead[0])).toBe("not json");
    });

    it("commits offsets only after landing, so a rejected batch is read again", async () => {
      await produce([envelope("c", null, { id: 3, email: "c@example.test" })]);
      const before = (await batches(source.id)).length;
      const rejected = await runWorker(worker.file, { ...env(), INGEST_TOKEN: BAD_TOKEN }, 60000);
      expect(rejected.output).toContain("status 401");
      expect(await batches(source.id)).toHaveLength(before);
      const run = await runWorker(worker.file, env());
      expect(run.code, run.output).toBe(0);
      const events = await allEvents();
      expect(events.map((e: { op: string }) => e.op)).toEqual(["c", "c", "u", "d", "c"]);
      expect(events[4].after).toEqual({ id: 3, email: "c@example.test" });
    });

    it("with DEAD_LETTER_POLICY=fail stops before the offset moves, then stores on the next run", async () => {
      await produce(["{broken", envelope("c", null, { id: 4, email: "d@example.test" })]);
      const landedBefore = (await batches(source.id)).length;
      const failed = await runWorker(worker.file, { ...env(), DEAD_LETTER_POLICY: "fail" }, 60000);
      expect(failed.code, failed.output).toBe(2);
      expect(failed.output).toContain("invalid_json");
      expect(await deadLetters()).toHaveLength(3);
      expect(await batches(source.id)).toHaveLength(landedBefore);
      // The offset was not committed: the default policy re-reads both records.
      const run = await runWorker(worker.file, env());
      expect(run.code, run.output).toBe(0);
      const dead = await deadLetters();
      expect(dead.map((d: { error_code: string }) => d.error_code)).toEqual([
        "invalid_json",
        "unrecognized_event",
        "invalid_event",
        "invalid_json",
      ]);
      expect(await rawValue(dead[3])).toBe("{broken");
      const events = await allEvents();
      expect(events[events.length - 1].after).toEqual({ id: 4, email: "d@example.test" });
    });

    it("keeps a recreated topic's different record at an old offset as a new dead letter", async () => {
      const send = (value: string) =>
        fetch(ENDPOINT, {
          method: "POST",
          headers: { "content-type": "application/json", "x-ingest-token": source.token },
          body: JSON.stringify({
            batchId: crypto.randomUUID(),
            deadLetters: [
              {
                topic,
                partition: 0,
                offset: "5",
                error: "invalid_json",
                key: null,
                value: Buffer.from(value).toString("base64"),
              },
            ],
          }),
        });
      const before = (await deadLetters()).length;
      expect((await send("not json")).status).toBe(202); // same position, same bytes: no new row
      expect((await deadLetters()).length).toBe(before);
      expect((await send("other bytes")).status).toBe(202); // same position, new bytes: recorded
      const after = await deadLetters();
      expect(after.length).toBe(before + 1);
    });
  },
);
