// @vitest-environment node
// Large Kafka records through the real agent and control plane (3-broker cluster in
// deploy/connect/certify.compose.yml): ~1 MB records, large binary values and full-size dead
// letters in one mixed run. Commits must split by bytes, stay under the 8 MiB request and landing
// limits, and move the stream position only with the last segment of each checkpoint.
import { createClient } from "@supabase/supabase-js";
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { sha256 } from "@/lib/ingest";
const url = process.env["SUPABASE_URL"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"],
  key = process.env["SUPABASE_PUBLISHABLE_KEY"] ?? process.env["VITE_SUPABASE_PUBLISHABLE_KEY"],
  brokers = process.env["KAFKA_CERTIFY_BROKERS"],
  app = process.env["APP_URL"] ?? "http://localhost:8080",
  python = process.env["PYTHON"] ?? "python3";
const topic = `certify.batch.${crypto.randomUUID().slice(0, 8)}`;
const LIMIT = 8 * 1024 * 1024;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let db: any, owner: any, admin: any, producer: any;
let uid = "",
  connection = "",
  stream = "",
  folder = "";
const binaries = [randomBytes(1_050_000), randomBytes(1_090_000), randomBytes(1_020_000)];
function agent() {
  return new Promise<void>((resolve, reject) => {
    const p = spawn(python, ["deploy/connect/agent.py"], {
      env: {
        ...process.env,
        PYTHONPATH: "deploy/connect",
        PYTHONUTF8: "1",
        MERIDIAN_URL: app,
        INGEST_TOKEN_FILE: join(folder, "ingest_token"),
        CONNECT_SECRET_DIR: folder,
        CONNECT_ONCE: "1",
      },
      stdio: ["ignore", "ignore", "ignore"],
    });
    p.on("exit", (code) => (code === 0 ? resolve() : reject(new Error("agent failed"))));
  });
}
async function enqueue(operation: string) {
  const r = await owner.rpc("connect_enqueue", {
    _connection: connection,
    _request: crypto.randomUUID(),
    _operation: operation,
  });
  if (r.error) throw r.error;
  return r.data as string;
}
describe.skipIf(!url || !service || !key || !brokers)(
  "Connect Kafka byte-bounded commits",
  { timeout: 300000 },
  () => {
    beforeAll(async () => {
      db = createClient(url!, service!, { auth: { persistSession: false } });
      const email = `connect-batch-${crypto.randomUUID()}@example.test`,
        password = `P-${crypto.randomUUID()}`;
      const u = await db.auth.admin.createUser({ email, password, email_confirm: true });
      if (u.error) throw u.error;
      uid = u.data.user.id;
      owner = createClient(url!, key!, { auth: { persistSession: false } });
      await owner.auth.signInWithPassword({ email, password });
      const token = `mi_${crypto.randomUUID().replaceAll("-", "")}${crypto.randomUUID().replaceAll("-", "")}`;
      const s = await db
        .from("ingest_sources")
        .insert({
          name: "Connect Kafka batching evidence",
          kind: "kafka",
          owner_id: uid,
          target_table: "connect_kafka_batching",
          config: {},
          token_hash: await sha256(token),
        })
        .select("id")
        .single();
      if (s.error) throw s.error;
      const c = await db
        .from("connect_connections")
        .insert({
          source_id: s.data.id,
          owner_id: uid,
          name: "Kafka batching",
          credential_ref: "kafka",
          source_version: "kafka-v1",
        })
        .select("id")
        .single();
      if (c.error) throw c.error;
      connection = c.data.id;
      folder = await mkdtemp(join(tmpdir(), "connect-batch-"));
      await writeFile(join(folder, "ingest_token"), token, { mode: 0o600 });
      await writeFile(join(folder, "kafka.json"), JSON.stringify({ bootstrap_servers: brokers }), {
        mode: 0o600,
      });
      const { Kafka, logLevel } = await import("kafkajs");
      const kafka = new Kafka({ brokers: brokers!.split(","), logLevel: logLevel.NOTHING });
      admin = kafka.admin();
      await admin.connect();
      // kafkajs' waitForLeaders uses metadata v0, which this KRaft cluster answers with an error.
      await admin.createTopics({
        waitForLeaders: false,
        topics: [
          {
            topic,
            numPartitions: 1,
            replicationFactor: 3,
            configEntries: [{ name: "max.message.bytes", value: "2000000" }],
          },
        ],
      });
      for (let i = 0; ; i++) {
        const meta = await admin.fetchTopicMetadata({ topics: [topic] }).catch(() => null);
        if (meta?.topics[0]?.partitions?.[0]?.leader >= 0) break;
        if (i > 30) throw new Error("topic leader not elected");
        await new Promise((r) => setTimeout(r, 1000));
      }
      producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
      await producer.connect();
      // Mixed: 16 records of ~950 KB, three 1.02-1.09 MB binary values (dead letters kept whole)
      // and small records in between.
      const messages = [];
      for (let i = 0; i < 16; i++) {
        messages.push({ value: JSON.stringify({ id: i, blob: "x".repeat(950_000) }) });
        if (i % 6 === 2) messages.push({ value: binaries[messages.length % 3]! });
        messages.push({ value: JSON.stringify({ id: 100 + i }) });
      }
      for (const m of messages) await producer.send({ topic, acks: -1, messages: [m] });
      await enqueue("discover");
      await agent();
      const cat = (
        await db
          .from("connect_connections")
          .select("discovered_catalog")
          .eq("id", connection)
          .single()
      ).data.discovered_catalog;
      const found = cat.find(
        (x: { json_schema: Record<string, unknown> }) => x.json_schema["x-kafka-topic"] === topic,
      );
      const st = await db
        .from("connect_streams")
        .insert({
          connection_id: connection,
          namespace: found.namespace,
          name: found.name,
          primary_key: "_partition,_offset",
          key_columns: ["_partition", "_offset"],
          cursor_field: "_offset",
          schema_json: found.json_schema,
        })
        .select("id")
        .single();
      if (st.error) throw st.error;
      stream = st.data.id;
    }, 240000);
    afterAll(async () => {
      await producer?.disconnect();
      await admin?.deleteTopics({ topics: [topic] }).catch(() => undefined);
      await admin?.disconnect();
      if (db) await db.auth.admin.deleteUser(uid);
    }, 60000);

    it("splits by bytes, lands every record and keeps dead letters whole", async () => {
      const run = await enqueue("sync");
      await agent();
      const r = await db
        .from("connect_runs")
        .select("status,rows_committed")
        .eq("id", run)
        .single();
      expect(r.data).toEqual({ status: "succeeded", rows_committed: 32 });
      const commits = (
        await db
          .from("connect_commits")
          .select("sequence,byte_count,row_count,checkpoint,batch_id")
          .eq("run_id", run)
          .order("sequence")
      ).data;
      // Six ~950 KB records cannot share one 6 MiB budget: several segments, each under 8 MiB.
      expect(commits.length).toBeGreaterThan(3);
      expect(commits.map((c: { sequence: number }) => c.sequence)).toEqual(
        commits.map((_: unknown, i: number) => i),
      );
      for (const c of commits) expect(c.byte_count).toBeLessThan(LIMIT);
      for (const c of commits) {
        const b = await db
          .from("ingest_landing_batches")
          .select("object_key")
          .eq("id", c.batch_id)
          .single();
        const file = await db.storage.from("ingest-files").download(b.data.object_key);
        expect(file.data.size).toBeLessThan(LIMIT);
      }
      // Segments before the final one carry the earlier checkpoint: the position moved only at
      // the end (the actor emits one checkpoint for this run).
      const last = commits[commits.length - 1];
      expect(last.checkpoint.positions).toEqual({ "0": "35" });
      for (const c of commits.slice(0, -1)) expect(c.checkpoint.positions ?? {}).toEqual({});
      // All 32 records landed once; the three binary values are dead letters with every byte.
      const dead = (
        await db
          .from("connect_dead_letters")
          .select("position,error_code,object_key,line")
          .eq("stream_id", stream)
      ).data;
      expect(dead.map((d: { error_code: string }) => d.error_code)).toEqual([
        "record_too_large",
        "record_too_large",
        "record_too_large",
      ]);
      for (const d of dead) {
        const file = await db.storage.from("ingest-files").download(d.object_key);
        const entry = JSON.parse((await file.data.text()).split("\n")[d.line]);
        const bytes = Buffer.from(entry.dead_letter.payload.value_b64, "base64");
        expect(binaries.some((b) => b.equals(bytes))).toBe(true);
      }
      const st = (await db.from("connect_streams").select("state").eq("id", stream).single()).data
        .state;
      expect(st.positions).toEqual({ "0": "35" });
    });
  },
);
