// @vitest-environment node
// Kafka → Connect agent → control plane, against the 3-broker cluster and Karapace in
// deploy/connect/certify.compose.yml (scripts/e2e-local.mjs sets KAFKA_CERTIFY_* when they answer).
// Proves: positions stored with landed commits, a crashed attempt resumed without loss or
// duplicates, dead letters stored with their commit, replay after a registry fix, the dead-letter
// budget, and owner-only rewind.
import { createClient } from "@supabase/supabase-js";
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn } from "node:child_process";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { sha256 } from "@/lib/ingest";
const url = process.env["SUPABASE_URL"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"],
  key = process.env["SUPABASE_PUBLISHABLE_KEY"] ?? process.env["VITE_SUPABASE_PUBLISHABLE_KEY"],
  brokers = process.env["KAFKA_CERTIFY_BROKERS"],
  registry = process.env["KAFKA_CERTIFY_REGISTRY"],
  app = process.env["APP_URL"] ?? "http://localhost:8080",
  python = process.env["PYTHON"] ?? "python3";
const topic = `certify.e2e.${crypto.randomUUID().slice(0, 8)}`;
const unique = `certify.e2e${crypto.randomUUID().replaceAll("-", "")}`;
const avroSchema = {
  type: "record",
  name: "E",
  namespace: unique,
  fields: [{ name: "id", type: "long" }],
};
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let db: any, owner: any, peer: any;
let uid = "",
  peerId = "",
  connection = "",
  stream = "",
  token = "",
  folder = "",
  avroId = 0;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let admin: any;
const env = () => ({
  ...process.env,
  PYTHONPATH: "deploy/connect",
  PYTHONUTF8: "1",
  MERIDIAN_URL: app,
  INGEST_TOKEN_FILE: join(folder, "ingest_token"),
  CONNECT_SECRET_DIR: folder,
  CONNECT_ONCE: "1",
});
function run(args: string[]) {
  return new Promise<string>((resolve, reject) => {
    const p = spawn(python, args, { env: env(), stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    p.stdout.on("data", (b) => (out += b));
    p.on("exit", (code) => (code === 0 ? resolve(out) : reject(new Error(`${args[0]} failed`))));
  });
}
const agent = () => run(["deploy/connect/agent.py"]);

// Seed with the same independently tested client as the actors. Assert broker offsets below,
// so a fixture transport retry cannot masquerade as duplicate ingestion.
async function seedTopic(input: {
  topic: string;
  acks: number;
  messages: { key?: string; value: string | Buffer; partition?: number }[];
}) {
  const code = `import sys,json,base64
from confluent_kafka import Producer
s=json.load(sys.stdin); errors=[]
p=Producer({'bootstrap.servers':s['brokers'],'enable.idempotence':True,'acks':'all'})
def done(error,message):
 if error: errors.append(str(error.code()))
for m in s['messages']:
 p.produce(s['topic'],key=m['key'],value=base64.b64decode(m['value']),partition=m['partition'],on_delivery=done)
 p.poll(0)
if p.flush(30) or errors: raise RuntimeError('fixture_delivery_failed')
`;
  await new Promise<void>((resolve, reject) => {
    const child = spawn(python, ["-c", code], { env: env(), stdio: ["pipe", "ignore", "pipe"] });
    let diagnostics = "";
    child.stderr.on("data", (chunk) => {
      diagnostics = (diagnostics + chunk).slice(-2000);
    });
    child.on("error", reject);
    const timer = setTimeout(() => {
      child.kill();
      reject(new Error("fixture_delivery_timeout"));
    }, 40000);
    child.on("exit", (status) => {
      clearTimeout(timer);
      if (status === 0) resolve();
      else reject(new Error(`fixture_delivery_failed (${status}): ${diagnostics}`));
    });
    child.stdin.end(
      JSON.stringify({
        brokers,
        topic: input.topic,
        messages: input.messages.map((m) => ({
          key: m.key ?? null,
          value: Buffer.from(m.value).toString("base64"),
          partition: m.partition ?? -1,
        })),
      }),
    );
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
async function registerAvro() {
  const r = await fetch(`${registry}/subjects/${topic}-value/versions`, {
    method: "POST",
    headers: { "Content-Type": "application/vnd.schemaregistry.v1+json" },
    body: JSON.stringify({ schema: JSON.stringify(avroSchema) }),
  });
  return ((await r.json()) as { id: number }).id;
}
// Confluent wire format for {id: 9}: magic 0, big-endian schema id, Avro zig-zag long 9 = 0x12.
const avroValue = (id: number) =>
  Buffer.from([0, id >>> 24, (id >>> 16) & 255, (id >>> 8) & 255, id & 255, 0x12]);
async function landedKeys(run: string) {
  const commits = await db.from("connect_commits").select("batch_id,sequence").eq("run_id", run);
  const keys: string[] = [];
  for (const c of commits.data) {
    const b = await db
      .from("ingest_landing_batches")
      .select("object_key")
      .eq("id", c.batch_id)
      .single();
    const file = await db.storage.from("ingest-files").download(b.data.object_key);
    for (const line of (await file.data.text()).split("\n").filter(Boolean))
      keys.push(JSON.stringify(JSON.parse(line).primary_key));
  }
  return { keys, sequences: commits.data.map((c: { sequence: number }) => c.sequence).sort() };
}
// Offsets are decimal strings in checkpoints.
const positionsTotal = (s: { positions?: Record<string, string> }) =>
  Object.values(s.positions ?? {}).reduce((a, b) => a + Number(b), 0);
describe.skipIf(!url || !service || !key || !brokers || !registry)(
  "Connect live Kafka (3 brokers + registry) to bronze",
  { timeout: 240000 },
  () => {
    beforeAll(async () => {
      db = createClient(url!, service!, { auth: { persistSession: false } });
      for (let i = 0; i < 2; i++) {
        const email = `connect-kafka-${crypto.randomUUID()}@example.test`,
          password = `P-${crypto.randomUUID()}`;
        const u = await db.auth.admin.createUser({ email, password, email_confirm: true });
        if (u.error) throw u.error;
        const c = createClient(url!, key!, { auth: { persistSession: false } });
        await c.auth.signInWithPassword({ email, password });
        if (i === 0) [uid, owner] = [u.data.user.id, c];
        else [peerId, peer] = [u.data.user.id, c];
      }
      token = `mi_${crypto.randomUUID().replaceAll("-", "")}${crypto.randomUUID().replaceAll("-", "")}`;
      const s = await db
        .from("ingest_sources")
        .insert({
          name: "Connect Kafka evidence",
          kind: "kafka",
          owner_id: uid,
          target_table: "connect_kafka_bronze",
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
          name: "Kafka certification",
          credential_ref: "kafka",
          source_version: "kafka-v1",
          dead_letter_budget: 5,
        })
        .select("id")
        .single();
      if (c.error) throw c.error;
      connection = c.data.id;
      folder = await mkdtemp(join(tmpdir(), "connect-kafka-"));
      await writeFile(join(folder, "ingest_token"), token, { mode: 0o600 });
      await writeFile(
        join(folder, "kafka.json"),
        JSON.stringify({ bootstrap_servers: brokers, schema_registry_url: registry }),
        { mode: 0o600 },
      );
      const { Kafka, logLevel } = await import("kafkajs");
      const kafka = new Kafka({ brokers: brokers!.split(","), logLevel: logLevel.NOTHING });
      admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({
        // kafkajs' waitForLeaders uses metadata v0, which this KRaft cluster answers with an error.
        waitForLeaders: false,
        topics: [
          {
            topic,
            numPartitions: 3,
            replicationFactor: 3,
            configEntries: [{ name: "min.insync.replicas", value: "2" }],
          },
        ],
      });
      // The fixture producer can bootstrap through any broker. A successful create and
      // metadata response from one broker do not mean the others know this topic yet.
      for (const broker of brokers!.split(",")) {
        const probe = new Kafka({ brokers: [broker], logLevel: logLevel.NOTHING }).admin();
        await probe.connect();
        try {
          for (let i = 0; ; i++) {
            const meta = await probe.fetchTopicMetadata({ topics: [topic] }).catch(() => null);
            const parts = meta?.topics[0]?.partitions ?? [];
            if (parts.length === 3 && parts.every((p: { leader: number }) => p.leader >= 0)) break;
            if (i > 30) throw new Error(`topic leaders not elected on ${broker}`);
            await new Promise((r) => setTimeout(r, 1000));
          }
        } finally {
          await probe.disconnect();
        }
      }
      // Probe the registry's next global id; the message below uses it before it exists.
      const probe = await fetch(`${registry}/subjects/${topic}-probe/versions`, {
        method: "POST",
        headers: { "Content-Type": "application/vnd.schemaregistry.v1+json" },
        body: JSON.stringify({
          schema: JSON.stringify({
            type: "record",
            name: "P",
            namespace: unique,
            fields: [{ name: "p", type: "int" }],
          }),
        }),
      });
      avroId = ((await probe.json()) as { id: number }).id + 1;
      await seedTopic({
        topic,
        acks: -1,
        messages: [
          ...Array.from({ length: 1200 }, (_, i) => ({
            key: `k${i % 30}`,
            value: JSON.stringify({ id: i }),
          })),
          { key: "poison", value: Buffer.from([0xff, 0x01]), partition: 0 },
          { key: "avro", value: avroValue(avroId), partition: 1 },
        ],
      });
      // Distinguish fixture delivery from ingestion replay: retries during topic leader
      // changes must not silently turn the 1202-record fixture into a different dataset.
      const offsets = await admin.fetchTopicOffsets(topic);
      expect(
        offsets.reduce((sum: number, p: { offset: string }) => sum + Number(p.offset), 0),
      ).toBe(1202);
    }, 120000);
    afterAll(async () => {
      await admin?.deleteTopics({ topics: [topic] }).catch(() => undefined);
      await admin?.disconnect();
      if (db) {
        await db.auth.admin.deleteUser(uid);
        await db.auth.admin.deleteUser(peerId);
      }
    }, 60000);
    it("discovers the topic through the agent", async () => {
      await enqueue("discover");
      await agent();
      const c = await db
        .from("connect_connections")
        .select("discovered_catalog")
        .eq("id", connection)
        .single();
      const found = c.data.discovered_catalog.find(
        (s: { json_schema: Record<string, unknown> }) => s.json_schema["x-kafka-topic"] === topic,
      );
      expect(found.primary_key).toEqual(["_partition", "_offset"]);
      const s = await db
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
      if (s.error) throw s.error;
      stream = s.data.id;
    });
    it("resumes a crashed attempt from its committed positions without loss or duplicates", async () => {
      const runId = await enqueue("sync");
      // Attempt 1: an agent that commits its first batch and dies.
      const claim = (await db.rpc("connect_claim", { _connection: connection })).data;
      const s = claim.streams[0];
      await writeFile(join(folder, "catalog.json"), JSON.stringify(s));
      await writeFile(join(folder, "state.json"), JSON.stringify(s.state));
      const out = await run([
        "deploy/connect/source_kafka.py",
        "read",
        "--config",
        join(folder, "kafka.json"),
        "--catalog",
        join(folder, "catalog.json"),
        "--state",
        join(folder, "state.json"),
      ]);
      const messages = out
        .split("\n")
        .filter(Boolean)
        .map((l) => JSON.parse(l));
      const cut = messages.findIndex((m) => m.type === "STATE");
      const first = await fetch(`${app}/api/public/connect-agent`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "x-ingest-token": token },
        body: JSON.stringify({
          action: "commit",
          runId: runId,
          token: claim.lease_token,
          streamId: s.id,
          sequence: 0,
          records: messages.slice(0, cut).filter((m) => m.type === "RECORD"),
          dead_letters: messages.slice(0, cut).filter((m) => m.type === "DEAD_LETTER"),
          state: messages[cut],
        }),
      });
      expect(first.status).toBe(200);
      await db
        .from("connect_runs")
        .update({ lease_until: new Date(Date.now() - 1000).toISOString() })
        .eq("id", runId);
      // Attempt 2: the real agent resumes from the committed positions.
      await agent();
      const r = await db
        .from("connect_runs")
        .select("status,attempts,rows_committed")
        .eq("id", runId)
        .single();
      expect(r.data).toMatchObject({ status: "succeeded", attempts: 2, rows_committed: 1200 });
      const { keys, sequences } = await landedKeys(runId);
      expect(keys.length).toBe(1200);
      expect(new Set(keys).size).toBe(1200);
      expect(sequences).toEqual(sequences.map((_: number, i: number) => i));
      const st = await db.from("connect_streams").select("state").eq("id", stream).single();
      expect(positionsTotal(st.data.state)).toBe(1202);
      const dead = await db
        .from("connect_dead_letters")
        .select("position,error_code,status")
        .eq("stream_id", stream)
        .order("error_code");
      expect(
        dead.data.map((d: { position: string; error_code: string; status: string }) => [
          // position = <topic id>:<partition>:<offset>
          d.position.split(":")[1],
          d.error_code,
          d.status,
        ]),
      ).toEqual([
        ["0", "deserialization_failed", "open"],
        ["1", "schema_not_found", "open"],
      ]);
      // The peer sees none of it.
      expect(
        (await peer.from("connect_dead_letters").select("id").eq("stream_id", stream)).data,
      ).toEqual([]);
    });
    it("replays a dead letter once the registry has its schema, without moving the stream", async () => {
      expect(await registerAvro()).toBe(avroId);
      const before = (await db.from("connect_streams").select("state").eq("id", stream).single())
        .data.state;
      const letters = (
        await db.from("connect_dead_letters").select("id,error_code").eq("stream_id", stream)
      ).data;
      expect(
        (
          await peer.rpc("connect_enqueue_replay", {
            _connection: connection,
            _request: crypto.randomUUID(),
            _dead_letters: letters.map((d: { id: string }) => d.id),
          })
        ).error,
      ).toBeTruthy();
      const replay = await owner.rpc("connect_enqueue_replay", {
        _connection: connection,
        _request: crypto.randomUUID(),
        _dead_letters: letters.map((d: { id: string }) => d.id),
      });
      if (replay.error) throw replay.error;
      await agent();
      const r = await db
        .from("connect_runs")
        .select("status,rows_committed,operation")
        .eq("id", replay.data)
        .single();
      // One resolves and lands; the other still fails, so the run says so instead of succeeding.
      expect(r.data).toMatchObject({ status: "failed", rows_committed: 1, operation: "replay" });
      const outcome = await db
        .from("connect_events")
        .select("code,detail")
        .eq("run_id", replay.data)
        .not("detail", "is", null)
        .single();
      expect(outcome.data).toEqual({
        code: "replay_incomplete",
        detail: [
          { selected: "2", replayed: "1", still_failing: "1", deferred: "0", stream_id: stream },
        ],
      });
      const after = await db
        .from("connect_dead_letters")
        .select("error_code,status")
        .eq("stream_id", stream)
        .order("error_code");
      expect(after.data).toEqual([
        { error_code: "deserialization_failed", status: "open" },
        { error_code: "schema_not_found", status: "replayed" },
      ]);
      const { keys } = await landedKeys(replay.data);
      expect(keys).toHaveLength(1);
      expect(
        (await db.from("connect_streams").select("state").eq("id", stream).single()).data.state,
      ).toEqual(before);
    });
    it("fails the run on the dead-letter budget without advancing the position", async () => {
      await seedTopic({
        topic,
        acks: -1,
        messages: Array.from({ length: 6 }, () => ({ value: Buffer.from([0xfe]), partition: 2 })),
      });
      const before = (await db.from("connect_streams").select("state").eq("id", stream).single())
        .data.state;
      const runId = await enqueue("sync");
      await agent();
      const r = await db.from("connect_runs").select("status,error_code").eq("id", runId).single();
      expect(r.data).toEqual({ status: "failed", error_code: "dead_letter_budget" });
      expect(
        (await db.from("connect_streams").select("state").eq("id", stream).single()).data.state,
      ).toEqual(before);
      expect((await db.from("connect_dead_letters").select("id").eq("run_id", runId)).data).toEqual(
        [],
      );
    });
    it("lets only the owner rewind a stream, and records it", async () => {
      const target = { version: 1, positions: { "0": 0, "1": 0, "2": 0 } };
      expect(
        (await peer.rpc("connect_rewind_stream", { _stream: stream, _state: target })).error,
      ).toBeTruthy();
      expect(
        (await owner.rpc("connect_rewind_stream", { _stream: stream, _state: { bad: true } }))
          .error,
      ).toBeTruthy();
      expect(
        (await owner.rpc("connect_rewind_stream", { _stream: stream, _state: target })).error,
      ).toBeNull();
      expect(
        (await db.from("connect_streams").select("state").eq("id", stream).single()).data.state,
      ).toEqual(target);
      const events = await db
        .from("connect_events")
        .select("code,actor_id")
        .eq("connection_id", connection)
        .eq("code", "stream_rewound");
      expect(events.data).toEqual([{ code: "stream_rewound", actor_id: uid }]);
    });
  },
);
