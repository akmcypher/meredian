// @vitest-environment node
// Spark ingestion S4 (deploy/connect/README.md, "Spark ingestion"): Kafka on a multi-executor Spark
// cluster, in offset chunks per partition, against the 3-broker certification cluster
// (deploy/connect/certify.compose.yml). Every Spark run is compared with the single-worker Kafka
// actor (kafka-v1, one task per partition) reading the same topic: the same records and
// operations, the same dead letters and the same committed positions. Skips without the app, the
// Spark cluster or the brokers.
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { execFileSync, spawnSync } from "node:child_process";
import {
  SparkHarness,
  byKey,
  executors,
  live,
  MASTER,
  type Conn,
  type Line,
} from "./connect-spark-harness";

const brokers = process.env["KAFKA_CERTIFY_BROKERS"];
const NETWORK = process.env["KAFKA_CERTIFY_NETWORK"] ?? "meridian-certify_default";
const h = new SparkHarness();
const topic = `spark_s4_${crypto.randomUUID().slice(0, 8)}`;
const conn = {} as Record<"spark" | "base", Conn>;
const WORKERS = [
  MASTER,
  MASTER.replace("spark-master-1", "spark-worker-1-1"),
  MASTER.replace("spark-master-1", "spark-worker-2-1"),
];

function reachable() {
  if (!brokers) return false;
  try {
    execFileSync("docker", ["network", "inspect", NETWORK], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}
// Produces with the same client library as the actor; value null is a tombstone (a delete).
function produce(
  messages: { key: string | null; value: string | Uint8Array | null; partition: number }[],
) {
  const code = `import sys,json,base64
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
s=json.load(sys.stdin)
if s.get('create'):
    # Keep the client referenced until the future resolves, or its handle is destroyed.
    admin=AdminClient({'bootstrap.servers':s['brokers']})
    admin.create_topics([NewTopic(s['topic'],num_partitions=3,replication_factor=3)])[s['topic']].result(30)
    import time
    for _ in range(30):
        meta=admin.list_topics(s['topic'],timeout=10).topics.get(s['topic'])
        if meta and len(meta.partitions)==3 and all(x.leader>=0 for x in meta.partitions.values()): break
        time.sleep(1)
p=Producer({'bootstrap.servers':s['brokers'],'enable.idempotence':True,'acks':'all'}); errors=[]
def done(e,m):
    if e: errors.append(str(e))
for m in s['messages']:
    p.produce(s['topic'],key=m['key'],value=None if m['value'] is None else base64.b64decode(m['value']),partition=m['partition'],on_delivery=done); p.poll(0)
if p.flush(30) or errors: raise SystemExit('delivery failed')
`;
  return (create: boolean) => {
    const r = spawnSync(process.env["PYTHON"] ?? "python3", ["-c", code], {
      input: JSON.stringify({
        brokers,
        topic,
        create,
        messages: messages.map((m) => ({
          ...m,
          value: m.value === null ? null : Buffer.from(m.value).toString("base64"),
        })),
      }),
      encoding: "utf8",
      timeout: 120000,
    });
    if (r.status !== 0) throw new Error(`produce failed: ${r.stderr}`);
  };
}
const batch = (from: number, n: number) =>
  Array.from({ length: n }, (_, i) => ({
    key: `k${(from + i) % 50}`,
    value: JSON.stringify({ id: from + i, name: `n${from + i}`, tag: "é" }),
    partition: (from + i) % 3,
  }));
async function sync(c: Conn, options: Record<string, unknown> = {}) {
  const run = await h.enqueue(c, "sync");
  await h.agent(c, options);
  return run;
}
async function deadLetters(c: Conn) {
  return (
    (
      await h.db
        .from("connect_dead_letters")
        .select("position,error_code")
        .eq("connection_id", c.id)
    ).data as {
      position: string;
      error_code: string;
    }[]
  )
    .map((d) => `${d.position}:${d.error_code}`)
    .sort();
}
const operations = (lines: Line[]) =>
  lines.map((l) => `${JSON.stringify(l["primary_key"])}:${String(l["operation"])}`).sort();
const positions = async (c: Conn) => (await h.state(c)).positions as Record<string, string>;

describe.skipIf(!live || !reachable())(
  "Spark ingestion (S4): Kafka in offset chunks per partition",
  { timeout: 1200000 },
  () => {
    beforeAll(async () => {
      await h.setup();
      // Spark containers reach the brokers on the cluster's own network (their advertised
      // in-network listeners), as a Spark cluster next to Kafka would.
      for (const c of WORKERS)
        spawnSync("docker", ["network", "connect", NETWORK, c], { stdio: "ignore" });
      h.overrides = () => ({ bootstrap_servers: "kafka-1:9092,kafka-2:9092,kafka-3:9092" });
      produce([
        ...batch(0, 3000),
        // Undecodable (a dead letter) and a tombstone (a delete).
        { key: "poison", value: Uint8Array.from([0xff, 0x01]), partition: 0 },
        { key: "k1", value: null, partition: 1 },
      ])(true);
      await h.secret("kafka", { bootstrap_servers: brokers });
      conn.spark = await h.connection("spark", "spark-kafka-v1", "kafka");
      conn.base = await h.connection("base", "kafka-v1", "kafka");
      for (const c of [conn.spark, conn.base])
        await h.stream(
          c,
          {
            namespace: "kafka",
            name: topic,
            primary_key: "_partition,_offset",
            cursor_field: "_offset",
          },
          "incremental",
        );
    }, 1200000);
    afterAll(async () => {
      spawnSync(
        process.env["PYTHON"] ?? "python3",
        [
          "-c",
          `from confluent_kafka.admin import AdminClient\nAdminClient({'bootstrap.servers':'${brokers}'}).delete_topics(['${topic}'])`,
        ],
        { timeout: 60000 },
      );
      await h.teardown();
    }, 120000);

    it("reads every partition in chunks across executors; same records, deletes, dead letters and positions as the Kafka actor", async () => {
      const run = await sync(conn.spark, { chunk_records: 400 });
      expect(await h.run(run)).toMatchObject({ status: "succeeded", rows_committed: 3001 });
      const { lines, manifests } = await h.landed(run);
      expect(executors(manifests).size).toBeGreaterThanOrEqual(2);
      const state = await h.state(conn.spark);
      expect(state.spark.plan.length).toBeGreaterThanOrEqual(9);
      expect(state.positions).toEqual({ "0": "1001", "1": "1001", "2": "1000" });
      expect(state.cluster_id).toBeTruthy();
      const base = await sync(conn.base);
      expect(await h.run(base)).toMatchObject({ status: "succeeded", rows_committed: 3001 });
      const baseLines = (await h.landed(base)).lines;
      expect(byKey(lines)).toEqual(byKey(baseLines));
      expect(operations(lines)).toEqual(operations(baseLines));
      expect(operations(lines).filter((o) => o.endsWith(":delete"))).toHaveLength(1);
      expect(await positions(conn.spark)).toEqual(await positions(conn.base));
      const dead = await deadLetters(conn.spark);
      expect(dead).toHaveLength(1);
      expect(dead.map((d) => d.split(":").slice(1).join(":"))).toEqual(
        (await deadLetters(conn.base)).map((d) => d.split(":").slice(1).join(":")),
      );
    });

    it("resumes a failed attempt chunk by chunk, exactly once, and the next run continues from its positions", async () => {
      produce(batch(3000, 1500))(false);
      const run = await sync(conn.spark, { chunk_records: 200, fail_after_commits: 2 });
      expect((await h.run(run)).status).toBe("running");
      // Records produced after the run fixed its window wait for the next run.
      produce(batch(4500, 300))(false);
      await h.db
        .from("connect_run_streams")
        .update({ next_attempt_at: "2000-01-01T00:00:00Z" })
        .eq("run_id", run);
      await h.agent(conn.spark, { chunk_records: 200 });
      expect(await h.run(run)).toMatchObject({ status: "succeeded", rows_committed: 1500 });
      const first = (await h.landed(run)).lines;
      expect(new Set(first.map((l) => JSON.stringify(l["primary_key"]))).size).toBe(1500);
      const next = await sync(conn.spark, { chunk_records: 200 });
      expect(await h.run(next)).toMatchObject({ status: "succeeded", rows_committed: 300 });
      const later = (await h.landed(next)).lines;
      const base = await sync(conn.base);
      expect(byKey([...first, ...later])).toEqual(byKey((await h.landed(base)).lines));
      expect(await positions(conn.spark)).toEqual(await positions(conn.base));
      expect(await positions(conn.spark)).toEqual({ "0": "1601", "1": "1601", "2": "1600" });
      // Nothing new: the run completes without records and keeps the positions.
      const idle = await sync(conn.spark);
      expect(await h.run(idle)).toMatchObject({ status: "succeeded", rows_committed: 0 });
      expect(await positions(conn.spark)).toEqual({ "0": "1601", "1": "1601", "2": "1600" });
    });
  },
);
