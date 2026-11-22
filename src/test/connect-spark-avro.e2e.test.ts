// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { spawnSync } from "node:child_process";
import { Kafka, logLevel, type Admin, type Producer } from "kafkajs";
import {
  SparkHarness,
  byKey,
  executors,
  K8S,
  live,
  MASTER,
  type Conn,
} from "./connect-spark-harness";

const brokers = process.env["KAFKA_CERTIFY_BROKERS"];
const registry = process.env["KAFKA_CERTIFY_REGISTRY"];
const topic = `spark_avro_${crypto.randomUUID().slice(0, 8)}`;
const h = new SparkHarness();
let admin: Admin, producer: Producer;
let base: Conn, spark: Conn;
const ids: number[] = [];
const address = {
  type: "record",
  name: "Address",
  namespace: "example",
  fields: [{ name: "city", type: "string" }],
};
const v1 = {
  type: "record",
  name: "Event",
  namespace: "example",
  fields: [{ name: "id", type: "int" }],
};
const v2 = {
  ...v1,
  fields: [
    { name: "id", type: "long" },
    { name: "region", type: "string", default: "unknown" },
  ],
};
const v3 = {
  ...v2,
  fields: [...v2.fields, { name: "address", type: "example.Address", default: { city: "" } }],
};
const reader = {
  ...v3,
  fields: [...v2.fields, { name: "address", type: address, default: { city: "" } }],
};
const subjects = [`${topic}-address`, `${topic}-value`, `${topic}-breaking`];

async function register(subject: string, schema: unknown, references: unknown[] = []) {
  const response = await fetch(`${registry}/subjects/${encodeURIComponent(subject)}/versions`, {
    method: "POST",
    headers: { "content-type": "application/vnd.schemaregistry.v1+json" },
    body: JSON.stringify({ schema: JSON.stringify(schema), references }),
  });
  if (!response.ok)
    throw new Error(`Registry registration failed: ${response.status}: ${await response.text()}`);
  return ((await response.json()) as { id: number }).id;
}

function encode(id: number, schema: unknown, records: unknown[], dependencies: unknown[] = []) {
  const code = `import sys,json,io,struct,base64,fastavro
s=json.load(sys.stdin); names={}
for dependency in s['dependencies']: fastavro.parse_schema(dependency,names)
schema=fastavro.parse_schema(s['schema'],names); out=[]
for value in s['records']:
 b=io.BytesIO(); fastavro.schemaless_writer(b,schema,value)
 out.append(base64.b64encode(b'\\x00'+struct.pack('>I',s['id'])+b.getvalue()).decode())
print(json.dumps(out))`;
  const result = spawnSync(process.env["PYTHON"] ?? "python3", ["-c", code], {
    input: JSON.stringify({ id, schema, records, dependencies }),
    encoding: "utf8",
    timeout: 30000,
  });
  if (result.status !== 0) throw new Error("Avro fixture encoding failed");
  return (JSON.parse(result.stdout) as string[]).map((value) => Buffer.from(value, "base64"));
}

async function produce(start: number, count: number) {
  const messages = [v1, v2, v3].flatMap((schema, version) => {
    const rows = Array.from({ length: count }, (_, i) => ({
      id: start + version * count + i,
      ...(version >= 1 ? { region: "IN" } : {}),
      ...(version >= 2 ? { address: { city: "Pune" } } : {}),
    }));
    return encode(ids[version]!, schema, rows, version === 2 ? [address] : []).map((value, i) => ({
      partition: i % 3,
      value,
    }));
  });
  await producer.send({ topic, acks: -1, messages });
}

async function sync(c: Conn, options: Record<string, unknown> = {}) {
  const run = await h.enqueue(c, "sync");
  await h.agent(c, { chunk_records: 2, ...options });
  return run;
}

describe.skipIf(!live || !!K8S || !brokers || !registry)(
  "Kafka/Spark Avro registry evolution",
  { timeout: 600000 },
  () => {
    beforeAll(async () => {
      const kafka = new Kafka({ brokers: brokers!.split(","), logLevel: logLevel.NOTHING });
      admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({
        waitForLeaders: false,
        topics: [{ topic, numPartitions: 3, replicationFactor: 3 }],
      });
      for (const broker of brokers!.split(",")) {
        const probe = new Kafka({ brokers: [broker], logLevel: logLevel.NOTHING }).admin();
        await probe.connect();
        try {
          for (let attempt = 0; ; attempt++) {
            const metadata = await probe.fetchTopicMetadata({ topics: [topic] }).catch(() => null);
            if (
              metadata?.topics[0]?.partitions.length === 3 &&
              metadata.topics[0].partitions.every((p) => p.leader >= 0)
            )
              break;
            if (attempt >= 30) throw new Error("Topic metadata not ready");
            await new Promise((resolve) => setTimeout(resolve, 500));
          }
        } finally {
          await probe.disconnect();
        }
      }
      producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
      await producer.connect();
      await register(subjects[0]!, address);
      ids.push(
        await register(subjects[1]!, v1),
        await register(subjects[1]!, v2),
        await register(subjects[1]!, v3, [
          { name: "example.Address", subject: subjects[0], version: 1 },
        ]),
      );
      await produce(0, 12);
      for (const container of [
        MASTER,
        MASTER.replace("spark-master-1", "spark-worker-1-1"),
        MASTER.replace("spark-master-1", "spark-worker-2-1"),
      ])
        spawnSync("docker", ["network", "connect", "meridian-certify_default", container], {
          stdio: "ignore",
        });
      await h.setup();
      h.submitConf = ["--driver-memory", "512m", "--executor-memory", "512m"];
      h.overrides = () => ({
        bootstrap_servers: "kafka-1:9092,kafka-2:9092,kafka-3:9092",
        schema_registry_url:
          process.env["KAFKA_CERTIFY_REGISTRY_INTERNAL"] ??
          "http://avro-registry:8080/apis/ccompat/v7",
      });
      await h.secret("avro", { bootstrap_servers: brokers, schema_registry_url: registry });
      base = await h.connection(`${topic}_base`, "kafka-v1", "avro");
      spark = await h.connection(`${topic}_spark`, "spark-kafka-v1", "avro");
      for (const c of [base, spark]) {
        const source = await h.db
          .from("connect_connections")
          .select("source_id")
          .eq("id", c.id)
          .single();
        expect(source.error).toBeNull();
        expect(
          (
            await h.db
              .from("ingest_sources")
              .update({ kind: "kafka" })
              .eq("id", source.data.source_id)
          ).error,
        ).toBeNull();
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
      }
    }, 240000);

    afterAll(async () => {
      await producer?.disconnect();
      await admin?.deleteTopics({ topics: [topic] }).catch(() => undefined);
      await admin?.disconnect();
      for (const subject of [subjects[1], subjects[2], subjects[0]])
        await fetch(`${registry}/subjects/${encodeURIComponent(subject!)}`, {
          method: "DELETE",
        }).catch(() => undefined);
      await h.teardown();
    }, 60000);

    it("automatically reads mixed writer versions and referenced records on multiple executors", async () => {
      const distributed = await sync(spark),
        standalone = await sync(base);
      expect(await h.run(distributed)).toMatchObject({ status: "succeeded", rows_committed: 36 });
      expect(await h.run(standalone)).toMatchObject({ status: "succeeded", rows_committed: 36 });
      const result = await h.landed(distributed);
      expect(executors(result.manifests).size).toBeGreaterThanOrEqual(2);
      expect(byKey(result.lines)).toEqual(byKey((await h.landed(standalone)).lines));
      expect(result.lines.filter((line) => line.data["address"])).toHaveLength(12);
      expect((await h.state(spark)).positions).toEqual({ "0": "12", "1": "12", "2": "12" });
    });

    it("pins evolved readers with defaults/promotions and resumes a failed Spark publication", async () => {
      for (const c of [base, spark]) {
        const saved = await h.db
          .from("connect_streams")
          .select("schema_json")
          .eq("id", c.stream)
          .single();
        expect(saved.error).toBeNull();
        expect(
          (
            await h.db
              .from("connect_streams")
              .update({
                schema_json: { ...saved.data.schema_json, "x-avro-reader-schema": reader },
              })
              .eq("id", c.stream)
          ).error,
        ).toBeNull();
      }
      await produce(36, 6);
      const distributed = await sync(spark, { fail_after_commits: 2 });
      expect((await h.run(distributed)).status).toBe("running");
      expect(
        (
          await h.db
            .from("connect_run_streams")
            .update({ next_attempt_at: "2000-01-01T00:00:00Z" })
            .eq("run_id", distributed)
        ).error,
      ).toBeNull();
      await h.agent(spark, { chunk_records: 2 });
      const standalone = await sync(base);
      expect(await h.run(distributed)).toMatchObject({ status: "succeeded", rows_committed: 18 });
      const result = await h.landed(distributed);
      expect(result.lines).toHaveLength(18);
      expect(new Set(result.lines.map((line) => JSON.stringify(line["primary_key"]))).size).toBe(
        18,
      );
      expect(byKey(result.lines)).toEqual(byKey((await h.landed(standalone)).lines));
      expect(result.lines.every((line) => "region" in line.data && "address" in line.data)).toBe(
        true,
      );
      expect(result.lines.filter((line) => line.data["region"] === "unknown")).toHaveLength(6);
      expect((await h.state(spark)).positions).toEqual({ "0": "18", "1": "18", "2": "18" });
    });

    it("retains incompatible writer records as dead letters in both paths", async () => {
      const incompatible = { ...v1, fields: [{ name: "id", type: "string" }] };
      const id = await register(subjects[2]!, incompatible);
      await producer.send({
        topic,
        acks: -1,
        messages: encode(id, incompatible, [{ id: "bad" }, { id: "bad" }, { id: "bad" }]).map(
          (value, partition) => ({ value, partition }),
        ),
      });
      for (const c of [spark, base]) {
        const run = await sync(c);
        expect(await h.run(run)).toMatchObject({ status: "succeeded", rows_committed: 0 });
        const dead = await h.db
          .from("connect_dead_letters")
          .select("error_code,position")
          .eq("connection_id", c.id);
        expect(dead.error).toBeNull();
        expect(dead.data).toHaveLength(3);
        expect(
          dead.data.every((d: { error_code: string }) => d.error_code === "schema_incompatible"),
        ).toBe(true);
        expect((await h.state(c)).positions).toEqual({ "0": "19", "1": "19", "2": "19" });
      }
    });

    it("replays incompatible Avro after a reader fix without moving live positions", async () => {
      const repairedReader = { ...v1, fields: [{ name: "id", type: "string" }] };
      for (const c of [spark, base]) {
        const saved = await h.db
          .from("connect_streams")
          .select("schema_json")
          .eq("id", c.stream)
          .single();
        expect(saved.error).toBeNull();
        expect(
          (
            await h.db
              .from("connect_streams")
              .update({
                schema_json: { ...saved.data.schema_json, "x-avro-reader-schema": repairedReader },
              })
              .eq("id", c.stream)
          ).error,
        ).toBeNull();
        const letters = await h.db
          .from("connect_dead_letters")
          .select("id")
          .eq("connection_id", c.id)
          .eq("status", "open");
        expect(letters.error).toBeNull();
        expect(letters.data).toHaveLength(3);
        const queued = await h.owner.rpc("connect_enqueue_replay", {
          _connection: c.id,
          _request: crypto.randomUUID(),
          _dead_letters: letters.data.map((d: { id: string }) => d.id),
        });
        expect(queued.error).toBeNull();
        await h.agent(c);
        expect(await h.run(queued.data)).toMatchObject({ status: "succeeded", rows_committed: 3 });
        expect((await h.landed(queued.data)).lines.map((line) => line.data["id"])).toEqual([
          "bad",
          "bad",
          "bad",
        ]);
        expect((await h.state(c)).positions).toEqual({ "0": "19", "1": "19", "2": "19" });
        const open = await h.db
          .from("connect_dead_letters")
          .select("id")
          .eq("connection_id", c.id)
          .eq("status", "open");
        expect(open.error).toBeNull();
        expect(open.data).toEqual([]);
      }
    });
  },
);
