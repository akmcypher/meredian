// @vitest-environment node
// Actual group actor -> leased agent -> fenced Postgres checkpoint + Bronze storage.
// Uses an existing app/Supabase/certification cluster; never manages Docker lifecycle.
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Kafka, logLevel, type Admin, type Producer } from "kafkajs";
import { SparkHarness, key, service, url, type Conn, type Line } from "./connect-spark-harness";

const brokers = process.env["KAFKA_CERTIFY_BROKERS"];
const topic = `group_e2e_${crypto.randomUUID().slice(0, 8)}`;
const group = `meridian-${topic}`;
const h = new SparkHarness(false);
let admin: Admin;
let producer: Producer;
let connection: Conn;
const config = {
  bootstrap_servers: brokers,
  consumer_group: true,
  group_id: group,
  max_batch_records: 2,
  max_run_seconds: 60,
  max_run_records: 4,
};

async function produce(start: number, count: number) {
  await producer.send({
    topic,
    acks: -1,
    messages: Array.from({ length: 2 }, (_, partition) =>
      Array.from({ length: count }, (_, n) => ({
        partition,
        value: JSON.stringify({ id: `${partition}:${start + n}` }),
      })),
    ).flat(),
  });
}

async function sync() {
  const run = await h.enqueue(connection, "sync");
  await h.agent(connection);
  return { run, result: await h.run(run), ...(await h.landed(run)) };
}

function keys(lines: Line[]) {
  return lines.map((line) => JSON.stringify(line["primary_key"]));
}

async function assertWholeTopicTask(run: string) {
  const tasks = await h.db
    .from("connect_run_streams")
    .select("shard,task_status,checkpoint")
    .eq("run_id", run);
  expect(tasks.error).toBeNull();
  expect(tasks.data).toHaveLength(1);
  expect(tasks.data[0]).toMatchObject({ shard: "", task_status: "done" });
  expect(tasks.data[0].checkpoint.positions).toEqual((await h.state(connection)).positions);
}

async function assertNoBrokerCommits() {
  const offsets = await admin.fetchOffsets({ groupId: group, topics: [topic] });
  expect(offsets[0]!.partitions.map((partition) => partition.offset)).toEqual(["-1", "-1"]);
}

describe.skipIf(!url || !service || !key || !brokers)(
  "Kafka consumer groups: real fenced Bronze landing",
  { timeout: 180000 },
  () => {
    beforeAll(async () => {
      const kafka = new Kafka({ brokers: brokers!.split(","), logLevel: logLevel.NOTHING });
      admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({
        waitForLeaders: false,
        topics: [{ topic, numPartitions: 2, replicationFactor: 3 }],
      });
      // Topic creation must propagate to every broker before producing or discovering.
      for (const broker of brokers!.split(",")) {
        const probe = new Kafka({ brokers: [broker], logLevel: logLevel.NOTHING }).admin();
        await probe.connect();
        try {
          for (let attempt = 0; ; attempt++) {
            const metadata = await probe.fetchTopicMetadata({ topics: [topic] }).catch(() => null);
            const partitions = metadata?.topics[0]?.partitions ?? [];
            if (partitions.length === 2 && partitions.every((partition) => partition.leader >= 0))
              break;
            if (attempt >= 30) throw new Error(`topic not ready on ${broker}`);
            await new Promise((resolve) => setTimeout(resolve, 1000));
          }
        } finally {
          await probe.disconnect();
        }
      }
      producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
      await producer.connect();
      await produce(0, 5);
      await h.setup();
      await h.secret("groups", config);
      connection = await h.connection(topic, "kafka-v1", "groups");
      // SparkHarness predates Kafka and defaults this source metadata to postgres.
      const source = await h.db
        .from("connect_connections")
        .select("source_id")
        .eq("id", connection.id)
        .single();
      expect(source.error).toBeNull();
      const updated = await h.db
        .from("ingest_sources")
        .update({ kind: "kafka" })
        .eq("id", source.data.source_id);
      expect(updated.error).toBeNull();
      await h.stream(
        connection,
        {
          namespace: "kafka",
          name: topic,
          primary_key: "_partition,_offset",
          cursor_field: "_offset",
        },
        "incremental",
      );
    }, 180000);

    afterAll(async () => {
      await producer?.disconnect();
      await admin?.deleteTopics({ topics: [topic] }).catch(() => undefined);
      await admin?.disconnect();
      await h.teardown();
    }, 60000);

    it("keeps one whole-topic task and resumes bounded runs from actual Bronze commits", async () => {
      const first = await sync();
      expect(first.result).toMatchObject({ status: "succeeded", rows_committed: 4 });
      expect(first.lines).toHaveLength(4);
      const checkpoint = await h.state(connection);
      expect(
        Object.values(checkpoint.positions).reduce<number>(
          (sum, offset) => sum + Number(offset),
          0,
        ),
      ).toBe(4);
      expect(checkpoint.cluster_id).toBeTruthy();
      expect(checkpoint.topic_id).toBeTruthy();
      await assertWholeTopicTask(first.run);

      await h.secret("groups", { ...config, max_run_records: 100 });
      const remainder = await sync();
      expect(remainder.result).toMatchObject({ status: "succeeded", rows_committed: 6 });
      const initial = [...first.lines, ...remainder.lines];
      expect(initial).toHaveLength(10);
      expect(new Set(keys(initial)).size).toBe(10);
      expect(initial.map((line) => line.data["id"]).sort()).toEqual(
        Array.from({ length: 2 }, (_, p) => Array.from({ length: 5 }, (_, n) => `${p}:${n}`))
          .flat()
          .sort(),
      );
      expect((await h.state(connection)).positions).toEqual({ "0": "5", "1": "5" });
      await assertWholeTopicTask(remainder.run);

      await produce(5, 2);
      const appended = await sync();
      expect(appended.result).toMatchObject({ status: "succeeded", rows_committed: 4 });
      expect(appended.lines.map((line) => line.data["id"]).sort()).toEqual([
        "0:5",
        "0:6",
        "1:5",
        "1:6",
      ]);
      expect(new Set(keys([...initial, ...appended.lines])).size).toBe(14);
      const state = await h.state(connection);
      expect(state.positions).toEqual({ "0": "7", "1": "7" });
      const commits = await h.db
        .from("connect_commits")
        .select("checkpoint")
        .eq("run_id", appended.run)
        .order("sequence");
      expect(commits.error).toBeNull();
      expect(commits.data.at(-1).checkpoint.positions).toEqual(state.positions);
      await assertWholeTopicTask(appended.run);

      const empty = await sync();
      expect(empty.result).toMatchObject({ status: "succeeded", rows_committed: 0 });
      expect(empty.lines).toEqual([]);
      expect((await h.state(connection)).positions).toEqual(state.positions);
      await assertNoBrokerCommits();
    });

    it("a rejected Bronze commit leaves Postgres offsets unchanged and recovery lands the batch", async () => {
      const before = await h.state(connection);
      const budget = await h.db
        .from("connect_connections")
        .update({ dead_letter_budget: 0 })
        .eq("id", connection.id);
      expect(budget.error).toBeNull();
      await producer.send({
        topic,
        acks: -1,
        messages: [
          { partition: 0, value: Buffer.from([0xff, 0xfe]) },
          { partition: 0, value: JSON.stringify({ id: "after-poison" }) },
        ],
      });
      const rejected = await sync();
      expect(rejected.result).toMatchObject({ status: "failed", error_code: "dead_letter_budget" });
      expect(rejected.lines).toEqual([]);
      expect(await h.state(connection)).toEqual(before);
      const failedLetters = await h.db
        .from("connect_dead_letters")
        .select("id")
        .eq("run_id", rejected.run);
      expect(failedLetters.data).toEqual([]);
      await assertNoBrokerCommits();

      const raised = await h.db
        .from("connect_connections")
        .update({ dead_letter_budget: 1 })
        .eq("id", connection.id);
      expect(raised.error).toBeNull();
      const recovered = await sync();
      expect(recovered.result).toMatchObject({ status: "succeeded", rows_committed: 1 });
      expect(recovered.lines.map((line) => line.data["id"])).toEqual(["after-poison"]);
      expect((await h.state(connection)).positions).toEqual({ "0": "9", "1": "7" });
      const letters = await h.db
        .from("connect_dead_letters")
        .select("error_code,position")
        .eq("run_id", recovered.run);
      expect(letters.data).toEqual([
        { error_code: "deserialization_failed", position: `${before.topic_id}:0:7` },
      ]);
      await assertWholeTopicTask(recovered.run);
      await assertNoBrokerCommits();
    });
  },
);
