// @vitest-environment node
// Real membership, partition actors, fenced task leases and Bronze; no Docker lifecycle here.
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { spawn, execFileSync, type ChildProcess } from "node:child_process";
import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import { Kafka, logLevel, type Admin, type Producer } from "kafkajs";
import { APP, SparkHarness, key, service, url, type Conn } from "./connect-spark-harness";

const brokers = process.env["KAFKA_CERTIFY_BROKERS"];
const topic = `shared_e2e_${crypto.randomUUID().slice(0, 8)}`;
const group = `meridian-${topic}`;
const h = new SparkHarness(false);
let admin: Admin;
let producer: Producer;
let connection: Conn;
const children: ChildProcess[] = [];
const agentLogs: { name: string; logs: () => string }[] = [];

async function until<T>(
  label: string,
  read: () => Promise<T>,
  ready: (value: T) => boolean,
  ms = 120000,
) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    const value = await read();
    if (ready(value)) return value;
    await new Promise((resolve) => setTimeout(resolve, 300));
  }
  throw new Error(`Timed out: ${label}`);
}

async function produce(start: number, count: number) {
  await producer.send({
    topic,
    acks: -1,
    messages: Array.from({ length: 4 }, (_, partition) =>
      Array.from({ length: count }, (_, n) => ({
        partition,
        value: JSON.stringify({ id: `${partition}:${start + n}` }),
      })),
    ).flat(),
  });
}

async function start(name: string, slots = 2) {
  const tokenFile = join(h.folder, `token-${name}`);
  await writeFile(tokenFile, connection.token, { mode: 0o600 });
  const child = spawn(process.env["PYTHON"] ?? "python3", ["deploy/connect/agent.py"], {
    env: {
      ...process.env,
      PYTHONPATH: "deploy/connect",
      MERIDIAN_URL: APP,
      INGEST_TOKEN_FILE: tokenFile,
      CONNECT_SECRET_DIR: h.folder,
      CONNECT_ONCE: "1",
      CONNECT_KAFKA_SHARED: "1",
      CONNECT_WORKER_ID: name,
      CONNECT_SLOTS: String(slots),
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  children.push(child);
  let logs = "";
  child.stdout!.on("data", (data: Buffer) => {
    logs += data.toString();
  });
  child.stderr!.on("data", (data: Buffer) => {
    logs += data.toString();
  });
  agentLogs.push({ name, logs: () => logs });
  const done = new Promise<number | null>((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code) => resolve(code));
  });
  return { child, done, logs: () => logs };
}

function kill(child: ChildProcess) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  if (process.platform === "win32")
    execFileSync("taskkill", ["/pid", String(child.pid), "/T", "/F"], { stdio: "ignore" });
  else child.kill("SIGKILL");
}

async function members(count: number) {
  await until(
    `${count} stable group members`,
    () => admin.describeGroups([group]),
    (result) => result.groups[0]?.state === "Stable" && result.groups[0].members.length === count,
  );
}

async function tasks(run: string) {
  const result = await h.db
    .from("connect_run_streams")
    .select("shard,task_status,worker,lease_token,handoffs,checkpoint")
    .eq("run_id", run);
  if (result.error) throw result.error;
  return result.data as {
    shard: string;
    task_status: string;
    worker: string | null;
    lease_token: string | null;
    handoffs: number;
    checkpoint: Record<string, unknown>;
  }[];
}

async function assertLanded(run: string, start: number, count: number) {
  await until(
    "successful shared run",
    () => h.run(run),
    (r) => r.status === "succeeded",
    300000,
  );
  const result = await h.landed(run);
  expect(result.lines).toHaveLength(count * 4);
  expect(new Set(result.lines.map((line) => JSON.stringify(line["primary_key"]))).size).toBe(
    count * 4,
  );
  expect(result.lines.map((line) => line.data["id"]).sort()).toEqual(
    Array.from({ length: 4 }, (_, p) =>
      Array.from({ length: count }, (_, n) => `${p}:${start + n}`),
    )
      .flat()
      .sort(),
  );
  expect((await h.state(connection)).positions).toEqual(
    Object.fromEntries(Array.from({ length: 4 }, (_, p) => [String(p), String(start + count)])),
  );
  const offsets = await admin.fetchOffsets({ groupId: group, topics: [topic] });
  expect(offsets[0]!.partitions.map((p) => p.offset)).toEqual(["100", "100", "100", "100"]);
  expect((await tasks(run)).filter((task) => task.shard !== "")).toHaveLength(4);
}

describe.skipIf(!url || !service || !key || !brokers)(
  "shared Kafka: two real agents",
  { timeout: 420000 },
  () => {
    beforeAll(async () => {
      const kafka = new Kafka({ brokers: brokers!.split(","), logLevel: logLevel.NOTHING });
      admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({
        waitForLeaders: false,
        topics: [{ topic, numPartitions: 4, replicationFactor: 3 }],
      });
      for (const broker of brokers!.split(",")) {
        const probe = new Kafka({ brokers: [broker], logLevel: logLevel.NOTHING }).admin();
        await probe.connect();
        try {
          await until(
            `topic metadata on ${broker}`,
            () => probe.fetchTopicMetadata({ topics: [topic] }).catch(() => null),
            (metadata) =>
              metadata?.topics[0]?.partitions.length === 4 &&
              metadata.topics[0].partitions.every((partition) => partition.leader >= 0),
            30000,
          );
        } finally {
          await probe.disconnect();
        }
      }
      producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
      await producer.connect();
      await produce(0, 100);
      // Deliberately ahead of Postgres. Group membership must never make these authoritative.
      await admin.setOffsets({
        groupId: group,
        topic,
        partitions: Array.from({ length: 4 }, (_, partition) => ({ partition, offset: "100" })),
      });
      await h.setup();
      await h.secret("shared", {
        bootstrap_servers: brokers,
        consumer_group: true,
        group_id: group,
        // One record per commit keeps every partition task running long enough to be revoked.
        max_batch_records: 1,
        max_run_seconds: 300,
      });
      connection = await h.connection(topic, "kafka-v1", "shared");
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

    afterEach(async ({ task }) => {
      // A failed test must not leave agents behind to claim the next test's run.
      const running = children
        .splice(0)
        .filter((c) => c.exitCode === null && c.signalCode === null);
      const exited = running.map((c) => new Promise((resolve) => c.once("close", resolve)));
      for (const child of running) kill(child);
      await Promise.all(exited);
      if (task.result?.state === "fail") {
        // Agents run as child processes; their output is the only evidence of a membership race.
        for (const agent of agentLogs)
          console.error(`--- ${agent.name}\n${agent.logs().slice(-20000)}`);
        const runs = await h.db
          .from("connect_run_streams")
          .select("run_id,shard,task_status,worker,handoffs,attempts,started_at,finished_at")
          .gte("started_at", new Date(Date.now() - 600000).toISOString())
          .order("started_at");
        console.error(JSON.stringify(runs.data));
      }
      agentLogs.length = 0;
    }, 60000);

    afterAll(async () => {
      for (const child of children) kill(child);
      await producer?.disconnect();
      await admin?.deleteTopics({ topics: [topic] }).catch(() => undefined);
      await admin?.deleteGroups([group]).catch(() => undefined);
      await admin?.disconnect();
      await h.teardown();
    }, 60000);

    it("joins during ingestion, hands off fenced leases and ignores broker offsets", async () => {
      const run = await h.enqueue(connection, "sync");
      // A holds every partition task before B exists, so B's join must revoke and hand off.
      const a = await start("shared-join-a", 4);
      await until(
        "first agent leases every partition and lands",
        async () => [await h.run(run), await tasks(run)] as const,
        ([r, rows]) =>
          r.rows_committed > 0 &&
          rows.filter(
            (row) => row.task_status === "running" && row.worker?.startsWith("shared-join-a:"),
          ).length === 4,
      );
      const b = await start("shared-join-b");
      await members(2);
      await until(
        "both agents own partition tasks",
        () => tasks(run),
        (rows) =>
          rows.some((r) => r.worker?.startsWith("shared-join-a:")) &&
          rows.some((r) => r.worker?.startsWith("shared-join-b:")),
      );
      await assertLanded(run, 0, 100);
      expect((await tasks(run)).reduce((sum, task) => sum + task.handoffs, 0)).toBeGreaterThan(0);
      expect(await a.done, a.logs()).toBe(0);
      expect(await b.done, b.logs()).toBe(0);
    });

    it("recovers a killed agent through real lease expiry without lost or duplicate Bronze", async () => {
      await h.secret("shared", {
        bootstrap_servers: brokers,
        consumer_group: true,
        group_id: group,
        // Small commits keep A's task running after its first checkpoint, before it is killed.
        max_batch_records: 5,
        max_run_seconds: 300,
      });
      await produce(100, 400);
      const run = await h.enqueue(connection, "sync");
      const a = await start("shared-death-a");
      const b = await start("shared-death-b");
      await members(2);
      const owned = await until(
        "two leased workers with committed progress",
        () => tasks(run),
        (rows) =>
          rows.some(
            (r) =>
              r.task_status === "running" &&
              r.worker?.startsWith("shared-death-a:") &&
              Number(
                (r.checkpoint["positions"] as Record<string, unknown> | undefined)?.[r.shard] ?? 0,
              ) > 100,
          ) &&
          rows.some((r) => r.task_status === "running" && r.worker?.startsWith("shared-death-b:")),
      );
      const stale = owned.find(
        (r) =>
          r.task_status === "running" &&
          r.worker?.startsWith("shared-death-a:") &&
          Number(
            (r.checkpoint["positions"] as Record<string, unknown> | undefined)?.[r.shard] ?? 0,
          ) > 100,
      )!;
      kill(a.child);
      await members(1);
      await until(
        "survivor reclaims the killed member's lease",
        () => tasks(run),
        (rows) =>
          rows.some(
            (r) =>
              r.shard === stale.shard &&
              r.task_status === "running" &&
              r.worker?.startsWith("shared-death-b:") &&
              r.lease_token !== stale.lease_token,
          ),
        180000,
      );
      const rejected = await fetch(`${APP}/api/public/connect-agent`, {
        method: "POST",
        headers: { "x-ingest-token": connection.token, "content-type": "application/json" },
        body: JSON.stringify({ action: "heartbeat", runId: run, token: stale.lease_token }),
      });
      expect(rejected.status).toBe(409);
      await assertLanded(run, 100, 400);
      expect(await b.done, b.logs()).toBe(0);
    });
  },
);
