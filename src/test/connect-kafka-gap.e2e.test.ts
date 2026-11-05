// @vitest-environment node
// Kafka positions below retention through the real agent and control plane, against the 3-broker
// cluster in deploy/connect/certify.compose.yml: records the stream never read are deleted, the
// run fails closed without moving the checkpoint and reports the gap, only the owner can accept
// it, and the accepted run resumes at the low watermark with the gap kept in commit history.
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
  app = process.env["APP_URL"] ?? "http://localhost:8080",
  python = process.env["PYTHON"] ?? "python3";
const topic = `certify.gap.${crypto.randomUUID().slice(0, 8)}`;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let db: any, owner: any, peer: any, admin: any, producer: any;
let uid = "",
  peerId = "",
  connection = "",
  stream = "",
  folder = "";
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
async function sync() {
  const r = await owner.rpc("connect_enqueue", {
    _connection: connection,
    _request: crypto.randomUUID(),
    _operation: "sync",
  });
  if (r.error) throw r.error;
  await agent();
  return (
    await db
      .from("connect_runs")
      .select("id,status,error_code,rows_committed")
      .eq("id", r.data)
      .single()
  ).data;
}
const streamState = async () =>
  (await db.from("connect_streams").select("state").eq("id", stream).single()).data.state;
describe.skipIf(!url || !service || !key || !brokers)(
  "Connect Kafka offsets below retention",
  { timeout: 240000 },
  () => {
    beforeAll(async () => {
      db = createClient(url!, service!, { auth: { persistSession: false } });
      for (let i = 0; i < 2; i++) {
        const email = `connect-gap-${crypto.randomUUID()}@example.test`,
          password = `P-${crypto.randomUUID()}`;
        const u = await db.auth.admin.createUser({ email, password, email_confirm: true });
        if (u.error) throw u.error;
        const c = createClient(url!, key!, { auth: { persistSession: false } });
        await c.auth.signInWithPassword({ email, password });
        if (i === 0) [uid, owner] = [u.data.user.id, c];
        else [peerId, peer] = [u.data.user.id, c];
      }
      const token = `mi_${crypto.randomUUID().replaceAll("-", "")}${crypto.randomUUID().replaceAll("-", "")}`;
      const s = await db
        .from("ingest_sources")
        .insert({
          name: "Connect Kafka gap evidence",
          kind: "kafka",
          owner_id: uid,
          target_table: "connect_kafka_gap",
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
          name: "Kafka gap",
          credential_ref: "kafka",
          source_version: "kafka-v1",
        })
        .select("id")
        .single();
      if (c.error) throw c.error;
      connection = c.data.id;
      folder = await mkdtemp(join(tmpdir(), "connect-gap-"));
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
        topics: [{ topic, numPartitions: 1, replicationFactor: 3 }],
      });
      for (let i = 0; ; i++) {
        const meta = await admin.fetchTopicMetadata({ topics: [topic] }).catch(() => null);
        if (meta?.topics[0]?.partitions?.[0]?.leader >= 0) break;
        if (i > 30) throw new Error("topic leader not elected");
        await new Promise((r) => setTimeout(r, 1000));
      }
      producer = kafka.producer({ idempotent: true });
      await producer.connect();
      await producer.send({
        topic,
        acks: -1,
        messages: Array.from({ length: 20 }, (_, i) => ({ value: JSON.stringify({ id: i }) })),
      });
      const discover = await owner.rpc("connect_enqueue", {
        _connection: connection,
        _request: crypto.randomUUID(),
        _operation: "discover",
      });
      if (discover.error) throw discover.error;
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
    }, 120000);
    afterAll(async () => {
      await producer?.disconnect();
      await admin?.deleteTopics({ topics: [topic] }).catch(() => undefined);
      await admin?.disconnect();
      if (db) {
        await db.auth.admin.deleteUser(uid);
        await db.auth.admin.deleteUser(peerId);
      }
    }, 60000);

    const gapEvents = async (runId: string) =>
      (
        await db
          .from("connect_events")
          .select("detail")
          .eq("run_id", runId)
          .not("detail", "is", null)
      ).data.map((e: { detail: unknown }) => e.detail);

    it("fails closed below retention and resumes only for the exact gap the owner approved", async () => {
      expect(await sync()).toMatchObject({ status: "succeeded", rows_committed: 20 });
      const first = await streamState();
      expect(first.positions).toEqual({ "0": "20" });
      expect(first.topic_id).toBeTruthy();
      // The stream had read up to offset 5 when retention removed offsets 0-9.
      const saved = { ...first, positions: { "0": "5" } };
      expect(
        (await owner.rpc("connect_rewind_stream", { _stream: stream, _state: saved })).error,
      ).toBeNull();
      await admin.deleteTopicRecords({ topic, partitions: [{ partition: 0, offset: "10" }] });

      const failed = await sync();
      expect(failed).toMatchObject({ status: "failed", error_code: "offset_out_of_range" });
      expect(await streamState()).toEqual(saved);
      expect((await db.from("connect_commits").select("id").eq("run_id", failed.id)).data).toEqual(
        [],
      );
      expect(await gapEvents(failed.id)).toEqual([
        [{ partition: 0, offset: "5", low: "10", gap: "5", stream_id: stream }],
      ]);

      expect((await peer.rpc("connect_accept_offset_gap", { _stream: stream })).error).toBeTruthy();
      expect((await owner.rpc("connect_accept_offset_gap", { _stream: stream })).error).toBeNull();
      const approval = (await streamState()).gap_approval;
      expect(approval).toMatchObject({
        topic,
        topic_id: first.topic_id,
        generation: 0,
        approved_by: uid,
        partitions: [{ partition: 0, offset: "5", low: "10", gap: "5" }],
      });

      // Retention moves on after the approval: the run fails again with the larger gap.
      await admin.deleteTopicRecords({ topic, partitions: [{ partition: 0, offset: "12" }] });
      const stale = await sync();
      expect(stale).toMatchObject({ status: "failed", error_code: "offset_out_of_range" });
      expect(await gapEvents(stale.id)).toEqual([
        [{ partition: 0, offset: "5", low: "12", gap: "7", stream_id: stream }],
      ]);
      expect((await streamState()).positions).toEqual({ "0": "5" });

      expect((await owner.rpc("connect_accept_offset_gap", { _stream: stream })).error).toBeNull();
      const accepted = await db
        .from("connect_events")
        .select("actor_id,detail")
        .eq("connection_id", connection)
        .eq("code", "offset_gap_accepted")
        .order("created_at");
      expect(accepted.data.map((e: { actor_id: string }) => e.actor_id)).toEqual([uid, uid]);
      expect(accepted.data[1].detail).toEqual([{ partition: 0, offset: "5", low: "12", gap: "7" }]);

      const resumed = await sync();
      expect(resumed).toMatchObject({ status: "succeeded", rows_committed: 8 });
      const commit = await db
        .from("connect_commits")
        .select("checkpoint")
        .eq("run_id", resumed.id)
        .order("sequence", { ascending: false })
        .limit(1)
        .single();
      expect(commit.data.checkpoint.gaps).toEqual([{ partition: 0, from: "5", to: "12" }]);
      const after = await streamState();
      expect(after.positions).toEqual({ "0": "20" });
      expect(after.gap_approval).toBeUndefined();
      // The approval was for that gap only: the next run is ordinary.
      expect(await sync()).toMatchObject({ status: "succeeded", rows_committed: 0 });
    });

    it("rewind sets positions only, cannot forge an approval, and records what it set", async () => {
      const st = await streamState();
      const forged = {
        ...st,
        gap_approval: {
          topic,
          generation: 0,
          partitions: [{ partition: 0, offset: "0", low: "99" }],
        },
      };
      expect(
        (await owner.rpc("connect_rewind_stream", { _stream: stream, _state: forged })).error,
      ).toBeTruthy();
      for (const positions of [{ "0": "-1" }, { "0": "1e3" }, { x: "1" }, { "0": { n: 1 } }])
        expect(
          (
            await owner.rpc("connect_rewind_stream", {
              _stream: stream,
              _state: { version: 1, positions },
            })
          ).error,
          JSON.stringify(positions),
        ).toBeTruthy();
      expect(
        (
          await owner.rpc("connect_rewind_stream", {
            _stream: stream,
            _state: { ...st, positions: { "0": "20" } },
          })
        ).error,
      ).toBeNull();
      const ev = await db
        .from("connect_events")
        .select("actor_id,detail")
        .eq("connection_id", connection)
        .eq("code", "stream_rewound")
        .order("created_at", { ascending: false })
        .limit(1)
        .single();
      expect(ev.data).toEqual({
        actor_id: uid,
        detail: [{ stream_id: stream, positions: { "0": "20" } }],
      });
    });

    it("fails closed when the topic is recreated, and an explicit reset starts over", async () => {
      await admin.deleteTopics({ topics: [topic] });
      // Deletion completes asynchronously: retry creation until it is accepted once.
      let created = false;
      for (let i = 0; ; i++) {
        if (!created)
          created = await admin
            .createTopics({
              waitForLeaders: false,
              topics: [{ topic, numPartitions: 1, replicationFactor: 3 }],
            })
            .then(() => true)
            .catch(() => false);
        const meta = created
          ? await admin.fetchTopicMetadata({ topics: [topic] }).catch(() => null)
          : null;
        if (meta?.topics[0]?.partitions?.[0]?.leader >= 0) break;
        if (i > 30) throw new Error("topic not recreated");
        await new Promise((r) => setTimeout(r, 1000));
      }
      await producer.send({ topic, acks: -1, messages: [{ value: JSON.stringify({ id: 100 }) }] });
      const before = await streamState();
      const changed = await sync();
      expect(changed).toMatchObject({ status: "failed", error_code: "source_changed" });
      const detail = (await gapEvents(changed.id))[0];
      expect(detail.map((d: { reason: string }) => d.reason)).toContain("topic_id");
      expect(await streamState()).toEqual(before);
      const reset = await owner.rpc("connect_enqueue", {
        _connection: connection,
        _request: crypto.randomUUID(),
        _operation: "reset",
      });
      if (reset.error) throw reset.error;
      await agent();
      const r = await db
        .from("connect_runs")
        .select("status,rows_committed")
        .eq("id", reset.data)
        .single();
      expect(r.data).toEqual({ status: "succeeded", rows_committed: 1 });
      expect((await streamState()).topic_id).not.toBe(before.topic_id);
    });
  },
);
