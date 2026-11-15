// @vitest-environment node
// Kafka per-partition tasks (migration 0095) on the live stack: a Kafka stream's planning task is
// split into one task per partition; each partition task is leased, committed and retried on its
// own, merges only its own offset into the stream state, and its commits keep one stream-wide
// sequence for Bronze readers. No broker is needed: the agent's plan step is played by the test.
import { createClient } from "@supabase/supabase-js";
import { beforeAll, describe, expect, it } from "vitest";
import { sha256 } from "@/lib/ingest";

const url = process.env["SUPABASE_URL"],
  key = process.env["SUPABASE_PUBLISHABLE_KEY"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"];

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Client = any;
type Claim = {
  run_id: string;
  lease_token: string;
  task?: boolean;
  streams: {
    id: string;
    sequence: number;
    shard: string;
    splittable: boolean;
    state: Record<string, unknown>;
  }[];
};
type Task = {
  stream_id: string;
  shard: string;
  task_status: string;
  checkpoint: Record<string, unknown>;
  sequence: number;
};
let db: Client, owner: Client;
let ownerId = "",
  source = "",
  kafka = "",
  postgres = "",
  stream = "";
const saved = {
  positions: { "0": "5", "1": "7" },
  cluster_id: "cluster-a",
  topic_id: "topic-a",
  completed: true,
};

async function identity() {
  const email = `kparts-${crypto.randomUUID()}@example.test`,
    password = `P-${crypto.randomUUID()}`;
  const u = await db.auth.admin.createUser({ email, password, email_confirm: true });
  if (u.error) throw new Error("Test identity creation failed");
  const c = createClient(url!, key!, { auth: { persistSession: false } });
  const s = await c.auth.signInWithPassword({ email, password });
  if (s.error) throw new Error("Test sign-in failed");
  return { id: u.data.user.id as string, c };
}

async function connection(name: string, version: string) {
  const s = await db
    .from("ingest_sources")
    .insert({
      owner_id: ownerId,
      name,
      kind: "kafka",
      target_table: `raw_${name}`,
      config: {},
    })
    .select("id")
    .single();
  if (s.error) throw new Error(s.error.message);
  source ||= s.data.id;
  const c = await db
    .from("connect_connections")
    .insert({
      source_id: s.data.id,
      owner_id: ownerId,
      name,
      credential_ref: "test-only",
      source_version: version,
    })
    .select("id")
    .single();
  if (c.error) throw new Error(c.error.message);
  const st = await db
    .from("connect_streams")
    .insert({
      connection_id: c.data.id,
      namespace: "kafka",
      name: "events",
      primary_key: "_partition,_offset",
      cursor_field: "_offset",
      key_columns: ["_partition", "_offset"],
      schema_json: { "x-kafka-topic": "events" },
      state: saved,
    })
    .select("id")
    .single();
  if (st.error) throw new Error(st.error.message);
  return { id: c.data.id as string, stream: st.data.id as string, source: s.data.id as string };
}

async function enqueue(conn: string) {
  const q = await owner.rpc("connect_enqueue", {
    _connection: conn,
    _request: crypto.randomUUID(),
    _operation: "sync",
  });
  if (q.error) throw new Error(q.error.message);
  return q.data as string;
}

async function claimTask(conn: string, worker = "w:0") {
  const r = await db.rpc("connect_claim_task", { _connection: conn, _worker: worker });
  if (r.error) throw new Error(r.error.message);
  return r.data as Claim | null;
}

const split = (run: string, token: string, shards: string[]) =>
  db.rpc("connect_split_task", { _run: run, _token: token, _shards: shards });

// Lands one real Bronze batch and commits it under the given lease token.
async function commit(
  run: string,
  token: string,
  sequence: number,
  checkpoint: Record<string, unknown>,
  done: boolean,
  src = source,
  st = stream,
) {
  const batch = crypto.randomUUID();
  const objectKey = `${ownerId}/published/${batch}/landing.ndjson`;
  const text = JSON.stringify({ protocol: "meridian-connect/1", data: { batch } }) + "\n";
  const digest = await sha256(text);
  const up = await db.storage.from("ingest-files").upload(objectKey, new Blob([text]));
  if (up.error) throw new Error(up.error.message);
  const landed = await db.rpc("register_ingest_landing_batch", {
    _actor: ownerId,
    _source: src,
    _key: objectKey,
    _sha: digest,
    _rows: 1,
    _mode: "append",
    _batch: batch,
  });
  if (landed.error) throw new Error(landed.error.message);
  const args = {
    _run: run,
    _token: token,
    _stream: st,
    _sequence: sequence,
    _batch: landed.data,
    _manifest: `${ownerId}/published/${batch}/manifest.json`,
    _sha: digest,
    _rows: 1,
    _bytes: text.length,
    _checkpoint: { ...checkpoint, completed: done },
    _completed: done,
  };
  return {
    result: await db.rpc("connect_commit", args),
    retry: () => db.rpc("connect_commit", args),
  };
}

const finish = (run: string, token: string, ok: boolean, code = ok ? "ok" : "connector_failed") =>
  db.rpc("connect_finish", { _run: run, _token: token, _ok: ok, _code: code });

async function tasks(run: string) {
  return (
    (await db.from("connect_run_streams").select("*").eq("run_id", run).order("shard"))
      .data as Task[]
  ).map((t) => ({ ...t }));
}
async function streamState(id = stream) {
  return (await db.from("connect_streams").select("state").eq("id", id).single()).data.state;
}
async function cancelActive(conn: string) {
  await db
    .from("connect_runs")
    .update({ status: "cancelled", lease_token: null, lease_until: null })
    .eq("connection_id", conn)
    .in("status", ["queued", "running", "retry_wait"]);
}

describe.skipIf(!url || !key || !service)(
  "Kafka per-partition tasks on the live stack",
  { timeout: 120000 },
  () => {
    beforeAll(async () => {
      db = createClient(url!, service!, { auth: { persistSession: false } });
      ({ id: ownerId, c: owner } = await identity());
      const k = await connection("kparts", "kafka-v1");
      kafka = k.id;
      stream = k.stream;
      source = k.source;
      const p = await connection("kparts_pg", "postgres-v1");
      postgres = p.id;
    });

    it("splits a Kafka stream into one task per partition, each with its own offsets", async () => {
      const run = await enqueue(kafka);
      const planner = (await claimTask(kafka))!;
      expect(planner.run_id).toBe(run);
      expect(planner.streams[0]).toMatchObject({ shard: "", splittable: true });

      const s = await split(run, planner.lease_token, ["0", "1", "2"]);
      expect(s.error).toBeNull();
      expect(s.data).toBe(3);

      const ts = await tasks(run);
      expect(ts.map((t) => [t.shard, t.task_status])).toEqual([
        ["0", "queued"],
        ["1", "queued"],
        ["2", "queued"],
      ]);
      // Positions restricted to the partition; identity kept; a new partition starts empty.
      expect(ts[0]!.checkpoint).toMatchObject({ positions: { "0": "5" }, topic_id: "topic-a" });
      expect(ts[1]!.checkpoint).toMatchObject({ positions: { "1": "7" }, cluster_id: "cluster-a" });
      expect(ts[2]!.checkpoint).toMatchObject({ positions: {} });

      // The planner's lease ended with the split: it can neither commit nor split again.
      expect(
        (await commit(run, planner.lease_token, 0, { positions: { "0": "6" } }, false)).result
          .error,
      ).not.toBeNull();
      expect((await split(run, planner.lease_token, ["0"])).error).not.toBeNull();

      // Three workers hold the three partitions at once.
      const claims = await Promise.all(["a:0", "b:0", "c:0"].map((w) => claimTask(kafka, w)));
      expect(claims.map((c) => c!.streams[0]!.shard).sort()).toEqual(["0", "1", "2"]);
      expect(claims.every((c) => c!.streams[0]!.splittable === false)).toBe(true);
      expect(new Set(claims.map((c) => c!.lease_token)).size).toBe(3);
      const by = Object.fromEntries(claims.map((c) => [c!.streams[0]!.shard, c!]));

      // Partition 1 commits first; partition 0 then gets the next stream-wide sequence.
      const c1 = await commit(
        run,
        by["1"]!.lease_token,
        0,
        { positions: { "1": "9" }, topic_id: "topic-a" },
        true,
      );
      expect(c1.result.error).toBeNull();
      const c0 = await commit(
        run,
        by["0"]!.lease_token,
        0,
        { positions: { "0": "8" }, topic_id: "topic-a" },
        false,
      );
      expect(c0.result.error).toBeNull();
      const commits = (
        await db
          .from("connect_commits")
          .select("sequence,shard,shard_sequence")
          .eq("run_id", run)
          .order("sequence")
      ).data;
      expect(commits).toEqual([
        { sequence: 0, shard: "1", shard_sequence: 0 },
        { sequence: 1, shard: "0", shard_sequence: 0 },
      ]);
      // A retried commit (lost response) returns the same commit, adding nothing.
      const again = await c0.retry();
      expect(again.data).toBe(c0.result.data);

      // Each commit merged only its own partition; others keep their saved offsets.
      expect(await streamState()).toMatchObject({
        positions: { "0": "8", "1": "9" },
        cluster_id: "cluster-a",
      });

      // A partition task may not move another partition's offset, nor use another task's token.
      expect(
        (await commit(run, by["2"]!.lease_token, 0, { positions: { "0": "100" } }, true)).result
          .error,
      ).not.toBeNull();
      expect(
        (await commit(run, by["0"]!.lease_token, 1, { positions: { "2": "3" } }, true)).result
          .error,
      ).not.toBeNull();

      // Partition 0 fails once: only it goes back to retry; the run keeps running.
      expect(
        (await finish(run, by["0"]!.lease_token, false, "source_unavailable")).error,
      ).toBeNull();
      expect((await finish(run, by["1"]!.lease_token, true)).error).toBeNull();
      const mid = await tasks(run);
      expect(mid.map((t) => t.task_status)).toEqual(["retry_wait", "done", "running"]);
      await db
        .from("connect_run_streams")
        .update({ next_attempt_at: "2000-01-01T00:00:00Z" })
        .eq("run_id", run)
        .eq("shard", "0");
      const retried = (await claimTask(kafka, "d:0"))!;
      expect(retried.streams[0]).toMatchObject({
        shard: "0",
        sequence: 1,
        state: { positions: { "0": "8" } },
      });

      expect(
        (await commit(run, retried.lease_token, 1, { positions: { "0": "12" } }, true)).result
          .error,
      ).toBeNull();
      expect(
        (await commit(run, by["2"]!.lease_token, 0, { positions: { "2": "4" } }, true)).result
          .error,
      ).toBeNull();
      expect((await finish(run, retried.lease_token, true)).error).toBeNull();
      expect((await finish(run, by["2"]!.lease_token, true)).error).toBeNull();
      const done = (await db.from("connect_runs").select("status").eq("id", run).single()).data;
      expect(done.status).toBe("succeeded");
      expect((await streamState()).positions).toEqual({ "0": "12", "1": "9", "2": "4" });
      // Bronze readers still page one gap-free sequence per stream.
      const seqs = (
        await db.from("connect_commits").select("sequence").eq("run_id", run).order("sequence")
      ).data;
      expect(seqs.map((c: { sequence: number }) => c.sequence)).toEqual([0, 1, 2, 3]);
    });

    it("refuses splits that are not a Kafka planning task under its live lease", async () => {
      // Not Kafka: a PostgreSQL stream is never splittable.
      const pgRun = await enqueue(postgres);
      const pg = (await claimTask(postgres))!;
      expect(pg.streams[0]).toMatchObject({ shard: "", splittable: false });
      expect((await split(pgRun, pg.lease_token, ["0"])).error).not.toBeNull();
      await cancelActive(postgres);

      // A planning task that already committed reads the whole stream instead.
      const run = await enqueue(kafka);
      const t = (await claimTask(kafka))!;
      expect(
        (await commit(run, t.lease_token, 0, { positions: { "0": "12" } }, false)).result.error,
      ).toBeNull();
      expect((await split(run, t.lease_token, ["0", "1"])).error).not.toBeNull();
      // Malformed shard lists.
      for (const bad of [[], ["0", "0"], ["-1"], ["x"]])
        expect((await split(run, t.lease_token, bad)).error).not.toBeNull();
      await cancelActive(kafka);

      // A pre-task agent leases the whole run: no shard, no split.
      const old = await enqueue(kafka);
      const claim = (await db.rpc("connect_claim", { _connection: kafka })).data as Claim;
      expect(claim.run_id).toBe(old);
      expect(claim.streams[0]!.shard).toBeUndefined();
      expect((await split(old, claim.lease_token, ["0"])).error).not.toBeNull();
      await cancelActive(kafka);
    });

    it("records partition lag only under the reporting task's lease, readable by the owner", async () => {
      const run = await enqueue(kafka);
      const planner = (await claimTask(kafka))!;
      expect((await split(run, planner.lease_token, ["0", "1"])).error).toBeNull();
      const t0 = (await claimTask(kafka, "l:0"))!;
      expect(t0.streams[0]!.shard).toBe("0");
      const lag = (token: string, partitions: unknown) =>
        db.rpc("connect_record_lag", {
          _run: run,
          _token: token,
          _stream: stream,
          _partitions: partitions,
        });
      expect(
        (await lag(t0.lease_token, [{ partition: "0", position: "12", end: "40" }])).error,
      ).toBeNull();
      // Another partition, the ended planner lease, malformed and duplicate entries: refused.
      for (const [token, partitions] of [
        [t0.lease_token, [{ partition: "1", position: "1", end: "2" }]],
        [planner.lease_token, [{ partition: "0", position: "1", end: "2" }]],
        [t0.lease_token, [{ partition: "0", position: "-1", end: "2" }]],
        [t0.lease_token, [{ partition: "0", position: "1.5", end: "2" }]],
        [t0.lease_token, []],
        [
          t0.lease_token,
          [
            { partition: "0", position: "1", end: "2" },
            { partition: "0", position: "1", end: "2" },
          ],
        ],
      ] as [string, unknown][])
        expect((await lag(token, partitions)).error, JSON.stringify(partitions)).not.toBeNull();
      // Later observations replace earlier ones; lag never goes negative.
      expect(
        (await lag(t0.lease_token, [{ partition: "0", position: "50", end: "45" }])).error,
      ).toBeNull();
      const rows = (
        await owner
          .from("connect_partition_lag")
          .select("partition,position,end_offset,lag")
          .eq("stream_id", stream)
      ).data;
      expect(rows).toEqual([{ partition: 0, position: 50, end_offset: 45, lag: 0 }]);
      // Clients can neither write lag rows nor call the RPC.
      expect(
        (
          await owner
            .from("connect_partition_lag")
            .insert({ stream_id: stream, partition: 9, position: 0, end_offset: 0, run_id: run })
        ).error,
      ).not.toBeNull();
      expect(
        (
          await owner.rpc("connect_record_lag", {
            _run: run,
            _token: t0.lease_token,
            _stream: stream,
            _partitions: [{ partition: "0", position: "1", end: "2" }],
          })
        ).error,
      ).not.toBeNull();
      // Another account sees nothing.
      const outsider = (await identity()).c;
      expect(
        (await outsider.from("connect_partition_lag").select("partition").eq("stream_id", stream))
          .data,
      ).toEqual([]);
      await cancelActive(kafka);
    });

    it("keeps the split RPC service-only", async () => {
      const r = await owner.rpc("connect_split_task", {
        _run: crypto.randomUUID(),
        _token: crypto.randomUUID(),
        _shards: ["0"],
      });
      expect(r.error).not.toBeNull();
    });
  },
);
