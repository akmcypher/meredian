// @vitest-environment node
// Worker pools (migration 0108, deploy/connect/README.md "Worker pools") on the live stack: three
// agents with four slots each, holding one pool token, drain sync runs of 50 connections. Every
// run lands each row once with one commit per sequence, and the work spreads across the agents.
// A connection's parallel task limit holds on pool claims, and a pool token never reaches another
// workspace. Needs the dev app and the seeded connect_acceptance fixture; skips otherwise.
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn } from "node:child_process";
import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import { Client as PgClient } from "pg";
import { sha256 } from "@/lib/ingest";
import { APP, SparkHarness, url, service, key, type Conn } from "./connect-spark-harness";

const source = {
  host: process.env["CONNECT_TEST_HOST"] ?? "127.0.0.1",
  port: Number(process.env["CONNECT_TEST_PORT"] ?? 54322),
  database: process.env["CONNECT_TEST_DB"] ?? "connect_source",
  user: "postgres",
  password: process.env["PGPASSWORD"] ?? "postgres",
};
// The PostgreSQL actor's credential file: libpq keys only.
const credential = () => ({
  host: source.host,
  port: source.port,
  dbname: source.database,
  user: source.user,
  password: source.password,
  sslmode: "disable",
});
const CONNECTIONS = 50;
const table = {
  namespace: "public",
  name: "connect_acceptance",
  primary_key: "id",
  cursor_field: "updated_at",
};
const extra = `pool_limit_${crypto.randomUUID().slice(0, 8)}`;
const h = new SparkHarness(false);
const other = new SparkHarness(false);
const conns: Conn[] = [];
let pool = { id: "", token: "" };
let schema: unknown;

const newToken = () =>
  `mp_${crypto.randomUUID().replaceAll("-", "")}${crypto.randomUUID().replaceAll("-", "")}`;
async function createPool(db: SparkHarness, name: string) {
  const token = newToken();
  const r = await db.db
    .from("connect_pools")
    .insert({ owner_id: db.uid, name, token_hash: await sha256(token) })
    .select("id")
    .single();
  if (r.error) throw r.error;
  return { id: r.data.id as string, token };
}
// One pool agent pass (CONNECT_ONCE): each slot claims until nothing is claimable.
async function poolAgent(token: string, id: string, slots: number) {
  const tokenFile = join(h.folder, `pool_${id}`);
  await writeFile(tokenFile, token, { mode: 0o600 });
  return await new Promise<string>((resolve, reject) => {
    const p = spawn(process.env["PYTHON"] ?? "python3", ["deploy/connect/agent.py"], {
      env: {
        ...process.env,
        PYTHONPATH: "deploy/connect",
        MERIDIAN_URL: APP,
        INGEST_TOKEN_FILE: tokenFile,
        CONNECT_SECRET_DIR: h.folder,
        CONNECT_ONCE: "1",
        CONNECT_SLOTS: String(slots),
        CONNECT_WORKER_ID: id,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let out = "";
    p.stdout!.setEncoding("utf8").on("data", (d: string) => (out += d));
    p.stderr!.resume();
    p.on("close", (code) => (code === 0 ? resolve(out) : reject(new Error("Agent failed"))));
  });
}
async function call(token: string, payload: Record<string, unknown>) {
  const res = await fetch(`${APP}/api/public/connect-agent`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-ingest-token": token },
    body: JSON.stringify(payload),
  });
  return { status: res.status, body: await res.json() };
}

describe.skipIf(!url || !service || !key)("Worker pools (M2)", { timeout: 900000 }, () => {
  beforeAll(async () => {
    await h.setup();
    await other.setup();
    const config = credential();
    // Credential files are found by credential_ref, which is unique within a pool.
    for (let i = 0; i < CONNECTIONS; i++) {
      await h.secret(`pg_${i}`, config);
      conns.push(await h.connection(`pool_${i}`, "postgres-v1", `pg_${i}`));
    }
    // Discover once; every connection reads the same table with the same schema.
    await h.stream(conns[0]!, table, "full_refresh");
    schema = (
      await h.db.from("connect_streams").select("schema_json").eq("id", conns[0]!.stream).single()
    ).data.schema_json;
    for (const c of conns.slice(1)) {
      const st = await h.db
        .from("connect_streams")
        .insert({ connection_id: c.id, ...table, sync_mode: "full_refresh", schema_json: schema })
        .select("id")
        .single();
      if (st.error) throw st.error;
      c.stream = st.data.id;
    }
    pool = await createPool(h, "drain");
    const r = await h.db
      .from("connect_connections")
      .update({ pool_id: pool.id })
      .in(
        "id",
        conns.map((c) => c.id),
      );
    if (r.error) throw r.error;
  }, 900000);
  afterAll(async () => {
    const pg = new PgClient(source);
    await pg.connect();
    await pg.query(`DROP TABLE IF EXISTS public.${extra}`).catch(() => undefined);
    await pg.end();
    await h.teardown();
    await other.teardown();
  }, 60000);

  it("three agents with four slots drain 50 connections, each row landed once", async () => {
    const runs = await Promise.all(conns.map((c) => h.enqueue(c, "sync")));
    await Promise.all(["a", "b", "c"].map((id) => poolAgent(pool.token, `pool-agent-${id}`, 4)));
    for (const run of runs)
      expect(await h.run(run)).toMatchObject({ status: "succeeded", rows_committed: 1200 });
    // One commit per sequence, and each row once, in every run.
    const commits = (
      await h.db.from("connect_commits").select("run_id,sequence,row_count").in("run_id", runs)
    ).data as { run_id: string; sequence: number; row_count: number }[];
    for (const run of runs) {
      const mine = commits.filter((c) => c.run_id === run);
      expect(new Set(mine.map((c) => c.sequence)).size).toBe(mine.length);
      expect(mine.reduce((n, c) => n + c.row_count, 0)).toBe(1200);
    }
    for (const run of runs.slice(0, 5)) {
      const ids = (await h.landed(run)).lines.map((l) => l.data["id"]);
      expect(new Set(ids).size).toBe(1200);
    }
    // The work spread across the agents, and each reported itself to the pool.
    const workers = (await h.db.from("connect_run_streams").select("worker").in("run_id", runs))
      .data as { worker: string }[];
    const agents = new Set(workers.map((w) => w.worker.split(":")[0]));
    expect(agents.size).toBeGreaterThanOrEqual(2);
    const registry = (
      await h.db.from("connect_workers").select("agent,slots,version").eq("pool_id", pool.id)
    ).data as { agent: string; slots: number; version: string }[];
    expect(registry.map((w) => w.agent).sort()).toEqual([
      "pool-agent-a",
      "pool-agent-b",
      "pool-agent-c",
    ]);
    expect(registry.every((w) => w.slots === 4 && w.version === "m2")).toBe(true);
    // The owner sees the pool and its workers, never the token hash.
    expect(
      (await h.owner.from("connect_pools").select("id,name").eq("id", pool.id)).data,
    ).toHaveLength(1);
    expect((await h.owner.from("connect_pools").select("token_hash")).error).not.toBeNull();
    expect(
      (await h.owner.from("connect_workers").select("agent").eq("pool_id", pool.id)).data,
    ).toHaveLength(3);
  });

  it("holds a connection's parallel task limit on pool claims", async () => {
    const pg = new PgClient(source);
    await pg.connect();
    await pg.query(
      `CREATE TABLE public.${extra} AS SELECT * FROM public.connect_acceptance WHERE id <= 100`,
    );
    await pg.query(`ALTER TABLE public.${extra} ADD PRIMARY KEY (id)`);
    await pg.end();
    await h.secret("pg_limit", credential());
    const c = await h.connection("pool_limit", "postgres-v1", "pg_limit");
    await h.stream(c, table, "full_refresh");
    await h.stream(c, { ...table, name: extra }, "full_refresh");
    const limited = await createPool(h, "limited");
    await h.db
      .from("connect_connections")
      .update({ pool_id: limited.id, max_parallel_tasks: 1 })
      .eq("id", c.id);
    const run = await h.enqueue(c, "sync");
    const first = await call(limited.token, { action: "claim_task", worker: "limit:0" });
    expect(first.body.claim).toMatchObject({ run_id: run, connection_id: c.id });
    // The run's second stream waits while the first holds the connection's only slot.
    expect(
      (await call(limited.token, { action: "claim_task", worker: "limit:1" })).body.claim,
    ).toBe(null);
    await h.db.from("connect_connections").update({ max_parallel_tasks: null }).eq("id", c.id);
    const second = await call(limited.token, { action: "claim_task", worker: "limit:1" });
    expect(second.body.claim).toMatchObject({ run_id: run });
    expect(second.body.claim.streams[0].id).not.toBe(first.body.claim.streams[0].id);
    await h.db
      .from("connect_runs")
      .update({ status: "cancelled", lease_token: null, lease_until: null })
      .eq("id", run);
  });

  it("keeps a pool token inside its workspace", async () => {
    const foreign = await createPool(other, "foreign");
    // Nothing of another workspace is claimable or reachable with this token.
    expect((await call(foreign.token, { action: "claim_task" })).body.claim).toBe(null);
    const run = await h.enqueue(conns[0]!, "sync");
    const claim = (await call(pool.token, { action: "claim_task", worker: "t:0" })).body.claim;
    expect(claim.run_id).toBe(run);
    const ids = { runId: claim.run_id, token: claim.lease_token };
    expect((await call(foreign.token, { action: "heartbeat", ...ids })).status).toBe(409);
    expect((await call(pool.token, { action: "heartbeat", ...ids })).status).toBe(200);
    // A connection can't join a pool of another workspace, or share a credential file in a pool.
    const moved = await h.db
      .from("connect_connections")
      .update({ pool_id: foreign.id })
      .eq("id", conns[1]!.id);
    expect(moved.error?.message).toMatch(/outside the connection workspace/);
    const dup = await h.db
      .from("connect_connections")
      .update({ credential_ref: "pg_2" })
      .eq("id", conns[1]!.id);
    expect(dup.error?.message).toMatch(/credential file pg_2/);
    // A disabled pool claims nothing, and its agents' calls stop working.
    await h.db.from("connect_pools").update({ enabled: false }).eq("id", pool.id);
    expect((await call(pool.token, { action: "heartbeat", ...ids })).status).toBe(401);
    expect((await call(pool.token, { action: "claim_task" })).status).toBe(401);
    await h.db.from("connect_pools").update({ enabled: true }).eq("id", pool.id);
    await h.db
      .from("connect_runs")
      .update({ status: "cancelled", lease_token: null, lease_until: null })
      .eq("id", run);
  });
});
