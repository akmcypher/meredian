// @vitest-environment node
// Real outbound agent -> PostgreSQL CDC actor -> Bronze commit -> retired-slot cleanup.
import { createHash } from "node:crypto";
import { Client as PgClient } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { APP, SparkHarness, url, service, key, type Conn } from "./connect-spark-harness";

const source = {
  host: process.env["CONNECT_TEST_HOST"] ?? "127.0.0.1",
  port: Number(process.env["CONNECT_TEST_PORT"] ?? 58622),
  dbname: process.env["CONNECT_TEST_DB"] ?? "connect_source",
  user: "postgres",
  password: process.env["PGPASSWORD"] ?? "postgres",
};
const local = !!url && ["localhost", "127.0.0.1"].includes(new URL(url).hostname);
const suffix = crypto.randomUUID().replaceAll("-", "").slice(0, 12);
const table = `retirement_agent_${suffix}`;
const publication = `retirement_agent_${suffix}`;
const h = new SparkHarness(false);
let pg: PgClient;
let conn: Conn | undefined;
let slot = "";
const past = "2020-01-01T00:00:00Z";
async function slotExists() {
  return (
    (
      await pg.query(
        "SELECT 1 FROM pg_replication_slots WHERE slot_name=$1 AND database=current_database()",
        [slot],
      )
    ).rowCount === 1
  );
}
async function task(run: string) {
  const result = await h.db
    .from("connect_run_streams")
    .select("task_status,attempts,error_code")
    .eq("run_id", run)
    .eq("stream_id", conn!.stream)
    .single();
  if (result.error) throw result.error;
  return result.data;
}
async function heartbeat(run: string, token: string) {
  return fetch(`${APP}/api/public/connect-agent`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-ingest-token": conn!.token },
    body: JSON.stringify({ action: "heartbeat", runId: run, token }),
  });
}

describe.skipIf(!local || !service || !key)(
  "retirement through the real CDC agent",
  { timeout: 120000 },
  () => {
    beforeAll(async () => {
      pg = new PgClient({ ...source, database: source.dbname, connectionTimeoutMillis: 5000 });
      await pg.connect();
      expect((await pg.query("SHOW wal_level")).rows[0].wal_level).toBe("logical");
      await pg.query(
        `CREATE TABLE public.${table} (id int PRIMARY KEY, name text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now())`,
      );
      await pg.query(
        `INSERT INTO public.${table}(id,name) VALUES (1,'retained bronze'),(2,'second row')`,
      );
      await pg.query(`CREATE PUBLICATION ${publication} FOR TABLE public.${table}`);
      await h.setup();
      const workspace = await h.owner.rpc("ensure_workspace");
      if (workspace.error) throw workspace.error;
      await h.secret("retirement_pg", { ...source, sslmode: "disable", publication });
      conn = await h.connection(`retirement_agent_${suffix}`, "postgres-cdc-v1", "retirement_pg");
      const c = await h.db
        .from("connect_connections")
        .select("source_id")
        .eq("id", conn.id)
        .single();
      if (c.error) throw c.error;
      for (const [relation, id] of [
        ["ingest_sources", c.data.source_id],
        ["connect_connections", conn.id],
      ]) {
        const scope = await h.db
          .from(relation)
          .update({ organization_id: workspace.data.workspaceId })
          .eq("id", id);
        if (scope.error) throw scope.error;
      }
      await h.stream(
        conn,
        { namespace: "public", name: table, primary_key: "id", cursor_field: "updated_at" },
        "cdc",
      );
      slot = `meridian_${createHash("sha256").update(conn.stream).digest("hex").slice(0, 24)}`;
    }, 120000);
    afterAll(async () => {
      if (pg) {
        await pg.query("SELECT pg_advisory_unlock_all()");
        if (slot && (await slotExists()))
          await pg.query("SELECT pg_drop_replication_slot($1)", [slot]);
        await pg.query(`DROP PUBLICATION IF EXISTS ${publication}`);
        await pg.query(`DROP TABLE IF EXISTS public.${table}`);
        await pg.end();
      }
      if (conn) await h.db.from("connect_connections").update({ enabled: false }).eq("id", conn.id);
      await h.teardown();
    }, 30000);

    it("retries busy-source cleanup, fences stale attempts and preserves prior Bronze after retirement", async () => {
      const snapshot = await h.enqueue(conn!, "sync");
      await h.agent(conn!);
      expect(await h.run(snapshot)).toMatchObject({ status: "succeeded", rows_committed: 2 });
      const bronze = await h.landed(snapshot);
      expect(bronze.lines.map((line) => line.data["name"]).sort()).toEqual([
        "retained bronze",
        "second row",
      ]);
      expect(await slotExists()).toBe(true);
      const priorState = await h.state(conn!);
      const age = await h.db
        .from("connect_runs")
        .update({ finished_at: past })
        .eq("connection_id", conn!.id)
        .eq("status", "succeeded");
      if (age.error) throw age.error;
      const retired = await h.owner.rpc("connect_retire_stream", { _stream: conn!.stream });
      expect(retired.error).toBeNull();
      expect(typeof retired.data).toBe("string");
      const cleanup = retired.data as string;
      const abandoned = await h.db.rpc("connect_claim_task", {
        _connection: conn!.id,
        _worker: "abandoned-cleanup",
      });
      expect(abandoned.error).toBeNull();
      const oldToken = abandoned.data.lease_token as string;
      const expire = await h.db
        .from("connect_run_streams")
        .update({ lease_until: past })
        .eq("run_id", cleanup)
        .eq("stream_id", conn!.stream);
      if (expire.error) throw expire.error;
      expect((await heartbeat(cleanup, oldToken)).status).toBe(409);

      // A real competing source session prevents the actor from dropping its slot.
      await pg.query("SELECT pg_advisory_lock(hashtextextended($1,0))", [slot]);
      try {
        await h.agent(conn!);
      } finally {
        await pg.query("SELECT pg_advisory_unlock(hashtextextended($1,0))", [slot]);
      }
      expect(await task(cleanup)).toMatchObject({
        task_status: "retry_wait",
        error_code: "source_unavailable",
      });
      expect(await slotExists()).toBe(true);
      expect(await h.state(conn!)).toMatchObject({
        retirement: { status: "pending" },
        cdc: priorState.cdc,
      });
      const retry = await h.db
        .from("connect_run_streams")
        .update({ next_attempt_at: past })
        .eq("run_id", cleanup)
        .eq("stream_id", conn!.stream);
      if (retry.error) throw retry.error;
      await h.agent(conn!);
      expect(await h.run(cleanup)).toMatchObject({ status: "succeeded", rows_committed: 0 });
      expect(await task(cleanup)).toMatchObject({ task_status: "done", attempts: 3 });
      expect(await slotExists()).toBe(false);
      expect(await h.state(conn!)).toMatchObject({
        retirement: { status: "done" },
        cdc: priorState.cdc,
      });
      expect((await heartbeat(cleanup, oldToken)).status).toBe(409);
      expect(await h.landed(snapshot)).toEqual(bronze);
      const cleanupBronze = await h.landed(cleanup);
      expect(cleanupBronze.lines).toHaveLength(0);
      expect(cleanupBronze.commits).toHaveLength(1);

      const ageCleanup = await h.db
        .from("connect_runs")
        .update({ finished_at: past })
        .eq("id", cleanup);
      if (ageCleanup.error) throw ageCleanup.error;
      const repeated = await h.owner.rpc("connect_retire_stream", { _stream: conn!.stream });
      expect(repeated.error).toBeNull();
      expect(repeated.data).toBeNull();
      await h.agent(conn!);
      expect(await slotExists()).toBe(false);
      expect(await h.landed(snapshot)).toEqual(bronze);
    });
  },
);
