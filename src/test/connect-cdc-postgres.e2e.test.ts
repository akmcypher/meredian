// @vitest-environment node
// PostgreSQL CDC (deploy/connect/README.md, "Change data capture"): a `postgres-cdc-v1` stream copies the
// table, then lands every insert, update and delete from the logical replication slot, in commit
// order, with exactly-once landing across runs. It fails closed on unchanged TOAST values without
// REPLICA IDENTITY FULL and on TRUNCATE, and a reset starts over with a new slot. Needs the dev
// app and a source database with wal_level=logical (the connect_source fixture); skips otherwise.
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { Client as PgClient } from "pg";
import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SparkHarness, url, service, key, type Conn, type Line } from "./connect-spark-harness";

const source = {
  host: process.env["CONNECT_TEST_HOST"] ?? "127.0.0.1",
  port: Number(process.env["CONNECT_TEST_PORT"] ?? 55439),
  dbname: process.env["CONNECT_TEST_DB"] ?? "connect_source",
  user: "postgres",
  password: process.env["PGPASSWORD"] ?? "postgres",
};
const suffix = crypto.randomUUID().slice(0, 8);
const tableName = `cdc_${suffix}`;
const publication = `meridian_test_${suffix}`;
const h = new SparkHarness(false);
let conn: Conn;
let pg: PgClient;

async function logical() {
  const c = new PgClient({ ...source, database: source.dbname });
  try {
    await c.connect();
    return (await c.query("SHOW wal_level")).rows[0].wal_level === "logical";
  } catch {
    return false;
  } finally {
    await c.end().catch(() => undefined);
  }
}
const live = !!url && !!service && !!key && (await logical());

// The table as the landed envelopes say it is: apply them in order by key.
function apply(lines: Line[], into = new Map<string, Record<string, unknown>>()) {
  for (const l of lines) {
    const k = JSON.stringify(l["primary_key"]);
    if (l["operation"] === "delete") into.delete(k);
    else into.set(k, l.data);
  }
  return into;
}
async function table() {
  const rows = (
    await pg.query(`SELECT id, name, amount::text AS amount, body FROM public.${tableName}`)
  ).rows as { id: number; name: string; amount: string; body: string | null }[];
  return new Map(rows.map((r) => [JSON.stringify(r.id), r]));
}
function same(
  landed: Map<string, Record<string, unknown>>,
  rows: Awaited<ReturnType<typeof table>>,
) {
  expect([...landed.keys()].sort()).toEqual([...rows.keys()].sort());
  for (const [k, r] of rows) {
    const d = landed.get(k)!;
    expect({ id: d["id"], name: d["name"], amount: d["amount"], body: d["body"] }).toEqual(r);
  }
}
async function sync(operation = "sync") {
  const run = await h.enqueue(conn, operation);
  const out = await h.agent(conn);
  return { run, out, result: await h.run(run), lines: (await h.landed(run)).lines };
}
async function slots() {
  return (
    await pg.query("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'meridian_%'")
  ).rows.map((r) => r.slot_name as string);
}

describe.skipIf(!live)(
  "PostgreSQL CDC (CDC-1): logical replication into Bronze",
  { timeout: 600000 },
  () => {
    const landed = new Map<string, Record<string, unknown>>();
    let before: string[] = [];

    beforeAll(async () => {
      pg = new PgClient({ ...source, database: source.dbname });
      await pg.connect();
      before = await slots();
      await pg.query(`CREATE TABLE public.${tableName} (id int PRIMARY KEY, name text NOT NULL,
      amount numeric(12,2), body text, updated_at timestamptz NOT NULL DEFAULT now())`);
      await pg.query(`INSERT INTO public.${tableName}(id,name,amount)
      SELECT g, 'row ' || g, g * 1.5 FROM generate_series(1,50) g`);
      await pg.query(`CREATE PUBLICATION ${publication} FOR TABLE public.${tableName}`);
      await h.setup();
      await h.secret("pgcdc", { ...source, sslmode: "disable", publication });
      conn = await h.connection(`cdc_${suffix}`, "postgres-cdc-v1", "pgcdc");
      await h.stream(
        conn,
        { namespace: "public", name: tableName, primary_key: "id", cursor_field: "updated_at" },
        "cdc",
      );
    }, 600000);
    afterAll(async () => {
      for (const s of await slots())
        if (!before.includes(s))
          await pg.query("SELECT pg_drop_replication_slot($1)", [s]).catch(() => undefined);
      await pg.query(`DROP PUBLICATION IF EXISTS ${publication}`).catch(() => undefined);
      await pg.query(`DROP TABLE IF EXISTS public.${tableName}`).catch(() => undefined);
      await pg.end();
      await h.teardown();
    }, 60000);

    it("a CDC connection refuses a cursor stream", async () => {
      const r = await h.db
        .from("connect_streams")
        .update({ sync_mode: "incremental" })
        .eq("id", conn.stream);
      expect(r.error?.message).toMatch(/CDC connections take cdc streams only/);
    });

    it("the first run creates a slot and copies the table", async () => {
      const { result, lines } = await sync();
      expect(result).toMatchObject({ status: "succeeded", rows_committed: 50 });
      expect(lines.every((l) => l["operation"] === "upsert")).toBe(true);
      expect((await slots()).filter((s) => !before.includes(s))).toHaveLength(1);
      expect((await h.state(conn)).cdc).toMatchObject({ phase: "stream" });
      same(apply(lines, landed), await table());
    });

    it("lands inserts, updates and deletes in commit order, each change once", async () => {
      await pg.query(
        `INSERT INTO public.${tableName}(id,name,amount) VALUES (51,'new',1),(52,'new',2)`,
      );
      await pg.query(`UPDATE public.${tableName} SET name='renamed', amount=99.99 WHERE id=3`);
      await pg.query("BEGIN");
      await pg.query(`DELETE FROM public.${tableName} WHERE id IN (4,5)`);
      await pg.query(`UPDATE public.${tableName} SET name='after delete' WHERE id=6`);
      await pg.query("COMMIT");
      await pg.query(`UPDATE public.${tableName} SET id=60 WHERE id=7`);
      const { result, lines } = await sync();
      expect(result).toMatchObject({ status: "succeeded", rows_committed: 8 });
      expect(lines.map((l) => [l["operation"], l["primary_key"]])).toEqual([
        ["upsert", 51],
        ["upsert", 52],
        ["upsert", 3],
        ["delete", 4],
        ["delete", 5],
        ["upsert", 6],
        // A changed key: a delete of the old key, then the row under its new key.
        ["delete", 7],
        ["upsert", 60],
      ]);
      // Deletes carry the key only without REPLICA IDENTITY FULL.
      expect(lines.find((l) => l["primary_key"] === 4)!.data).toEqual({ id: 4 });
      // Lag: bytes of log the landed position is behind the source, reported after the commit.
      const lag = (
        await h.db
          .from("connect_partition_lag")
          .select("partition,position,end_offset,lag")
          .eq("stream_id", conn.stream)
      ).data as { partition: number; position: string; end_offset: string; lag: string }[];
      expect(lag).toHaveLength(1);
      expect(lag[0]!.partition).toBe(0);
      expect(Number(lag[0]!.end_offset)).toBeGreaterThanOrEqual(Number(lag[0]!.position));
      expect(lines.find((l) => l["primary_key"] === 3)!.data).toMatchObject({
        name: "renamed",
        amount: "99.99",
      });
      same(apply(lines, landed), await table());
    });

    it("a run with no new changes lands nothing and keeps the position", async () => {
      const position = (await h.state(conn)).cdc.lsn;
      const { result } = await sync();
      expect(result).toMatchObject({ status: "succeeded", rows_committed: 0 });
      expect((await h.state(conn)).cdc.lsn).toBe(position);
    });

    it("an attempt that read changes but landed nothing loses none of them", async () => {
      for (let i = 0; i < 30; i++)
        await pg.query(`UPDATE public.${tableName} SET amount=amount+1 WHERE id=${10 + i}`);
      // The actor reads with the committed checkpoint, as a new attempt would, and its output is
      // dropped as if the agent died before committing: the slot is only peeked, never consumed.
      const s = (await h.db.from("connect_streams").select("*").eq("id", conn.stream).single())
        .data as Record<string, unknown>;
      const dir = mkdtempSync(join(tmpdir(), "cdc-"));
      const file = (n: string, v: unknown) => {
        const p = join(dir, n);
        writeFileSync(p, JSON.stringify(v), { mode: 0o600 });
        return p;
      };
      const lost = spawnSync(
        process.env["PYTHON"] ?? "python3",
        [
          "deploy/connect/source_postgres_cdc.py",
          "read",
          "--config",
          file("config.json", { ...source, sslmode: "disable", publication }),
          "--catalog",
          file("catalog.json", { ...s, sync_mode: "cdc", schema: s["schema_json"] }),
          "--state",
          file("state.json", s["state"]),
        ],
        { encoding: "utf8", env: { ...process.env, PYTHONPATH: "deploy/connect" } },
      );
      expect(lost.status).toBe(0);
      expect(lost.stdout.split("\n").filter((l) => l.includes('"RECORD"'))).toHaveLength(30);
      const { result, lines } = await sync();
      expect(result).toMatchObject({ status: "succeeded", rows_committed: 30 });
      expect(new Set(lines.map((l) => l["primary_key"])).size).toBe(30);
      same(apply(lines, landed), await table());
    });

    it("fails closed on an unchanged TOAST value without REPLICA IDENTITY FULL", async () => {
      // A large incompressible value is stored out of line; an update that doesn't touch it sends
      // "unchanged" instead of the value.
      await pg.query(
        `UPDATE public.${tableName} SET body=(SELECT string_agg(md5(random()::text), '') FROM generate_series(1,400)) WHERE id=8`,
      );
      await pg.query(`UPDATE public.${tableName} SET name='touched' WHERE id=8`);
      const { result, out } = await sync();
      expect(result?.status).not.toBe("succeeded");
      expect(out).toContain('"code": "cdc_unchanged_value"');
      expect(out).toContain('"columns": ["body"]');
      await h.db
        .from("connect_runs")
        .update({ status: "cancelled", lease_token: null, lease_until: null })
        .eq("connection_id", conn.id)
        .in("status", ["queued", "running", "retry_wait"]);
    });

    it("a reset starts a new slot and copies the table again; with REPLICA IDENTITY FULL the value is kept", async () => {
      await pg.query(`ALTER TABLE public.${tableName} REPLICA IDENTITY FULL`);
      const reset = await sync("reset");
      expect(reset.result).toMatchObject({ status: "succeeded" });
      expect((await slots()).filter((s) => !before.includes(s))).toHaveLength(1);
      const fresh = apply(reset.lines);
      same(fresh, await table());
      await pg.query(`UPDATE public.${tableName} SET name='touched again' WHERE id=8`);
      await pg.query(`DELETE FROM public.${tableName} WHERE id=9`);
      const { result, lines } = await sync();
      expect(result).toMatchObject({ status: "succeeded", rows_committed: 2 });
      expect((lines[0]!.data["body"] as string).length).toBe(12800);
      // With REPLICA IDENTITY FULL a delete carries the whole old row.
      expect(lines[1]!.data).toMatchObject({ id: 9, name: "row 9" });
      same(apply(lines, fresh), await table());
    });

    it("TRUNCATE fails the run closed", async () => {
      await pg.query(`TRUNCATE public.${tableName}`);
      const { result, out } = await sync();
      expect(result?.status).not.toBe("succeeded");
      expect(out).toContain('"code": "cdc_truncate"');
    });
  },
);
