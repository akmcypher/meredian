// @vitest-environment node
// MySQL CDC (deploy/connect/README.md, "Change data capture"): a `mysql-cdc-v1` stream copies the table, then
// lands every insert, update and delete from the binlog, in commit order, with the same values as
// the copy. It refuses a server without full row metadata, and fails closed on TRUNCATE and on a
// purged binlog position; a reset starts over. Needs the dev app and the MySQL test source
// (deploy/ingest/test-sources.compose.yml, row binlog on); skips otherwise.
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import mysql from "mysql2/promise";
import { SparkHarness, url, service, key, type Conn, type Line } from "./connect-spark-harness";

const MYSQL = process.env["MYSQL_TEST_URL"];
const DB = `cdc_${crypto.randomUUID().slice(0, 8)}`;
const h = new SparkHarness(false);
let conn: Conn;
let my: mysql.Connection;
let metadata = "MINIMAL";

function apply(lines: Line[], into = new Map<string, Record<string, unknown>>()) {
  for (const l of lines) {
    const k = JSON.stringify(l["primary_key"]);
    if (l["operation"] === "delete") into.delete(k);
    else into.set(k, l.data);
  }
  return into;
}
async function sync(operation = "sync") {
  const run = await h.enqueue(conn, operation);
  const out = await h.agent(conn);
  return { run, out, result: await h.run(run), lines: (await h.landed(run)).lines };
}
async function settle() {
  await h.db
    .from("connect_runs")
    .update({ status: "cancelled", lease_token: null, lease_until: null })
    .eq("connection_id", conn.id)
    .in("status", ["queued", "running", "retry_wait"]);
}

describe.skipIf(!url || !service || !key || !MYSQL)(
  "MySQL CDC (CDC-2): binlog into Bronze",
  { timeout: 900000 },
  () => {
    const landed = new Map<string, Record<string, unknown>>();

    beforeAll(async () => {
      const u = new URL(MYSQL!);
      my = await mysql.createConnection({
        host: u.hostname,
        port: Number(u.port),
        user: decodeURIComponent(u.username),
        password: decodeURIComponent(u.password),
        multipleStatements: true,
      });
      metadata = (
        (await my.query("SELECT @@GLOBAL.binlog_row_metadata AS m")) as never as [{ m: string }[]]
      )[0][0]!.m;
      await my.query(
        `SET SESSION time_zone='+00:00';
         CREATE DATABASE ${DB} CHARACTER SET utf8mb4; USE ${DB};
         CREATE TABLE orders(region varchar(8) NOT NULL, id bigint NOT NULL, updated_at datetime(6) NOT NULL,
           amount decimal(40,9), ok tinyint(1), doc json, note text, at timestamp(6) NULL, PRIMARY KEY(region,id));
         INSERT INTO orders WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<40)
           SELECT ELT(1+n%3,'eu','us','apac'), n, TIMESTAMP('2026-01-01') + INTERVAL n SECOND,
             n*1000000000.000000001, n%2, JSON_OBJECT('n',n,'tags',JSON_ARRAY('a','名')), CONCAT('order ',n,' ✓'),
             TIMESTAMP('2026-06-01') + INTERVAL n MICROSECOND FROM seq;`,
      );
      await h.setup();
      await h.secret("mysqlcdc", {
        host: u.hostname,
        port: Number(u.port),
        user: decodeURIComponent(u.username),
        password: decodeURIComponent(u.password),
        database: DB,
      });
      conn = await h.connection(`cdc_${DB}`, "mysql-cdc-v1", "mysqlcdc");
      await h.stream(
        conn,
        {
          namespace: DB,
          name: "orders",
          primary_key: "region,id",
          cursor_field: "updated_at",
        },
        "cdc",
      );
    }, 600000);
    afterAll(async () => {
      await my.query(`SET GLOBAL binlog_row_metadata='${metadata}'`).catch(() => undefined);
      await my.query(`DROP DATABASE IF EXISTS ${DB}`).catch(() => undefined);
      await my.end();
      await h.teardown();
    }, 60000);

    it("refuses a server without full row metadata, naming the setting", async () => {
      await my.query("SET GLOBAL binlog_row_metadata='MINIMAL'");
      const { result, out, lines } = await sync();
      expect(result?.status).not.toBe("succeeded");
      expect(out).toContain('"code": "cdc_binlog_settings"');
      expect(out).toContain('"binlog_row_metadata": "MINIMAL"');
      expect(lines).toHaveLength(0);
      await settle();
      await my.query("SET GLOBAL binlog_row_metadata='FULL'");
    });

    it("the first run copies the table", async () => {
      const { result, lines } = await sync();
      expect(result).toMatchObject({ status: "succeeded", rows_committed: 40 });
      expect(lines.every((l) => l["operation"] === "upsert")).toBe(true);
      expect((await h.state(conn)).cdc).toMatchObject({ phase: "stream" });
      apply(lines, landed);
    });

    it("lands inserts, updates, deletes and key changes in commit order", async () => {
      await my.query(
        `USE ${DB}; SET SESSION time_zone='+00:00';
         INSERT INTO orders VALUES ('eu',100,'2026-02-01 00:00:00.5',1.5,1,JSON_OBJECT('x',JSON_ARRAY(1,2)),'neu','2026-02-02 03:04:05.000006');
         UPDATE orders SET amount=7.000000001, note='geändert', ok=0 WHERE region='us' AND id=1;
         START TRANSACTION;
           DELETE FROM orders WHERE region='eu' AND id=3;
           UPDATE orders SET doc=JSON_OBJECT('n',NULL,'deep',JSON_OBJECT('k','v')) WHERE region='apac' AND id=2;
         COMMIT;
         UPDATE orders SET id=400 WHERE region='us' AND id=4;`,
      );
      const { result, lines } = await sync();
      expect(result).toMatchObject({ status: "succeeded", rows_committed: 6 });
      expect(lines.map((l) => [l["operation"], l["primary_key"]])).toEqual([
        ["upsert", ["eu", 100]],
        ["upsert", ["us", 1]],
        ["delete", ["eu", 3]],
        ["upsert", ["apac", 2]],
        ["delete", ["us", 4]],
        ["upsert", ["us", 400]],
      ]);
      // With binlog_row_image=FULL a delete carries the whole old row.
      expect(lines[2]!.data).toMatchObject({ region: "eu", id: 3, note: "order 3 ✓" });
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
      apply(lines, landed);
    });

    it("a run with no new changes lands nothing and keeps the position", async () => {
      const position = (await h.state(conn)).cdc.binlog;
      const { result } = await sync();
      expect(result).toMatchObject({ status: "succeeded", rows_committed: 0 });
      expect((await h.state(conn)).cdc.binlog).toEqual(position);
    });

    it("changed rows land with the same values as a fresh copy", async () => {
      // A reset copies the table again: every row, including the ones that came from the binlog
      // (JSON, decimals, timestamps, tinyint(1), Unicode), must match what the changes produced.
      const { result, lines } = await sync("reset");
      expect(result).toMatchObject({ status: "succeeded", rows_committed: 40 });
      expect(apply(lines)).toEqual(landed);
    });

    it("a purged binlog position fails closed", async () => {
      const s = await h.state(conn);
      await h.db
        .from("connect_streams")
        .update({
          state: { ...s, cdc: { ...s.cdc, binlog: { file: "binlog.999999", position: 4 } } },
        })
        .eq("id", conn.stream);
      const { result, out } = await sync();
      expect(result?.status).not.toBe("succeeded");
      expect(out).toContain('"code": "cdc_binlog_purged"');
      await settle();
      await h.db.from("connect_streams").update({ state: s }).eq("id", conn.stream);
    });

    it("TRUNCATE fails the run closed", async () => {
      await my.query(`TRUNCATE TABLE ${DB}.orders`);
      const { result, out } = await sync();
      expect(result?.status).not.toBe("succeeded");
      expect(out).toContain('"code": "cdc_truncate"');
      await settle();
    });
  },
);
