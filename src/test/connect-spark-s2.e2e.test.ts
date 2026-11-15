// @vitest-environment node
// Spark ingestion S2 (deploy/connect/README.md, "Spark ingestion") on the live stack: MySQL full
// refresh and incremental (cursor) streams for MySQL and PostgreSQL on a multi-executor Spark
// cluster. Every Spark run is compared with the single-worker actor reading the same rows: same
// landed data and the same committed (cursor, key) position. Needs the dev app, the Spark cluster
// (deploy/connect/spark/compose.yml), the local PostgreSQL and the MySQL test source
// (deploy/ingest/test-sources.compose.yml); skips otherwise.
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import mysql from "mysql2/promise";
import pg from "pg";
import {
  SparkHarness,
  byKey,
  executors,
  live,
  type Conn,
  type Line,
} from "./connect-spark-harness";

const MYSQL = process.env["MYSQL_TEST_URL"];
const DB = "spark_s2";
const ROWS = 25000;
const h = new SparkHarness();
const conn = {} as Record<
  "myFull" | "myFullBase" | "myInc" | "myIncBase" | "pgInc" | "pgIncBase",
  Conn
>;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let my: any, pgc: pg.Client;

async function sync(c: Conn, options: Record<string, unknown> = {}) {
  const run = await h.enqueue(c, "sync");
  await h.agent(c, options);
  return run;
}
async function rows(c: Conn, run: string) {
  return (await h.landed(run)).lines as Line[];
}
// The committed position a single-worker run reached must be the one the Spark run reached.
async function position(c: Conn) {
  const s = await h.state(c);
  return { cursor: s.cursor, primary_key: s.primary_key, completed: s.completed };
}

describe.skipIf(!live || !MYSQL)(
  "Spark ingestion (S2): MySQL and incremental streams",
  { timeout: 1200000 },
  () => {
    beforeAll(async () => {
      await h.setup();
      const u = new URL(MYSQL!);
      my = await mysql.createConnection({
        host: u.hostname,
        port: Number(u.port),
        user: decodeURIComponent(u.username),
        password: decodeURIComponent(u.password),
        multipleStatements: true,
      });
      await my.query(
        `SET SESSION time_zone='+00:00', SESSION cte_max_recursion_depth=100000;
         DROP DATABASE IF EXISTS ${DB}; CREATE DATABASE ${DB} CHARACTER SET utf8mb4; USE ${DB};
         CREATE TABLE orders(region varchar(8) NOT NULL, id bigint NOT NULL, updated_at datetime(6) NOT NULL,
           amount decimal(40,9), ok tinyint(1), doc json, note text, at timestamp(6) NULL, PRIMARY KEY(region,id));
         INSERT INTO orders WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<${ROWS})
           SELECT ELT(1+n%3,'eu','us','apac'), n, TIMESTAMP('2026-01-01') + INTERVAL n SECOND,
             n*1000000000.000000001, n%2, JSON_OBJECT('n',n,'tags',JSON_ARRAY('a','名')), CONCAT('order ',n,' ✓'),
             TIMESTAMP('2026-06-01') + INTERVAL n MICROSECOND FROM seq;`,
      );
      await h.secret("mysql", {
        host: u.hostname,
        port: Number(u.port),
        user: decodeURIComponent(u.username),
        password: decodeURIComponent(u.password),
        database: DB,
      });
      const pgConfig = {
        host: process.env["CONNECT_TEST_HOST"] ?? "127.0.0.1",
        port: Number(process.env["CONNECT_TEST_PORT"] ?? 55439),
        dbname: process.env["CONNECT_TEST_DB"] ?? "postgres",
        user: "postgres",
        password: process.env["PGPASSWORD"] ?? "postgres",
        sslmode: "disable",
      };
      await h.secret("postgres", pgConfig);
      pgc = new pg.Client({ ...pgConfig, database: pgConfig.dbname });
      await pgc.connect();
      await pgc.query(`DROP SCHEMA IF EXISTS spark_s2 CASCADE; CREATE SCHEMA spark_s2;
        CREATE TABLE spark_s2.events(id bigint PRIMARY KEY, updated_at timestamptz NOT NULL, v text);
        INSERT INTO spark_s2.events SELECT n, timestamptz '2026-01-01 00:00:00+00' + n * interval '1 second', 'event ' || n
          FROM generate_series(1,3000) n;`);
      const orders = {
        namespace: DB,
        name: "orders",
        primary_key: "region,id",
        cursor_field: "updated_at",
      };
      const events = {
        namespace: "spark_s2",
        name: "events",
        primary_key: "id",
        cursor_field: "updated_at",
      };
      const make = async (
        name: keyof typeof conn,
        version: string,
        credential: string,
        table: typeof orders,
        mode: "full_refresh" | "incremental",
      ) => {
        conn[name] = await h.connection(name, version, credential);
        await h.stream(conn[name], table, mode);
      };
      await make("myFull", "spark-mysql-v1", "mysql", orders, "full_refresh");
      await make("myFullBase", "mysql-v1", "mysql", orders, "full_refresh");
      await make("myInc", "spark-mysql-v1", "mysql", orders, "incremental");
      await make("myIncBase", "mysql-v1", "mysql", orders, "incremental");
      await make("pgInc", "spark-postgres-v1", "postgres", events, "incremental");
      await make("pgIncBase", "postgres-v1", "postgres", events, "incremental");
    }, 1200000);
    afterAll(async () => {
      await my?.query(`DROP DATABASE IF EXISTS ${DB}`);
      await my?.end();
      await pgc?.query("DROP SCHEMA IF EXISTS spark_s2 CASCADE");
      await pgc?.end();
      await h.teardown();
    }, 60000);

    it("MySQL full refresh: several executors, parts split at 10,000 rows, same data as the MySQL actor", async () => {
      const run = await sync(conn.myFull, { partitions: 2 });
      expect(await h.run(run)).toMatchObject({ status: "succeeded", rows_committed: ROWS });
      const { lines, manifests } = await h.landed(run);
      expect(executors(manifests).size).toBeGreaterThanOrEqual(2);
      // About 12,500 rows per partition: each partition rolls over into a second part.
      const parts = manifests.map((m) => (m["produced_by"] as { part?: string }).part);
      expect(parts).toEqual(expect.arrayContaining(["0-0", "0-1", "1-0", "1-1"]));
      expect(new Set(lines.map((l) => JSON.stringify(l["primary_key"]))).size).toBe(ROWS);
      const base = await sync(conn.myFullBase);
      expect(byKey(lines)).toEqual(byKey(await rows(conn.myFullBase, base)));
      expect((await h.state(conn.myFull)).spark).toMatchObject({
        run,
        partitions: 2,
        consistency: "partition",
      });
    });

    it("MySQL incremental: each window lands what the MySQL actor lands and ends at the same position", async () => {
      // First run: the whole table.
      let s = await sync(conn.myInc, { partitions: 3 });
      let b = await sync(conn.myIncBase);
      expect(await h.run(s)).toMatchObject({ status: "succeeded", rows_committed: ROWS });
      expect(byKey(await rows(conn.myInc, s))).toEqual(byKey(await rows(conn.myIncBase, b)));
      expect(await position(conn.myInc)).toEqual(await position(conn.myIncBase));
      // Changes: 300 updated rows and 50 new ones, all after the committed position.
      await my.query(
        `UPDATE ${DB}.orders SET updated_at=TIMESTAMP('2027-01-01') + INTERVAL id SECOND, note='changed' WHERE id % 83 = 0 LIMIT 300;
         INSERT INTO ${DB}.orders(region,id,updated_at,amount,ok,doc,note) WITH RECURSIVE seq(n) AS (SELECT ${ROWS + 1} UNION ALL SELECT n+1 FROM seq WHERE n<${ROWS + 50})
           SELECT 'eu', n, TIMESTAMP('2027-02-01') + INTERVAL n SECOND, 1.5, 1, JSON_OBJECT('new',true), 'new' FROM seq;`,
      );
      s = await sync(conn.myInc, { partitions: 3 });
      b = await sync(conn.myIncBase);
      const changed = await rows(conn.myInc, s);
      expect(changed.length).toBe(350);
      expect(byKey(changed)).toEqual(byKey(await rows(conn.myIncBase, b)));
      expect(await position(conn.myInc)).toEqual(await position(conn.myIncBase));
      // Nothing new: the run completes without rows and keeps the position.
      const before = await position(conn.myInc);
      s = await sync(conn.myInc, { partitions: 3 });
      expect(await h.run(s)).toMatchObject({ status: "succeeded", rows_committed: 0 });
      expect(await position(conn.myInc)).toEqual(before);
    });

    it("an incremental attempt resumes inside the same window; later changes wait for the next run", async () => {
      await my.query(
        `UPDATE ${DB}.orders SET updated_at=TIMESTAMP('2028-01-01') + INTERVAL id SECOND WHERE id % 47 = 0`,
      );
      const [[{ n: inWindow }]] = await my.query(
        `SELECT count(*) n FROM ${DB}.orders WHERE updated_at >= '2028-01-01'`,
      );
      const run = await sync(conn.myInc, { partitions: 3, fail_after_commits: 1 });
      expect((await h.run(run)).status).toBe("running");
      // Changed after the window was fixed: not part of this run.
      await my.query(
        `UPDATE ${DB}.orders SET updated_at=TIMESTAMP('2029-01-01') + INTERVAL id SECOND WHERE id % 1009 = 0 AND id % 47 <> 0`,
      );
      await h.db
        .from("connect_run_streams")
        .update({ next_attempt_at: "2000-01-01T00:00:00Z" })
        .eq("run_id", run);
      await h.agent(conn.myInc, { partitions: 3 });
      expect(await h.run(run)).toMatchObject({ status: "succeeded", rows_committed: inWindow });
      const first = await rows(conn.myInc, run);
      expect(new Set(first.map((l) => JSON.stringify(l["primary_key"]))).size).toBe(inWindow);
      expect((await h.state(conn.myInc)).spark.consistency).toBe("mixed");
      const next = await sync(conn.myInc, { partitions: 3 });
      const later = await rows(conn.myInc, next);
      expect(later.length).toBeGreaterThan(0);
      expect(later.every((l) => String(l["cursor"]) >= "2029-01-01")).toBe(true);
      // Together the two runs land exactly what one single-worker run lands for both changes.
      const base = await sync(conn.myIncBase);
      expect(byKey([...first, ...later])).toEqual(byKey(await rows(conn.myIncBase, base)));
      expect(await position(conn.myInc)).toEqual(await position(conn.myIncBase));
    });

    it("PostgreSQL incremental: same rows and position as the PostgreSQL actor, one snapshot", async () => {
      let s = await sync(conn.pgInc, { partitions: 4 });
      let b = await sync(conn.pgIncBase);
      expect(await h.run(s)).toMatchObject({ status: "succeeded", rows_committed: 3000 });
      expect(byKey(await rows(conn.pgInc, s))).toEqual(byKey(await rows(conn.pgIncBase, b)));
      expect(await position(conn.pgInc)).toEqual(await position(conn.pgIncBase));
      await pgc.query(`UPDATE spark_s2.events SET updated_at=updated_at + interval '1 year', v='changed' WHERE id % 31 = 0;
        INSERT INTO spark_s2.events SELECT n, timestamptz '2028-01-01 00:00:00+00' + n * interval '1 second', 'new' FROM generate_series(3001,3010) n;`);
      s = await sync(conn.pgInc, { partitions: 4 });
      b = await sync(conn.pgIncBase);
      const changed = await rows(conn.pgInc, s);
      expect(changed.length).toBe(Math.floor(3000 / 31) + 10);
      expect(byKey(changed)).toEqual(byKey(await rows(conn.pgIncBase, b)));
      expect(await position(conn.pgInc)).toEqual(await position(conn.pgIncBase));
      expect((await h.state(conn.pgInc)).spark.consistency).toBe("single");
    });
  },
);
