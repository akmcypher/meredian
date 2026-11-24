// @vitest-environment node
// Real outbound agent -> SQL Server actor over verified TLS -> fenced Bronze commits.
// Opt in with SQLSERVER_TEST_PORT, SQLSERVER_TEST_CA, SQLSERVER_TEST_WRONG_CA and
// SQLSERVER_TEST_SA_PASSWORD (deploy/connect/README.md, "SQL Server"). The local engine is
// Azure SQL Edge on ARM64: SQL Server engine evidence, not SQL Server 2022 certification.
import { spawnSync } from "node:child_process";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { SparkHarness, url, service, key, type Conn } from "./connect-spark-harness";

const port = process.env["SQLSERVER_TEST_PORT"];
const ca = process.env["SQLSERVER_TEST_CA"];
const wrongCa = process.env["SQLSERVER_TEST_WRONG_CA"];
const sa = process.env["SQLSERVER_TEST_SA_PASSWORD"];
const local = !!url && ["localhost", "127.0.0.1"].includes(new URL(url).hostname);
const database = `meridian_e2e_${crypto.randomUUID().replaceAll("-", "").slice(0, 10)}`;
const reader = `reader_${crypto.randomUUID().replaceAll("-", "").slice(0, 10)}`;
const readerPassword = `R${crypto.randomUUID().replaceAll("-", "")}a!9`;
const h = new SparkHarness(false);
let conn: Conn | undefined;

// Admin statements run through the actor's own pinned driver, never the agent's credentials.
function sql(statements: string[], db = "master") {
  const script = `
import json, os, sys, pytds
statements = json.loads(sys.stdin.buffer.read().decode('utf-8'))
with pytds.connect(dsn='localhost', port=int(os.environ['SQLSERVER_TEST_PORT']), database=sys.argv[1],
                   user='sa', password=os.environ['SQLSERVER_TEST_SA_PASSWORD'],
                   cafile=os.environ['SQLSERVER_TEST_CA'], validate_host=True,
                   enc_login_only=False, autocommit=True, login_timeout=30) as conn:
    with conn.cursor() as cursor:
        for statement in statements: cursor.execute(statement)
`;
  const result = spawnSync(process.env["PYTHON"] ?? "python3", ["-c", script, db], {
    input: JSON.stringify(statements),
    encoding: "utf8",
    env: { ...process.env, PYTHONPATH: "deploy/connect" },
  });
  if (result.status !== 0) throw new Error(`SQL fixture failed: ${result.stderr.slice(-400)}`);
}
function config(overrides: Record<string, unknown> = {}) {
  return {
    host: "localhost",
    port: Number(port),
    database,
    user: reader,
    password: readerPassword,
    ssl_ca: ca,
    ...overrides,
  };
}
async function task(run: string) {
  const result = await h.db
    .from("connect_run_streams")
    .select("task_status,error_code")
    .eq("run_id", run)
    .eq("stream_id", conn!.stream)
    .single();
  if (result.error) throw result.error;
  return result.data;
}

describe.skipIf(!local || !service || !key || !port || !ca || !sa)(
  "SQL Server through the real agent",
  { timeout: 180000 },
  () => {
    beforeAll(async () => {
      sql([
        `CREATE DATABASE [${database}]`,
        `ALTER DATABASE [${database}] SET ALLOW_SNAPSHOT_ISOLATION ON`,
        `CREATE LOGIN [${reader}] WITH PASSWORD='${readerPassword}', CHECK_POLICY=OFF`,
      ]);
      sql(
        [
          `CREATE USER [${reader}] FOR LOGIN [${reader}]`,
          `ALTER ROLE db_datareader ADD MEMBER [${reader}]`,
          `CREATE TABLE dbo.events (id bigint NOT NULL PRIMARY KEY, updated datetime2(7) NOT NULL,
             name nvarchar(40) NULL, amount decimal(38,10) NULL, at_offset datetimeoffset(7) NULL)`,
          `INSERT dbo.events VALUES
             (1,'2026-11-22T10:00:00.1234567',N'café',12345678901234567890123456.0123456789,'2026-11-22T10:00:00.1234567+05:30'),
             (2,'2026-11-22T10:00:00.1234567',N'tie',NULL,NULL),
             (9007199254740993,'2026-11-22T10:00:01.0000001',N'big',NULL,NULL)`,
        ],
        database,
      );
      await h.setup();
      await h.secret("sqlserver_e2e", config());
      conn = await h.connection(`sqlserver_${database}`, "sqlserver-v1", "sqlserver_e2e");
      await h.stream(
        conn,
        { namespace: "dbo", name: "events", primary_key: "id", cursor_field: "updated" },
        "incremental",
      );
    }, 180000);
    afterAll(async () => {
      if (conn) await h.db.from("connect_connections").update({ enabled: false }).eq("id", conn.id);
      await h.teardown();
      sql([
        `IF DB_ID('${database}') IS NOT NULL BEGIN ALTER DATABASE [${database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [${database}] END`,
        `IF SUSER_ID('${reader}') IS NOT NULL DROP LOGIN [${reader}]`,
      ]);
    }, 60000);

    it("lands exact values, resumes after the committed position and fails closed on a wrong CA", async () => {
      const first = await h.enqueue(conn!, "sync");
      await h.agent(conn!);
      expect(await h.run(first)).toMatchObject({ status: "succeeded", rows_committed: 3 });
      const bronze = await h.landed(first);
      const byId = Object.fromEntries(bronze.lines.map((line) => [String(line.data["id"]), line]));
      expect(Object.keys(byId).sort()).toEqual(["1", "2", "9007199254740993"]);
      expect(byId["1"]!.data).toMatchObject({
        name: "café",
        updated: "2026-11-22T10:00:00.1234567",
        at_offset: "2026-11-22T10:00:00.1234567+05:30",
      });
      expect(String(byId["1"]!.data["amount"])).toBe("12345678901234567890123456.0123456789");
      const committed = await h.state(conn!);
      expect(committed).toMatchObject({ cursor: "2026-11-22T10:00:01.0000001" });

      // A row tied on the cursor with a higher key and a later row: only those land.
      sql(
        [
          `INSERT dbo.events (id,updated,name) VALUES
             (9007199254740994,'2026-11-22T10:00:01.0000001',N'tie later key'),
             (3,'2026-11-22T10:00:02',N'later')`,
        ],
        database,
      );
      const second = await h.enqueue(conn!, "sync");
      await h.agent(conn!);
      expect(await h.run(second)).toMatchObject({ status: "succeeded", rows_committed: 2 });
      expect((await h.landed(second)).lines.map((line) => String(line.data["id"])).sort()).toEqual([
        "3",
        "9007199254740994",
      ]);

      const unchanged = await h.enqueue(conn!, "sync");
      await h.agent(conn!);
      expect(await h.run(unchanged)).toMatchObject({ status: "succeeded", rows_committed: 0 });
      const before = await h.state(conn!);

      if (wrongCa) {
        await h.secret("sqlserver_e2e", config({ ssl_ca: wrongCa }));
        const refused = await h.enqueue(conn!, "sync");
        await h.agent(conn!);
        expect(await h.run(refused)).toMatchObject({ status: "failed", rows_committed: 0 });
        expect(await task(refused)).toMatchObject({ error_code: "auth_failed" });
        expect(await h.state(conn!)).toEqual(before);
        await h.secret("sqlserver_e2e", config());
      }
      // A reset re-reads every row as a full refresh; the incremental sync after it resumes
      // from the reset's checkpoint and picks up a later row.
      const reset = await h.enqueue(conn!, "reset");
      await h.agent(conn!);
      expect(await h.run(reset)).toMatchObject({ status: "succeeded", rows_committed: 5 });
      sql(
        [`INSERT dbo.events (id,updated,name) VALUES (4,'2026-11-22T10:00:03',N'after reset')`],
        database,
      );
      const after = await h.enqueue(conn!, "sync");
      await h.agent(conn!);
      expect(await h.run(after)).toMatchObject({ status: "succeeded" });
      expect((await h.landed(after)).lines.map((line) => String(line.data["id"]))).toContain("4");

      // Earlier Bronze is untouched by later runs.
      expect(await h.landed(first)).toEqual(bronze);
    });
  },
);
