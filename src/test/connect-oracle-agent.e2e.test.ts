// @vitest-environment node
// Real outbound agent -> Oracle actor over verified TLS (TCPS) -> fenced Bronze commits.
// Opt in with ORACLE_TEST_PORT, ORACLE_TEST_CA, ORACLE_TEST_WRONG_CA and
// ORACLE_TEST_SYSTEM_PASSWORD (deploy/connect/README.md, "Oracle"). The local engine is
// Oracle Database Free: engine evidence, not Enterprise Edition or managed-service certification.
import { spawnSync } from "node:child_process";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { SparkHarness, url, service, key, type Conn } from "./connect-spark-harness";

const port = process.env["ORACLE_TEST_PORT"];
const ca = process.env["ORACLE_TEST_CA"];
const wrongCa = process.env["ORACLE_TEST_WRONG_CA"];
const system = process.env["ORACLE_TEST_SYSTEM_PASSWORD"];
const serviceName = process.env["ORACLE_TEST_SERVICE"] ?? "FREEPDB1";
const local = !!url && ["localhost", "127.0.0.1"].includes(new URL(url).hostname);
const suffix = crypto.randomUUID().replaceAll("-", "").slice(0, 10).toUpperCase();
const owner = `E2E_SRC_${suffix}`;
const reader = `E2E_READER_${suffix}`;
const readerPassword = `R${crypto.randomUUID().replaceAll("-", "")}`;
const h = new SparkHarness(false);
let conn: Conn | undefined;

// Admin statements run through the actor's own pinned driver, never the agent's credentials.
function sql(statements: string[]) {
  const script = `
import json, os, sys
import source_oracle as oracle
statements = json.loads(sys.stdin.buffer.read().decode('utf-8'))
conn = oracle.connection({'host': 'localhost', 'port': int(os.environ['ORACLE_TEST_PORT']),
                          'service_name': sys.argv[1], 'user': 'system',
                          'password': os.environ['ORACLE_TEST_SYSTEM_PASSWORD'],
                          'ssl_ca': os.environ['ORACLE_TEST_CA']})
with conn.cursor() as cursor:
    for statement in statements: cursor.execute(statement)
conn.commit(); conn.close()
`;
  const result = spawnSync(process.env["PYTHON"] ?? "python3", ["-c", script, serviceName], {
    input: JSON.stringify(statements),
    encoding: "utf8",
    env: { ...process.env, PYTHONPATH: "deploy/connect" },
  });
  if (result.status !== 0) throw new Error(`Oracle fixture failed: ${result.stderr.slice(-400)}`);
}
const dropUser = (user: string) =>
  `BEGIN EXECUTE IMMEDIATE 'DROP USER ${user} CASCADE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -1918 THEN RAISE; END IF; END;`;
function config(overrides: Record<string, unknown> = {}) {
  return {
    host: "localhost",
    port: Number(port),
    service_name: serviceName,
    user: reader,
    password: readerPassword,
    schema: owner,
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

describe.skipIf(!local || !service || !key || !port || !ca || !system)(
  "Oracle through the real agent",
  { timeout: 180000 },
  () => {
    beforeAll(async () => {
      sql([
        `CREATE USER ${owner} IDENTIFIED BY "O${crypto.randomUUID().replaceAll("-", "")}" QUOTA UNLIMITED ON USERS`,
        `CREATE USER ${reader} IDENTIFIED BY "${readerPassword}"`,
        `GRANT CREATE SESSION TO ${reader}`,
        `CREATE TABLE ${owner}.EVENTS (ID NUMBER(20) PRIMARY KEY, UPDATED TIMESTAMP(9) NOT NULL,
           NAME NVARCHAR2(40), AMOUNT NUMBER(38,10), AT_TZ TIMESTAMP(9) WITH TIME ZONE)`,
        `GRANT SELECT ON ${owner}.EVENTS TO ${reader}`,
        `INSERT INTO ${owner}.EVENTS VALUES (1, TIMESTAMP '2026-11-22 10:00:00.123456789', N'café',
           12345678901234567890123456.0123456789, TIMESTAMP '2026-11-22 10:00:00.123456789 +05:30')`,
        `INSERT INTO ${owner}.EVENTS (ID, UPDATED, NAME) VALUES (2, TIMESTAMP '2026-11-22 10:00:00.123456789', N'tie')`,
        `INSERT INTO ${owner}.EVENTS (ID, UPDATED, NAME) VALUES (9007199254740993, TIMESTAMP '2026-11-22 10:00:01.000000001', N'big')`,
      ]);
      await h.setup();
      await h.secret("oracle_e2e", config());
      conn = await h.connection(`oracle_${suffix.toLowerCase()}`, "oracle-v1", "oracle_e2e");
      await h.stream(
        conn,
        { namespace: owner, name: "EVENTS", primary_key: "ID", cursor_field: "UPDATED" },
        "incremental",
      );
    }, 180000);
    afterAll(async () => {
      if (conn) await h.db.from("connect_connections").update({ enabled: false }).eq("id", conn.id);
      await h.teardown();
      sql([dropUser(reader), dropUser(owner)]);
    }, 60000);

    it("lands exact values, resumes after the committed position and fails closed on a wrong CA", async () => {
      const first = await h.enqueue(conn!, "sync");
      await h.agent(conn!);
      expect(await h.run(first)).toMatchObject({ status: "succeeded", rows_committed: 3 });
      const bronze = await h.landed(first);
      const byId = Object.fromEntries(bronze.lines.map((line) => [String(line.data["ID"]), line]));
      expect(Object.keys(byId).sort()).toEqual(["1", "2", "9007199254740993"]);
      expect(byId["1"]!.data).toMatchObject({
        NAME: "café",
        UPDATED: "2026-11-22T10:00:00.123456789",
        AT_TZ: "2026-11-22T10:00:00.123456789+05:30",
      });
      expect(String(byId["1"]!.data["AMOUNT"])).toBe("12345678901234567890123456.0123456789");
      expect(await h.state(conn!)).toMatchObject({ cursor: "2026-11-22T10:00:01.000000001" });

      // A row tied on the cursor with a higher key and a later row: only those land.
      sql([
        `INSERT INTO ${owner}.EVENTS (ID, UPDATED, NAME) VALUES (9007199254740994, TIMESTAMP '2026-11-22 10:00:01.000000001', N'tie later key')`,
        `INSERT INTO ${owner}.EVENTS (ID, UPDATED, NAME) VALUES (3, TIMESTAMP '2026-11-22 10:00:02', N'later')`,
      ]);
      const second = await h.enqueue(conn!, "sync");
      await h.agent(conn!);
      expect(await h.run(second)).toMatchObject({ status: "succeeded", rows_committed: 2 });
      expect((await h.landed(second)).lines.map((line) => String(line.data["ID"])).sort()).toEqual([
        "3",
        "9007199254740994",
      ]);

      const unchanged = await h.enqueue(conn!, "sync");
      await h.agent(conn!);
      expect(await h.run(unchanged)).toMatchObject({ status: "succeeded", rows_committed: 0 });
      const before = await h.state(conn!);

      if (wrongCa) {
        await h.secret("oracle_e2e", config({ ssl_ca: wrongCa }));
        const refused = await h.enqueue(conn!, "sync");
        await h.agent(conn!);
        expect(await h.run(refused)).toMatchObject({ status: "failed", rows_committed: 0 });
        expect(await task(refused)).toMatchObject({ error_code: "auth_failed" });
        expect(await h.state(conn!)).toEqual(before);
        await h.secret("oracle_e2e", config());
      }
      // A reset re-reads every row as a full refresh; the incremental sync after it resumes
      // from the reset's checkpoint and picks up a later row.
      const reset = await h.enqueue(conn!, "reset");
      await h.agent(conn!);
      expect(await h.run(reset)).toMatchObject({ status: "succeeded", rows_committed: 5 });
      sql([
        `INSERT INTO ${owner}.EVENTS (ID, UPDATED, NAME) VALUES (4, TIMESTAMP '2026-11-22 10:00:03', N'after reset')`,
      ]);
      const after = await h.enqueue(conn!, "sync");
      await h.agent(conn!);
      expect(await h.run(after)).toMatchObject({ status: "succeeded" });
      expect((await h.landed(after)).lines.map((line) => String(line.data["ID"]))).toContain("4");

      // Earlier Bronze is untouched by later runs.
      expect(await h.landed(first)).toEqual(bronze);
    });
  },
);
