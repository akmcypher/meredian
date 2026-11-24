// @vitest-environment node
// Real outbound agent -> MongoDB change-stream actor -> fenced Bronze commits.
// Opt in like connect-mongodb-agent.e2e.test.ts (MONGODB_TEST_PORT, MONGODB_TEST_CA,
// MONGODB_TEST_ROOT_PASSWORD); see deploy/connect/README.md, "MongoDB change streams".
import { spawnSync } from "node:child_process";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { SparkHarness, url, service, key, type Conn } from "./connect-spark-harness";

const port = process.env["MONGODB_TEST_PORT"];
const ca = process.env["MONGODB_TEST_CA"];
const root = process.env["MONGODB_TEST_ROOT_PASSWORD"];
const local = !!url && ["localhost", "127.0.0.1"].includes(new URL(url).hostname);
const database = `meridian_cdc_${crypto.randomUUID().replaceAll("-", "").slice(0, 10)}`;
const readerPassword = crypto.randomUUID().replaceAll("-", "");
const h = new SparkHarness(false);
let conn: Conn | undefined;

function mongo(script: string) {
  const prelude = `
import os, sys
from pymongo import MongoClient
client = MongoClient('localhost', int(os.environ['MONGODB_TEST_PORT']), username='root',
                     password=os.environ['MONGODB_TEST_ROOT_PASSWORD'], authSource='admin', tls=True,
                     tlsCAFile=os.environ['MONGODB_TEST_CA'], replicaSet='rs0')
db = client[sys.argv[1]]
`;
  const result = spawnSync(process.env["PYTHON"] ?? "python3", ["-c", prelude + script, database], {
    encoding: "utf8",
    env: { ...process.env, PYTHONPATH: "deploy/connect", READER_PASSWORD: readerPassword },
  });
  if (result.status !== 0) throw new Error(`MongoDB fixture failed: ${result.stderr.slice(-400)}`);
}
async function sync() {
  const run = await h.enqueue(conn!, "sync");
  await h.agent(conn!);
  return run;
}
const changes = async (run: string) =>
  (await h.landed(run)).lines.map((line) => [line["operation"], line["primary_key"]]);

describe.skipIf(!local || !service || !key || !port || !ca || !root)(
  "MongoDB change streams through the real agent",
  { timeout: 180000 },
  () => {
    beforeAll(async () => {
      mongo(`
db.command('createUser', 'reader', pwd=os.environ['READER_PASSWORD'], roles=[{'role': 'read', 'db': sys.argv[1]}])
db.orders.insert_many([{'_id': i, 'v': i} for i in (1, 2, 3)])
`);
      await h.setup();
      await h.secret("mongodb_cdc_e2e", {
        hosts: "localhost",
        port: Number(port),
        database,
        user: "reader",
        password: readerPassword,
        auth_source: database,
        replica_set: "rs0",
        ssl_ca: ca,
      });
      conn = await h.connection(`mongodb_cdc_${database}`, "mongodb-cdc-v1", "mongodb_cdc_e2e");
      await h.stream(
        conn,
        { namespace: database, name: "orders", primary_key: "_id", cursor_field: "_id" },
        "cdc",
      );
    }, 180000);
    afterAll(async () => {
      if (conn) await h.db.from("connect_connections").update({ enabled: false }).eq("id", conn.id);
      await h.teardown();
      mongo("client.drop_database(sys.argv[1])");
    }, 60000);

    it("copies, then lands inserts, updates and deletes once each across runs", async () => {
      const snapshot = await sync();
      expect(await h.run(snapshot)).toMatchObject({ status: "succeeded", rows_committed: 3 });
      expect(await changes(snapshot)).toEqual([
        ["upsert", 1],
        ["upsert", 2],
        ["upsert", 3],
      ]);
      expect(await h.state(conn!)).toMatchObject({ cdc: { phase: "stream" } });

      mongo(`
db.orders.insert_one({'_id': 4, 'v': 4})
db.orders.update_one({'_id': 2}, {'$set': {'v': 20}})
db.orders.delete_one({'_id': 1})
`);
      const stream = await sync();
      expect(await h.run(stream)).toMatchObject({ status: "succeeded", rows_committed: 3 });
      expect(await changes(stream)).toEqual([
        ["upsert", 4],
        ["upsert", 2],
        ["delete", 1],
      ]);
      const updated = (await h.landed(stream)).lines[1]!;
      expect(updated.data).toEqual({ _id: 2, v: 20 });

      const quiet = await sync();
      expect(await h.run(quiet)).toMatchObject({ status: "succeeded", rows_committed: 0 });

      mongo("db.orders.drop()");
      const dropped = await sync();
      expect(await h.run(dropped)).toMatchObject({ status: "failed", rows_committed: 0 });
    });
  },
);
