// @vitest-environment node
// Real outbound agent -> MongoDB actor over verified TLS -> fenced Bronze commits.
// Opt in with MONGODB_TEST_PORT, MONGODB_TEST_CA, MONGODB_TEST_WRONG_CA and
// MONGODB_TEST_ROOT_PASSWORD (deploy/connect/README.md, "MongoDB"): a TLS-only,
// authenticated replica set `rs0` advertised as localhost:<port>.
import { spawnSync } from "node:child_process";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { SparkHarness, url, service, key, type Conn } from "./connect-spark-harness";

const port = process.env["MONGODB_TEST_PORT"];
const ca = process.env["MONGODB_TEST_CA"];
const wrongCa = process.env["MONGODB_TEST_WRONG_CA"];
const root = process.env["MONGODB_TEST_ROOT_PASSWORD"];
const local = !!url && ["localhost", "127.0.0.1"].includes(new URL(url).hostname);
const database = `meridian_e2e_${crypto.randomUUID().replaceAll("-", "").slice(0, 10)}`;
const readerPassword = crypto.randomUUID().replaceAll("-", "");
const h = new SparkHarness(false);
let conn: Conn | undefined;

// Fixture changes run through the actor's own pinned driver as the root user.
function mongo(script: string) {
  const prelude = `
import datetime, json, os, sys, bson
from pymongo import MongoClient
T0 = datetime.datetime(2026, 11, 22, 10, 0, 0, 123000, tzinfo=datetime.timezone.utc)
client = MongoClient('localhost', int(os.environ['MONGODB_TEST_PORT']), username='root',
                     password=os.environ['MONGODB_TEST_ROOT_PASSWORD'], authSource='admin', tls=True,
                     tlsCAFile=os.environ['MONGODB_TEST_CA'], replicaSet='rs0', tz_aware=True)
db = client[sys.argv[1]]
`;
  const result = spawnSync(process.env["PYTHON"] ?? "python3", ["-c", prelude + script, database], {
    encoding: "utf8",
    env: { ...process.env, PYTHONPATH: "deploy/connect", READER_PASSWORD: readerPassword },
  });
  if (result.status !== 0) throw new Error(`MongoDB fixture failed: ${result.stderr.slice(-400)}`);
}
function config(overrides: Record<string, unknown> = {}) {
  return {
    hosts: "localhost",
    port: Number(port),
    database,
    user: "reader",
    password: readerPassword,
    auth_source: database,
    replica_set: "rs0",
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
const oid = (n: number) => `6566a1b2c3d4e5f6012345${String(n).padStart(2, "0")}`;

describe.skipIf(!local || !service || !key || !port || !ca || !root)(
  "MongoDB through the real agent",
  { timeout: 180000 },
  () => {
    beforeAll(async () => {
      mongo(`
db.command('createUser', 'reader', pwd=os.environ['READER_PASSWORD'], roles=[{'role': 'read', 'db': sys.argv[1]}])
db.orders.insert_many([
    {'_id': bson.ObjectId('${oid(1)}'), 'updated': T0, 'name': 'café', 'big': bson.Int64(2**53+1),
     'amount': bson.Decimal128('12345678901234567890.0123456789'), 'items': [{'sku': 'a', 'qty': 2}]},
    {'_id': bson.ObjectId('${oid(2)}'), 'updated': T0, 'name': 'tie'},
    {'_id': bson.ObjectId('${oid(3)}'), 'updated': T0 + datetime.timedelta(seconds=1), 'name': 'later'},
])
`);
      await h.setup();
      await h.secret("mongodb_e2e", config());
      conn = await h.connection(`mongodb_${database}`, "mongodb-v1", "mongodb_e2e");
      await h.stream(
        conn,
        { namespace: database, name: "orders", primary_key: "_id", cursor_field: "updated" },
        "incremental",
      );
    }, 180000);
    afterAll(async () => {
      if (conn) await h.db.from("connect_connections").update({ enabled: false }).eq("id", conn.id);
      await h.teardown();
      mongo("client.drop_database(sys.argv[1])");
    }, 60000);

    it("lands typed documents, resumes after the committed position and fails closed on a wrong CA", async () => {
      const first = await h.enqueue(conn!, "sync");
      await h.agent(conn!);
      expect(await h.run(first)).toMatchObject({ status: "succeeded", rows_committed: 3 });
      const bronze = await h.landed(first);
      const byId = Object.fromEntries(
        bronze.lines.map((line) => [(line.data["_id"] as { $oid: string }).$oid, line]),
      );
      expect(Object.keys(byId).sort()).toEqual([oid(1), oid(2), oid(3)]);
      expect(byId[oid(1)]!.data).toMatchObject({
        updated: { $date: "2026-11-22T10:00:00.123Z" },
        name: "café",
        big: { $numberLong: "9007199254740993" },
        amount: { $numberDecimal: "12345678901234567890.0123456789" },
        items: [{ sku: "a", qty: 2 }],
      });
      expect(await h.state(conn!)).toMatchObject({
        cursor: { $date: "2026-11-22T10:00:01.123Z" },
        primary_key: { $oid: oid(3) },
      });

      // A document tied on the cursor with a higher _id and a later one: only those land.
      mongo(`
db.orders.insert_many([
    {'_id': bson.ObjectId('${oid(4)}'), 'updated': T0 + datetime.timedelta(seconds=1), 'name': 'tie later id'},
    {'_id': bson.ObjectId('${oid(5)}'), 'updated': T0 + datetime.timedelta(seconds=2), 'name': 'newest'},
])
`);
      const second = await h.enqueue(conn!, "sync");
      await h.agent(conn!);
      expect(await h.run(second)).toMatchObject({ status: "succeeded", rows_committed: 2 });
      expect(
        (await h.landed(second)).lines.map((line) => (line.data["_id"] as { $oid: string }).$oid),
      ).toEqual([oid(4), oid(5)]);

      const unchanged = await h.enqueue(conn!, "sync");
      await h.agent(conn!);
      expect(await h.run(unchanged)).toMatchObject({ status: "succeeded", rows_committed: 0 });
      const before = await h.state(conn!);

      if (wrongCa) {
        await h.secret("mongodb_e2e", config({ ssl_ca: wrongCa }));
        const refused = await h.enqueue(conn!, "sync");
        await h.agent(conn!);
        expect(await h.run(refused)).toMatchObject({ status: "failed", rows_committed: 0 });
        expect(await task(refused)).toMatchObject({ error_code: "auth_failed" });
        expect(await h.state(conn!)).toEqual(before);
        await h.secret("mongodb_e2e", config());
      }

      // A reset re-reads every document as a full refresh; the incremental sync after it
      // resumes from the reset's checkpoint and picks up a later document.
      const reset = await h.enqueue(conn!, "reset");
      await h.agent(conn!);
      expect(await h.run(reset)).toMatchObject({ status: "succeeded", rows_committed: 5 });
      mongo(`
db.orders.insert_one({'_id': bson.ObjectId('${oid(6)}'), 'updated': T0 + datetime.timedelta(seconds=3), 'name': 'after reset'})
`);
      const after = await h.enqueue(conn!, "sync");
      await h.agent(conn!);
      expect(await h.run(after)).toMatchObject({ status: "succeeded" });
      expect(
        (await h.landed(after)).lines.map((line) => (line.data["_id"] as { $oid: string }).$oid),
      ).toContain(oid(6));
      expect(await h.landed(first)).toEqual(bronze);
    });
  },
);
