// @vitest-environment node
// Spark ingestion S3 (deploy/connect/README.md, "Spark ingestion"): object-store files read in
// parallel ranges on a multi-executor Spark cluster, through the S3 protocol of the local Supabase
// Storage. Each Spark run is compared with the single-worker files actor reading the same objects
// (one range per file): the same records with the same (_file, _position) keys. Needs the dev app,
// the Spark cluster (deploy/connect/spark/compose.yml) and S3_PROTOCOL_* keys; skips otherwise.
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SparkHarness, byKey, executors, live, url, type Conn } from "./connect-spark-harness";

const S3 = {
  key: process.env["S3_PROTOCOL_ACCESS_KEY_ID"],
  secret: process.env["S3_PROTOCOL_ACCESS_KEY_SECRET"],
  region: process.env["S3_PROTOCOL_REGION"] ?? "local",
};
const CSV_ROWS = 300000;
const RANGE = 2 * 1024 * 1024;
const h = new SparkHarness();
const bucket = `spark-files-${crypto.randomUUID().slice(0, 8)}`;
const conn = {} as Record<
  "csv" | "csvBase" | "pq" | "pqBase" | "jsonl" | "jsonlBase" | "avro" | "avroBase" | "incremental",
  Conn
>;
let fixtureDir = "";

// Fixtures written by pyarrow / Python: a CSV with quoted commas, doubled quotes, unicode
// and CRLF endings (~16 MB), a small second CSV, multi-row-group Parquet and JSON Lines.
const FIXTURES = `
import json, sys, pyarrow as pa, pyarrow.parquet as pq
from fastavro import writer
d=sys.argv[1]; n=int(sys.argv[2])
with open(f'{d}/big.csv','w',encoding='utf-8',newline='') as f:
    f.write('id,name,note,amount\\r\\n')
    for i in range(n): f.write(f'{i},"name {i}, x","said ""hi"" {i%97}",{i*1.25}\\r\\n')
with open(f'{d}/small.csv','w',encoding='utf-8',newline='') as f:
    f.write('id,name,note,amount\\n1,é,"",0\\n2,ü,"a,b",1')
t=pa.table({'id':pa.array(range(200000),pa.int64()),'v':pa.array([f'v{i}' for i in range(200000)]),'x':pa.array([i/8 for i in range(200000)])})
pq.write_table(t,f'{d}/a.parquet',row_group_size=10000)
pq.write_table(t.slice(0,7),f'{d}/b.parquet',row_group_size=2)
with open(f'{d}/a.jsonl','w',encoding='utf-8',newline='') as f:
    for i in range(60000): f.write(json.dumps({'id':i,'tags':['é',i%3],'big':2**60+i,'o':{'k':i}})+'\\n')
schema={'type':'record','name':'Event','fields':[{'name':'id','type':'long'},
    {'name':'name','type':['null','string'],'default':None},
    {'name':'nested','type':{'type':'array','items':'int'}},{'name':'big','type':'long'}]}
def avro(name, ids):
    with open(f'{d}/{name}','wb') as f:
        writer(f,schema,({'id':i,'name':None if i%2 else chr(233),'nested':[i,i%3],'big':2**60+i} for i in ids),codec='deflate',sync_interval=8192)
avro('a.avro',range(60000)); avro('b.avro',range(60000,60007))
avro('inc.avro',[1,2]); avro('late.avro',[3]); avro('changed.avro',[9])
`;

async function sync(c: Conn, options: Record<string, unknown> = {}) {
  const run = await h.enqueue(c, "sync");
  await h.agent(c, options);
  return run;
}

describe.skipIf(!live || !S3.key || !S3.secret)(
  "Spark ingestion (S3): object-store files in parallel ranges",
  { timeout: 1200000 },
  () => {
    beforeAll(async () => {
      await h.setup();
      const dir = await mkdtemp(join(tmpdir(), "spark-files-"));
      fixtureDir = dir;
      const py = spawnSync(
        process.env["PYTHON"] ?? "python3",
        ["-c", FIXTURES, dir, String(CSV_ROWS)],
        {
          encoding: "utf8",
        },
      );
      if (py.status !== 0) throw new Error(py.stderr);
      const made = await h.db.storage.createBucket(bucket, { public: false });
      if (made.error) throw made.error;
      const place: Record<string, string> = {
        "big.csv": "csv/big.csv",
        "small.csv": "csv/part/small.csv",
        "a.parquet": "pq/a.parquet",
        "b.parquet": "pq/b.parquet",
        "a.jsonl": "jsonl/a.jsonl",
        "a.avro": "avro/a.avro",
        "b.avro": "avro/b.avro",
        "inc.avro": "incremental/b.avro",
      };
      for (const f of await readdir(dir)) {
        if (!place[f]) continue;
        const up = await h.db.storage.from(bucket).upload(place[f]!, await readFile(join(dir, f)), {
          contentType: "application/octet-stream",
        });
        if (up.error) throw up.error;
      }
      const endpoint = `${url!.replace("localhost", "127.0.0.1")}/storage/v1/s3`;
      // The source as Spark containers see it: the same storage through the host gateway.
      h.overrides = (gateway) => ({ endpoint: endpoint.replace("127.0.0.1", gateway) });
      const make = async (
        name: keyof typeof conn,
        version: string,
        format: string,
        prefix: string,
        mode: "full_refresh" | "incremental" = "full_refresh",
      ) => {
        await h.secret(`files_${name}`, {
          endpoint,
          region: S3.region,
          bucket,
          prefix,
          format,
          access_key_id: S3.key,
          secret_access_key: S3.secret,
          allow_insecure_endpoint: true,
        });
        conn[name] = await h.connection(name, version, `files_${name}`);
        await h.stream(
          conn[name],
          {
            namespace: bucket.replaceAll("-", "_"),
            name: prefix,
            primary_key: "_file,_position",
            cursor_field: "_modified",
          },
          mode,
        );
      };
      await make("csv", "spark-files-v1", "csv", "csv");
      await make("csvBase", "files-v1", "csv", "csv");
      await make("pq", "spark-files-v1", "parquet", "pq");
      await make("pqBase", "files-v1", "parquet", "pq");
      await make("jsonl", "spark-files-v1", "jsonl", "jsonl");
      await make("jsonlBase", "files-v1", "jsonl", "jsonl");
      await make("avro", "spark-files-v1", "avro", "avro");
      await make("avroBase", "files-v1", "avro", "avro");
      await make("incremental", "files-v1", "avro", "incremental", "incremental");
    }, 1200000);
    afterAll(async () => {
      if (h.db) {
        await h.db.storage.emptyBucket(bucket);
        await h.db.storage.deleteBucket(bucket);
      }
      await h.teardown();
    }, 120000);

    it("splits a CSV into record-aligned ranges across executors; same records as one worker", async () => {
      const run = await sync(conn.csv, { range_bytes: RANGE });
      expect(await h.run(run)).toMatchObject({ status: "succeeded", rows_committed: CSV_ROWS + 2 });
      const { lines, manifests } = await h.landed(run);
      expect(executors(manifests).size).toBeGreaterThanOrEqual(2);
      const state = await h.state(conn.csv);
      // ~16 MB in 2 MiB ranges, plus the small file.
      expect(state.spark.plan.length).toBeGreaterThanOrEqual(8);
      expect(state.spark.consistency).toBe("single");
      expect(new Set(lines.map((l) => JSON.stringify(l["primary_key"]))).size).toBe(CSV_ROWS + 2);
      const sample = lines.find((l) => l.data["id"] === "12345")!;
      expect(sample.data).toMatchObject({
        name: "name 12345, x",
        note: 'said "hi" 26',
        _file: "csv/big.csv",
      });
      const base = await sync(conn.csvBase);
      expect(byKey(lines)).toEqual(byKey((await h.landed(base)).lines));
    });

    it("resumes a failed attempt range by range without loss or duplicates", async () => {
      const run = await sync(conn.csv, { range_bytes: RANGE, fail_after_commits: 3 });
      expect((await h.run(run)).status).toBe("running");
      await h.db
        .from("connect_run_streams")
        .update({ next_attempt_at: "2000-01-01T00:00:00Z" })
        .eq("run_id", run);
      await h.agent(conn.csv, { range_bytes: RANGE });
      expect(await h.run(run)).toMatchObject({ status: "succeeded", rows_committed: CSV_ROWS + 2 });
      const keys = (await h.landed(run)).lines.map((l) => JSON.stringify(l["primary_key"]));
      expect(keys.length).toBe(CSV_ROWS + 2);
      expect(new Set(keys).size).toBe(CSV_ROWS + 2);
    });

    it("Parquet by row groups and JSON Lines by byte ranges match the single worker", async () => {
      for (const [spark, base, rows] of [
        [conn.pq, conn.pqBase, 200007],
        [conn.jsonl, conn.jsonlBase, 60000],
      ] as const) {
        const run = await sync(spark, { range_bytes: 512 * 1024 });
        expect(await h.run(run)).toMatchObject({ status: "succeeded", rows_committed: rows });
        const { lines, manifests } = await h.landed(run);
        expect(executors(manifests).size).toBeGreaterThanOrEqual(2);
        const b = await sync(base);
        expect(byKey(lines)).toEqual(byKey((await h.landed(b)).lines));
      }
      const pq = (
        await h.landed(
          (
            await h.db
              .from("connect_runs")
              .select("id")
              .eq("connection_id", conn.pq.id)
              .order("created_at", { ascending: false })
              .limit(1)
              .single()
          ).data.id,
        )
      ).lines;
      expect(pq.find((l) => l.data["id"] === 199999)!.data).toMatchObject({
        v: "v199999",
        x: 24999.875,
        _position: 199999,
      });
    });

    it("Avro container blocks run across executors and match standalone rows and keys", async () => {
      const run = await sync(conn.avro, { range_bytes: 64 * 1024 });
      expect(await h.run(run)).toMatchObject({ status: "succeeded", rows_committed: 60007 });
      const { lines, manifests } = await h.landed(run);
      expect(executors(manifests).size).toBeGreaterThanOrEqual(2);
      const state = await h.state(conn.avro);
      expect(state.spark.plan.length).toBeGreaterThan(2);
      expect(state.spark.source).toMatch(/^[0-9a-f]{64}$/);
      expect(new Set(lines.map((line) => JSON.stringify(line["primary_key"]))).size).toBe(60007);
      expect(lines.find((line) => line.data["id"] === 12346)!.data).toMatchObject({
        name: "é",
        nested: [12346, 1],
        big: "1152921504606859322",
        _file: "avro/a.avro",
        _position: 12346,
      });
      const base = await sync(conn.avroBase);
      expect(await h.run(base)).toMatchObject({ status: "succeeded", rows_committed: 60007 });
      expect(byKey(lines)).toEqual(byKey((await h.landed(base)).lines));
    });

    it("standalone incremental Avro skips unchanged objects, reads late keys and rejects replacements", async () => {
      const initial = await sync(conn.incremental);
      expect(await h.run(initial)).toMatchObject({ status: "succeeded", rows_committed: 2 });
      expect((await h.landed(initial)).lines.map((line) => line.data["id"])).toEqual([1, 2]);
      const unchanged = await sync(conn.incremental);
      expect(await h.run(unchanged)).toMatchObject({ status: "succeeded", rows_committed: 0 });
      const late = await h.db.storage
        .from(bucket)
        .upload("incremental/a.avro", await readFile(join(fixtureDir, "late.avro")));
      if (late.error) throw late.error;
      const appended = await sync(conn.incremental);
      expect(await h.run(appended)).toMatchObject({ status: "succeeded", rows_committed: 1 });
      expect((await h.landed(appended)).lines.map((line) => line.data["id"])).toEqual([3]);
      const replacement = await h.db.storage
        .from(bucket)
        .upload("incremental/b.avro", await readFile(join(fixtureDir, "changed.avro")), {
          upsert: true,
        });
      if (replacement.error) throw replacement.error;
      const changed = await sync(conn.incremental);
      expect(await h.run(changed)).toMatchObject({
        status: "failed",
        error_code: "source_changed",
        rows_committed: 0,
      });
    });

    it("an object replaced during the run fails the run as source_changed instead of mixing versions", async () => {
      const run = await sync(conn.csv, { range_bytes: RANGE, fail_after_commits: 1 });
      expect((await h.run(run)).status).toBe("running");
      const up = await h.db.storage
        .from(bucket)
        .upload("csv/part/small.csv", new TextEncoder().encode("id,name,note,amount\n9,z,,9\n"), {
          upsert: true,
        });
      if (up.error) throw up.error;
      await h.db
        .from("connect_run_streams")
        .update({ next_attempt_at: "2000-01-01T00:00:00Z" })
        .eq("run_id", run);
      await h.agent(conn.csv, { range_bytes: RANGE });
      const rs = (
        await h.db
          .from("connect_run_streams")
          .select("task_status,error_code")
          .eq("run_id", run)
          .single()
      ).data;
      expect(rs.error_code).toBe("source_changed");
      await h.db
        .from("connect_runs")
        .update({ status: "cancelled", lease_token: null, lease_until: null })
        .eq("id", run);
    });
  },
);
