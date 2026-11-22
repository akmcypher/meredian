// @vitest-environment node
// Actual secured Spark workers, local S3, fenced Bronze commits and cross-run range recovery.
import { spawnSync } from "node:child_process";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { SparkHarness, executors, live, url, type Conn } from "./connect-spark-harness";

const key = process.env["S3_PROTOCOL_ACCESS_KEY_ID"];
const secret = process.env["S3_PROTOCOL_ACCESS_KEY_SECRET"];
const h = new SparkHarness();
const bucket = `spark-incremental-${crypto.randomUUID().slice(0, 8)}`;
const connections = {} as Record<"incremental" | "retry" | "cross" | "legacy", Conn>;
const rows = 240;
let directory = "";
const fixture = `
from fastavro import writer
import sys
schema={'type':'record','name':'Row','fields':[{'name':'id','type':'long'},{'name':'name','type':'string'},{'name':'big','type':'long'}]}
for name,values in [('initial',range(240)),('late',range(1000,1003)),('replacement',range(2000,2005))]:
    with open(sys.argv[1]+'/'+name+'.avro','wb') as f:
        writer(f,schema,({'id':i,'name':str(i)+':'+chr(233)*60,'big':2**60+i} for i in values),codec='deflate',sync_interval=512)
`;
async function sync(c: Conn, options: Record<string, unknown> = {}, operation = "sync") {
  const run = await h.enqueue(c, operation);
  await h.agent(c, { range_bytes: 256, ...options });
  return run;
}
async function unique(run: string) {
  const landed = await h.landed(run);
  return { ...landed, keys: landed.lines.map((line) => JSON.stringify(line["primary_key"])) };
}

describe.skipIf(!live || !key || !secret)(
  "incremental Spark files through secured S3 workers",
  { timeout: 600000 },
  () => {
    beforeAll(async () => {
      await h.setup();
      directory = await mkdtemp(join(tmpdir(), "spark-incremental-"));
      const made = spawnSync(process.env["PYTHON"] ?? "python3", ["-c", fixture, directory], {
        encoding: "utf8",
      });
      if (made.status !== 0) throw new Error(made.stderr);
      const created = await h.db.storage.createBucket(bucket, { public: false });
      if (created.error) throw created.error;
      const endpoint = `${url!.replace("localhost", "127.0.0.1")}/storage/v1/s3`;
      h.overrides = (gateway) => ({ endpoint: endpoint.replace("127.0.0.1", gateway) });
      for (const name of ["incremental", "retry", "cross", "legacy"] as const) {
        const uploaded = await h.db.storage
          .from(bucket)
          .upload(`${name}/b.avro`, await readFile(join(directory, "initial.avro")));
        if (uploaded.error) throw uploaded.error;
        await h.secret(name, {
          endpoint,
          allow_insecure_endpoint: true,
          region: "local",
          bucket,
          prefix: name,
          format: "avro",
          access_key_id: key,
          secret_access_key: secret,
        });
        const c = await h.connection(`spark_${name}`, "spark-files-v1", name);
        connections[name] = c;
        await h.stream(
          c,
          {
            namespace: bucket.replaceAll("-", "_"),
            name,
            primary_key: "_file,_position",
            cursor_field: "_modified",
          },
          name === "legacy" ? "full_refresh" : "incremental",
        );
      }
    }, 180000);
    afterAll(async () => {
      if (h.db) {
        await h.db.storage
          .from(bucket)
          .remove(["incremental/b.avro", "incremental/a.avro", "retry/b.avro", "cross/b.avro"]);
        await h.db.storage.deleteBucket(bucket);
      }
      await h.teardown();
    }, 30000);

    it("skips unchanged files, reads late keys, rejects replacement and rebuilds on reset", async () => {
      const c = connections.incremental;
      const first = await sync(c);
      expect(await h.run(first)).toMatchObject({ status: "succeeded", rows_committed: rows });
      const initial = await unique(first);
      expect(new Set(initial.keys).size).toBe(rows);
      expect(executors(initial.manifests).size).toBeGreaterThanOrEqual(2);
      const state = await h.state(c);
      expect(state.files.version).toBe(2);
      expect(state.files.objects["incremental/b.avro"].done).toBe(true);
      expect(JSON.stringify(state)).not.toContain(secret!);
      const unchanged = await sync(c);
      expect(await h.run(unchanged)).toMatchObject({ status: "succeeded", rows_committed: 0 });
      const late = await h.db.storage
        .from(bucket)
        .upload("incremental/a.avro", await readFile(join(directory, "late.avro")));
      if (late.error) throw late.error;
      const appended = await sync(c);
      expect(await h.run(appended)).toMatchObject({ status: "succeeded", rows_committed: 3 });
      expect((await unique(appended)).lines.map((line) => line.data["id"]).sort()).toEqual([
        1000, 1001, 1002,
      ]);
      const replaced = await h.db.storage
        .from(bucket)
        .upload("incremental/b.avro", await readFile(join(directory, "replacement.avro")), {
          upsert: true,
        });
      if (replaced.error) throw replaced.error;
      const rejected = await sync(c);
      expect(await h.run(rejected)).toMatchObject({
        status: "failed",
        error_code: "source_changed",
        rows_committed: 0,
      });
      const reset = await sync(c, {}, "reset");
      expect(await h.run(reset)).toMatchObject({ status: "succeeded", rows_committed: 8 });
      expect((await h.state(c)).files.version).toBe(2);
      expect(await h.run(await sync(c))).toMatchObject({ status: "succeeded", rows_committed: 0 });
    });

    it("resumes the same run after a committed range without duplicate Bronze keys", async () => {
      const c = connections.retry;
      const run = await sync(c, { fail_after_commits: 1 });
      const partial = await h.run(run);
      expect(partial.status).toBe("running");
      expect(Number(partial.rows_committed)).toBeGreaterThan(0);
      expect(Number(partial.rows_committed)).toBeLessThan(rows);
      expect((await h.state(c)).files.objects["retry/b.avro"].done).toBe(false);
      const due = await h.db
        .from("connect_run_streams")
        .update({ next_attempt_at: "2000-01-01T00:00:00Z" })
        .eq("run_id", run);
      if (due.error) throw due.error;
      await h.agent(c, { range_bytes: 4096 });
      expect(await h.run(run)).toMatchObject({ status: "succeeded", rows_committed: rows });
      const landed = await unique(run);
      expect(landed.keys).toHaveLength(rows);
      expect(new Set(landed.keys).size).toBe(rows);
    });

    it("keeps existing full-refresh range checkpoints compatible", async () => {
      const c = connections.legacy;
      const run = await sync(c);
      expect(await h.run(run)).toMatchObject({ status: "succeeded", rows_committed: rows });
      const landed = await unique(run);
      expect(new Set(landed.keys).size).toBe(rows);
      const state = await h.state(c);
      expect(state.files).toBeUndefined();
      expect(state.spark.plan.length).toBeGreaterThan(1);
      expect(state.spark.mode).toBeUndefined();
      const reset = await sync(c, {}, "reset");
      expect(await h.run(reset)).toMatchObject({ status: "succeeded", rows_committed: rows });
      expect((await h.state(c)).files).toBeUndefined();
      const normal = await sync(c);
      expect(await h.run(normal)).toMatchObject({ status: "succeeded", rows_committed: rows });
    });

    it("a new run resumes cancelled partial ranges rather than rereading committed ranges", async () => {
      const c = connections.cross;
      const abandoned = await sync(c, { fail_after_commits: 1 });
      const previous = await unique(abandoned);
      expect(previous.keys.length).toBeGreaterThan(0);
      expect(previous.keys.length).toBeLessThan(rows);
      const cancelled = await h.owner.rpc("connect_cancel", { _run: abandoned });
      expect(cancelled.error).toBeNull();
      const resumed = await sync(c, { range_bytes: 4096 });
      expect(await h.run(resumed)).toMatchObject({
        status: "succeeded",
        rows_committed: rows - previous.keys.length,
      });
      const next = await unique(resumed);
      const keys = [...previous.keys, ...next.keys];
      expect(keys).toHaveLength(rows);
      expect(new Set(keys).size).toBe(rows);
      expect((await h.state(c)).files.objects["cross/b.avro"].done).toBe(true);
    });
  },
);
