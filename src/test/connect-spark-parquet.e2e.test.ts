// @vitest-environment node
// Spark S6 (deploy/connect/README.md, "Parquet Bronze parts (S6)"): with `bronze_format: parquet` a Spark run
// lands its Bronze parts as Parquet (layout meridian-bronze-parquet/1). The parts decode to the same
// envelopes a single-worker NDJSON run lands, the registry records each part's format, and the
// server refuses a Parquet part that is not that layout. Needs the dev app, the seeded
// connect_acceptance fixture and the Spark cluster; skips otherwise.
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { Client as PgClient } from "pg";
import { sha256Hex } from "@/lib/file-ingest";
import { APP, SparkHarness, executors, live, type Conn } from "./connect-spark-harness";

// The seeded fixture database runs on the dev stack's Postgres server; the registry is its `postgres` database.
const source = {
  host: process.env["CONNECT_TEST_HOST"] ?? "127.0.0.1",
  port: Number(process.env["CONNECT_TEST_PORT"] ?? 55439),
  database: process.env["CONNECT_TEST_DB"] ?? "postgres",
  user: "postgres",
  password: process.env["PGPASSWORD"] ?? "postgres",
};
const h = new SparkHarness();
const conn = {} as { spark: Conn; base: Conn };
const table = {
  namespace: "public",
  name: "connect_acceptance",
  primary_key: "id",
  cursor_field: "updated_at",
};
const byId = (rows: { data: Record<string, unknown>; primary_key?: unknown }[]) =>
  rows
    .map((l) => [l.data["id"] as number, JSON.stringify(l.data), JSON.stringify(l.primary_key)])
    .sort((a, b) => Number(a[0]) - Number(b[0]));

describe.skipIf(!live)("Spark S6: Bronze parts as Parquet", { timeout: 600000 }, () => {
  beforeAll(async () => {
    await h.setup();
    await h.secret("postgres", {
      host: source.host,
      port: source.port,
      dbname: source.database,
      user: source.user,
      password: source.password,
      sslmode: "disable",
    });
    conn.spark = await h.connection("spark", "spark-postgres-v1", "postgres");
    conn.base = await h.connection("base", "postgres-v1", "postgres");
    for (const c of [conn.spark, conn.base]) await h.stream(c, table, "full_refresh");
  }, 600000);
  afterAll(() => h.teardown(), 60000);

  it("lands Parquet parts that read back as the single-worker run's records", async () => {
    const run = await h.enqueue(conn.spark, "sync");
    await h.agent(conn.spark, { bronze_format: "parquet" });
    expect(await h.run(run)).toMatchObject({ status: "succeeded", rows_committed: 1200 });
    const { commits, lines, manifests } = await h.landed(run);
    expect(executors(manifests).size).toBeGreaterThanOrEqual(2);
    // Every part with records is Parquet; only the driver's empty completion part may be NDJSON.
    commits.forEach((c, i) => {
      const key = String(manifests[i]!["object_key"]);
      if (c.row_count) expect(key).toMatch(/\/landing\.parquet$/);
      else expect(key).toMatch(/\/landing\.(ndjson|parquet)$/);
    });
    const base = await h.enqueue(conn.base, "sync");
    await h.agent(conn.base);
    expect(byId(lines)).toEqual(byId((await h.landed(base)).lines));
    // The registry names each version's format from its object key (migration 0106).
    const pg = new PgClient({ ...source, database: "postgres" });
    await pg.connect();
    const versions = (
      await pg
        .query("SELECT format, object_key FROM core.dataset_versions WHERE object_key = ANY($1)", [
          manifests.map((m) => m["object_key"]),
        ])
        .finally(() => pg.end())
    ).rows as { format: string; object_key: string }[];
    expect(versions).toHaveLength(commits.length);
    for (const v of versions)
      expect(v.format).toBe(v.object_key.endsWith("/landing.parquet") ? "parquet" : "ndjson");
  });

  it("refuses a part claimed as Parquet that is not the Bronze layout", async () => {
    await h.enqueue(conn.spark, "sync");
    const call = async (payload: Record<string, unknown>) => {
      const res = await fetch(`${APP}/api/public/connect-agent`, {
        method: "POST",
        headers: { "content-type": "application/json", "x-ingest-token": conn.spark.token },
        body: JSON.stringify(payload),
      });
      return { status: res.status, body: await res.json() };
    };
    const claim = (await call({ action: "claim_task", worker: "tamper:0" })).body.claim;
    const ids = { runId: claim.run_id, token: claim.lease_token, streamId: conn.spark.stream };
    const meta = (await call({ action: "upload_url", ...ids, part: "0-0", format: "parquet" }))
      .body;
    // NDJSON bytes uploaded under a Parquet key.
    const body = new TextEncoder().encode(
      JSON.stringify({
        protocol: "meridian-connect/1",
        connection_id: meta.connection_id,
        run_id: claim.run_id,
        stream_id: conn.spark.stream,
        schema: claim.streams[0].schema,
        extracted_at: meta.extracted_at,
        cursor: "x",
        primary_key: 1,
        operation: "upsert",
        source_namespace: "public",
        source_stream: "connect_acceptance",
        data: { id: 1 },
      }) + "\n",
    );
    const res = await fetch(meta.url, {
      method: "PUT",
      headers: { "content-type": "application/vnd.apache.parquet", "x-upsert": "false" },
      body,
    });
    expect(res.ok).toBe(true);
    const commit = await call({
      action: "commit_file",
      ...ids,
      sequence: 0,
      part: "0-0",
      format: "parquet",
      rows: 1,
      bytes: body.length,
      sha256: await sha256Hex(body),
      state: { type: "STATE", state: { version: 1, completed: false } },
    });
    expect(commit.status).toBe(409);
    await h.db
      .from("connect_runs")
      .update({ status: "cancelled", lease_token: null, lease_until: null })
      .eq("id", claim.run_id);
  });
});
