// @vitest-environment node
// Spark ingestion S1 (deploy/connect/README.md, "Spark ingestion") on the live stack: a real
// multi-executor Spark standalone cluster (deploy/connect/spark/compose.yml) loads the PostgreSQL
// fixture into Bronze with direct writes and fenced commits. Needs the dev app, the seeded
// connect_acceptance fixture and the Spark cluster; skips otherwise.
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { sha256 } from "@/lib/ingest";
import { APP, SparkHarness, executors, live, type Conn } from "./connect-spark-harness";

const h = new SparkHarness();
const conn = {} as { spark: Conn; base: Conn };
const table = {
  namespace: "public",
  name: "connect_acceptance",
  primary_key: "id",
  cursor_field: "updated_at",
};
const ids = (lines: { data: Record<string, unknown> }[]) =>
  lines.map((l) => l.data["id"] as number);

describe.skipIf(!live)(
  "Spark ingestion (S1): PostgreSQL full refresh with direct writes",
  { timeout: 600000 },
  () => {
    beforeAll(async () => {
      await h.setup();
      await h.secret("postgres", {
        host: process.env["CONNECT_TEST_HOST"] ?? "127.0.0.1",
        port: Number(process.env["CONNECT_TEST_PORT"] ?? 55439),
        dbname: process.env["CONNECT_TEST_DB"] ?? "postgres",
        user: "postgres",
        password: process.env["PGPASSWORD"] ?? "postgres",
        sslmode: "disable",
      });
      // The Spark connection discovers with the PostgreSQL actor.
      conn.spark = await h.connection("spark", "spark-postgres-v1", "postgres");
      conn.base = await h.connection("base", "postgres-v1", "postgres");
      for (const c of [conn.spark, conn.base]) await h.stream(c, table, "full_refresh");
    }, 600000);
    afterAll(() => h.teardown(), 60000);

    it("loads every row on several executors, identical to a single-worker run", async () => {
      const run = await h.enqueue(conn.spark, "sync");
      await h.agent(conn.spark);
      expect(await h.run(run)).toMatchObject({ status: "succeeded", rows_committed: 1200 });
      const { commits, lines, manifests } = await h.landed(run);
      expect(commits.map((c) => c.sequence)).toEqual(commits.map((_, i) => i));
      expect(executors(manifests).size).toBeGreaterThanOrEqual(2);
      expect(ids(lines).length).toBe(1200);
      expect(new Set(ids(lines)).size).toBe(1200);
      // Equivalence: the same data as the PostgreSQL actor lands for the same table.
      const base = await h.enqueue(conn.base, "sync");
      await h.agent(conn.base);
      const baseLines = (await h.landed(base)).lines;
      const byId = (rows: { data: Record<string, unknown>; primary_key?: unknown }[]) =>
        rows
          .map((l) => [
            l.data["id"] as number,
            JSON.stringify(l.data),
            JSON.stringify(l.primary_key),
          ])
          .sort((a, b) => Number(a[0]) - Number(b[0]));
      expect(byId(lines)).toEqual(byId(baseLines));
      const state = await h.state(conn.spark);
      expect(state.completed).toBe(true);
      expect(state.spark).toMatchObject({ run, partitions: 6, consistency: "single" });
    });

    it("resumes a failed attempt by key without loss or duplicates", async () => {
      const run = await h.enqueue(conn.spark, "sync");
      await h.agent(conn.spark, { fail_after_commits: 2 });
      expect((await h.run(run)).status).toBe("running");
      await h.db
        .from("connect_run_streams")
        .update({ next_attempt_at: "2000-01-01T00:00:00Z" })
        .eq("run_id", run);
      await h.agent(conn.spark);
      expect(await h.run(run)).toMatchObject({ status: "succeeded", rows_committed: 1200 });
      const landedIds = ids((await h.landed(run)).lines);
      expect(landedIds.length).toBe(1200);
      expect(new Set(landedIds).size).toBe(1200);
      expect((await h.state(conn.spark)).spark.consistency).toBe("mixed");
    });

    it("refuses parts that are not what the executor reported or not for this stream", async () => {
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
      const put = async (part: string, text: string) => {
        const u = (await call({ action: "upload_url", ...ids, part })).body;
        const res = await fetch(u.url, {
          method: "PUT",
          headers: { "content-type": "application/x-ndjson", "x-upsert": "false" },
          body: text,
        });
        expect(res.ok).toBe(true);
        return u;
      };
      const commit = (part: string, text: string, sha: string) =>
        call({
          action: "commit_file",
          ...ids,
          sequence: 0,
          part,
          rows: text.split("\n").filter(Boolean).length,
          bytes: new TextEncoder().encode(text).length,
          sha256: sha,
          state: { type: "STATE", state: { version: 1, completed: false } },
        });
      const meta = await put("0-0", "");
      const line = (stream: string) =>
        JSON.stringify({
          protocol: "meridian-connect/1",
          connection_id: meta.connection_id,
          run_id: claim.run_id,
          stream_id: stream,
          schema: claim.streams[0].schema,
          extracted_at: meta.extracted_at,
          cursor: "x",
          primary_key: 1,
          operation: "upsert",
          source_namespace: "public",
          source_stream: "connect_acceptance",
          data: { id: 1 },
        }) + "\n";
      // A line for another stream.
      const foreign = line(conn.base.stream);
      await put("0-1", foreign);
      expect((await commit("0-1", foreign, await sha256(foreign))).status).toBe(409);
      // Correct content, wrong digest.
      const good = line(conn.spark.stream);
      const stored = await put("0-2", good);
      expect((await commit("0-2", good, "0".repeat(64))).status).toBe(409);
      // An upload URL never overwrites: no new URL is signed for a stored part, and the
      // original URL cannot replace it.
      expect((await call({ action: "upload_url", ...ids, part: "0-2" })).body.url).toBeUndefined();
      const overwrite = await fetch(stored.url, {
        method: "PUT",
        headers: { "content-type": "application/x-ndjson", "x-upsert": "false" },
        body: line(conn.spark.stream) + line(conn.spark.stream),
      });
      expect(overwrite.ok).toBe(false);
      // The genuine part commits; after that no upload URL is issued for it again.
      expect((await commit("0-2", good, await sha256(good))).status).toBe(200);
      expect((await call({ action: "upload_url", ...ids, part: "0-2" })).status).toBe(409);
      // Another attempt's token cannot address this attempt's parts.
      expect(
        (await call({ action: "upload_url", ...ids, token: crypto.randomUUID(), part: "0-3" }))
          .status,
      ).toBe(409);
      await h.db
        .from("connect_runs")
        .update({ status: "cancelled", lease_token: null, lease_until: null })
        .eq("id", claim.run_id);
    });
  },
);
