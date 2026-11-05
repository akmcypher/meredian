// @vitest-environment node
import { createClient } from "@supabase/supabase-js";
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn } from "node:child_process";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { sha256 } from "@/lib/ingest";
import { verifyBatchPreview } from "@/lib/connect-inspection";
import { importEngineSigningKey, mintEngineToken } from "@/lib/engine-token.server";
const url = process.env["SUPABASE_URL"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"],
  key = process.env["SUPABASE_PUBLISHABLE_KEY"] ?? process.env["VITE_SUPABASE_PUBLISHABLE_KEY"];
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let db: any, owner: any, peer: any;
let uid = "",
  peerId = "",
  organization = "",
  source = "",
  connection = "",
  stream = "",
  publishedRun = "",
  token = "",
  folder = "";
async function execute() {
  await new Promise<void>((resolve, reject) => {
    const p = spawn(process.env["PYTHON"] ?? "python3", ["deploy/connect/agent.py"], {
      env: {
        ...process.env,
        PYTHONPATH: "deploy/connect",
        MERIDIAN_URL: process.env["APP_URL"] ?? "http://localhost:8080",
        INGEST_TOKEN_FILE: join(folder, "ingest_token"),
        CONNECT_SECRET_DIR: folder,
        CONNECT_ONCE: "1",
      },
      stdio: ["ignore", "ignore", "pipe"],
    });
    p.on("exit", (code) => (code === 0 ? resolve() : reject(new Error("Agent process failed"))));
  });
}
async function enqueue(operation: string) {
  const r = await owner.rpc("connect_enqueue", {
    _connection: connection,
    _request: crypto.randomUUID(),
    _operation: operation,
  });
  if (r.error) throw r.error;
  return r.data;
}
describe.skipIf(!url || !service || !key)(
  "Connect live PostgreSQL to bronze",
  { timeout: 120000 },
  () => {
    beforeAll(async () => {
      if (!url || !service || !key) return;
      db = createClient(url, service, { auth: { persistSession: false } });
      for (let i = 0; i < 2; i++) {
        const email = `connect-${crypto.randomUUID()}@example.test`,
          password = `P-${crypto.randomUUID()}`;
        const u = await db.auth.admin.createUser({ email, password, email_confirm: true });
        if (u.error) throw u.error;
        const c = createClient(url, key, { auth: { persistSession: false } });
        await c.auth.signInWithPassword({ email, password });
        if (i === 0) {
          uid = u.data.user.id;
          owner = c;
        } else {
          peerId = u.data.user.id;
          peer = c;
        }
      }
      const tenant = await db
        .from("organizations")
        .insert({ name: `bronze_primary_${crypto.randomUUID()}`, created_by: uid })
        .select("id")
        .single();
      if (tenant.error) throw tenant.error;
      organization = tenant.data.id;
      const membership = await db
        .from("org_members")
        .insert({ org_id: organization, user_id: uid });
      if (membership.error) throw membership.error;
      token = `mi_${crypto.randomUUID().replaceAll("-", "")}${crypto.randomUUID().replaceAll("-", "")}`;
      const s = await db
        .from("ingest_sources")
        .insert({
          name: "Connect live evidence",
          organization_id: organization,
          kind: "postgres",
          owner_id: uid,
          target_table: "connect_bronze",
          config: {},
          token_hash: await sha256(token),
        })
        .select("id")
        .single();
      if (s.error) throw s.error;
      source = s.data.id;
      const c = await db
        .from("connect_connections")
        .insert({
          source_id: source,
          owner_id: uid,
          name: "PostgreSQL contract evidence",
          organization_id: organization,
          credential_ref: "postgres",
        })
        .select("id")
        .single();
      if (c.error) throw c.error;
      connection = c.data.id;
      folder = await mkdtemp(join(tmpdir(), "connect-live-"));
      await writeFile(join(folder, "ingest_token"), token, { mode: 0o600 });
      await writeFile(
        join(folder, "postgres.json"),
        JSON.stringify({
          host: process.env["CONNECT_TEST_HOST"] ?? "127.0.0.1",
          port: Number(process.env["CONNECT_TEST_PORT"] ?? 55439),
          dbname: process.env["CONNECT_TEST_DB"] ?? "postgres",
          user: "postgres",
          sslmode: "disable",
          options: "-c client_encoding=UTF8",
        }),
        { mode: 0o600 },
      );
    }, 60000);
    afterAll(async () => {
      if (db) {
        await db.from("organizations").delete().eq("id", organization);
        await db.auth.admin.deleteUser(uid);
        await db.auth.admin.deleteUser(peerId);
      }
    }, 60000);
    it("checks actual source and discovers its streams through independent process", async () => {
      const check = await enqueue("check");
      await execute();
      const r = await db.from("connect_runs").select("status").eq("id", check).single();
      expect(r.data.status).toBe("succeeded");
      await enqueue("discover");
      await execute();
      const c = await db
        .from("connect_connections")
        .select("discovered_catalog")
        .eq("id", connection)
        .single();
      const catalog = c.data.discovered_catalog;
      const found = catalog.find((s: { name: string }) => s.name === "connect_acceptance");
      expect(found).toBeTruthy();
      const s = await db
        .from("connect_streams")
        .insert({
          connection_id: connection,
          namespace: "public",
          name: "connect_acceptance",
          primary_key: "id",
          cursor_field: "updated_at",
          schema_json: found.json_schema,
        })
        .select("id")
        .single();
      if (s.error) throw s.error;
      stream = s.data.id;
    });
    it("commits 1200 actual rows and incremental checkpoint to real storage", async () => {
      const run = await enqueue("sync");
      await execute();
      const r = await db
        .from("connect_runs")
        .select("status,rows_committed,bytes_committed")
        .eq("id", run)
        .single();
      expect(r.data.status).toBe("succeeded");
      expect(r.data.rows_committed).toBe(1200);
      expect(r.data.bytes_committed).toBeGreaterThan(0);
      const commits = await db
        .from("connect_commits")
        .select("batch_id,manifest_key,sha256,row_count,byte_count")
        .eq("run_id", run);
      expect(commits.data.length).toBe(4);
      const manifest = await owner.storage
        .from("ingest-files")
        .download(commits.data[0].manifest_key);
      expect(manifest.error).toBeNull();
      const parsed = JSON.parse(await manifest.data.text());
      expect(parsed.rows).toBe(500);
      const output = await owner.storage.from("ingest-files").download(parsed.object_key);
      expect(output.error).toBeNull();
      const inspected = await verifyBatchPreview(commits.data[0], parsed, output.data);
      expect(inspected.total).toBe(500);
      expect(inspected.records).toHaveLength(100);
      expect(
        (await peer.storage.from("ingest-files").download(parsed.object_key)).error,
      ).toBeTruthy();
      const state = await db.from("connect_streams").select("state").eq("id", stream).single();
      expect(state.data.state.primary_key).toBe(1200);
      publishedRun = run;
    });
    it("serves all Bronze batches to an authorized engine and denies invalid or revoked access", async () => {
      const raw = process.env["ENGINE_TOKEN_SIGNING_KEY"];
      if (!raw) throw new Error("ENGINE_TOKEN_SIGNING_KEY is required for live Bronze acceptance");
      const signing = await importEngineSigningKey(raw);
      const engine = await db
        .from("execution_engines")
        .insert({
          owner_id: uid,
          name: "Bronze acceptance",
          organization_id: organization,
          kind: "datafusion",
          endpoint: "https://engine.example.test",
          enabled: true,
        })
        .select("id")
        .single();
      expect(engine.error).toBeNull();
      const engineId = engine.data.id;
      const viewerEngine = await db
        .from("execution_engines")
        .insert({
          owner_id: peerId,
          name: "Foreign Bronze acceptance",
          kind: "datafusion",
          endpoint: "https://engine.example.test",
          enabled: true,
        })
        .select("id")
        .single();
      expect(viewerEngine.error).toBeNull();
      const app = process.env["APP_URL"] ?? "http://localhost:8080";
      const request = (
        token: string,
        runId = publishedRun,
        streamId = stream,
        afterSequence = -1,
      ) =>
        fetch(`${app}/api/public/serve-manifest`, {
          method: "POST",
          headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
          body: JSON.stringify({ bronze: { streamId, runId, afterSequence } }),
        });
      try {
        const token = await mintEngineToken(signing, { engineId, userId: uid });
        const response = await request(token);
        expect(response.status).toBe(200);
        expect((await db.auth.admin.updateUserById(uid, { ban_duration: "24h" })).error).toBeNull();
        try {
          expect((await request(token)).status).toBe(401);
          const gold = await fetch(`${app}/api/public/serve-manifest`, {
            method: "POST",
            headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
            body: JSON.stringify({ names: ["absent"] }),
          });
          expect(gold.status).toBe(401);
          const empty = await fetch(`${app}/api/public/serve-manifest`, {
            method: "POST",
            headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
            body: JSON.stringify({ names: [] }),
          });
          expect(empty.status).toBe(401);
          expect(
            (await db.rpc("serve_manifest", { _subject: uid, _names: ["absent"] })).error?.code,
          ).toBe("42501");
          expect(
            (
              await db.rpc("serve_connect_bronze", {
                _subject: uid,
                _engine: engineId,
                _stream: stream,
                _run: publishedRun,
                _after: -1,
              })
            ).error?.code,
          ).toBe("42501");
        } finally {
          expect(
            (await db.auth.admin.updateUserById(uid, { ban_duration: "none" })).error,
          ).toBeNull();
        }
        expect((await request(token)).status).toBe(200);
        const { bronze } = await response.json();
        expect(bronze.parts).toHaveLength(4);
        expect(bronze.nextSequence).toBeNull();
        const ids: number[] = [];
        for (const part of bronze.parts) {
          expect(part).toMatchObject({
            namespace: "connect",
            layer: "bronze",
            format: "ndjson",
            producedBy: { product: "ingest", runId: publishedRun, attempt: null },
          });
          expect(part.datasetId).toBe(bronze.datasetId);
          const download = await fetch(part.location.url);
          expect(download.status).toBe(200);
          const bytes = await download.arrayBuffer();
          expect(bytes.byteLength).toBe(part.location.bytes);
          const digest = await crypto.subtle.digest("SHA-256", bytes);
          expect(Buffer.from(digest).toString("hex")).toBe(part.location.sha256);
          const lines = new TextDecoder().decode(bytes).split("\n").filter(Boolean);
          expect(lines).toHaveLength(part.rows);
          for (const line of lines) ids.push(JSON.parse(line).data.id);
        }
        expect(ids).toHaveLength(1200);
        expect(new Set(ids).size).toBe(1200);
        const readTokenFile = join(folder, "engine_token");
        await writeFile(readTokenFile, token, { mode: 0o600 });
        const external = await new Promise<string>((resolve, reject) => {
          const child = spawn(
            process.env["PYTHON"] ?? "python3",
            [
              "deploy/connect/read_bronze.py",
              "--app",
              app,
              "--token-file",
              readTokenFile,
              "--stream",
              stream,
              "--run",
              publishedRun,
              "--output",
              join(folder, "bronze"),
              "--query-count",
            ],
            {
              env: {
                PATH: process.env["PATH"],
                SystemRoot: process.env["SystemRoot"],
                TEMP: process.env["TEMP"],
                TMP: process.env["TMP"],
                PYTHONUTF8: "1",
              },
              stdio: ["ignore", "pipe", "pipe"],
            },
          );
          let output = "",
            failure = "";
          child.stdout.on("data", (b) => {
            output += b;
          });
          child.stderr.on("data", (b) => {
            failure = (failure + b).slice(-2000);
          });
          const timer = setTimeout(() => {
            child.kill();
            reject(new Error("external_engine_timeout"));
          }, 60000);
          child.on("error", (e) => {
            clearTimeout(timer);
            reject(e);
          });
          child.on("exit", (code) => {
            clearTimeout(timer);
            if (code === 0) resolve(output);
            else reject(new Error(`external_engine_failed: ${failure}`));
          });
        });
        expect(JSON.parse(external)).toMatchObject({ rows: 1200, sqlRows: 1200, parts: 4 });
        expect((await request(token, crypto.randomUUID())).status).toBe(404);
        expect((await request(token, publishedRun, crypto.randomUUID())).status).toBe(404);
        expect((await request(token, publishedRun, stream, 3)).status).toBe(404);
        const tail = await (await request(token, publishedRun, stream, 1)).json();
        expect(tail.bronze.parts.map((p: { sequence: number }) => p.sequence)).toEqual([2, 3]);
        const peerToken = await mintEngineToken(signing, { engineId, userId: peerId });
        expect((await request(peerToken)).status).toBe(403);
        const foreign = await mintEngineToken(signing, {
          engineId: viewerEngine.data.id,
          userId: peerId,
        });
        expect((await request(foreign)).status).toBe(404);
        await db.from("ingest_sources").update({ enabled: false }).eq("id", source);
        expect((await request(token)).status).toBe(404);
        await db.from("ingest_sources").update({ enabled: true }).eq("id", source);
        await db.from("connect_connections").update({ enabled: false }).eq("id", connection);
        expect((await request(token)).status).toBe(404);
        await db.from("connect_connections").update({ enabled: true }).eq("id", connection);
        const org = await db
          .from("organizations")
          .insert({ name: `bronze_scope_${crypto.randomUUID()}`, created_by: uid })
          .select("id")
          .single();
        expect(org.error).toBeNull();
        try {
          expect(
            (await db.from("org_members").insert({ org_id: org.data.id, user_id: uid })).error,
          ).toBeNull();
          await db
            .from("execution_engines")
            .update({ organization_id: org.data.id })
            .eq("id", engineId);
          expect((await request(token)).status).toBe(404);
          await db.from("execution_engines").update({ organization_id: null }).eq("id", engineId);
          expect((await request(token)).status).toBe(404);
          await db
            .from("execution_engines")
            .update({ organization_id: organization })
            .eq("id", engineId);
          expect((await request(token)).status).toBe(200);
          expect(
            (
              await owner
                .from("ingest_sources")
                .update({ organization_id: org.data.id })
                .eq("id", source)
            ).error,
          ).toBeNull();
          const scoped = (await request(token)).status;
          await db.from("org_members").delete().eq("org_id", org.data.id).eq("user_id", uid);
          const revoked = (await request(token)).status;
          expect([scoped, revoked]).toEqual([404, 404]);
        } finally {
          await owner
            .from("ingest_sources")
            .update({ organization_id: organization })
            .eq("id", source);
          await db.from("org_members").delete().eq("org_id", org.data.id);
          await db.from("organizations").delete().eq("id", org.data.id);
        }
        const queued = await enqueue("sync");
        expect((await request(token, queued)).status).toBe(404);
        await owner.rpc("connect_cancel", { _run: queued });
        const expired = await mintEngineToken(signing, {
          engineId,
          userId: uid,
          now: Date.now() - 400000,
        });
        expect((await request(expired)).status).toBe(401);
        const unknown = await mintEngineToken(signing, {
          engineId: crypto.randomUUID(),
          userId: uid,
        });
        expect((await request(unknown)).status).toBe(401);
        expect((await request("invalid")).status).toBe(401);
        await db.from("execution_engines").update({ enabled: false }).eq("id", engineId);
        expect((await request(token)).status).toBe(401);
        expect(
          (
            await peer.rpc("serve_connect_bronze", {
              _subject: uid,
              _engine: engineId,
              _stream: stream,
              _run: publishedRun,
              _after: -1,
            })
          ).error,
        ).toBeTruthy();
      } finally {
        await db.from("execution_engines").delete().in("id", [engineId, viewerEngine.data.id]);
      }
    });
    it("incremental next run produces zero silent duplicates", async () => {
      const run = await enqueue("sync");
      await execute();
      const r = await db
        .from("connect_runs")
        .select("status,rows_committed")
        .eq("id", run)
        .single();
      expect(r.data.status).toBe("succeeded");
      expect(r.data.rows_committed).toBe(0);
    });
    it("fences stale attempt, preserves checkpoint after lease recovery and denies peer", async () => {
      const run = await enqueue("sync");
      const a = await db.rpc("connect_claim", { _connection: connection });
      await db
        .from("connect_runs")
        .update({ lease_until: new Date(Date.now() - 1000).toISOString() })
        .eq("id", run);
      const b = await db.rpc("connect_claim", { _connection: connection });
      expect(b.data.attempt).toBe(2);
      expect(b.data.streams[0].state.primary_key).toBe(1200);
      const old = await db.rpc("connect_heartbeat", { _run: run, _token: a.data.lease_token });
      expect(old.data).toBe(false);
      const other = await peer.from("connect_runs").select("id").eq("id", run);
      expect(other.data).toEqual([]);
      expect(
        (
          await peer.rpc("connect_enqueue", {
            _connection: connection,
            _request: crypto.randomUUID(),
            _operation: "sync",
          })
        ).error,
      ).toBeTruthy();
      await owner.rpc("connect_cancel", { _run: run });
      expect(
        (
          await db.rpc("connect_finish", {
            _run: run,
            _token: b.data.lease_token,
            _ok: true,
            _code: "ok",
          })
        ).error,
      ).toBeTruthy();
    });
  },
);
