// @vitest-environment node
// Live regression tests for the public surface and the server-side entitlements.
// Needs the app on APP_URL (default http://localhost:8080) and the local Supabase stack;
// scripts/e2e-local.mjs supplies the environment.
import { createClient } from "@supabase/supabase-js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { sha256 } from "@/lib/ingest";

const url = process.env["SUPABASE_URL"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"],
  anonKey = process.env["SUPABASE_PUBLISHABLE_KEY"] ?? process.env["VITE_SUPABASE_PUBLISHABLE_KEY"];
const APP = (process.env["APP_URL"] ?? "http://localhost:8080").replace(/\/+$/, "");
const enabled = !!(url && service && process.env["RUN_TENANT_E2E"] !== "0");

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let db: any;
let uid = "";
const tag = `hard_${crypto.randomUUID().replaceAll("-", "").slice(0, 10)}`;
const newToken = () =>
  `mi_${crypto.randomUUID().replaceAll("-", "")}${crypto.randomUUID().replaceAll("-", "")}`;

async function partitionCall(token: string, body: object) {
  const r = await fetch(`${APP}/api/public/ingest-partition`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-ingest-token": token },
    body: JSON.stringify(body),
  });
  return { status: r.status, body: (await r.json().catch(() => null)) as Record<string, unknown> };
}

async function sourceWithRun() {
  const token = newToken();
  const src = await db
    .from("ingest_sources")
    .insert({
      owner_id: uid,
      name: `${tag} source`,
      kind: "postgres",
      target_table: `${tag}_t${crypto.randomUUID().slice(0, 4)}`,
      config: {},
      token_hash: await sha256(token),
      enabled: true,
    })
    .select("id, target_table")
    .single();
  if (src.error) throw src.error;
  const run = await db.rpc("create_ingest_run_verified", {
    _actor: uid,
    _source: src.data.id,
    _target: src.data.target_table,
    _total: 10,
    _batch: 100,
    _concurrency: 1,
    _plan: { strategy: { kind: "range", column: "id", n: 1 } },
    _partitions: [
      {
        idx: 0,
        label: "p0",
        predicate: { kind: "range", column: "id", lo: 1, hi: 10, last: true },
        estRows: 10,
      },
    ],
    _pk: ["id"],
  });
  if (run.error) throw run.error;
  return { token, sourceId: src.data.id as string, runId: run.data as string };
}

beforeAll(async () => {
  if (!enabled) return;
  db = createClient(url!, service!, { auth: { persistSession: false } });
  const u = await db.auth.admin.createUser({
    email: `${tag}@example.test`,
    password: `P-${crypto.randomUUID()}`,
    email_confirm: true,
  });
  if (u.error) throw u.error;
  uid = u.data.user.id;
});

afterAll(async () => {
  if (!enabled || !uid) return;
  const srcs = (await db.from("ingest_sources").select("id").eq("owner_id", uid)).data ?? [];
  for (const { id } of srcs) {
    const runs = (await db.from("ingest_runs").select("id").eq("source_id", id)).data ?? [];
    for (const r of runs) {
      await db.from("ingest_rows").delete().eq("run_id", r.id);
      await db.from("ingest_partitions").delete().eq("run_id", r.id);
      await db.from("ingest_runs").delete().eq("id", r.id);
    }
    await db.from("ingest_workers").delete().eq("source_id", id);
    await db.from("ingest_sources").delete().eq("id", id);
  }
  await db.auth.admin.deleteUser(uid).catch(() => undefined);
});

describe.skipIf(!enabled)("public worker API hardening (live)", { timeout: 60_000 }, () => {
  it("a disabled source can no longer register, claim or write partitions", async () => {
    const { token, sourceId, runId } = await sourceWithRun();
    const worker = "w1";

    expect((await partitionCall(token, { action: "register", worker })).status).toBe(200);
    const claimed = await partitionCall(token, { action: "claim", worker, run_id: runId });
    expect(claimed.status).toBe(200);
    const lease = (claimed.body["partition"] as { lease_token: string; idx: number } | null)!;
    expect(lease.lease_token).toMatch(/^[0-9a-f-]{36}$/);

    await db.from("ingest_sources").update({ enabled: false }).eq("id", sourceId);

    for (const body of [
      { action: "register", worker },
      { action: "plan", worker },
      { action: "claim", worker, run_id: runId },
      {
        action: "write",
        worker,
        run_id: runId,
        idx: lease.idx,
        lease_token: lease.lease_token,
        rows: [{ id: 1 }],
        checkpoint: {},
        done: true,
      },
    ])
      expect((await partitionCall(token, body)).status, body.action).toBe(401);

    // The database refuses too, for a source disabled while a worker holds a lease.
    const write = await db.rpc("write_ingest_batch_fenced", {
      _run: runId,
      _idx: lease.idx,
      _worker: worker,
      _lease_token: lease.lease_token,
      _rows: [{ id: 1 }],
      _checkpoint: {},
      _done: true,
      _lease_seconds: 120,
    });
    expect(write.error?.message).toContain("run not active");
    const reclaim = await db.rpc("claim_ingest_partition", {
      _run: runId,
      _worker: "w2",
      _lease_seconds: 60,
    });
    expect(reclaim.error).toBeNull();
    expect(reclaim.data).toEqual([]);
    expect((await db.from("ingest_rows").select("pk").eq("run_id", runId)).data).toEqual([]);
  });
});

describe.skipIf(!enabled || !anonKey)("signed-out access (live)", { timeout: 60_000 }, () => {
  it("anon cannot read any public table, even before RLS is consulted", async () => {
    // PostgREST's OpenAPI document (service role) lists every exposed table and view.
    const spec = (await (
      await fetch(`${url}/rest/v1/`, {
        headers: { apikey: service!, Authorization: `Bearer ${service}` },
      })
    ).json()) as { paths: Record<string, unknown> };
    const tables = Object.keys(spec.paths)
      .filter((p) => p !== "/" && !p.startsWith("/rpc/"))
      .map((p) => p.slice(1));
    expect(tables).toEqual(
      expect.arrayContaining([
        "ingest_sources",
        "connect_runs",
        "execution_engines",
        "entitlements",
      ]),
    );
    const anon = createClient(url!, anonKey!, { auth: { persistSession: false } });
    const readable: string[] = [];
    for (const table of tables) {
      const r = await anon.from(table).select("*").limit(1);
      if (!r.error) readable.push(table);
      else expect(r.error.code, table).toBe("42501");
    }
    expect(readable).toEqual([]);
  });
});

describe.skipIf(!enabled || !anonKey)("request limits (live)", { timeout: 60_000 }, () => {
  it("rejects an oversized push body with 413 before parsing it", async () => {
    const { token } = await sourceWithRun();
    const r = await fetch(`${APP}/api/public/ingest`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-ingest-token": token },
      body: JSON.stringify({ rows: [{ blob: "x".repeat(9 * 1024 * 1024) }] }),
    });
    expect(r.status).toBe(413);
  });

  it("counts requests per key and is callable by the service role only", async () => {
    const key = `test:${tag}`;
    const results: boolean[] = [];
    for (let i = 0; i < 4; i++)
      results.push(
        (await db.rpc("consume_rate_limit", { _key: key, _limit: 3, _window_seconds: 60 })).data,
      );
    expect(results).toEqual([true, true, true, false]);
    const anon = createClient(url!, anonKey!, { auth: { persistSession: false } });
    const denied = await anon.rpc("consume_rate_limit", {
      _key: key,
      _limit: 100,
      _window_seconds: 1,
    });
    expect(denied.error).not.toBeNull();
    await db.from("rate_limit_windows").delete().eq("key", key);
  });
});

describe.skipIf(!enabled || !anonKey)("entitlements (live)", { timeout: 60_000 }, () => {
  it("only the service role grants entitlements, and only members can see them", async () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const people: { id: string; c: any }[] = [];
    let org = "";
    try {
      for (const who of ["member", "outsider"]) {
        const email = `${tag}-${who}@example.test`,
          password = `P-${crypto.randomUUID()}`;
        const u = await db.auth.admin.createUser({ email, password, email_confirm: true });
        if (u.error) throw u.error;
        const c = createClient(url!, anonKey!, { auth: { persistSession: false } });
        await c.auth.signInWithPassword({ email, password });
        people.push({ id: u.data.user.id, c });
      }
      const [member, outsider] = people as [(typeof people)[0], (typeof people)[0]];
      org = (
        await db
          .from("organizations")
          .insert({ name: `${tag} org`, created_by: member.id })
          .select("id")
          .single()
      ).data.id;
      await db.from("org_members").insert({ org_id: org, user_id: member.id });

      // A member cannot grant itself a licensed feature, directly or through the RPC.
      const direct = await member.c
        .from("entitlements")
        .insert({ organization_id: org, feature: "example.premium", licence_id: "self" });
      expect(direct.error).not.toBeNull();
      const viaRpc = await member.c.rpc("apply_licence_entitlements", {
        _actor: member.id,
        _licence: "self",
        _org: org,
        _features: ["example.premium"],
        _expires: null,
      });
      expect(viaRpc.error).not.toBeNull();
      expect(
        (await member.c.rpc("has_entitlement", { _org: org, _feature: "example.premium" })).data,
      ).toBe(false);

      const granted = await db.rpc("apply_licence_entitlements", {
        _actor: member.id,
        _licence: "lic-test",
        _org: org,
        _features: ["example.premium"],
        _expires: new Date(Date.now() + 86_400_000).toISOString(),
      });
      expect(granted.error).toBeNull();
      expect(
        (await member.c.rpc("has_entitlement", { _org: org, _feature: "example.premium" })).data,
      ).toBe(true);
      expect(
        (await outsider.c.rpc("has_entitlement", { _org: org, _feature: "example.premium" })).data,
      ).toBe(false);
      expect(
        (await outsider.c.from("entitlements").select("feature").eq("organization_id", org)).data,
      ).toEqual([]);
      expect(
        (await member.c.from("entitlements").select("feature").eq("organization_id", org)).data,
      ).toEqual([{ feature: "example.premium" }]);
      const update = await member.c
        .from("entitlements")
        .update({ expires_at: null })
        .eq("organization_id", org)
        .select("feature");
      expect(update.error ?? update.data?.length === 0).toBeTruthy();
    } finally {
      if (org) {
        await db.from("entitlements").delete().eq("organization_id", org);
        await db.from("org_members").delete().eq("org_id", org);
        await db.from("organizations").delete().eq("id", org);
      }
      for (const p of people) await db.auth.admin.deleteUser(p.id);
    }
  });
});
