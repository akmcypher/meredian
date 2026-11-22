// @vitest-environment node
// Requires an already-running local migrated Supabase stack; never starts Docker.
import { createClient } from "@supabase/supabase-js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
const url = process.env["SUPABASE_URL"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"];
const key = process.env["SUPABASE_PUBLISHABLE_KEY"] ?? process.env["VITE_SUPABASE_PUBLISHABLE_KEY"];
const local = !!url && ["localhost", "127.0.0.1"].includes(new URL(url).hostname);
const db = createClient(url ?? "http://127.0.0.1:54321", service ?? "missing", {
  auth: { persistSession: false },
});
const owner = createClient(url ?? "http://127.0.0.1:54321", key ?? "missing", {
  auth: { persistSession: false },
});
const stranger = createClient(url ?? "http://127.0.0.1:54321", key ?? "missing", {
  auth: { persistSession: false },
});
const users: string[] = [],
  connections: string[] = [];
async function fixture(cdc = true) {
  const source = await db
    .from("ingest_sources")
    .insert({
      owner_id: users[0],
      name: "retirement-test",
      kind: "postgres",
      target_table: `retire_${crypto.randomUUID().replaceAll("-", "")}`,
      config: {},
      token_hash: crypto.randomUUID().replaceAll("-", "").repeat(2),
    })
    .select("id")
    .single();
  if (source.error) throw source.error;
  const c = await db
    .from("connect_connections")
    .insert({
      source_id: source.data.id,
      owner_id: users[0],
      name: "retirement-test",
      credential_ref: "unused",
      source_version: cdc ? "postgres-cdc-v1" : "postgres-v1",
    })
    .select("id")
    .single();
  if (c.error) throw c.error;
  connections.push(c.data.id);
  const s = await db
    .from("connect_streams")
    .insert({
      connection_id: c.data.id,
      namespace: "public",
      name: "orders",
      primary_key: "id",
      cursor_field: "id",
      sync_mode: cdc ? "cdc" : "incremental",
    })
    .select("id")
    .single();
  if (s.error) throw s.error;
  return { connection: c.data.id, stream: s.data.id };
}
describe.skipIf(!local || !service || !key)("stream retirement database", () => {
  beforeAll(async () => {
    for (const client of [owner, stranger]) {
      const email = `retire-${crypto.randomUUID()}@example.test`,
        password = `P-${crypto.randomUUID()}`;
      const u = await db.auth.admin.createUser({ email, password, email_confirm: true });
      if (u.error) throw u.error;
      users.push(u.data.user.id);
      const login = await client.auth.signInWithPassword({ email, password });
      if (login.error) throw login.error;
    }
  });
  afterAll(async () => {
    if (connections.length)
      await db.from("connect_connections").update({ enabled: false }).in("id", connections);
    for (const uid of users) await db.auth.admin.deleteUser(uid);
  });
  it("rejects a foreign owner without disabling or queuing", async () => {
    const f = await fixture();
    expect(
      (await stranger.rpc("connect_retire_stream", { _stream: f.stream })).error,
    ).not.toBeNull();
    expect(
      (await db.from("connect_streams").select("enabled").eq("id", f.stream).single()).data
        ?.enabled,
    ).toBe(true);
  });
  it("disables CDC, fences its generation, queues cleanup and refuses revival", async () => {
    const f = await fixture();
    const result = await owner.rpc("connect_retire_stream", { _stream: f.stream });
    expect(result.error).toBeNull();
    const stream = await db
      .from("connect_streams")
      .select("enabled,state,state_generation")
      .eq("id", f.stream)
      .single();
    expect(stream.data).toMatchObject({
      enabled: false,
      state_generation: 1,
      state: { retirement: { status: "pending" } },
    });
    const claim = await db.rpc("connect_claim_task", {
      _connection: f.connection,
      _worker: "retire-test",
    });
    expect(claim.error).toBeNull();
    expect(claim.data).toMatchObject({
      streams: [{ id: f.stream, state: { retirement: { status: "pending" } } }],
    });
    expect(
      (await db.from("connect_streams").update({ enabled: true }).eq("id", f.stream)).error,
    ).not.toBeNull();
    expect((await owner.rpc("connect_retire_stream", { _stream: f.stream })).error).not.toBeNull();
  });
  it("retires a source without persistent slots immediately and idempotently", async () => {
    const f = await fixture(false);
    const first = await owner.rpc("connect_retire_stream", { _stream: f.stream });
    expect(first.error).toBeNull();
    expect(first.data).toBeNull();
    expect((await owner.rpc("connect_retire_stream", { _stream: f.stream })).error).toBeNull();
    expect(
      (await db.from("connect_streams").select("state").eq("id", f.stream).single()).data?.state,
    ).toMatchObject({ retirement: { status: "done" } });
  });
});
