// @vitest-environment node
// Run against an already-running migrated Supabase stack; no agent/source database required.
import { createClient } from "@supabase/supabase-js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const url = process.env["SUPABASE_URL"];
const service = process.env["SUPABASE_SERVICE_ROLE_KEY"];
const key = process.env["SUPABASE_PUBLISHABLE_KEY"] ?? process.env["VITE_SUPABASE_PUBLISHABLE_KEY"];
const local = !!url && ["localhost", "127.0.0.1"].includes(new URL(url).hostname);
const db = createClient(url ?? "http://127.0.0.1:54321", service ?? "missing", {
  auth: { persistSession: false },
});
const owner = createClient(url ?? "http://127.0.0.1:54321", key ?? "missing", {
  auth: { persistSession: false },
});
let uid = "";
let workspace = "";
const ids: string[] = [];
async function fixture(
  options: {
    enabled?: boolean;
    interval_minutes?: number;
    next_run_at?: string;
    sourceEnabled?: boolean;
    streams?: boolean;
  } = {},
) {
  const source = await db
    .from("ingest_sources")
    .insert({
      owner_id: uid,
      organization_id: workspace,
      name: "scheduler-test",
      kind: "postgres",
      target_table: `scheduler_${crypto.randomUUID().replaceAll("-", "")}`,
      config: {},
      token_hash: crypto.randomUUID().replaceAll("-", "").repeat(2),
      enabled: options.sourceEnabled ?? true,
    })
    .select("id")
    .single();
  if (source.error) throw source.error;
  const conn = await db
    .from("connect_connections")
    .insert({
      source_id: source.data.id,
      owner_id: uid,
      organization_id: workspace,
      name: "scheduler-test",
      credential_ref: "unused",
      interval_minutes: options.interval_minutes ?? 5,
      enabled: options.enabled ?? true,
      next_run_at: options.next_run_at ?? "2020-01-01T00:00:00Z",
    })
    .select("id")
    .single();
  if (conn.error) throw conn.error;
  ids.push(conn.data.id);
  if (options.streams !== false) {
    const streams = await db.from("connect_streams").insert([
      {
        connection_id: conn.data.id,
        namespace: "public",
        name: "incremental",
        primary_key: "id",
        cursor_field: "id",
        sync_mode: "incremental",
        enabled: true,
        state: { cursor: 7 },
        state_generation: 3,
      },
      {
        connection_id: conn.data.id,
        namespace: "public",
        name: "full",
        primary_key: "id",
        cursor_field: "id",
        sync_mode: "full_refresh",
        enabled: true,
        state: { cursor: 9 },
        state_generation: 0,
      },
      {
        connection_id: conn.data.id,
        namespace: "public",
        name: "paused",
        primary_key: "id",
        cursor_field: "id",
        enabled: false,
        sync_mode: "incremental",
        state: {},
        state_generation: 0,
      },
    ]);
    if (streams.error) throw streams.error;
  }
  return conn.data.id as string;
}
async function tick() {
  const result = await db.rpc("connect_schedule_due", { _limit: 100 });
  if (result.error) throw result.error;
  return result.data;
}
async function runs(connection: string) {
  const result = await db.from("connect_runs").select("*").eq("connection_id", connection);
  if (result.error) throw result.error;
  return result.data;
}

describe.skipIf(!local || !service || !key)("durable Connect scheduling database", () => {
  beforeAll(async () => {
    const email = `scheduler-${crypto.randomUUID()}@example.test`,
      password = `P-${crypto.randomUUID()}`;
    const user = await db.auth.admin.createUser({ email, password, email_confirm: true });
    if (user.error) throw user.error;
    uid = user.data.user.id;
    const login = await owner.auth.signInWithPassword({ email, password });
    if (login.error) throw login.error;
    const context = await owner.rpc("ensure_workspace");
    if (context.error) throw context.error;
    workspace = context.data.workspaceId;
    expect(context.data.status).toBe("active");
  });
  afterAll(async () => {
    // Preserve append-only run/event history; disable fixtures and remove their auth identity.
    if (ids.length) await db.from("connect_connections").update({ enabled: false }).in("id", ids);
    if (uid) await db.auth.admin.deleteUser(uid);
  });
  it("queues without workers, snapshots only enabled streams and coalesces overlapping ticks", async () => {
    const connection = await fixture();
    await Promise.all([tick(), tick(), tick()]);
    const queued = await runs(connection);
    expect(queued).toHaveLength(1);
    expect(queued[0]).toMatchObject({ status: "queued", attempts: 0, lease_token: null });
    const snapshots = await db
      .from("connect_run_streams")
      .select("initial_state,checkpoint,generation")
      .eq("run_id", queued[0]!.id);
    expect(snapshots.error).toBeNull();
    expect(snapshots.data).toEqual(
      expect.arrayContaining([
        { initial_state: { cursor: 7 }, checkpoint: { cursor: 7 }, generation: 3 },
        { initial_state: {}, checkpoint: {}, generation: 0 },
      ]),
    );
    expect(snapshots.data).toHaveLength(2);
    const claim = await db.rpc("connect_claim_task", {
      _connection: connection,
      _worker: "scheduler-test",
    });
    expect(claim.error).toBeNull();
    expect(claim.data).toMatchObject({ run_id: queued[0]!.id, task: true });
    await db
      .from("connect_connections")
      .update({ next_run_at: "2020-01-01T00:00:00Z" })
      .eq("id", connection);
    await tick();
    expect(await runs(connection)).toHaveLength(1);
  });
  it("skips disabled, manual, future, empty and disabled-source connections", async () => {
    const skipped = await Promise.all([
      fixture({ enabled: false }),
      fixture({ interval_minutes: 0 }),
      fixture({ next_run_at: "2099-01-01T00:00:00Z" }),
      fixture({ streams: false }),
      fixture({ sourceEnabled: false }),
    ]);
    await tick();
    for (const connection of skipped) expect(await runs(connection)).toHaveLength(0);
  });
  it("does not create due runs on either agent claim path", async () => {
    const connection = await fixture();
    for (const name of ["connect_claim", "connect_claim_task"]) {
      const result = await db.rpc(name, { _connection: connection });
      expect(result.error).toBeNull();
      expect(result.data).toBeNull();
    }
    expect(await runs(connection)).toHaveLength(0);
    await tick();
    expect(await runs(connection)).toHaveLength(1);
  });
  it("serializes a concurrent manual request and scheduler tick into one active run", async () => {
    const connection = await fixture();
    const [, manual] = await Promise.all([
      tick(),
      owner.rpc("connect_enqueue", {
        _connection: connection,
        _request: crypto.randomUUID(),
        _operation: "sync",
      }),
    ]);
    // If the scheduler wins, the existing manual enqueue reports the unique active-run
    // constraint. If the manual request wins, the scheduler observes and skips its run.
    if (manual.error) expect(manual.error.code).toBe("23505");
    const active = await runs(connection);
    expect(active).toHaveLength(1);
    expect(active[0]).toMatchObject({ status: "queued", operation: "sync" });
  });
  it("denies authenticated users the global scheduler", async () => {
    const result = await owner.rpc("connect_schedule_due", { _limit: 1 });
    expect(result.error).not.toBeNull();
  });
  it("does not schedule a source whose workspace differs from its connection", async () => {
    const connection = await fixture();
    const { data: c } = await db
      .from("connect_connections")
      .select("source_id")
      .eq("id", connection)
      .single();
    const update = await db
      .from("ingest_sources")
      .update({ organization_id: null })
      .eq("id", c!.source_id);
    if (update.error) throw update.error;
    await tick();
    expect(await runs(connection)).toHaveLength(0);
  });
  it("does not schedule when the owner lost workspace membership", async () => {
    const connection = await fixture();
    const removed = await db
      .from("org_members")
      .delete()
      .eq("org_id", workspace)
      .eq("user_id", uid);
    if (removed.error) throw removed.error;
    try {
      await tick();
      expect(await runs(connection)).toHaveLength(0);
    } finally {
      const restored = await db
        .from("org_members")
        .insert({ org_id: workspace, user_id: uid, workspace_role: "owner" });
      expect(restored.error).toBeNull();
    }
  });
  it("does not schedule an inactive workspace", async () => {
    const connection = await fixture();
    const inactive = await db
      .from("organizations")
      .update({ status: "suspended" })
      .eq("id", workspace);
    if (inactive.error) throw inactive.error;
    try {
      await tick();
      expect(await runs(connection)).toHaveLength(0);
    } finally {
      const restored = await db
        .from("organizations")
        .update({ status: "active" })
        .eq("id", workspace);
      expect(restored.error).toBeNull();
    }
  });
  it("does not schedule a banned owner", async () => {
    const connection = await fixture();
    const banned = await db.auth.admin.updateUserById(uid, { ban_duration: "24h" });
    if (banned.error) throw banned.error;
    try {
      await tick();
      expect(await runs(connection)).toHaveLength(0);
    } finally {
      const restored = await db.auth.admin.updateUserById(uid, { ban_duration: "none" });
      expect(restored.error).toBeNull();
    }
  });
});
