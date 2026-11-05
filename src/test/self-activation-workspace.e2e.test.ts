// @vitest-environment node
import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import { beforeAll, afterAll, describe, it, expect } from "vitest";
const url = process.env["SUPABASE_URL"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"],
  key = process.env["SUPABASE_PUBLISHABLE_KEY"];
const enabled = !!url && !!service && !!key && process.env["RUN_TENANT_E2E"] !== "0";
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Client = SupabaseClient<any>;
const options = { auth: { persistSession: false, autoRefreshToken: false } };
let db: Client;
const users: { id: string; client: Client }[] = [];
async function workspace(client: Client) {
  const r = await client.rpc("ensure_workspace");
  if (r.error) throw r.error;
  return r.data;
}
// The upgrade fixture (scripts/seed-legacy-auth.mjs, before migrating past 0076) sets AUTH_LEGACY_*.
const legacy = !!process.env["AUTH_LEGACY_ADMIN_ID"];

describe.skipIf(!enabled)("self-activation workspace boundary", () => {
  beforeAll(async () => {
    db = createClient(url!, service!, options);
    for (let i = 0; i < 2; i++) {
      const email = `self-workspace-${crypto.randomUUID()}@example.test`,
        password = `Local-A9-${crypto.randomUUID()}`;
      const u = await db.auth.admin.createUser({
        email,
        password,
        email_confirm: true,
        user_metadata: { full_name: "New developer", role: "admin", workspace_role: "owner" },
      });
      if (u.error) throw u.error;
      const client = createClient(url!, key!, options);
      const login = await client.auth.signInWithPassword({ email, password });
      if (login.error) throw login.error;
      users.push({ id: u.data.user.id, client });
    }
  });
  afterAll(async () => {
    if (!db) return;
    for (const u of users) {
      await db.from("organizations").delete().eq("created_by", u.id);
      await db.from("org_members").delete().eq("user_id", u.id);
      await db.auth.admin.deleteUser(u.id);
    }
  });
  it("creates exactly one workspace with scoped owner membership and no global role", async () => {
    const w = await workspace(users[0]!.client);
    expect(w.workspaceRole).toBe("owner");
    expect(w.onboardingStep).toBe(0);
    expect(w.onboardingCompleted).toBe(false);
    expect(
      (await db.from("org_members").select("org_id,workspace_role").eq("user_id", users[0]!.id))
        .data,
    ).toEqual([{ org_id: w.workspaceId, workspace_role: "owner" }]);
    expect((await db.from("user_roles").select("role").eq("user_id", users[0]!.id)).data).toEqual(
      [],
    );
  });
  it("allows independent users with identical display names to verify and own separate workspaces", async () => {
    const a = await workspace(users[0]!.client);
    const b = await workspace(users[1]!.client);
    expect(a.workspaceId).not.toBe(b.workspaceId);
    expect(a.name).toBe(b.name);
    expect(b.workspaceRole).toBe("owner");
  });
  it.skipIf(!legacy)(
    "preserves private legacy audiences and does not attach a previous owner's reused-name history",
    async () => {
      const table = await db
        .from("user_tables")
        .select("organization_id,rows")
        .eq("id", process.env["AUTH_LEGACY_PRIVATE_TABLE_ID"]!)
        .single();
      expect(table.error).toBeNull();
      if (!table.data) throw new Error("Missing legacy private table fixture");
      expect(table.data.organization_id).not.toBe(process.env["AUTH_LEGACY_ORG_ID"]);
      expect(table.data.rows).toEqual([{ secret_fixture: 1 }]);
      const members = await db
        .from("org_members")
        .select("user_id")
        .eq("org_id", table.data.organization_id);
      expect(members.data).toEqual([{ user_id: process.env["AUTH_LEGACY_ADMIN_ID"] }]);
      const history = await db
        .from("table_versions")
        .select("table_id,rows")
        .eq("id", process.env["AUTH_LEGACY_OLD_VERSION_ID"]!)
        .single();
      expect(history.error).toBeNull();
      if (!history.data) throw new Error("Missing legacy historical version fixture");
      expect(history.data.table_id).toBeNull();
      expect(history.data.rows).toEqual([{ former_owner_fixture: 1 }]);
      const peer = createClient(url!, key!, options);
      expect(
        (
          await peer.auth.signInWithPassword({
            email: process.env["AUTH_LEGACY_PEER_EMAIL"]!,
            password: process.env["AUTH_LEGACY_PEER_PASSWORD"]!,
          })
        ).error,
      ).toBeNull();
      const visible = await peer.rpc("secure_tables");
      expect(visible.error).toBeNull();
      expect(
        visible.data.some(
          (t: { id: string }) => t.id === process.env["AUTH_LEGACY_PRIVATE_TABLE_ID"],
        ),
      ).toBe(false);
      expect(
        (await peer.rpc("select_workspace", { _workspace: table.data.organization_id })).error,
      ).toBeTruthy();
      await peer.auth.signOut();
    },
  );
  it("serializes concurrent provisioning and callbacks without duplicates", async () => {
    const results = await Promise.all(
      Array.from({ length: 12 }, () => workspace(users[0]!.client)),
    );
    expect(new Set(results.map((r) => r.workspaceId)).size).toBe(1);
    expect(
      (await db.from("organizations").select("id").eq("created_by", users[0]!.id)).data,
    ).toHaveLength(1);
  });
  it("isolates two workspaces from direct reads, updates and deletes", async () => {
    const a = await workspace(users[0]!.client),
      b = await workspace(users[1]!.client);
    expect(a.workspaceId).not.toBe(b.workspaceId);
    const client = users[0]!.client;
    expect((await client.from("organizations").select("id").eq("id", b.workspaceId)).data).toEqual(
      [],
    );
    expect(
      (
        await client
          .from("organizations")
          .update({ name: "takeover" })
          .eq("id", b.workspaceId)
          .select("id")
      ).data,
    ).toEqual([]);
    expect(
      (await client.from("organizations").delete().eq("id", b.workspaceId).select("id")).data,
    ).toEqual([]);
    expect((await db.from("organizations").select("id").eq("id", b.workspaceId)).data).toHaveLength(
      1,
    );
  });
  it("rejects joining another workspace and direct workspace/global role escalation", async () => {
    const b = await workspace(users[1]!.client),
      a = await workspace(users[0]!.client);
    const client = users[0]!.client;
    expect(
      (
        await client
          .from("org_members")
          .insert({ org_id: b.workspaceId, user_id: users[0]!.id, workspace_role: "owner" })
      ).error,
    ).toBeTruthy();
    expect(
      (
        await client
          .from("org_members")
          .update({ workspace_role: "admin" })
          .eq("org_id", a.workspaceId)
          .eq("user_id", users[0]!.id)
          .select("user_id")
      ).data,
    ).toEqual([]);
    expect(
      (await client.from("user_roles").insert({ user_id: users[0]!.id, role: "admin" })).error,
    ).toBeTruthy();
    expect(
      (await client.rpc("bootstrap_platform_admin", { _email: "nobody@example.test" })).error,
    ).toBeTruthy();
  });
  it("rejects profile context/progress spoofing and foreign workspace selection", async () => {
    const a = await workspace(users[0]!.client),
      b = await workspace(users[1]!.client);
    expect(
      (
        await users[0]!.client
          .from("profiles")
          .update({ active_organization_id: b.workspaceId, onboarding_step: 3 })
          .eq("id", users[0]!.id)
      ).error,
    ).toBeTruthy();
    expect(
      (await users[0]!.client.rpc("select_workspace", { _workspace: b.workspaceId })).error,
    ).toBeTruthy();
    expect((await workspace(users[0]!.client)).workspaceId).toBe(a.workspaceId);
  });
  it("validates progress transitions and persists completion safely on retry", async () => {
    const client = users[0]!.client;
    expect((await client.rpc("update_onboarding", { _step: 3 })).error).toBeTruthy();
    expect((await client.rpc("update_onboarding", { _step: 1, _name: " " })).error).toBeTruthy();
    expect((await workspace(client)).onboardingStep).toBe(0);
    for (const data of [
      { _step: 1, _name: "Developer workspace", _description: "Own verified tenant" },
      { _step: 2, _first_source: "postgres" },
      { _step: 3 },
    ])
      expect((await client.rpc("update_onboarding", data)).error).toBeNull();
    const done = await workspace(client);
    expect(done.onboardingCompleted).toBe(true);
    expect(done.name).toBe("Developer workspace");
    expect((await client.rpc("update_onboarding", { _step: 3 })).error).toBeNull();
    expect((await workspace(client)).workspaceId).toBe(done.workspaceId);
  });
  it("lets an administrator-assigned member finish personal onboarding without changing workspace settings", async () => {
    const owner = await workspace(users[0]!.client);
    const email = `invited-member-${crypto.randomUUID()}@example.test`,
      password = `Local-A9-${crypto.randomUUID()}`;
    const created = await db.auth.admin.createUser({ email, password, email_confirm: false });
    if (created.error) throw created.error;
    const client = createClient(url!, key!, options);
    users.push({ id: created.data.user.id, client });
    expect(
      (
        await db.from("org_members").insert({
          org_id: owner.workspaceId,
          user_id: created.data.user.id,
          workspace_role: "member",
        })
      ).error,
    ).toBeNull();
    expect(
      (await db.auth.admin.updateUserById(created.data.user.id, { email_confirm: true })).error,
    ).toBeNull();
    expect((await client.auth.signInWithPassword({ email, password })).error).toBeNull();
    const member = await workspace(client);
    expect(member.workspaceRole).toBe("member");
    expect(member.workspaceId).toBe(owner.workspaceId);
    expect(
      (await client.rpc("update_onboarding", { _step: 1, _name: "Take over" })).error,
    ).toBeTruthy();
    for (const input of [{ _step: 1 }, { _step: 2, _first_source: "kafka" }, { _step: 3 }])
      expect((await client.rpc("update_onboarding", input)).error).toBeNull();
    expect((await workspace(client)).onboardingCompleted).toBe(true);
    expect((await workspace(users[0]!.client)).name).toBe(owner.name);
    expect(
      (await db.from("organizations").select("id").eq("created_by", created.data.user.id)).data,
    ).toEqual([]);
  });
  it.skipIf(!legacy)(
    "preserves an existing administrator, workspace and populated ingestion/storage data",
    async () => {
      const client = createClient(url!, key!, options);
      const login = await client.auth.signInWithPassword({
        email: process.env["AUTH_LEGACY_ADMIN_EMAIL"]!,
        password: process.env["AUTH_LEGACY_ADMIN_PASSWORD"]!,
      });
      expect(login.error).toBeNull();
      // Private legacy files may become the initial workspace during migration.
      expect(
        (await client.rpc("select_workspace", { _workspace: process.env["AUTH_LEGACY_ORG_ID"]! }))
          .error,
      ).toBeNull();
      const w = await workspace(client);
      expect(w.workspaceId).toBe(process.env["AUTH_LEGACY_ORG_ID"]);
      expect(w.onboardingCompleted).toBe(true);
      expect(
        (
          await client.rpc("has_role", {
            _user_id: process.env["AUTH_LEGACY_ADMIN_ID"],
            _role: "admin",
          })
        ).data,
      ).toBe(true);
      const source = await client
        .from("ingest_sources")
        .select("id,organization_id,enabled")
        .eq("id", process.env["AUTH_LEGACY_SOURCE_ID"]!)
        .single();
      expect(source.error).toBeNull();
      expect(source.data).toMatchObject({ organization_id: w.workspaceId, enabled: false });
      const connection = await client
        .from("connect_connections")
        .select("id,source_id,organization_id")
        .eq("id", process.env["AUTH_LEGACY_CONNECTION_ID"]!)
        .single();
      expect(connection.error).toBeNull();
      expect(connection.data).toMatchObject({
        source_id: source.data!.id,
        organization_id: w.workspaceId,
      });
      const run = await client
        .from("connect_runs")
        .select("connection_id,status")
        .eq("id", process.env["AUTH_LEGACY_RUN_ID"]!)
        .single();
      expect(run.error).toBeNull();
      expect(run.data).toMatchObject({ connection_id: connection.data!.id, status: "succeeded" });
      const table = await client
        .from("user_tables")
        .select("row_count,rows")
        .eq("id", process.env["AUTH_LEGACY_TABLE_ID"]!)
        .single();
      expect(table.error).toBeNull();
      expect(table.data).toMatchObject({ row_count: 1, rows: [{ id: 1 }] });
      const object = await client.storage
        .from("ingest-files")
        .download(process.env["AUTH_LEGACY_OBJECT_KEY"]!);
      expect(object.error).toBeNull();
      expect(JSON.parse(await object.data!.text())).toEqual({ id: 1 });
    },
  );
  it("does not expose provisioning or context to anonymous clients", async () => {
    const anon = createClient(url!, key!, options);
    expect((await anon.rpc("ensure_workspace")).error).toBeTruthy();
    expect(
      (await anon.rpc("provision_workspace_for_user", { _user: users[0]!.id })).error,
    ).toBeTruthy();
  });
});
