// @vitest-environment node
import { createClient } from "@supabase/supabase-js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
const url = process.env["SUPABASE_URL"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"],
  key = process.env["SUPABASE_PUBLISHABLE_KEY"];
const enabled = !!url && !!service && !!key && process.env["RUN_TENANT_E2E"] !== "0";
const options = { auth: { persistSession: false, autoRefreshToken: false } };
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const users: { id: string; org: string; client: any }[] = [];
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let admin: any;
const name = `customers_${crypto.randomUUID().replaceAll("-", "")}`;
const tables: string[] = [];
async function write(index: number, value: number, tableId?: string) {
  return users[index]!.client.rpc("write_native_table", {
    _name: name,
    _rows: [{ value }],
    _columns: [],
    _operation: "upload",
    ...(tableId ? { _table_id: tableId } : {}),
  });
}
describe.skipIf(!enabled)("native table workspace identity", () => {
  beforeAll(async () => {
    admin = createClient(url!, service!, options);
    for (let i = 0; i < 2; i++) {
      const email = `native-${crypto.randomUUID()}@example.test`,
        password = `Local-A9-${crypto.randomUUID()}`;
      const u = await admin.auth.admin.createUser({ email, password, email_confirm: true });
      if (u.error) throw u.error;
      const client = createClient(url!, key!, options);
      const login = await client.auth.signInWithPassword({ email, password });
      if (login.error) throw login.error;
      const w = await client.rpc("ensure_workspace");
      if (w.error) throw w.error;
      users.push({ id: u.data.user.id, org: w.data.workspaceId, client });
    }
  });
  afterAll(async () => {
    if (!admin) return;
    for (const u of users) {
      await admin.from("table_versions").delete().eq("created_by", u.id);
      await admin.from("user_tables").delete().eq("owner_id", u.id);
      await admin.from("organizations").delete().eq("created_by", u.id);
      await admin.auth.admin.deleteUser(u.id);
    }
  });
  it("stores the same filename independently and assigns versions by table identity", async () => {
    for (let i = 0; i < 2; i++) {
      const saved = await write(i, i + 1);
      expect(saved.error).toBeNull();
      expect(saved.data).toBe(1);
      const result = await users[i]!.client.from("user_tables")
        .select("id,rows")
        .eq("name", name)
        .single();
      expect(result.error).toBeNull();
      expect(result.data.rows).toEqual([{ value: i + 1 }]);
      tables.push(result.data.id);
    }
    expect(tables[0]).not.toBe(tables[1]);
    expect(
      (await users[0]!.client.from("table_versions").select("id").eq("table_id", tables[1])).data,
    ).toEqual([]);
    const foreign = await users[0]!.client
      .from("table_versions")
      .insert({ table_id: tables[1], table_name: name, version: 99, created_by: users[0]!.id });
    expect(foreign.error).toBeTruthy();
  });
  it("serializes concurrent writes and records both versions atomically", async () => {
    const results = await Promise.all([write(0, 10), write(0, 20)]);
    expect(results.every((r) => !r.error)).toBe(true);
    expect(results.map((r) => r.data).sort()).toEqual([2, 3]);
    const versions = await users[0]!.client
      .from("table_versions")
      .select("version,rows")
      .eq("table_id", tables[0])
      .order("version");
    expect(versions.data.map((v: { version: number }) => v.version)).toEqual([1, 2, 3]);
    expect(versions.data[0].rows).toEqual([{ value: 1 }]);
  });
  it("scopes listing and restore identity after a workspace switch", async () => {
    expect(
      (
        await admin
          .from("org_members")
          .insert({ user_id: users[0]!.id, org_id: users[1]!.org, workspace_role: "engineer" })
      ).error,
    ).toBeNull();
    expect(
      (await users[0]!.client.rpc("select_workspace", { _workspace: users[1]!.org })).error,
    ).toBeNull();
    const visible = await users[0]!.client.rpc("secure_tables");
    expect(visible.error).toBeNull();
    expect(
      visible.data
        .filter((t: { name: string }) => t.name === name)
        .map((t: { id: string }) => t.id),
    ).toEqual([tables[1]]);
    expect((await write(0, 30, tables[0])).error).toBeTruthy();
    expect((await write(0, 30)).error).toBeTruthy();
    expect(
      (await users[0]!.client.from("table_versions").select("id").eq("table_id", tables[1])).data,
    ).toEqual([]);
    const bob = await users[1]!.client
      .from("user_tables")
      .select("rows")
      .eq("id", tables[1])
      .single();
    expect(bob.data.rows).toEqual([{ value: 2 }]);
  });
});
