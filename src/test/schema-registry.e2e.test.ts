// @vitest-environment node
// Meridian schema registry against the live local database: server-side compatibility, idempotent
// registration, concurrent writers, the service-only write path and tenant isolation.
import { createClient } from "@supabase/supabase-js";
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { latestSchema, registerSchemaVersion } from "@/lib/schema-registry.server";
const url = process.env["SUPABASE_URL"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"],
  key = process.env["SUPABASE_PUBLISHABLE_KEY"] ?? process.env["VITE_SUPABASE_PUBLISHABLE_KEY"];
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let db: any, eng: any, member: any, outsider: any;
const ids: string[] = [];
let org = "";
const subject = `orders-value-${crypto.randomUUID().slice(0, 8)}`;
const v1 = {
  type: "record",
  name: "Order",
  fields: [
    { name: "id", type: "long" },
    { name: "amount", type: "double" },
  ],
};
const v2 = { ...v1, fields: [...v1.fields, { name: "region", type: "string", default: "eu" }] };
const breaking = { ...v1, fields: [...v1.fields, { name: "channel", type: "string" }] };
async function user(role: string | null) {
  const email = `schema-${crypto.randomUUID()}@example.test`,
    password = `P-${crypto.randomUUID()}`;
  const u = await db.auth.admin.createUser({ email, password, email_confirm: true });
  if (u.error) throw u.error;
  ids.push(u.data.user.id);
  if (role) await db.from("user_roles").insert({ user_id: u.data.user.id, role });
  const c = createClient(url!, key!, { auth: { persistSession: false } });
  await c.auth.signInWithPassword({ email, password });
  return { id: u.data.user.id as string, client: c };
}
const register = (who: { id: string; client: unknown }, schema: unknown, organizationId = org) =>
  registerSchemaVersion(who.client as never, db, who.id, { subject, organizationId, schema });
describe.skipIf(!url || !service || !key)("Schema registry (live)", { timeout: 120000 }, () => {
  beforeAll(async () => {
    db = createClient(url!, service!, { auth: { persistSession: false } });
    eng = await user("engineer");
    member = await user("analyst");
    outsider = await user("engineer");
    const o = await db
      .from("organizations")
      .insert({ name: `Schema tenant ${crypto.randomUUID()}`, created_by: eng.id })
      .select("id")
      .single();
    if (o.error) throw o.error;
    org = o.data.id;
    await db.from("org_members").insert([
      { org_id: org, user_id: eng.id },
      { org_id: org, user_id: member.id },
    ]);
  }, 60000);
  afterAll(async () => {
    for (const id of ids) await db.auth.admin.deleteUser(id);
  }, 60000);
  it("registers v1, accepts a BACKWARD-compatible v2 and is idempotent", async () => {
    expect(await register(eng, v1)).toMatchObject({ compatible: true, version: 1, created: true });
    expect(await register(eng, v2)).toMatchObject({ compatible: true, version: 2, created: true });
    // Same schema with keys in another order: same fingerprint, same version.
    const reordered = { fields: v2.fields, name: "Order", type: "record" };
    expect(await register(eng, reordered)).toMatchObject({ version: 2, created: false });
  });
  it("refuses an incompatible version with the issue, server-side", async () => {
    const r = await register(eng, breaking);
    expect(r.compatible).toBe(false);
    if (!r.compatible) expect(JSON.stringify(r.issues)).toContain("channel");
    const versions = await eng.client.from("schema_versions").select("version");
    expect(versions.data.map((v: { version: number }) => v.version).sort()).toEqual([1, 2]);
  });
  it("gives concurrent writers distinct sequential versions", async () => {
    const field = (n: string) => ({ name: n, type: ["null", "string"], default: null });
    const variants = ["a", "b", "c"].map((n) => ({ ...v2, fields: [...v2.fields, field(n)] }));
    const results = await Promise.all(variants.map((s) => register(eng, s)));
    // BACKWARD against whatever landed first: each variant adds an optional field, so all fit.
    const versions = results.map((r) => (r.compatible ? r.version : 0)).sort();
    expect(versions).toEqual([3, 4, 5]);
  });
  it("keeps writes service-only and enforces roles and tenancy", async () => {
    const direct = await eng.client.rpc("schema_register_version", {
      _actor: eng.id,
      _organization: org,
      _name: subject,
      _schema: breaking,
      _fingerprint: "0".repeat(64),
      _expected_latest: 5,
    });
    expect(direct.error).toBeTruthy();
    expect(
      await eng.client
        .from("schema_versions")
        .insert({})
        .then((r: { error: unknown }) => r.error),
    ).toBeTruthy();
    // Same-org analyst can read but not register; an outsider sees nothing and cannot write here.
    expect(
      (await member.client.from("schema_subjects").select("name").eq("name", subject)).data,
    ).toHaveLength(1);
    await expect(register(member, v1)).rejects.toThrow(/engineer, steward or admin/);
    expect(
      (await outsider.client.from("schema_subjects").select("id").eq("name", subject)).data,
    ).toEqual([]);
    await expect(register(outsider, v1)).rejects.toThrow(/Organization membership/);
    // The outsider's personal subject of the same name is separate.
    expect(await register(outsider, breaking, null as never)).toMatchObject({
      version: 1,
      created: true,
    });
  });
  it("pins only subjects the caller can read as a stream reader schema", async () => {
    const subj = await member.client
      .from("schema_subjects")
      .select("id")
      .eq("name", subject)
      .eq("organization_id", org)
      .single();
    const pinned = await latestSchema(member.client, subj.data.id);
    expect(pinned.version).toBe(5);
    await expect(latestSchema(outsider.client, subj.data.id)).rejects.toThrow(/not found/);
  });
});
