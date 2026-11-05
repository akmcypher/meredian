// @vitest-environment node
import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import { Client as PgClient } from "pg";
import { beforeAll, afterAll, describe, it, expect } from "vitest";
const url = process.env["SUPABASE_URL"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"],
  key = process.env["SUPABASE_PUBLISHABLE_KEY"];
const enabled =
  !!url &&
  !!service &&
  !!key &&
  process.env["RUN_TENANT_E2E"] !== "0" &&
  process.env["AUTH_TEST_DB_ISOLATED"] === "1";
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Client = SupabaseClient<any>;
const options = { auth: { persistSession: false, autoRefreshToken: false } };
let db: Client;
const ids: string[] = [],
  objects: string[] = [];
async function user(confirmed = true) {
  const email = `activation-security-${crypto.randomUUID()}@example.test`,
    password = `Local-A9-${crypto.randomUUID()}`;
  const r = await db.auth.admin.createUser({ email, password, email_confirm: confirmed });
  if (r.error) throw r.error;
  ids.push(r.data.user.id);
  return { id: r.data.user.id, email, password };
}
async function sql() {
  const host = process.env["CONNECT_TEST_HOST"] ?? "127.0.0.1";
  if (!["127.0.0.1", "localhost", "::1"].includes(host))
    throw Error("Isolated local database required");
  const c = new PgClient({
    host,
    port: Number(process.env["CONNECT_TEST_PORT"]),
    database: "postgres",
    user: "postgres",
    password: process.env["PGPASSWORD"],
  });
  await c.connect();
  return c;
}
describe.skipIf(!enabled)(
  "self-activation active-account security (isolated database required)",
  () => {
    beforeAll(() => {
      db = createClient(url!, service!, options);
    });
    afterAll(async () => {
      if (!db) return;
      if (objects.length) await db.storage.from("ingest-files").remove(objects);
      for (const id of ids) {
        await db.from("ingest_tables").delete().eq("owner_id", id);
        await db.from("ingest_runs").delete().eq("owner_id", id);
        await db.from("organizations").delete().eq("created_by", id);
        await db.from("org_members").delete().eq("user_id", id);
        await db.auth.admin.deleteUser(id);
      }
    });
    it("does not trust verified/admin metadata or JWT claims for an unverified account", async () => {
      const u = await user(false);
      expect((await db.from("org_members").select("org_id").eq("user_id", u.id)).data).toEqual([]);
      const c = await sql();
      try {
        await c.query("BEGIN");
        await c.query("select set_config('request.jwt.claims',$1,true)", [
          JSON.stringify({
            sub: u.id,
            role: "authenticated",
            email_verified: true,
            user_metadata: { role: "admin" },
          }),
        ]);
        await c.query("SET LOCAL ROLE authenticated");
        expect((await c.query("select public.is_active_user() as active")).rows[0].active).toBe(
          false,
        );
        await expect(c.query("select public.ensure_workspace()")).rejects.toThrow();
      } finally {
        await c.query("ROLLBACK");
        await c.end();
      }
    });
    it("rolls back failed verification provisioning and safely retries", async () => {
      const u = await user(false),
        tag = "activation_fault_" + crypto.randomUUID().replaceAll("-", "");
      const c = await sql();
      try {
        await c.query(
          `CREATE FUNCTION public.${tag}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.created_by='${u.id}'::uuid THEN RAISE EXCEPTION 'injected_provisioning_failure'; END IF; RETURN NEW; END $$`,
        );
        await c.query(
          `CREATE TRIGGER ${tag} BEFORE INSERT ON public.organizations FOR EACH ROW EXECUTE FUNCTION public.${tag}()`,
        );
        expect(
          (await db.auth.admin.updateUserById(u.id, { email_confirm: true })).error,
        ).toBeTruthy();
        expect((await db.from("organizations").select("id").eq("created_by", u.id)).data).toEqual(
          [],
        );
        expect((await db.auth.admin.getUserById(u.id)).data.user?.email_confirmed_at).toBeFalsy();
      } finally {
        await c.query(`DROP TRIGGER IF EXISTS ${tag} ON public.organizations`);
        await c.query(`DROP FUNCTION IF EXISTS public.${tag}()`);
        await c.end();
      }
      expect((await db.auth.admin.updateUserById(u.id, { email_confirm: true })).error).toBeNull();
      expect((await db.from("org_members").select("org_id").eq("user_id", u.id)).data).toHaveLength(
        1,
      );
      expect((await db.from("user_roles").select("role").eq("user_id", u.id)).data).toEqual([]);
    });
    it("denies another workspace access to uploaded objects and tenant rows", async () => {
      const a = await user(),
        b = await user(),
        alice = createClient(url!, key!, options),
        bob = createClient(url!, key!, options);
      await alice.auth.signInWithPassword({ email: a.email, password: a.password });
      await bob.auth.signInWithPassword({ email: b.email, password: b.password });
      const w = await alice.rpc("ensure_workspace");
      const object = a.id + "/sources/" + crypto.randomUUID() + "/owned.json";
      objects.push(object);
      expect(
        (
          await alice.storage
            .from("ingest-files")
            .upload(object, new Blob(['{"id":1}'], { type: "application/json" }))
        ).error,
      ).toBeNull();
      expect((await bob.storage.from("ingest-files").download(object)).error).toBeTruthy();
      expect(
        (
          await alice.from("user_tables").insert({
            name: "activation_" + crypto.randomUUID().replaceAll("-", ""),
            owner_id: a.id,
            organization_id: w.data.workspaceId,
            columns: [],
            rows: [{ id: 1 }],
            row_count: 1,
          })
        ).error,
      ).toBeNull();
      expect((await bob.from("user_tables").select("id").eq("owner_id", a.id)).data).toEqual([]);
    });
    it("blocks security-definer RPCs for an already issued JWT after deactivation", async () => {
      const u = await user(),
        client = createClient(url!, key!, options);
      const login = await client.auth.signInWithPassword({ email: u.email, password: u.password });
      if (login.error) throw login.error;
      const token = login.data.session!.access_token;
      const name = "banned_" + crypto.randomUUID().replaceAll("-", "");
      const run = await db
        .from("ingest_runs")
        .insert({
          owner_id: u.id,
          source_kind: "demo_orders",
          target_table: name,
          status: "published",
          written_rows: 1,
        })
        .select("id")
        .single();
      if (run.error) throw run.error;
      expect(
        (
          await db
            .from("ingest_rows")
            .insert({ run_id: run.data.id, pk: "1", partition_idx: 0, data: { id: 1 } })
        ).error,
      ).toBeNull();
      expect(
        (
          await db
            .from("ingest_tables")
            .insert({ owner_id: u.id, name, current_run_id: run.data.id, row_count: 1 })
        ).error,
      ).toBeNull();
      const request = () =>
        fetch(url + "/rest/v1/rpc/read_ingest_table", {
          method: "POST",
          headers: {
            apikey: key!,
            authorization: "Bearer " + token,
            "content-type": "application/json",
          },
          body: JSON.stringify({ _name: name }),
        });
      expect((await request()).status).toBe(200);
      expect((await db.auth.admin.updateUserById(u.id, { ban_duration: "24h" })).error).toBeNull();
      const denied = await request();
      expect([401, 403]).toContain(denied.status);
    });
  },
);
