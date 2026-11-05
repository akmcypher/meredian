// @vitest-environment node
// Community foundation contracts only: no dependency on other products.
import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import { beforeAll, afterAll, describe, expect, it } from "vitest";
import { sha256 } from "@/lib/ingest";

const url = process.env["SUPABASE_URL"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"],
  key = process.env["SUPABASE_PUBLISHABLE_KEY"];
const enabled = !!url && !!service && !!key && process.env["RUN_TENANT_E2E"] !== "0";
const options = { auth: { persistSession: false, autoRefreshToken: false } };
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Client = SupabaseClient<any>;
const users: { id: string; client: Client }[] = [];
let admin: Client,
  org = "",
  otherOrg = "",
  source = "",
  connection = "",
  generic = "",
  run = "";
const tag = `community_${crypto.randomUUID().slice(0, 8)}`;

describe.skipIf(!enabled)("Community tenant foundation", { timeout: 60000 }, () => {
  beforeAll(async () => {
    admin = createClient(url!, service!, options);
    for (let i = 0; i < 4; i++) {
      const email = `${tag}-${i}@example.test`,
        password = `P-${crypto.randomUUID()}`;
      const created = await admin.auth.admin.createUser({ email, password, email_confirm: true });
      if (created.error) throw created.error;
      const client = createClient(url!, key!, options);
      const login = await client.auth.signInWithPassword({ email, password });
      if (login.error) throw login.error;
      users.push({ id: created.data.user.id, client });
    }
    const groups = await admin
      .from("organizations")
      .insert([
        { name: `${tag}_A`, created_by: users[0]!.id },
        { name: `${tag}_B`, created_by: users[2]!.id },
      ])
      .select("id,name");
    if (groups.error) throw groups.error;
    org = groups.data.find((x) => x.name.endsWith("_A"))!.id;
    otherOrg = groups.data.find((x) => x.name.endsWith("_B"))!.id;
    const membership = await admin.from("org_members").insert([
      { org_id: org, user_id: users[0]!.id },
      { org_id: org, user_id: users[1]!.id },
      { org_id: otherOrg, user_id: users[2]!.id },
    ]);
    if (membership.error) throw membership.error;
    const roles = await admin
      .from("user_roles")
      .insert(users.slice(0, 3).map((u) => ({ user_id: u.id, role: "engineer" })));
    if (roles.error) throw roles.error;
    const src = await admin
      .from("ingest_sources")
      .insert({
        owner_id: users[0]!.id,
        organization_id: org,
        kind: "postgres",
        name: tag,
        target_table: tag,
        config: {},
        token_hash: await sha256(`mi_${"a".repeat(64)}${tag}`),
      })
      .select("id")
      .single();
    if (src.error) throw src.error;
    source = src.data.id;
    const conn = await admin
      .from("connect_connections")
      .insert({
        owner_id: users[0]!.id,
        organization_id: org,
        source_id: source,
        name: tag,
        credential_ref: "fixture",
      })
      .select("id")
      .single();
    if (conn.error) throw conn.error;
    connection = conn.data.id;
    const c = await users[0]!.client
      .from("connections")
      .insert({
        created_by: users[0]!.id,
        organization_id: org,
        name: tag,
        kind: "livy",
        config: {},
      })
      .select("id")
      .single();
    if (c.error) throw c.error;
    generic = c.data.id;
    const queued = await users[0]!.client.rpc("connect_enqueue", {
      _connection: connection,
      _request: crypto.randomUUID(),
      _operation: "check",
    });
    if (queued.error) throw queued.error;
    run = queued.data;
  });
  afterAll(async () => {
    // Immutable run/event evidence is retained. Remove the disposable login identities.
    for (const user of users) {
      const removed = await admin.auth.admin.deleteUser(user.id);
      if (removed.error) throw removed.error;
    }
  });

  it("gives a new personal account no implicit roles", async () => {
    const r = await users[3]!.client.from("user_roles").select("role").eq("user_id", users[3]!.id);
    expect(r.error).toBeNull();
    expect(r.data).toEqual([]);
  });
  it("shares connection metadata within an organization and hides it from other tenants and personal accounts", async () => {
    for (const [i, expected] of [
      [0, 1],
      [1, 1],
      [2, 0],
      [3, 0],
    ]) {
      for (const [table, id] of [
        ["connections", generic],
        ["connect_connections", connection],
        ["connect_runs", run],
      ]) {
        const r = await users[i!]!.client.from(table!).select("id").eq("id", id);
        expect(r.error).toBeNull();
        expect(r.data).toHaveLength(expected!);
      }
    }
  });
  it("keeps source configuration owner-only even for colleagues", async () => {
    for (let i = 0; i < users.length; i++) {
      const r = await users[i]!.client.from("ingest_sources").select("id").eq("id", source);
      expect(r.error).toBeNull();
      expect(r.data).toHaveLength(i === 0 ? 1 : 0);
    }
  });
  it("denies role escalation, foreign organization membership and service-only worker claims", async () => {
    const attacker = users[2]!.client;
    expect(
      (await attacker.from("user_roles").insert({ user_id: users[2]!.id, role: "admin" })).error,
    ).toBeTruthy();
    expect(
      (await attacker.from("org_members").insert({ user_id: users[2]!.id, org_id: org })).error,
    ).toBeTruthy();
    expect((await attacker.rpc("connect_claim", { _connection: connection })).error).toBeTruthy();
    expect(
      (await attacker.rpc("bootstrap_platform_admin", { _email: "nobody@example.test" })).error,
    ).toBeTruthy();
  });
  it("denies mutation and RPC cancellation by peers, including same-organization colleagues", async () => {
    for (const i of [1, 2, 3]) {
      const caller = users[i]!.client;
      await caller.from("connections").update({ name: "forged" }).eq("id", generic);
      const cancelled = await caller.rpc("connect_cancel", { _run: run });
      expect(cancelled.error).toBeNull();
      expect(cancelled.data).toBe(false);
      expect(
        (
          await caller.rpc("connect_enqueue", {
            _connection: connection,
            _request: crypto.randomUUID(),
            _operation: "check",
          })
        ).error,
      ).toBeTruthy();
    }
    expect(
      (await admin.from("connections").select("name").eq("id", generic).single()).data?.name,
    ).toBe(tag);
    expect(
      (await admin.from("connect_runs").select("status").eq("id", run).single()).data?.status,
    ).toBe("queued");
  });
  it("rejects owner and organization spoofing", async () => {
    const owner = users[0]!.client;
    expect(
      (await owner.from("connections").update({ organization_id: otherOrg }).eq("id", generic))
        .error,
    ).toBeTruthy();
    expect(
      (await owner.from("connections").update({ created_by: users[2]!.id }).eq("id", generic))
        .error,
    ).toBeTruthy();
    expect(
      (
        await users[2]!.client.from("ingest_sources").insert({
          owner_id: users[0]!.id,
          organization_id: org,
          kind: "rest",
          name: "forged",
          target_table: "forged",
          config: {},
        })
      ).error,
    ).toBeTruthy();
  });
  it("rejects anonymous access", async () => {
    const anonymous = createClient(url!, key!, options);
    const r = await anonymous.from("connect_connections").select("id").eq("id", connection);
    expect(r.error || r.data?.length === 0).toBeTruthy();
    expect(
      (
        await anonymous.rpc("connect_enqueue", {
          _connection: connection,
          _request: crypto.randomUUID(),
          _operation: "check",
        })
      ).error,
    ).toBeTruthy();
  });
  it("protects raw file objects while enforcing teammate masks and row policies", async () => {
    const prefix = `${users[0]!.id}/${tag}_file`,
      dataset = `${tag}_file`;
    const object = `${prefix}/part-0.ndjson`;
    const rows = [
      { id: 1, ssn: "synthetic-1", region: "eu" },
      { id: 2, ssn: "synthetic-2", region: "us" },
    ];
    const body = rows.map((r) => JSON.stringify(r) + "\n").join("");
    const uploaded = await users[0]!.client.storage
      .from("ingest-files")
      .upload(object, new Blob([body]));
    expect(uploaded.error).toBeNull();
    const table = await admin
      .from("file_tables")
      .insert({
        owner_id: users[0]!.id,
        organization_id: org,
        name: dataset,
        manifest_prefix: prefix,
        columns: ["id", "ssn", "region"],
        part_count: 1,
        row_count: 2,
        source_sha256: await sha256(body),
      })
      .select("id")
      .single();
    if (table.error) throw table.error;
    try {
      const mask = await admin.from("access_policies").insert({
        dataset,
        column_name: "ssn",
        classification: "pii",
        masking: "full",
        allowed_roles: ["steward"],
        organization_id: org,
      });
      expect(mask.error).toBeNull();
      const own = await users[0]!.client.rpc("apply_file_table_policies", {
        _table: table.data.id,
        _rows: rows,
      });
      expect(own.error).toBeNull();
      expect(own.data[0].ssn).toBe("synthetic-1");
      const mate = await users[1]!.client.rpc("apply_file_table_policies", {
        _table: table.data.id,
        _rows: rows,
      });
      expect(mate.error).toBeNull();
      expect(mate.data.map((r: { ssn: string }) => r.ssn)).toEqual(["••••", "••••"]);
      for (const i of [1, 2, 3]) {
        expect(
          (await users[i]!.client.storage.from("ingest-files").download(object)).error,
        ).toBeTruthy();
      }
      for (const i of [2, 3])
        expect(
          (
            await users[i]!.client.rpc("apply_file_table_policies", {
              _table: table.data.id,
              _rows: rows,
            })
          ).error,
        ).toBeTruthy();
      const policy = await admin.from("row_policies").insert({
        table_name: dataset,
        role: "engineer",
        column_name: "region",
        op: "=",
        value: "eu",
        filter_sql: "region = 'eu'",
        created_by: users[0]!.id,
        organization_id: org,
      });
      expect(policy.error).toBeNull();
      const filtered = await users[1]!.client.rpc("apply_file_table_policies", {
        _table: table.data.id,
        _rows: rows,
      });
      expect(filtered.error).toBeNull();
      expect(filtered.data.map((r: { id: number }) => r.id)).toEqual([1]);
      expect(
        (
          await users[0]!.client.storage
            .from("ingest-files")
            .upload(object, new Blob(["forged"]), { upsert: true })
        ).error,
      ).toBeTruthy();
    } finally {
      await admin.from("row_policies").delete().eq("table_name", dataset);
      await admin.from("access_policies").delete().eq("dataset", dataset);
      await admin.from("file_tables").delete().eq("id", table.data.id);
      await admin.storage.from("ingest-files").remove([object]);
    }
  });
  it("enforces file policy page bounds even for the owner", async () => {
    const table = await admin
      .from("file_tables")
      .insert({
        owner_id: users[0]!.id,
        name: `${tag}_budget`,
        manifest_prefix: `${users[0]!.id}/${tag}_budget`,
        columns: ["id"],
        part_count: 1,
        row_count: 1,
        source_sha256: "a".repeat(64),
      })
      .select("id")
      .single();
    if (table.error) throw table.error;
    try {
      const within = await users[0]!.client.rpc("apply_file_table_policies", {
        _table: table.data.id,
        _rows: Array.from({ length: 1000 }, (_, id) => ({ id })),
      });
      expect(within.error).toBeNull();
      expect(within.data).toHaveLength(1000);
      expect(
        (
          await users[0]!.client.rpc("apply_file_table_policies", {
            _table: table.data.id,
            _rows: Array.from({ length: 1001 }, (_, id) => ({ id })),
          })
        ).error,
      ).toBeTruthy();
    } finally {
      await admin.from("file_tables").delete().eq("id", table.data.id);
    }
  });
  it("fences reads and execution after organization membership is revoked", async () => {
    const removed = await admin
      .from("org_members")
      .delete()
      .eq("org_id", org)
      .eq("user_id", users[0]!.id);
    expect(removed.error).toBeNull();
    const r = await users[0]!.client.from("connect_connections").select("id").eq("id", connection);
    expect(r.error).toBeNull();
    expect(r.data).toEqual([]);
    expect(
      (
        await users[0]!.client.rpc("connect_enqueue", {
          _connection: connection,
          _request: crypto.randomUUID(),
          _operation: "check",
        })
      ).error,
    ).toBeTruthy();
    const claimed = await admin.rpc("connect_claim", { _connection: connection });
    expect(claimed.error || claimed.data === null).toBeTruthy();
  });
});
