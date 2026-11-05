// @vitest-environment node
/**
 * Cross-tenant ingestion, file, connection and role security tests against the real backend API
 * (PostgREST + RLS + SECURITY DEFINER functions).
 * Creates six temporary users in three organizations, attacks with real JWTs, and deletes everything afterwards.
 * Skipped automatically when the backend URL / service key are not available.
 *   org A: alice (engineer+steward, owner of fixtures), erin (engineer)
 *   org B: bob (engineer+steward), carol (analyst)
 *   dave: engineer outside the test organizations (only his own provisioned workspace)
 * Confirmed accounts get a provisioned workspace; the fixtures switch members to the test organizations.
 */
import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const URL = process.env["SUPABASE_URL"];
const SERVICE = process.env["SUPABASE_SERVICE_ROLE_KEY"];
const ANON =
  process.env["SUPABASE_PUBLISHABLE_KEY"] ?? process.env["VITE_SUPABASE_PUBLISHABLE_KEY"];
const enabled = !!(URL && SERVICE && ANON && process.env["RUN_TENANT_E2E"] !== "0");

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type C = SupabaseClient<any>;
const opts = { auth: { persistSession: false, autoRefreshToken: false } };
const tag = `iso${Date.now().toString(36)}`;
const pw = `Pw-${crypto.randomUUID()}`;

let admin: C;
type Name = "alice" | "erin" | "bob" | "carol" | "dave" | "frank";
type Key = "orgA" | "orgB" | "orgC" | "conn" | "run";
const users: Partial<Record<Name, { id: string; c: C }>> = {};
const ids: Partial<Record<Key, string>> = {};

async function mkUser(name: Name, roles: string[]) {
  const email = `${tag}-${name}@example.test`;
  const { data, error } = await admin.auth.admin.createUser({
    email,
    password: pw,
    email_confirm: true,
  });
  if (error) throw error;
  const id = data.user.id;
  await admin.from("user_roles").delete().eq("user_id", id);
  await admin.from("user_roles").insert(roles.map((role) => ({ user_id: id, role })));
  const c = createClient(URL!, ANON!, opts) as C;
  const s = await c.auth.signInWithPassword({ email, password: pw });
  if (s.error) throw s.error;
  users[name] = { id, c };
}
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const one = async (p: PromiseLike<{ data: any; error: unknown }>): Promise<any> => {
  const r = await p;
  if (r.error) throw r.error;
  return r.data;
};
const n = async (c: C, table: string, col: string, val: string) =>
  ((await c.from(table).select(col).eq(col, val)).data ?? []).length;

describe.skipIf(!enabled)(
  "tenant isolation: ingestion and governance (live backend)",
  { timeout: 60_000 },
  () => {
    beforeAll(async () => {
      admin = createClient(URL!, SERVICE!, opts) as C;
      await mkUser("alice", ["engineer", "steward"]);
      await mkUser("erin", ["engineer"]);
      await mkUser("bob", ["engineer", "steward"]);
      await mkUser("carol", ["analyst"]);
      await mkUser("dave", ["engineer"]);
      await mkUser("frank", ["analyst"]);
      const A = users.alice!.id,
        E = users.erin!.id,
        B = users.bob!.id,
        Cr = users.carol!.id;
      const orgs = await one(
        admin
          .from("organizations")
          .insert([
            { name: `${tag} A`, created_by: A },
            { name: `${tag} B`, created_by: B },
            { name: `${tag} C`, created_by: users.frank?.id },
          ])
          .select("id,name"),
      );
      ids.orgA = orgs.find((o: { name: string }) => o.name.endsWith(" A"))!.id;
      ids.orgB = orgs.find((o: { name: string }) => o.name.endsWith(" B"))!.id;
      ids.orgC = orgs.find((o: { name: string }) => o.name.endsWith(" C"))?.id;
      await one(
        admin.from("org_members").insert([
          { org_id: ids.orgA, user_id: A },
          { org_id: ids.orgA, user_id: E },
          { org_id: ids.orgB, user_id: B },
          { org_id: ids.orgB, user_id: Cr },
          { org_id: ids.orgC, user_id: users.frank?.id },
        ]),
      );
      for (const [name, org] of [
        ["alice", ids.orgA],
        ["erin", ids.orgA],
        ["bob", ids.orgB],
        ["carol", ids.orgB],
      ] as const) {
        const selected = await users[name]!.c.rpc("select_workspace", { _workspace: org });
        if (selected.error) throw selected.error;
      }
      ids.conn = (
        await one(
          admin
            .from("connections")
            .insert({
              kind: "livy",
              name: `${tag} conn`,
              config: {},
              created_by: A,
              organization_id: ids.orgA,
            })
            .select("id")
            .single(),
        )
      ).id;
      await one(
        admin.from("user_tables").insert([
          {
            name: `${tag}_a_tbl`,
            columns: ["id", "ssn"],
            rows: [{ id: 1, ssn: "123-45" }],
            row_count: 1,
            owner_id: A,
            organization_id: ids.orgA,
          },
          {
            name: `${tag}_b_tbl`,
            columns: ["id"],
            rows: [{ id: 9 }],
            row_count: 1,
            owner_id: B,
            organization_id: ids.orgB,
          },
        ]),
      );
      await one(
        admin.from("access_policies").insert([
          {
            dataset: `${tag}_a_tbl`,
            column_name: "ssn",
            classification: "pii",
            masking: "full",
            allowed_roles: ["steward"],
          },
          {
            dataset: `${tag}_ing`,
            column_name: "ssn",
            classification: "pii",
            masking: "full",
            allowed_roles: ["steward"],
          },
        ]),
      );
      await one(
        admin.from("change_requests").insert([
          {
            title: `${tag} B change`,
            description: "x",
            language: "sql",
            code: "select 1",
            target: "t",
            status: "pending",
            validation: {},
            submitted_by: B,
          },
          {
            title: `${tag} E change`,
            description: "x",
            language: "sql",
            code: "select 1",
            target: "t",
            status: "pending",
            validation: {},
            submitted_by: E,
          },
        ]),
      );
      await one(
        admin.from("audit_log").insert([
          { actor_id: E, action: `${tag}.erin`, details: {} },
          { actor_id: B, action: `${tag}.bob`, details: {} },
        ]),
      );
      ids.run = (
        await one(
          admin
            .from("ingest_runs")
            .insert({
              owner_id: A,
              source_kind: "demo_orders",
              target_table: `${tag}_ing`,
              plan: {},
              organization_id: ids.orgA,
              expected_rows: 1,
            })
            .select("id")
            .single(),
        )
      ).id;
      await one(
        admin.from("ingest_partitions").insert({
          run_id: ids.run,
          owner_id: A,
          idx: 0,
          label: "p0",
          predicate: {},
          est_rows: 1,
          status: "running",
          worker: "w",
          lease_token: "11111111-1111-4111-8111-111111111111",
          lease_until: new Date(Date.now() + 60_000).toISOString(),
        }),
      );
      await one(
        admin.rpc("write_ingest_batch_fenced", {
          _run: ids.run,
          _idx: 0,
          _worker: "w",
          _lease_token: "11111111-1111-4111-8111-111111111111",
          _rows: [{ id: 1, ssn: "999-99" }],
          _checkpoint: {},
          _done: true,
        }),
      );
      const pub = await users.alice!.c.rpc("publish_ingest_run", { _run: ids.run });
      if (pub.error) throw pub.error;
    }, 60_000);

    afterAll(async () => {
      if (!admin) return;
      const engineIds = (
        (await admin.from("execution_engines").select("id").like("name", `${tag}%`)).data ?? []
      ).map((engine: { id: string }) => engine.id);
      if (engineIds.length) {
        await admin.from("execution_engines").delete().in("id", engineIds);
      }
      const del = (t: string, col: string, v: string | string[]) =>
        Array.isArray(v) ? admin.from(t).delete().in(col, v) : admin.from(t).delete().eq(col, v);
      const runs = (
        (await admin.from("ingest_runs").select("id").like("target_table", `${tag}%`)).data ?? []
      ).map((r: { id: string }) => r.id);
      await admin.from("ingest_tables").delete().like("name", `${tag}%`);
      if (runs.length) {
        await del("ingest_rows", "run_id", runs);
        await del("ingest_partitions", "run_id", runs);
        await del("ingest_runs", "id", runs);
      }
      const sources = (
        (await admin.from("ingest_sources").select("id").like("name", `${tag}%`)).data ?? []
      ).map((s: { id: string }) => s.id);
      if (sources.length)
        await admin.from("ingest_landing_batches").delete().in("source_id", sources);
      await admin.from("ingest_sources").delete().like("name", `${tag}%`);
      await admin.from("access_policies").delete().like("dataset", `${tag}%`);
      await admin.from("user_tables").delete().like("name", `${tag}%`);
      await admin.from("change_requests").delete().like("title", `${tag}%`);
      await admin.from("connections").delete().like("name", `${tag}%`);
      for (const o of [ids.orgA, ids.orgB, ids.orgC].filter(Boolean) as string[]) {
        await del("org_members", "org_id", o);
        await del("organizations", "id", o);
      }
      for (const u of Object.values(users)) await admin.auth.admin.deleteUser(u.id);
    }, 60_000);

    it("isolates native engine registrations and denies client mutation", async () => {
      const alice = users.alice,
        bob = users.bob,
        erin = users.erin;
      if (!alice || !bob || !erin) throw new Error("Missing engine fixtures");
      const payload = {
        owner_id: alice.id,
        organization_id: ids.orgA,
        name: `${tag}_engine`,
        kind: "datafusion",
        endpoint: "https://compute.example.test",
      };
      const engine = await one(
        admin.from("execution_engines").insert(payload).select("id").single(),
      );
      expect(await n(alice.c, "execution_engines", "id", engine.id)).toBe(1);
      expect(await n(erin.c, "execution_engines", "id", engine.id)).toBe(1);
      expect(await n(bob.c, "execution_engines", "id", engine.id)).toBe(0);
      expect((await alice.c.from("execution_engines").insert(payload)).error).not.toBeNull();
      await alice.c
        .from("execution_engines")
        .update({ endpoint: "https://attacker.example.test" })
        .eq("id", engine.id);
      expect(
        (await admin.from("execution_engines").select("endpoint").eq("id", engine.id).single()).data
          ?.endpoint,
      ).toBe(payload.endpoint);
    });

    it("isolates immutable landing metadata and refuses forged or conflicting batch registration", async () => {
      const alice = users.alice;
      const bob = users.bob;
      if (!alice || !bob) throw new Error("Missing fixture");
      const source = await one(
        alice.c
          .from("ingest_sources")
          .insert({
            owner_id: alice.id,
            organization_id: ids.orgA,
            name: `${tag}_landing`,
            kind: "postgres",
            target_table: `${tag}_landing`,
            config: {},
          })
          .select("id")
          .single(),
      );
      const batch = crypto.randomUUID();
      const args = {
        _actor: alice.id,
        _source: source.id,
        _key: `${alice.id}/published/${batch}/landing.ndjson`,
        _sha: "a".repeat(64),
        _rows: 2,
        _mode: "append",
        _batch: batch,
      };
      expect((await alice.c.rpc("register_ingest_landing_batch", args)).error).not.toBeNull();
      const committed = await admin.rpc("register_ingest_landing_batch", args);
      expect(committed.error).toBeNull();
      expect((await admin.rpc("register_ingest_landing_batch", args)).data).toBe(committed.data);
      expect(
        (await admin.rpc("register_ingest_landing_batch", { ...args, _sha: "b".repeat(64) })).error
          ?.message,
      ).toMatch(/conflict/);
      expect(
        (await admin.rpc("register_ingest_landing_batch", { ...args, _actor: bob.id })).error
          ?.message,
      ).toMatch(/authorized/);
      expect(await n(bob.c, "ingest_landing_batches", "source_id", source.id)).toBe(0);
      expect(await n(alice.c, "ingest_landing_batches", "source_id", source.id)).toBe(1);
      await alice.c
        .from("ingest_landing_batches")
        .update({ sha256: "b".repeat(64) })
        .eq("source_id", source.id);
      const unchanged = await alice.c
        .from("ingest_landing_batches")
        .select("sha256")
        .eq("source_id", source.id)
        .single();
      expect(unchanged.data?.sha256).toBe("a".repeat(64));
    });

    it("enforces file-policy page limits before owner bypass", async () => {
      const alice = users.alice;
      if (!alice) throw new Error("Missing fixture");
      const ft = await one(
        admin
          .from("file_tables")
          .insert({
            owner_id: alice.id,
            name: `${tag}_budget`,
            manifest_prefix: `${alice.id}/runs/${tag}_budget`,
            columns: ["id"],
            part_count: 1,
            row_count: 1,
            source_sha256: "a".repeat(64),
          })
          .select("id")
          .single(),
      );
      try {
        expect(
          (
            await alice.c.rpc("apply_file_table_policies", {
              _table: ft.id,
              _rows: Array.from({ length: 1000 }, (_, id) => ({ id })),
            })
          ).error,
        ).toBeNull();
        expect(
          (
            await alice.c.rpc("apply_file_table_policies", {
              _table: ft.id,
              _rows: Array.from({ length: 1001 }, (_, id) => ({ id })),
            })
          ).error?.message,
        ).toMatch(/budget/);
        expect(
          (await alice.c.rpc("apply_file_table_policies", { _table: ft.id, _rows: { id: 1 } }))
            .error?.message,
        ).toMatch(/array/);
      } finally {
        await admin.from("file_tables").delete().eq("id", ft.id);
      }
    });

    it("refuses scope changes at publication and preserves the prior run pointer", async () => {
      const alice = users.alice;
      if (!alice || !ids.orgA) throw new Error("Missing fixture");
      for (const [idx, scopes] of [
        [null, ids.orgA],
        [ids.orgA, null],
        [ids.orgA, ids.orgB],
      ].entries()) {
        const name = `${tag}_scope_${idx}`;
        const make = async (org: string | null | undefined) => {
          const run = await one(
            admin
              .from("ingest_runs")
              .insert({
                owner_id: alice.id,
                source_kind: "demo_orders",
                target_table: name,
                plan: {},
                organization_id: org,
                expected_rows: 1,
              })
              .select("id")
              .single(),
          );
          await one(
            admin.from("ingest_partitions").insert({
              run_id: run.id,
              owner_id: alice.id,
              idx: 0,
              label: "p",
              predicate: {},
              est_rows: 1,
              status: "done",
            }),
          );
          await one(
            admin
              .from("ingest_rows")
              .insert({ run_id: run.id, pk: "1", partition_idx: 0, data: { id: 1 } }),
          );
          return run.id;
        };
        if (scopes[1] === ids.orgB)
          await one(admin.from("org_members").insert({ org_id: ids.orgB, user_id: alice.id }));
        try {
          const first = await make(scopes[0]);
          expect((await alice.c.rpc("publish_ingest_run", { _run: first })).error).toBeNull();
          const second = await make(scopes[1]);
          expect(
            (await alice.c.rpc("publish_ingest_run", { _run: second })).error?.message,
          ).toMatch(/scope conflict/);
          const pointer = await one(
            admin
              .from("ingest_tables")
              .select("current_run_id")
              .eq("owner_id", alice.id)
              .eq("name", name)
              .single(),
          );
          expect(pointer.current_run_id).toBe(first);
          const same = await make(scopes[0]);
          expect((await alice.c.rpc("publish_ingest_run", { _run: same })).error).toBeNull();
        } finally {
          if (scopes[1] === ids.orgB)
            await admin.from("org_members").delete().eq("org_id", ids.orgB).eq("user_id", alice.id);
        }
      }
    });

    it("bounds legacy warehouse reads without silently truncating data", async () => {
      const alice = users.alice;
      if (!alice) throw new Error("Missing fixture");
      const name = `${tag}_oversized`;
      await one(
        admin.from("user_tables").insert({
          owner_id: alice.id,
          organization_id: ids.orgA,
          name,
          columns: ["id"],
          rows: Array.from({ length: 5001 }, (_, id) => ({ id })),
          row_count: 5001,
        }),
      );
      try {
        expect((await alice.c.rpc("secure_tables")).error?.message).toMatch(/read budget/);
      } finally {
        await admin.from("user_tables").delete().eq("name", name);
      }
    });

    it("blocks another organization's reads (connections, ingestion, tables, people)", async () => {
      const b = users.bob!.c;
      expect(await n(b, "connections", "id", ids.conn!)).toBe(0);
      expect(await n(b, "ingest_runs", "id", ids.run!)).toBe(0);
      expect(await n(b, "ingest_rows", "run_id", ids.run!)).toBe(0);
      expect(await n(b, "user_tables", "name", `${tag}_a_tbl`)).toBe(0);
      expect(await n(b, "profiles", "id", users.alice!.id)).toBe(0);
      expect(await n(b, "profiles", "id", users.carol!.id)).toBe(1);
      expect(await n(b, "user_roles", "user_id", users.alice!.id)).toBe(0);
      expect(await n(b, "organizations", "id", ids.orgA!)).toBe(0);
      expect(await n(b, "org_members", "org_id", ids.orgA!)).toBe(0);
      expect(await n(b, "change_requests", "title", `${tag} E change`)).toBe(0);
      const st = (await b.rpc("secure_tables")).data as { name: string }[];
      expect(st.some((t) => t.name === `${tag}_a_tbl`)).toBe(false);
      expect(st.some((t) => t.name === `${tag}_b_tbl`)).toBe(true);
      expect(((await b.rpc("read_ingest_table", { _name: `${tag}_ing` })).data ?? []).length).toBe(
        0,
      );
      expect((await b.rpc("has_role", { _user_id: users.alice!.id, _role: "steward" })).data).toBe(
        false,
      );
    });

    it("blocks writes, ID spoofing and privilege escalation across organizations", async () => {
      const b = users.bob!.c,
        B = users.bob!.id,
        A = users.alice!.id;
      const upd = async (t: string, patch: object, id: string) =>
        ((await b.from(t).update(patch).eq("id", id).select("id")).data ?? []).length;
      expect(await upd("connections", { name: "pwned" }, ids.conn!)).toBe(0);
      const denied = [
        await b.from("org_members").insert({ org_id: ids.orgA, user_id: B }),
        await b.from("user_roles").insert({ user_id: B, role: "admin" }),
        await b.from("ingest_runs").insert({
          owner_id: B,
          source_kind: "demo_orders",
          target_table: `${tag}_x`,
          plan: {},
          organization_id: ids.orgA,
        }),
        await b.rpc("publish_ingest_run", { _run: ids.run }),
        await b.rpc("write_ingest_batch_fenced", {
          _run: ids.run,
          _idx: 0,
          _worker: "w",
          _lease_token: "11111111-1111-4111-8111-111111111111",
          _rows: [],
          _checkpoint: {},
          _done: true,
        }),
        await b.rpc("claim_ingest_partition", { _run: ids.run, _worker: "w" }),
        await b.rpc("heartbeat_ingest_partition_fenced", {
          _run: ids.run,
          _idx: 0,
          _worker: "w",
          _lease_token: "11111111-1111-4111-8111-111111111111",
        }),
      ];
      denied.forEach((r, i) => expect(r.error, `attack #${i} succeeded`).not.toBeNull());
    });

    it("rejects the legacy global engineer update and ownership/org reassignment", async () => {
      const alice = users.alice,
        bob = users.bob,
        frank = users.frank;
      if (!alice || !bob || !frank) throw new Error("Missing fixtures");
      for (const c of [bob.c, frank.c]) {
        const r = await c
          .from("user_tables")
          .update({ rows: [{ id: 77 }], row_count: 1 })
          .eq("name", `${tag}_a_tbl`)
          .select("id");
        expect(r.data ?? []).toHaveLength(0);
        expect(
          (await c.from("user_tables").delete().eq("name", `${tag}_a_tbl`).select("id")).data ?? [],
        ).toHaveLength(0);
      }
      expect(
        (
          await alice.c
            .from("user_tables")
            .update({ organization_id: ids.orgB })
            .eq("name", `${tag}_a_tbl`)
        ).error,
      ).not.toBeNull();
      expect(
        (await alice.c.from("user_tables").update({ owner_id: bob.id }).eq("name", `${tag}_a_tbl`))
          .error,
      ).not.toBeNull();
      const saved = await one(
        admin.from("user_tables").select("rows").eq("name", `${tag}_a_tbl`).single(),
      );
      expect(saved.rows).toEqual([{ id: 1, ssn: "123-45" }]);
    });

    it("rejects connection reassignment outside membership and creator spoofing", async () => {
      const alice = users.alice,
        bob = users.bob;
      if (!alice || !bob || !ids.conn) throw new Error("Missing fixtures");
      expect(
        (await alice.c.from("connections").update({ organization_id: ids.orgB }).eq("id", ids.conn))
          .error,
      ).not.toBeNull();
      expect(
        (await alice.c.from("connections").update({ created_by: bob.id }).eq("id", ids.conn)).error,
      ).not.toBeNull();
      const saved = await one(
        admin.from("connections").select("created_by,organization_id").eq("id", ids.conn).single(),
      );
      expect(saved).toEqual({ created_by: alice.id, organization_id: ids.orgA });
      expect(
        (
          await alice.c
            .from("connections")
            .update({ name: `${tag} updated conn` })
            .eq("id", ids.conn)
            .select("id")
        ).data,
      ).toHaveLength(1);
    });

    it("fences old tokens even when the newer worker reuses the same name", async () => {
      const alice = users.alice;
      if (!alice) throw new Error("Missing fixture");
      const run = await one(
        admin
          .from("ingest_runs")
          .insert({
            owner_id: alice.id,
            source_kind: "demo_orders",
            target_table: `${tag}_fenced`,
            plan: {},
          })
          .select("id")
          .single(),
      );
      await one(
        admin.from("ingest_partitions").insert({
          run_id: run.id,
          owner_id: alice.id,
          idx: 0,
          label: "p",
          predicate: {},
          est_rows: 1,
        }),
      );
      const claim = async () =>
        (
          await one(
            admin.rpc("claim_specific_partition", { _run: run.id, _idx: 0, _worker: "same-name" }),
          )
        )[0];
      const old = await claim();
      expect(await claim()).toBeUndefined();
      await one(
        admin
          .from("ingest_partitions")
          .update({ lease_until: new Date(Date.now() - 1000).toISOString() })
          .eq("run_id", run.id),
      );
      const fresh = await claim();
      expect(fresh.lease_token).not.toBe(old.lease_token);
      expect(fresh.claim_version).toBe(old.claim_version + 1);
      const args = { _run: run.id, _idx: 0, _worker: "same-name", _lease_token: old.lease_token };
      expect((await admin.rpc("heartbeat_ingest_partition_fenced", args)).data).toBe(false);
      expect((await admin.rpc("yield_ingest_partition_fenced", args)).data).toBe(false);
      expect(
        (await admin.rpc("fail_ingest_partition_fenced", { ...args, _error: "stale failure" }))
          .data,
      ).toBe(false);
      expect(
        (
          await admin.rpc("write_ingest_batch_fenced", {
            ...args,
            _rows: [{ id: 999 }],
            _checkpoint: { last_id: 999 },
            _done: true,
          })
        ).error?.message,
      ).toMatch(/lease lost/);
      expect(
        (
          await admin.rpc("write_ingest_batch", {
            _run: run.id,
            _idx: 0,
            _worker: "same-name",
            _rows: [],
            _checkpoint: {},
            _done: true,
          })
        ).error,
      ).not.toBeNull();
      expect(
        (await alice.c.from("ingest_partitions").update({ status: "done" }).eq("run_id", run.id))
          .error,
      ).not.toBeNull();
      expect(
        (
          await admin.rpc("write_ingest_batch_fenced", {
            ...args,
            _lease_token: fresh.lease_token,
            _rows: [{ id: 1 }],
            _checkpoint: { last_id: 1 },
            _done: true,
          })
        ).error,
      ).toBeNull();
      const saved = await one(
        admin
          .from("ingest_partitions")
          .select("status,checkpoint,rows_written")
          .eq("run_id", run.id)
          .single(),
      );
      expect(saved).toEqual({ status: "done", checkpoint: { last_id: 1 }, rows_written: 1 });
    });

    it("hands off bounded requests with a new token while preserving committed checkpoints", async () => {
      const alice = users.alice;
      if (!alice) throw new Error("Missing fixture");
      const run = await one(
        admin
          .from("ingest_runs")
          .insert({
            owner_id: alice.id,
            source_kind: "demo_orders",
            target_table: `${tag}_yield`,
            plan: {},
          })
          .select("id")
          .single(),
      );
      await one(
        admin.from("ingest_partitions").insert({
          run_id: run.id,
          owner_id: alice.id,
          idx: 0,
          label: "p",
          predicate: {},
          est_rows: 2,
        }),
      );
      const args = { _run: run.id, _idx: 0, _worker: "bounded" };
      const old = (await one(admin.rpc("claim_specific_partition", args)))[0];
      await one(
        admin.rpc("write_ingest_batch_fenced", {
          ...args,
          _lease_token: old.lease_token,
          _rows: [{ id: 1 }],
          _checkpoint: { last_id: 1 },
          _done: false,
        }),
      );
      expect(
        (
          await admin.rpc("yield_ingest_partition_fenced", {
            ...args,
            _lease_token: old.lease_token,
          })
        ).data,
      ).toBe(true);
      const fresh = (await one(admin.rpc("claim_specific_partition", args)))[0];
      expect(fresh.checkpoint).toEqual({ last_id: 1 });
      expect(fresh.lease_token).not.toBe(old.lease_token);
      expect(fresh.claim_version).toBe(old.claim_version + 1);
      expect(
        (
          await admin.rpc("yield_ingest_partition_fenced", {
            ...args,
            _lease_token: old.lease_token,
          })
        ).data,
      ).toBe(false);
      expect(
        (
          await alice.c.rpc("yield_ingest_partition_fenced", {
            ...args,
            _lease_token: fresh.lease_token,
          })
        ).error,
      ).not.toBeNull();
      await one(
        admin.rpc("write_ingest_batch_fenced", {
          ...args,
          _lease_token: fresh.lease_token,
          _rows: [{ id: 2 }],
          _checkpoint: { last_id: 2 },
          _done: true,
        }),
      );
      expect(await n(admin, "ingest_rows", "run_id", run.id)).toBe(2);
    });

    it("shares within an organization but keeps writes owner-only and masks PII for non-stewards", async () => {
      const e = users.erin!.c;
      expect(await n(e, "connections", "id", ids.conn!)).toBe(1);
      expect(
        (
          (await e.from("connections").update({ name: "x" }).eq("id", ids.conn!).select("id"))
            .data ?? []
        ).length,
      ).toBe(0);
      const st = (
        (await e.rpc("secure_tables")).data as { name: string; rows: { ssn: string }[] }[]
      ).find((t) => t.name === `${tag}_a_tbl`);
      expect(st?.rows[0]?.ssn).toBe("••••");
      const ing = (await e.rpc("read_ingest_table", { _name: `${tag}_ing` })).data as {
        data: { ssn: string };
      }[];
      expect(ing).toHaveLength(1);
      expect(ing[0]!.data.ssn).toBe("••••");
      expect(await n(e, "ingest_rows", "run_id", ids.run!)).toBe(0);
    });

    it("scopes steward powers to the steward's own organization", async () => {
      const a = users.alice!.c;
      const st = (await a.rpc("secure_tables")).data as { name: string }[];
      expect(st.some((t) => t.name === `${tag}_b_tbl`)).toBe(false);
      expect(await n(a, "user_tables", "name", `${tag}_b_tbl`)).toBe(0);
      expect(await n(a, "change_requests", "title", `${tag} B change`)).toBe(0);
      const approveB = await a
        .from("change_requests")
        .update({ status: "approved", reviewed_by: users.alice!.id })
        .eq("title", `${tag} B change`)
        .select("id");
      expect(approveB.data ?? []).toHaveLength(0);
      const approveE = await a
        .from("change_requests")
        .update({ status: "approved", reviewed_by: users.alice!.id })
        .eq("title", `${tag} E change`)
        .select("id");
      expect(approveE.data ?? []).toHaveLength(1);
      expect(await n(a, "audit_log", "action", `${tag}.erin`)).toBe(1);
      expect(await n(a, "audit_log", "action", `${tag}.bob`)).toBe(0);
      expect(await n(a, "profiles", "id", users.dave!.id)).toBe(0);
    });

    it("isolates users with no organization and limits analysts", async () => {
      const d = users.dave!.c;
      expect(
        ((await d.rpc("secure_tables")).data as { name: string }[]).filter((t) =>
          t.name.startsWith(tag),
        ),
      ).toHaveLength(0);
      const r = await users
        .carol!.c.from("connections")
        .insert({ kind: "livy", name: `${tag} analyst`, config: {}, created_by: users.carol!.id });
      expect(r.error).not.toBeNull();
    });

    it("refuses anonymous callers", async () => {
      const anon = createClient(URL!, ANON!, opts) as C;
      for (const fn of ["secure_tables", "read_ingest_table", "has_role"] as const) {
        const args =
          fn === "read_ingest_table"
            ? { _name: `${tag}_ing` }
            : fn === "has_role"
              ? { _user_id: users.alice!.id, _role: "steward" }
              : {};
        const r = await anon.rpc(fn, args);
        expect(r.error, `${fn} callable anonymously`).not.toBeNull();
      }
      expect(
        ((await anon.from("connections").select("id").eq("id", ids.conn!)).data ?? []).length,
      ).toBe(0);
    });

    it("keeps composite keys collision-free and refuses to revive expired leases", async () => {
      const A = users.alice!.id;
      const run = (
        await one(
          admin
            .from("ingest_runs")
            .insert({
              owner_id: A,
              source_kind: "postgres",
              target_table: `${tag}_ck`,
              plan: {},
              pk_columns: ["a", "b"],
            })
            .select("id")
            .single(),
        )
      ).id;
      await one(
        admin.from("ingest_partitions").insert({
          run_id: run,
          owner_id: A,
          idx: 0,
          label: "p",
          predicate: {},
          est_rows: 2,
          status: "running",
          worker: "w1",
          lease_token: "11111111-1111-4111-8111-111111111111",
          lease_until: new Date(Date.now() + 60_000).toISOString(),
        }),
      );
      await one(
        admin.rpc("write_ingest_batch_fenced", {
          _run: run,
          _idx: 0,
          _worker: "w1",
          _lease_token: "11111111-1111-4111-8111-111111111111",
          _rows: [
            { a: "x|y", b: "z" },
            { a: "x", b: "y|z" },
            { a: "∅", b: "1" },
          ],
          _checkpoint: {},
          _done: false,
        }),
      );
      expect(
        ((await admin.from("ingest_rows").select("pk").eq("run_id", run)).data ?? []).length,
      ).toBe(3);
      expect(
        (
          await admin.rpc("write_ingest_batch_fenced", {
            _run: run,
            _idx: 0,
            _worker: "w1",
            _lease_token: "11111111-1111-4111-8111-111111111111",
            _rows: [{ a: null, b: "z" }],
            _checkpoint: {},
            _done: false,
          })
        ).error,
      ).not.toBeNull();
      await admin
        .from("ingest_partitions")
        .update({ lease_until: new Date(Date.now() - 1000).toISOString() })
        .eq("run_id", run);
      expect(
        (
          await admin.rpc("heartbeat_ingest_partition_fenced", {
            _run: run,
            _idx: 0,
            _worker: "w1",
            _lease_token: "11111111-1111-4111-8111-111111111111",
          })
        ).data,
      ).toBe(false);
      expect((await users.alice!.c.rpc("publish_ingest_run", { _run: run })).error).not.toBeNull();
    });

    it("scopes governance rules to their organization", async () => {
      const b = users.bob!.c,
        B = users.bob!.id;
      const own = await b.from("access_policies").insert({
        dataset: `${tag}_a_tbl`,
        column_name: "id",
        classification: "x",
        masking: "full",
        allowed_roles: ["admin"],
        organization_id: ids.orgB,
      });
      expect(own.error).toBeNull();
      const st = (
        (await users.erin!.c.rpc("secure_tables")).data as {
          name: string;
          rows: { id: unknown }[];
        }[]
      ).find((t) => t.name === `${tag}_a_tbl`);
      expect(st?.rows[0]?.id).toBe(1);
      expect(
        (
          await b.from("access_policies").insert({
            dataset: `${tag}_a_tbl`,
            column_name: "id",
            classification: "x",
            masking: "full",
            allowed_roles: ["admin"],
          })
        ).error,
      ).not.toBeNull();
      expect(
        (
          await b.from("access_policies").insert({
            dataset: `${tag}_a_tbl`,
            column_name: "id",
            classification: "x",
            masking: "full",
            allowed_roles: ["admin"],
            organization_id: ids.orgA,
          })
        ).error,
      ).not.toBeNull();
      expect(
        (
          await b.from("row_policies").insert({
            table_name: `${tag}_a_tbl`,
            role: "engineer",
            column_name: "id",
            op: "=",
            value: "0",
            filter_sql: "",
            created_by: B,
            organization_id: ids.orgA,
          })
        ).error,
      ).not.toBeNull();
    });

    it("lets each organization mask a column name another organization already masks", async () => {
      const policy = {
        dataset: `${tag}_shared_ds`,
        column_name: "email",
        classification: "pii",
        masking: "full",
        allowed_roles: ["admin" as const],
      };
      expect(
        (await admin.from("access_policies").insert({ ...policy, organization_id: ids.orgA }))
          .error,
      ).toBeNull();
      const bob = users.bob!.c;
      expect(
        (await bob.from("access_policies").insert({ ...policy, organization_id: ids.orgB })).error,
      ).toBeNull();
      const visible = await bob
        .from("access_policies")
        .select("organization_id")
        .eq("dataset", policy.dataset);
      expect(visible.data?.map((p) => p.organization_id)).toEqual([ids.orgB]);
      const duplicate = await bob
        .from("access_policies")
        .insert({ ...policy, organization_id: ids.orgB });
      expect((duplicate.error as { code?: string } | null)?.code).toBe("23505");
      await admin.from("access_policies").delete().eq("dataset", policy.dataset);
    });

    it("rejects alert addresses that add recipients or inject mail headers", async () => {
      const bob = users.bob!;
      const save = (alert_email: string) =>
        bob.c.from("alert_settings").upsert({ user_id: bob.id, alert_email });
      for (const hostile of ["me@x.example, a@y.example", "me@x.example\r\nBcc: list@evil.test"]) {
        expect(((await save(hostile)).error as { code?: string } | null)?.code).toBe("23514");
      }
      expect((await save("bob.alerts@example.test")).error).toBeNull();
      await admin.from("alert_settings").delete().eq("user_id", bob.id);
    });

    it("gives new accounts no roles and keeps admin bootstrap service-only", async () => {
      const { data, error } = await admin.auth.admin.createUser({
        email: `${tag}-norole@example.test`,
        password: pw,
        email_confirm: true,
      });
      expect(error).toBeNull();
      const id = data.user!.id;
      try {
        const roles = await admin.from("user_roles").select("role").eq("user_id", id);
        expect(roles.data).toEqual([]);
        const attempt = await users.bob!.c.rpc("bootstrap_platform_admin", {
          _email: `${tag}-norole@example.test`,
        });
        expect((attempt.error as { code?: string } | null)?.code).toBe("42501");
        expect((await admin.from("user_roles").select("role").eq("user_id", id)).data).toEqual([]);
      } finally {
        await admin.auth.admin.deleteUser(id);
      }
    });

    it("gives a partition to one live worker at a time and refuses legacy key encodings", async () => {
      const A = users.alice!.id;
      const run = (
        await one(
          admin
            .from("ingest_runs")
            .insert({
              owner_id: A,
              source_kind: "demo_orders",
              target_table: `${tag}_lease`,
              plan: {},
            })
            .select("id")
            .single(),
        )
      ).id;
      await one(
        admin
          .from("ingest_partitions")
          .insert({ run_id: run, owner_id: A, idx: 0, label: "p", predicate: {}, est_rows: 1 }),
      );
      const [w1, w2] = await Promise.all([
        admin.rpc("claim_specific_partition", { _run: run, _idx: 0, _worker: "w1" }),
        admin.rpc("claim_specific_partition", { _run: run, _idx: 0, _worker: "w2" }),
      ]);
      expect([(w1.data ?? []).length, (w2.data ?? []).length].sort()).toEqual([0, 1]);
      expect(
        (await users.alice!.c.rpc("claim_specific_partition", { _run: run, _idx: 0, _worker: "x" }))
          .error,
      ).not.toBeNull();
      const legacy = (
        await one(
          admin
            .from("ingest_runs")
            .insert({
              owner_id: A,
              source_kind: "postgres",
              target_table: `${tag}_legacy`,
              plan: {},
              pk_columns: ["a", "b"],
              pk_encoding: "legacy_pipe",
            })
            .select("id")
            .single(),
        )
      ).id;
      await one(
        admin.from("ingest_partitions").insert({
          run_id: legacy,
          owner_id: A,
          idx: 0,
          label: "p",
          predicate: {},
          est_rows: 1,
          status: "running",
          worker: "w",
          lease_token: "11111111-1111-4111-8111-111111111111",
          lease_until: new Date(Date.now() + 60_000).toISOString(),
        }),
      );
      const w = await admin.rpc("write_ingest_batch_fenced", {
        _run: legacy,
        _idx: 0,
        _worker: "w",
        _lease_token: "11111111-1111-4111-8111-111111111111",
        _rows: [{ a: 1, b: 2 }],
        _checkpoint: {},
        _done: true,
      });
      expect(w.error?.message).toMatch(/key encoding/);
    });

    it("serves file tables to teammates only with row rules and masks applied", async () => {
      const alice = users.alice;
      if (!alice) throw new Error("Missing fixture");
      const A = alice.id,
        pre = `${A}/${tag}_run`;
      const st = (c: C) => c.storage.from("ingest-files");
      expect(
        (
          await st(users.alice!.c).upload(`${pre}/part-0.ndjson`, new Blob(['{"id":1}\n']), {
            upsert: true,
          })
        ).error,
      ).toBeNull();
      const canRead = async (c: C, k: string) => !(await st(c).download(k)).error;
      const ft = await one(
        admin
          .from("file_tables")
          .insert({
            owner_id: A,
            organization_id: ids.orgA,
            name: `${tag}_ft`,
            manifest_prefix: pre,
            columns: ["id", "ssn", "region"],
            part_count: 1,
            row_count: 2,
            source_sha256: "a".repeat(64),
          })
          .select("id")
          .single(),
      );
      await one(
        admin.from("access_policies").insert({
          dataset: `${tag}_ft`,
          column_name: "ssn",
          classification: "pii",
          masking: "full",
          allowed_roles: ["steward"],
          organization_id: ids.orgA,
        }),
      );
      const rows = [
        { id: 1, ssn: "111-22", region: "eu" },
        { id: 2, ssn: "333-44", region: "us" },
      ];
      try {
        expect(await canRead(users.alice!.c, `${pre}/part-0.ndjson`)).toBe(true);
        expect(
          (
            await st(alice.c).upload(`${pre}/part-0.ndjson`, new Blob(['{"id":99}\n']), {
              upsert: true,
            })
          ).error,
        ).not.toBeNull();
        await st(alice.c).remove([`${pre}/part-0.ndjson`]);
        expect(await canRead(alice.c, `${pre}/part-0.ndjson`)).toBe(true);
        const unchanged = await st(alice.c).download(`${pre}/part-0.ndjson`);
        expect(await unchanged.data?.text()).toBe('{"id":1}\n');
        for (const u of ["erin", "bob", "dave", "frank"] as const)
          expect(await canRead(users[u]!.c, `${pre}/part-0.ndjson`)).toBe(false);
        const own = (
          await users.alice!.c.rpc("apply_file_table_policies", { _table: ft.id, _rows: rows })
        ).data as typeof rows;
        expect(own[0]!.ssn).toBe("111-22");
        const mate = (
          await users.erin!.c.rpc("apply_file_table_policies", { _table: ft.id, _rows: rows })
        ).data as typeof rows;
        expect(mate.map((r) => r.ssn)).toEqual(["••••", "••••"]);
        expect(
          (await users.bob!.c.rpc("apply_file_table_policies", { _table: ft.id, _rows: rows }))
            .error,
        ).not.toBeNull();
        await one(
          admin.from("row_policies").insert({
            table_name: `${tag}_ft`,
            role: "engineer",
            column_name: "region",
            op: "=",
            value: "eu",
            filter_sql: "region = 'eu'",
            created_by: A,
            organization_id: ids.orgA,
          }),
        );
        const filtered = (
          await users.erin!.c.rpc("apply_file_table_policies", { _table: ft.id, _rows: rows })
        ).data as typeof rows;
        expect(filtered.map((r) => r.id)).toEqual([1]);
        const erin = users.erin;
        if (!erin) throw new Error("Missing fixture");
        await one(admin.from("org_members").delete().eq("org_id", ids.orgA).eq("user_id", erin.id));
        expect(await n(erin.c, "file_tables", "id", ft.id)).toBe(0);
        expect(
          (await erin.c.rpc("apply_file_table_policies", { _table: ft.id, _rows: rows })).error,
        ).not.toBeNull();
        await one(admin.from("org_members").insert({ org_id: ids.orgA, user_id: erin.id }));
      } finally {
        await admin.from("row_policies").delete().eq("table_name", `${tag}_ft`);
        await admin.from("access_policies").delete().eq("dataset", `${tag}_ft`);
        await admin.from("file_tables").delete().eq("id", ft.id);
        await admin.storage.from("ingest-files").remove([`${pre}/part-0.ndjson`]);
      }
    });

    it("creates source runs transactionally, serializes races and rolls back bad partitions", async () => {
      const alice = users.alice,
        erin = users.erin;
      if (!alice || !erin) throw new Error("Missing fixture");
      const source = await one(
        admin
          .from("ingest_sources")
          .insert({
            owner_id: alice.id,
            organization_id: ids.orgA,
            name: `${tag} atomic source`,
            kind: "postgres",
            target_table: `${tag}_atomic`,
            config: {},
          })
          .select("id")
          .single(),
      );
      const args = {
        _actor: alice.id,
        _source: source.id,
        _target: `${tag}_atomic`,
        _total: 10,
        _batch: 100,
        _concurrency: 2,
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
      };
      expect((await erin.c.rpc("create_ingest_run_verified", args)).error).not.toBeNull();
      expect(
        (await admin.rpc("create_ingest_run_verified", { ...args, _actor: erin.id })).error,
      ).not.toBeNull();
      const bad = await admin.rpc("create_ingest_run_verified", {
        ...args,
        _partitions: [{ ...args._partitions[0], label: null }],
      });
      expect(bad.error).not.toBeNull();
      expect(await n(admin, "ingest_runs", "source_id", source.id)).toBe(0);
      const race = await Promise.all([
        admin.rpc("create_ingest_run_verified", args),
        admin.rpc("create_ingest_run_verified", args),
      ]);
      expect(race.filter((r) => !r.error)).toHaveLength(1);
      expect(await n(admin, "ingest_runs", "source_id", source.id)).toBe(1);
      const run = race.find((r) => !r.error)?.data;
      expect(await n(admin, "ingest_partitions", "run_id", run)).toBe(1);
      expect(
        (await alice.c.rpc("create_ingest_run_atomic", { ...args, _actor: undefined })).error,
      ).not.toBeNull();
    });

    it("reserves published destinations before registration and refuses direct registry forgery", async () => {
      const alice = users.alice;
      if (!alice) throw new Error("Missing fixture");
      const prefix = `${alice.id}/published/${crypto.randomUUID()}`;
      const bucket = alice.c.storage.from("ingest-files");
      expect(
        (await bucket.upload(`${prefix}/part-00000.ndjson`, new Blob(['{"id":1}\n']))).error,
      ).not.toBeNull();
      expect(
        (
          await alice.c.from("file_tables").insert({
            name: `${tag}_forged`,
            manifest_prefix: prefix,
            columns: ["id"],
            part_count: 1,
            row_count: 1,
            source_sha256: "a".repeat(64),
          })
        ).error,
      ).not.toBeNull();
      const args = {
        _owner: alice.id,
        _org: ids.orgA,
        _name: `${tag}_reserved`,
        _prefix: prefix,
        _columns: ["id"],
        _parts: 1,
        _rows: 1,
        _source_sha: "a".repeat(64),
        _manifest_sha: "b".repeat(64),
      };
      expect((await alice.c.rpc("register_verified_file_table", args)).error).not.toBeNull();
      const id = await one(admin.rpc("register_verified_file_table", args));
      expect(await n(alice.c, "file_tables", "id", id)).toBe(1);
      await admin.from("file_tables").delete().eq("id", id);
    });
  },
);
