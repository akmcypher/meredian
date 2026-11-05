// @vitest-environment node
import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import { beforeAll, afterAll, describe, it, expect } from "vitest";
const url = process.env["SUPABASE_URL"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"],
  key = process.env["SUPABASE_PUBLISHABLE_KEY"];
const enabled = !!url && !!service && !!key && process.env["RUN_TENANT_E2E"] !== "0";
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Client = SupabaseClient<any>;
let admin: Client,
  owner: Client,
  peer: Client,
  ownerId = "",
  peerId = "";
const runs: string[] = [];
const names: string[] = [];
async function fixture(done = true) {
  const name = "publish_" + crypto.randomUUID().replaceAll("-", "");
  names.push(name);
  const r = await admin
    .from("ingest_runs")
    .insert({ owner_id: ownerId, source_kind: "demo_orders", target_table: name, expected_rows: 2 })
    .select("id")
    .single();
  if (r.error) throw r.error;
  const id = r.data.id;
  runs.push(id);
  for (const result of [
    await admin.from("ingest_partitions").insert({
      run_id: id,
      owner_id: ownerId,
      idx: 0,
      label: "all",
      predicate: {},
      status: done ? "done" : "pending",
      rows_written: done ? 2 : 0,
    }),
    await admin
      .from("ingest_rows")
      .insert(
        [1, 2].map((n) => ({ run_id: id, pk: String(n), partition_idx: 0, data: { id: n } })),
      ),
  ])
    if (result.error) throw result.error;
  return { id, name };
}
// Regression: Community partition publication must not depend on an optional catalog.
// Value: protects=atomic owner-scoped publication; fails_when=catalog dependency returns;
describe.skipIf(!enabled)("Community partition publication", () => {
  beforeAll(async () => {
    admin = createClient(url!, service!, { auth: { persistSession: false } });
    for (const role of ["owner", "peer"]) {
      const email = `publication-${crypto.randomUUID()}@example.test`,
        password = `P-${crypto.randomUUID()}`;
      const u = await admin.auth.admin.createUser({ email, password, email_confirm: true });
      if (u.error) throw u.error;
      const c = createClient(url!, key!, { auth: { persistSession: false } });
      const login = await c.auth.signInWithPassword({ email, password });
      if (login.error) throw login.error;
      if (role === "owner") {
        owner = c;
        ownerId = u.data.user.id;
      } else {
        peer = c;
        peerId = u.data.user.id;
      }
    }
  });
  afterAll(async () => {
    if (!admin) return;
    await admin.from("ingest_tables").delete().in("name", names);
    await admin.from("ingest_runs").delete().in("id", runs);
    for (const id of [ownerId, peerId]) if (id) await admin.auth.admin.deleteUser(id);
  });
  it("publishes completed rows and exposes them only to the owner", async () => {
    const { id, name } = await fixture();
    const published = await owner.rpc("publish_ingest_run", { _run: id });
    expect(published.error).toBeNull();
    expect(Number(published.data)).toBe(2);
    const rows = await owner.rpc("read_ingest_table", { _name: name });
    expect(rows.error).toBeNull();
    expect(rows.data).toHaveLength(2);
    expect((await peer.rpc("read_ingest_table", { _name: name })).data).toEqual([]);
  });
  it("refuses unfinished partitions without publishing", async () => {
    const { id, name } = await fixture(false);
    expect((await owner.rpc("publish_ingest_run", { _run: id })).error).toBeTruthy();
    expect((await admin.from("ingest_tables").select("name").eq("name", name)).data).toEqual([]);
  });
  it("refuses a different owner without publishing", async () => {
    const { id, name } = await fixture();
    expect((await peer.rpc("publish_ingest_run", { _run: id })).error).toBeTruthy();
    expect((await admin.from("ingest_tables").select("name").eq("name", name)).data).toEqual([]);
  });
  it("denies anonymous publication", async () => {
    const { id } = await fixture();
    const anon = createClient(url!, key!, { auth: { persistSession: false } });
    expect((await anon.rpc("publish_ingest_run", { _run: id })).error).toBeTruthy();
  });
});
