// Disposable local upgrade fixture: run against schema0076 before applying0077.
import { appendFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { createClient } from "@supabase/supabase-js";
const url = process.env["SUPABASE_URL"];
if (process.env["AUTH_TEST_DB_ISOLATED"] !== "1")
  throw new Error("Explicit disposable-database authorization required for upgrade fixtures");
if (!url || !["localhost", "127.0.0.1", "[::1]"].includes(new URL(url).hostname))
  throw new Error("Legacy upgrade fixtures require a local disposable Auth service");
const db = createClient(url, process.env["SUPABASE_SERVICE_ROLE_KEY"], {
  auth: { persistSession: false },
});
const email = `legacy-upgrade-${crypto.randomUUID()}@example.test`;
const password = `Fixture-A9-${crypto.randomUUID()}`;
const user = await db.auth.admin.createUser({ email, password, email_confirm: true });
if (user.error) throw user.error;
const id = user.data.user.id;
for (const result of [
  await db.from("profiles").upsert({ id, email, display_name: "Existing administrator" }),
  await db.from("user_roles").insert({ user_id: id, role: "admin" }),
])
  if (result.error) throw result.error;
const org = await db
  .from("organizations")
  .insert({ name: "Existing upgrade workspace", created_by: id })
  .select("id")
  .single();
if (org.error) throw org.error;
const membership = await db.from("org_members").insert({ org_id: org.data.id, user_id: id });
if (membership.error) throw membership.error;
const source = await db
  .from("ingest_sources")
  .insert({
    owner_id: id,
    organization_id: org.data.id,
    name: "Existing upgrade source",
    kind: "postgres",
    target_table: "upgrade_fixture",
    config: {},
    enabled: false,
    token_hash: createHash("sha256").update(crypto.randomUUID()).digest("hex"),
  })
  .select("id")
  .single();
if (source.error) throw source.error;
const connection = await db
  .from("connect_connections")
  .insert({
    source_id: source.data.id,
    owner_id: id,
    organization_id: org.data.id,
    name: "Existing upgrade connection",
    credential_ref: "upgrade-fixture",
    enabled: false,
  })
  .select("id")
  .single();
if (connection.error) throw connection.error;
const run = await db
  .from("connect_runs")
  .insert({
    connection_id: connection.data.id,
    request_key: crypto.randomUUID(),
    operation: "check",
    status: "succeeded",
    finished_at: new Date().toISOString(),
  })
  .select("id")
  .single();
if (run.error) throw run.error;
const table = await db
  .from("user_tables")
  .insert({
    owner_id: id,
    organization_id: org.data.id,
    name: "upgrade_" + crypto.randomUUID().replaceAll("-", ""),
    columns: [{ name: "id", type: "number" }],
    rows: [{ id: 1 }],
    row_count: 1,
  })
  .select("id")
  .single();
if (table.error) throw table.error;
const object = id + "/upgrade-fixture/" + crypto.randomUUID() + ".json";
const uploaded = await db.storage
  .from("ingest-files")
  .upload(object, Buffer.from('{"id":1}'), { contentType: "application/json" });
if (uploaded.error) throw uploaded.error;
const peerEmail = `legacy-peer-${crypto.randomUUID()}@example.test`;
const peerPassword = `Fixture-A9-${crypto.randomUUID()}`;
const peer = await db.auth.admin.createUser({
  email: peerEmail,
  password: peerPassword,
  email_confirm: true,
});
if (peer.error) throw peer.error;
const peerId = peer.data.user.id;
for (const result of [
  await db
    .from("profiles")
    .upsert({ id: peerId, email: peerEmail, display_name: "Existing member" }),
  await db.from("org_members").insert({ org_id: org.data.id, user_id: peerId }),
])
  if (result.error) throw result.error;
const personal = await db
  .from("user_tables")
  .insert({
    owner_id: id,
    name: `private_${crypto.randomUUID().replaceAll("-", "")}`,
    rows: [{ secret_fixture: 1 }],
    row_count: 1,
  })
  .select("id,name")
  .single();
if (personal.error) throw personal.error;
const priorHistory = await db
  .from("table_versions")
  .insert({
    table_name: personal.data.name,
    version: 1,
    created_by: peerId,
    rows: [{ former_owner_fixture: 1 }],
    row_count: 1,
    created_at: "2000-01-01T00:00:00Z",
  })
  .select("id")
  .single();
if (priorHistory.error) throw priorHistory.error;
appendFileSync(
  ".env",
  `\nAUTH_LEGACY_PRIVATE_TABLE_ID=${personal.data.id}\nAUTH_LEGACY_OLD_VERSION_ID=${priorHistory.data.id}\nAUTH_LEGACY_PEER_EMAIL=${peerEmail}\nAUTH_LEGACY_PEER_PASSWORD=${peerPassword}\n`,
);
appendFileSync(
  ".env",
  `\nAUTH_LEGACY_ADMIN_ID=${id}\nAUTH_LEGACY_ORG_ID=${org.data.id}\nAUTH_LEGACY_ADMIN_EMAIL=${email}\nAUTH_LEGACY_ADMIN_PASSWORD=${password}\nAUTH_LEGACY_SOURCE_ID=${source.data.id}\nAUTH_LEGACY_CONNECTION_ID=${connection.data.id}\nAUTH_LEGACY_RUN_ID=${run.data.id}\nAUTH_LEGACY_TABLE_ID=${table.data.id}\nAUTH_LEGACY_OBJECT_KEY=${object}\n`,
);
console.log(
  "Seeded isolated pre-upgrade administrator/workspace, source, connection, run, table and private object; credentials stay in ignored .env.",
);
