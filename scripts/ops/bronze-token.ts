// Operator CLI: prints one short-lived token for an already registered, enabled engine.
// Redirect stdout into a private file. The signing key stays in the control plane's .env.
import { createClient } from "@supabase/supabase-js";
import { engineTokenFor } from "../../src/lib/engine-token.server";
const engineId = process.argv[process.argv.indexOf("--engine") + 1];
if (!process.argv.includes("--engine") || !/^[0-9a-f-]{36}$/i.test(engineId ?? ""))
  throw new Error("usage: bun scripts/ops/bronze-token.ts --engine <registered-engine-id>");
const url = process.env["SUPABASE_URL"],
  key = process.env["SUPABASE_SERVICE_ROLE_KEY"];
if (!url || !key) throw new Error("Control-plane environment is required");
const db = createClient(url, key, { auth: { persistSession: false } });
const row = await db
  .from("execution_engines")
  .select("owner_id,organization_id,enabled")
  .eq("id", engineId!)
  .single();
if (row.error || !row.data.enabled) throw new Error("Engine unavailable");
if (row.data.organization_id) {
  const member = await db
    .from("org_members")
    .select("user_id")
    .eq("org_id", row.data.organization_id)
    .eq("user_id", row.data.owner_id)
    .maybeSingle();
  if (member.error || !member.data)
    throw new Error("Engine owner is no longer an organization member");
}
console.log(await engineTokenFor(engineId!, row.data.owner_id));
