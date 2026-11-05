// Ingestion storage reconciliation. Dry run unless --apply is given.
//   SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... bun scripts/ops/reconcile-ingest-storage.ts \
//     [--apply] [--min-age-hours 168] [--owner <user id> ...]
// Prints a JSON summary; every would-delete or delete is recorded in storage_reconciliation_log
// under the printed run id.
import { createClient } from "@supabase/supabase-js";
import { reconcileIngestStorage } from "../../src/lib/storage-reconcile.server";

const args = process.argv.slice(2);
if (args.includes("--apply")) {
  console.error(
    "destructive_cleanup_disabled: only dry-run is supported until concurrent registration is protected",
  );
  process.exit(2);
}
const value = (flag: string) => {
  const i = args.indexOf(flag);
  return i >= 0 ? args[i + 1] : undefined;
};
const url = process.env["SUPABASE_URL"],
  key = process.env["SUPABASE_SERVICE_ROLE_KEY"];
if (!url || !key) {
  console.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required");
  process.exit(2);
}
const minAgeHours = Number(value("--min-age-hours") ?? 168);
const owners = args.flatMap((a, i) => (a === "--owner" && args[i + 1] ? [args[i + 1]!] : []));
const result = await reconcileIngestStorage(
  createClient(url, key, { auth: { persistSession: false } }),
  { apply: args.includes("--apply"), minAgeHours, ...(owners.length ? { owners } : {}) },
);
console.log(
  JSON.stringify(
    {
      ...result,
      orphans: result.orphans.map((o) => ({ key: o.key, kind: o.kind, bytes: o.bytes })),
    },
    null,
    2,
  ),
);
process.exit(result.failed ? 1 : 0);
