import type { Json } from "@/integrations/supabase/types";
import { applyCdc, mergeRows, schemaFor, type CdcEvent, type Row, type WriteMode } from "./ingest";

/** Legacy migration-only writer. Production ingest callers use immutable object landing. */
export async function writeLegacyMigrationBatch(
  sourceId: string,
  ownerId: string,
  table: string,
  batch: {
    rows?: Row[] | undefined;
    events?: CdcEvent[] | undefined;
    mode?: WriteMode | undefined;
    key?: string | undefined;
  },
) {
  const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
  const source = await supabaseAdmin
    .from("ingest_sources")
    .select("organization_id")
    .eq("id", sourceId)
    .eq("owner_id", ownerId)
    .single();
  if (source.error) throw new Error("Authorized migration source unavailable");
  const organization = source.data.organization_id;
  let current = supabaseAdmin
    .from("user_tables")
    .select("id, rows, owner_id")
    .eq("name", table)
    .eq("owner_id", ownerId);
  current = organization
    ? current.eq("organization_id", organization)
    : current.is("organization_id", null);
  const { data: existing, error: lookupError } = await current.maybeSingle();
  if (lookupError) throw new Error("Migration table lookup failed");
  if (existing && existing.owner_id !== ownerId)
    throw new Error(`Table ${table} belongs to someone else`);
  const prev = (existing?.rows as unknown as Row[]) ?? [];
  const rows = batch.events
    ? applyCdc(prev, batch.events, batch.key || "id")
    : mergeRows(prev, batch.rows ?? [], batch.mode ?? "replace", batch.key);
  const columns = schemaFor(rows) as unknown as NonNullable<Json>;
  const payload = { columns, rows: rows as unknown as NonNullable<Json>, row_count: rows.length };
  const res = existing
    ? await supabaseAdmin.from("user_tables").update(payload).eq("id", existing.id)
    : await supabaseAdmin
        .from("user_tables")
        .insert({ ...payload, name: table, owner_id: ownerId, organization_id: organization });
  if (res.error) throw new Error(res.error.message);
  await supabaseAdmin
    .from("ingest_sources")
    .update({ last_run_at: new Date().toISOString(), last_status: "ok", last_rows: rows.length })
    .eq("id", sourceId);
  await supabaseAdmin
    .from("audit_log")
    .insert({
      actor_id: ownerId,
      action: "ingest.batch",
      target: table,
      details: {
        source: sourceId,
        rows: batch.rows?.length ?? batch.events?.length ?? 0,
      } as NonNullable<Json>,
    })
    .then(
      () => undefined,
      () => undefined,
    );
  return rows.length;
}
