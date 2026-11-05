import { encodeLandingBatch, type LandingBatch } from "./landing-batch";

/** Immutable raw landing only; a downstream native table-format consumer must commit it. */
export async function landSourceBatch(sourceId: string, actorId: string, batch: LandingBatch) {
  const { supabaseAdmin: db } = await import("@/integrations/supabase/client.server");
  const { data: source, error } = await db
    .from("ingest_sources")
    .select("owner_id,organization_id,enabled")
    .eq("id", sourceId)
    .single();
  if (error || !source || source.owner_id !== actorId || !source.enabled)
    throw new Error("Source not authorized");
  if (source.organization_id) {
    const membership = await db
      .from("org_members")
      .select("user_id")
      .eq("org_id", source.organization_id)
      .eq("user_id", actorId)
      .maybeSingle();
    if (membership.error || !membership.data) throw new Error("Source membership revoked");
  }
  const encoded = encodeLandingBatch(batch);
  const id = batch.batchId ?? crypto.randomUUID();
  if (!/^[a-f0-9-]{36}$/i.test(id)) throw new Error("Invalid batch identifier");
  const key = `${actorId}/published/${id}/landing.ndjson`;
  const bucket = db.storage.from("ingest-files");
  const uploaded = await bucket.upload(key, encoded.blob, {
    upsert: false,
    contentType: "application/x-ndjson",
  });
  if (uploaded.error) {
    const previous = await db
      .from("ingest_landing_batches")
      .select("sha256,mode,row_count")
      .eq("source_id", sourceId)
      .eq("batch_key", id)
      .maybeSingle();
    if (previous.error) throw new Error("Landing registration lookup failed");
    if (previous.data) {
      if (
        previous.data.sha256 !== encoded.sha256 ||
        previous.data.mode !== encoded.mode ||
        previous.data.row_count !== encoded.rows
      )
        throw new Error("Landing batch identifier conflicts");
    } else {
      // A worker may lose the registration response after immutable upload.
      // Reconcile only exact bytes; never overwrite an uncertain object.
      const stored = await bucket.download(key);
      if (
        !stored.data ||
        stored.data.size > 8 * 1024 * 1024 ||
        stored.data.size !== encoded.blob.size
      )
        throw new Error("Unregistered landing object requires reconciliation");
      const { sha256 } = await import("./ingest");
      if ((await sha256(await stored.data.text())) !== encoded.sha256)
        throw new Error("Unregistered landing object checksum conflicts");
    }
  }
  const registered = await db.rpc("register_ingest_landing_batch", {
    _actor: actorId,
    _source: sourceId,
    _key: key,
    _sha: encoded.sha256,
    _rows: encoded.rows,
    _mode: encoded.mode,
    _batch: id,
  });
  if (registered.error)
    throw new Error("Landing registration failed; retained object requires reconciliation");
  return {
    id: registered.data,
    batchId: id,
    rows: encoded.rows,
    status: "landed_pending_format_commit" as const,
  };
}
