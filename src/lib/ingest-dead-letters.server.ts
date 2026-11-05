// Dead letters from push workers (/api/public/ingest). The raw records go to the ingest-files
// bucket as one NDJSON object per batch; ingest_dead_letters holds position and reason only.
// Idempotent: a retried batch rewrites the identical object and positions already held are kept.
import type { IngestDeadLetter } from "./ingest-dead-letters";
import type { SupabaseClient } from "@supabase/supabase-js";

export async function recordIngestDeadLetters(
  db: SupabaseClient,
  source: { id: string; owner_id: string; organization_id: string | null },
  batchId: string,
  letters: IngestDeadLetter[],
) {
  const objectKey = `${source.owner_id}/dead-letters/ingest/${source.id}/${batchId}.ndjson`;
  const body = letters.map((l) => JSON.stringify(l)).join("\n");
  const put = await db.storage
    .from("ingest-files")
    .upload(objectKey, new Blob([body]), { upsert: false, contentType: "application/x-ndjson" });
  if (put.error) {
    const old = await db.storage.from("ingest-files").download(objectKey);
    if (!old.data || (await old.data.text()) !== body) throw new Error("dead_letter_conflict");
  }
  const { sha256 } = await import("./ingest");
  const hashes = await Promise.all(letters.map((l) => sha256(`${l.key ?? ""}|${l.value ?? ""}`)));
  const rows = letters.map((l, line) => ({
    source_id: source.id,
    owner_id: source.owner_id,
    organization_id: source.organization_id,
    batch_id: batchId,
    topic: l.topic,
    partition: l.partition,
    offset: l.offset,
    error_code: l.error,
    object_key: objectKey,
    line,
    // Same position and same bytes = the same record re-sent; a recreated topic's different
    // record at an old offset is a new dead letter.
    payload_sha256: hashes[line]!,
  }));
  const r = await db.from("ingest_dead_letters").upsert(rows, {
    onConflict: "source_id,topic,partition,offset,payload_sha256",
    ignoreDuplicates: true,
  });
  if (r.error) throw new Error("dead_letter_record_failed");
  return letters.length;
}
