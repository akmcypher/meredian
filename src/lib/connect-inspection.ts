import { supabase } from "@/integrations/supabase/client";

export type CommittedBatch = {
  batch_id: string;
  manifest_key: string;
  sha256: string;
  row_count: number;
  byte_count: number;
};

export async function verifyBatchPreview(batch: CommittedBatch, manifest: unknown, body: Blob) {
  const m = manifest as Record<string, unknown> | null;
  if (
    !m ||
    m["batch_id"] !== batch.batch_id ||
    m["sha256"] !== batch.sha256 ||
    m["rows"] !== batch.row_count ||
    m["bytes"] !== batch.byte_count ||
    body.size !== batch.byte_count ||
    body.size > 8 * 1024 * 1024
  )
    throw new Error("Committed batch metadata does not match its stored output");
  const bytes = await body.arrayBuffer();
  const hash = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");
  if (hash !== batch.sha256) throw new Error("Committed batch checksum verification failed");
  const lines = new TextDecoder().decode(bytes).split("\n").filter(Boolean);
  if (lines.length !== batch.row_count)
    throw new Error("Committed batch record count does not match");
  return {
    manifest: m,
    records: lines.slice(0, 100).map((line) => JSON.parse(line) as unknown),
    total: lines.length,
  };
}

/** Uses the signed-in owner's storage policy, never service credentials or engine grants. */
export async function inspectCommittedBatch(batch: CommittedBatch) {
  const storage = supabase.storage.from("ingest-files");
  const manifest = await storage.download(batch.manifest_key);
  if (manifest.error || !manifest.data)
    throw new Error("Batch manifest is unavailable to this account");
  if (manifest.data.size > 1024 * 1024)
    throw new Error("Batch manifest exceeds the inspection limit");
  const parsed: unknown = JSON.parse(await manifest.data.text());
  const path = (parsed as Record<string, unknown> | null)?.["object_key"];
  if (typeof path !== "string") throw new Error("Batch manifest has no data object");
  const data = await storage.download(path);
  if (data.error || !data.data) throw new Error("Batch records are unavailable to this account");
  return verifyBatchPreview(batch, parsed, data.data);
}
