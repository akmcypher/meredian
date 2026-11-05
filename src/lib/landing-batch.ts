import { sha256 } from "@noble/hashes/sha2.js";
import type { CdcEvent, Row, WriteMode } from "./ingest";

export type LandingBatch = {
  rows?: Row[] | undefined;
  events?: CdcEvent[] | undefined;
  mode?: WriteMode | undefined;
  key?: string | undefined;
  batchId?: string | undefined;
};
export function encodeLandingBatch(batch: LandingBatch) {
  if (batch.rows && batch.events) throw new Error("Supply rows or CDC events, not both");
  const records = batch.events ?? batch.rows ?? [];
  if (records.length > 10000) throw new Error("Landing batch exceeds 10000 records");
  const chunks: Uint8Array[] = [];
  let size = 0;
  const hash = sha256.create();
  for (const record of records) {
    const bytes = new TextEncoder().encode(JSON.stringify(record) + "\n");
    size += bytes.length;
    if (size > 8 * 1024 * 1024) throw new Error("Landing batch exceeds 8 MiB");
    hash.update(bytes);
    chunks.push(bytes);
  }
  return {
    blob: new Blob(chunks as BlobPart[]),
    sha256: hash.digest().reduce((s, b) => s + b.toString(16).padStart(2, "0"), ""),
    rows: records.length,
    mode: batch.events ? "cdc" : (batch.mode ?? "replace"),
  };
}
