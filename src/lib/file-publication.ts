import { parseManifest, sha256Hex, type ObjectStore, type Manifest } from "./file-ingest";
import { validateObjectPrefix } from "./file-manifest";

/** Snapshot validated parts into a client-unwritable destination; never register staging bytes. */
export async function copyVerifiedFileRun(
  store: ObjectStore,
  staging: string,
  destination: string,
) {
  validateObjectPrefix(staging);
  validateObjectPrefix(destination);
  const raw = await store.get(`${staging}/_manifest.json`);
  const manifest = parseManifest(raw, staging);
  if (!manifest.parts.length) throw new Error("No parts to publish");
  const parts = [];
  for (const part of manifest.parts) {
    const bytes = await store.get(part.key);
    if (!bytes || bytes.length !== part.bytes || (await sha256Hex(bytes)) !== part.sha256)
      throw new Error("Staging part changed or is missing");
    const lines = new TextDecoder("utf-8", { fatal: true })
      .decode(bytes)
      .split("\n")
      .filter(Boolean);
    if (lines.length !== part.rows) throw new Error("Part row count mismatch");
    for (const line of lines) {
      const row: unknown = JSON.parse(line);
      if (!row || typeof row !== "object" || Array.isArray(row))
        throw new Error("Invalid part row");
    }
    const key = `${destination}/part-${String(part.idx).padStart(5, "0")}.ndjson`;
    await store.put(key, bytes);
    const copied = await store.get(key);
    if (!copied || copied.length !== bytes.length || (await sha256Hex(copied)) !== part.sha256)
      throw new Error("Publication copy verification failed");
    parts.push({ ...part, key });
  }
  const committed: Manifest = { ...manifest, version: 1, parts };
  const body = new TextEncoder().encode(JSON.stringify(committed));
  await store.put(`${destination}/_manifest.json`, body);
  const back = await store.get(`${destination}/_manifest.json`);
  const digest = await sha256Hex(body);
  if (!back || (await sha256Hex(back)) !== digest)
    throw new Error("Publication manifest verification failed");
  return { manifest: committed, digest };
}
