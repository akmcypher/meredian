import { parseManifest, sha256Hex, type Manifest, type ObjectStore } from "./file-ingest";
import { FILE_READ_LIMITS, validateObjectPrefix } from "./file-manifest";
import { myOrgId } from "./org";
import { publishVerifiedFileRun } from "./file-publication.functions";

/** Table names must be lower-case identifiers (also enforced by the database). */
export function toTableName(raw: string): string {
  const n = raw
    .toLowerCase()
    .replace(/\.[a-z0-9]+$/, "")
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 63);
  return /^[a-z_]/.test(n) ? n : `t_${n}`.slice(0, 63);
}

/** Publish a committed file run as a table — only after every part re-verifies against its fingerprint. */
export async function publishFileRun(_store: ObjectStore, prefix: string, name: string) {
  return publishVerifiedFileRun({ data: { prefix, name, organizationId: await myOrgId() } });
}

/** Read one page of rows by walking parts in order; only the parts that overlap the page are downloaded. */
export async function readFileTablePage(
  store: ObjectStore,
  prefix: string,
  offset: number,
  limit: number,
  registry?: {
    columns: string[];
    part_count: number;
    row_count: number;
    source_sha256: string;
    manifest_sha256?: string | null;
  },
) {
  validateObjectPrefix(prefix);
  if (
    !Number.isSafeInteger(offset) ||
    offset < 0 ||
    !Number.isInteger(limit) ||
    limit < 1 ||
    limit > FILE_READ_LIMITS.pageRows
  )
    throw new Error("invalid page bounds");
  const manifestRaw = await store.get(`${prefix}/_manifest.json`);
  const m: Manifest = parseManifest(manifestRaw, prefix);
  if (
    registry?.manifest_sha256 &&
    manifestRaw &&
    (await sha256Hex(manifestRaw)) !== registry.manifest_sha256
  )
    throw new Error("manifest fingerprint mismatch");
  if (
    registry &&
    (registry.row_count !== m.totalRows ||
      registry.part_count !== m.parts.length ||
      registry.source_sha256 !== m.sourceSha256 ||
      JSON.stringify(registry.columns) !== JSON.stringify(m.columns))
  )
    throw new Error("manifest does not match table registry");
  const rows: Record<string, unknown>[] = [];
  let responseBytes = 0;
  let seen = 0;
  for (const p of m.parts) {
    if (seen + p.rows <= offset) {
      seen += p.rows;
      continue;
    }
    const b = await store.get(p.key);
    if (!b) throw new Error(`${p.key} missing`);
    if (b.length !== p.bytes || (await sha256Hex(b)) !== p.sha256)
      throw new Error("part checksum or size mismatch");
    const lines = new TextDecoder().decode(b).split("\n").filter(Boolean);
    if (lines.length !== p.rows) throw new Error("part row count mismatch");
    for (const l of lines) {
      if (seen >= offset && rows.length < limit) {
        responseBytes += new TextEncoder().encode(l).length;
        if (responseBytes > FILE_READ_LIMITS.responseBytes)
          throw new Error("page exceeds byte limit");
        const row: unknown = JSON.parse(l);
        if (!row || typeof row !== "object" || Array.isArray(row))
          throw new Error("invalid table row");
        rows.push(row as Record<string, unknown>);
      }
      seen++;
    }
    if (rows.length >= limit) break;
  }
  return { columns: m.columns, rows, total: m.totalRows };
}
