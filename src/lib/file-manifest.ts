import { z } from "zod";

export const FILE_READ_LIMITS = {
  manifestBytes: 2 * 1024 * 1024,
  partBytes: 16 * 1024 * 1024,
  pageRows: 1000,
  responseBytes: 8 * 1024 * 1024,
};
const integer = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);
const digest = z.string().regex(/^[a-f0-9]{64}$/);
const schema = z.object({
  // Legacy manifests have the same v1 shape but omitted the version field.
  version: z.literal(1).optional(),
  source: z.string().min(1).max(1024),
  sourceBytes: integer,
  sourceSha256: digest,
  columns: z.array(z.string().min(1).max(256)).min(1).max(1000),
  parts: z
    .array(
      z.object({
        idx: integer,
        key: z.string().max(1024),
        rows: integer,
        expectedRows: integer,
        sha256: digest,
        bytes: integer.max(FILE_READ_LIMITS.partBytes),
        attempts: z.number().int().min(1).max(100),
      }),
    )
    .max(10000),
  totalRows: integer,
  committedAt: z.string().datetime(),
});

/** Reject ambiguous paths rather than normalizing them into a different authorization scope. */
export function validateObjectPrefix(prefix: string): string {
  if (
    !prefix ||
    prefix.length > 900 ||
    prefix.includes("%") ||
    prefix.includes("\\") ||
    prefix.includes(":")
  )
    throw new Error("invalid object prefix");
  if (prefix.split("/").some((s) => !s || s === "." || s === ".." || !/^[A-Za-z0-9_.-]+$/.test(s)))
    throw new Error("invalid object prefix");
  return prefix;
}

export function validatedManifest(raw: Uint8Array | null, prefix: string) {
  validateObjectPrefix(prefix);
  if (!raw) throw new Error("no manifest (run not committed)");
  if (raw.length > FILE_READ_LIMITS.manifestBytes) throw new Error("manifest exceeds size limit");
  const m = schema.parse(JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw)));
  if (new Set(m.columns).size !== m.columns.length) throw new Error("duplicate columns");
  let rows = 0;
  for (const [idx, p] of m.parts.entries()) {
    if (p.idx !== idx || p.key !== `${prefix}/part-${String(idx).padStart(5, "0")}.ndjson`)
      throw new Error("invalid manifest part key or index");
    if (p.rows !== p.expectedRows) throw new Error("part row count mismatch");
    rows += p.rows;
  }
  if (!Number.isSafeInteger(rows) || rows !== m.totalRows)
    throw new Error("manifest row total mismatch");
  const { version: _version, ...rest } = m;
  return { ...rest, version: 1 as const };
}
