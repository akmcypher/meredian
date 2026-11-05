import { parquetMetadataAsync, parquetReadObjects, type AsyncBuffer } from "hyparquet";
import { sha256Hex, type Manifest, type ObjectStore, type PartResult } from "./file-ingest";
import { sourceFingerprint, MAX_UNIT_BYTES } from "./ranged-source";

/** Make Parquet values JSON-safe: BigInt → number (or string if too large), Date → ISO, bytes → base64. */
export function jsonSafe(v: unknown): unknown {
  if (typeof v === "bigint") return Number.isSafeInteger(Number(v)) ? Number(v) : v.toString();
  if (v instanceof Date) return v.toISOString();
  if (v instanceof Uint8Array) return btoa(String.fromCharCode(...v));
  if (Array.isArray(v)) return v.map(jsonSafe);
  if (v && typeof v === "object")
    return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, jsonSafe(x)]));
  return v;
}

/** Group row groups into at most `parts` contiguous row ranges with roughly equal row counts. */
export function planRowGroups(
  groupRows: number[],
  parts: number,
): { idx: number; start: number; end: number }[] {
  if (!Number.isInteger(parts) || parts < 1) throw new Error("parts must be a positive integer");
  const total = groupRows.reduce((s, n) => s + n, 0);
  const target = Math.max(1, Math.ceil(total / parts));
  const out: { idx: number; start: number; end: number }[] = [];
  let start = 0,
    acc = 0,
    pos = 0;
  for (const n of groupRows) {
    pos += n;
    acc += n;
    if (acc >= target) {
      out.push({ idx: out.length, start, end: pos });
      start = pos;
      acc = 0;
    }
  }
  if (pos > start) out.push({ idx: out.length, start, end: pos });
  return out;
}

/**
 * Import a Parquet file: metadata is read with ranged reads, each worker decodes only its row groups,
 * writes one NDJSON part with a checksum, and the manifest is written last (same format as CSV runs).
 */
export async function runParquetIngest(
  store: ObjectStore,
  sourceKey: string,
  outPrefix: string,
  opts: {
    parts: number;
    concurrency: number;
    maxAttempts?: number;
    beforeWrite?: (idx: number, attempt: number) => void;
  },
): Promise<Manifest> {
  const maxAttempts = opts.maxAttempts ?? 3;
  const size = await store.size(sourceKey);
  const file: AsyncBuffer = {
    byteLength: size,
    slice: async (s: number, e?: number) => {
      const end = e ?? size;
      if (end - s > MAX_UNIT_BYTES) throw new Error("Parquet read exceeds 8 MiB budget");
      const b = await store.readRange(sourceKey, s, end);
      return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength) as ArrayBuffer;
    },
  };
  const meta = await parquetMetadataAsync(file);
  if (meta.row_groups.length > 10000) throw new Error("Parquet exceeds bounded row-group count");
  const columns = meta.schema
    .slice(1)
    .filter((s) => !s.num_children)
    .map((s) => s.name);
  if (!Number.isInteger(opts.parts) || opts.parts < 1)
    throw new Error("parts must be a positive integer");
  // Decode one bounded format unit per task; requested part count must not merge units without a budget.
  let position = 0;
  const ranges = meta.row_groups.map((group, idx) => {
    const start = position;
    position += Number(group.num_rows);
    return { idx, start, end: position };
  });
  for (const group of meta.row_groups) {
    if (Number(group.total_byte_size) > MAX_UNIT_BYTES || Number(group.num_rows) > 10000)
      throw new Error("Parquet row group exceeds bounded decoder budget");
  }
  const sourceSha256 = await sourceFingerprint(store, sourceKey);

  const results: PartResult[] = [];
  const queue = [...ranges];
  const worker = async () => {
    for (let r = queue.shift(); r; r = queue.shift()) {
      for (let attempt = 1; ; attempt++) {
        try {
          const rows = await parquetReadObjects({
            file,
            metadata: meta,
            rowStart: r.start,
            rowEnd: r.end,
          });
          const exp = r.end - r.start;
          if (rows.length !== exp)
            throw new Error(`part ${r.idx}: decoded ${rows.length} rows, expected ${exp}`);
          const body = new TextEncoder().encode(
            rows.map((x) => JSON.stringify(jsonSafe(x))).join("\n") + (rows.length ? "\n" : ""),
          );
          if (body.length > MAX_UNIT_BYTES) throw new Error("Parquet output exceeds 8 MiB budget");
          opts.beforeWrite?.(r.idx, attempt);
          const key = `${outPrefix}/part-${String(r.idx).padStart(5, "0")}.ndjson`;
          await store.put(key, body);
          const back = await store.get(key);
          const sha = await sha256Hex(body);
          if (!back || (await sha256Hex(back)) !== sha)
            throw new Error(`part ${r.idx}: checksum mismatch after write`);
          results.push({
            idx: r.idx,
            key,
            rows: rows.length,
            expectedRows: exp,
            sha256: sha,
            bytes: body.length,
            attempts: attempt,
          });
          break;
        } catch (e) {
          if (attempt >= maxAttempts)
            throw new Error(
              `part ${r.idx} failed after ${attempt} attempts: ${(e as Error).message}`,
            );
        }
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(opts.concurrency, ranges.length || 1) }, worker));
  results.sort((a, b) => a.idx - b.idx);
  if (results.length !== ranges.length)
    throw new Error("not every part completed; manifest not committed");
  const manifest: Manifest = {
    source: sourceKey,
    sourceBytes: size,
    sourceSha256,
    columns,
    parts: results,
    totalRows: results.reduce((s, p) => s + p.rows, 0),
    committedAt: new Date().toISOString(),
  };
  await store.put(
    `${outPrefix}/_manifest.json`,
    new TextEncoder().encode(JSON.stringify(manifest)),
  );
  return manifest;
}
