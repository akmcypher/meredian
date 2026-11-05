/**
 * Multi-worker physical file ingestion over an object store.
 * Plan: one pass finds record-aligned byte ranges (exact for quoted CSV). Workers then read ONLY their
 * range (HTTP Range / ranged get), parse it, and write one output part per range. Parts are idempotent
 * (same key on retry) and the run becomes visible only when the manifest — written last, after every
 * part's checksum and row count is verified — is committed.
 */
import type { ByteRange } from "./file-chunks";
import { planCsvRanges, sourceFingerprint, MAX_UNIT_BYTES } from "./ranged-source";
import { validatedManifest } from "./file-manifest";

export interface ObjectStore {
  size(key: string): Promise<number>;
  readRange(key: string, start: number, end: number): Promise<Uint8Array>; // end exclusive
  put(key: string, bytes: Uint8Array): Promise<void>;
  get(key: string): Promise<Uint8Array | null>;
}

export class MemoryStore implements ObjectStore {
  objects = new Map<string, Uint8Array>();
  rangeReads = 0;
  async size(k: string) {
    const o = this.objects.get(k);
    if (!o) throw new Error(`missing ${k}`);
    return o.length;
  }
  async readRange(k: string, s: number, e: number) {
    this.rangeReads++;
    const o = this.objects.get(k);
    if (!o) throw new Error(`missing ${k}`);
    return o.slice(s, e);
  }
  async put(k: string, b: Uint8Array) {
    this.objects.set(k, b.slice());
  }
  async get(k: string) {
    return this.objects.get(k) ?? null;
  }
}

/** Read-only store over URLs that honour `Range` (e.g. signed storage URLs, S3 presigned GETs). */
export class HttpRangeStore implements ObjectStore {
  constructor(private url: (key: string) => string) {}
  async size(k: string) {
    const r = await fetch(this.url(k), { headers: { Range: "bytes=0-0" } });
    const cr = r.headers.get("content-range");
    const m = cr?.match(/\/(\d+)$/);
    if (!m) throw new Error("server did not return Content-Range; ranged reads unsupported");
    return Number(m[1]);
  }
  async readRange(k: string, s: number, e: number) {
    if (e <= s) return new Uint8Array();
    const r = await fetch(this.url(k), { headers: { Range: `bytes=${s}-${e - 1}` } });
    if (r.status !== 206) throw new Error(`expected 206 Partial Content, got ${r.status}`);
    const b = new Uint8Array(await r.arrayBuffer());
    if (b.length !== e - s) throw new Error(`short read ${b.length}/${e - s}`);
    return b;
  }
  async put(): Promise<void> {
    throw new Error("HttpRangeStore is read-only");
  }
  async get(k: string) {
    const r = await fetch(this.url(k));
    return r.ok ? new Uint8Array(await r.arrayBuffer()) : null;
  }
}

export async function sha256Hex(b: Uint8Array): Promise<string> {
  const d = await crypto.subtle.digest("SHA-256", b as BufferSource);
  return [...new Uint8Array(d)].map((x) => x.toString(16).padStart(2, "0")).join("");
}

/** RFC 4180 parser for one record-aligned chunk. */
export function parseCsvChunk(text: string, delim = ","): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let f = "";
  let q = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i]!;
    if (q) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          f += '"';
          i++;
        } else q = false;
      } else f += c;
    } else if (c === '"') q = true;
    else if (c === delim) {
      row.push(f);
      f = "";
    } else if (c === "\n") {
      row.push(f.endsWith("\r") ? f.slice(0, -1) : f);
      rows.push(row);
      row = [];
      f = "";
    } else f += c;
  }
  if (f.length || row.length) {
    row.push(f.endsWith("\r") ? f.slice(0, -1) : f);
    rows.push(row);
  }
  return rows;
}

export type PartResult = {
  idx: number;
  key: string;
  rows: number;
  expectedRows: number;
  sha256: string;
  bytes: number;
  attempts: number;
};
export type Manifest = {
  version?: 1;
  source: string;
  sourceBytes: number;
  sourceSha256: string;
  columns: string[];
  parts: PartResult[];
  totalRows: number;
  committedAt: string;
};

export async function runFileIngest(
  store: ObjectStore,
  sourceKey: string,
  outPrefix: string,
  opts: {
    parts: number;
    concurrency: number;
    maxAttempts?: number;
    /** test hook: throw to simulate a worker crash */ beforeWrite?: (
      idx: number,
      attempt: number,
    ) => void;
  },
): Promise<Manifest> {
  const maxAttempts = opts.maxAttempts ?? 3;
  if (!Number.isInteger(opts.concurrency) || opts.concurrency < 1)
    throw new Error("concurrency must be a positive integer");
  const size = await store.size(sourceKey);
  // Planning pass (exact quote-aware boundaries). Workers below never see bytes outside their range.
  const sourceSha256 = await sourceFingerprint(store, sourceKey);
  const { header, ranges } = await planCsvRanges(store, sourceKey, opts.parts);
  const dec = new TextDecoder();
  const columns = parseCsvChunk(dec.decode(header))[0] ?? [];
  const expected = new Map(ranges.map((r) => [r.idx, r.rows]));

  const results: PartResult[] = [];
  const queue: ByteRange[] = [...ranges];
  const worker = async () => {
    for (let r = queue.shift(); r; r = queue.shift()) {
      let attempt = 0;
      for (;;) {
        attempt++;
        try {
          const chunk = await store.readRange(sourceKey, r.start, r.end);
          const rows = parseCsvChunk(dec.decode(chunk)).filter(
            (x) => !(x.length === 1 && x[0] === ""),
          );
          const exp = expected.get(r.idx);
          if (exp === undefined) throw new Error("Missing expected row count");
          if (rows.length !== exp)
            throw new Error(`part ${r.idx}: parsed ${rows.length} rows, expected ${exp}`);
          const body = new TextEncoder().encode(
            rows
              .map((v) =>
                JSON.stringify(Object.fromEntries(columns.map((c, i) => [c, v[i] ?? null]))),
              )
              .join("\n") + (rows.length ? "\n" : ""),
          );
          if (body.length > MAX_UNIT_BYTES) throw new Error("CSV output part exceeds 8 MiB budget");
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

/** Re-verify a committed run: every part exists with the recorded checksum. */
export async function verifyManifest(
  store: ObjectStore,
  outPrefix: string,
): Promise<{ ok: boolean; problems: string[] }> {
  const raw = await store.get(`${outPrefix}/_manifest.json`);
  if (!raw) return { ok: false, problems: ["no manifest (run not committed)"] };
  let m: Manifest;
  try {
    m = parseManifest(raw, outPrefix);
  } catch {
    return { ok: false, problems: ["invalid manifest"] };
  }
  const problems: string[] = [];
  for (const p of m.parts) {
    const b = await store.get(p.key);
    if (!b) problems.push(`${p.key} missing`);
    else if (b.length !== p.bytes || (await sha256Hex(b)) !== p.sha256)
      problems.push(`${p.key} checksum or size mismatch`);
  }
  return { ok: problems.length === 0, problems };
}

export function parseManifest(raw: Uint8Array | null, prefix: string): Manifest {
  return validatedManifest(raw, prefix);
}
