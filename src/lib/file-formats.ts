/** Parse lake files (Parquet, Avro, CSV, JSON / JSON Lines) into rows in the browser. */
import Papa from "papaparse";
import type { Row } from "./rows";

export type FileFormat = "parquet" | "avro" | "csv" | "json";
export const MAX_FILE_ROWS = 5000;

export function detectFormat(name: string): FileFormat | null {
  const n = name.toLowerCase().replace(/\.(gz|snappy)$/, "");
  if (n.endsWith(".parquet") || n.endsWith(".parq")) return "parquet";
  if (n.endsWith(".avro")) return "avro";
  if (n.endsWith(".csv") || n.endsWith(".tsv")) return "csv";
  if (n.endsWith(".json") || n.endsWith(".jsonl") || n.endsWith(".ndjson")) return "json";
  return null;
}

/** Make values JSON-safe and flat enough for the SQL engine. */
export function normalizeValue(v: unknown): unknown {
  if (v == null) return null;
  if (typeof v === "bigint") return Number.isSafeInteger(Number(v)) ? Number(v) : v.toString();
  if (v instanceof Date) return v.toISOString();
  if (v instanceof Uint8Array)
    return `0x${[...v.slice(0, 32)].map((b) => b.toString(16).padStart(2, "0")).join("")}${v.length > 32 ? "…" : ""}`;
  if (Array.isArray(v) || typeof v === "object")
    return JSON.stringify(v, (_k, x) => (typeof x === "bigint" ? x.toString() : x));
  return v;
}
const normRows = (rows: Record<string, unknown>[]): Row[] =>
  rows.map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, normalizeValue(v)])));

export async function parseFile(
  name: string,
  bytes: ArrayBuffer,
): Promise<{ rows: Row[]; total: number; format: FileFormat }> {
  const format = detectFormat(name);
  if (!format)
    throw new Error("Unsupported file type — use .parquet, .avro, .csv, .json or .jsonl");
  if (format === "parquet") {
    const { parquetReadObjects, parquetMetadata } = await import("hyparquet");
    const { compressors } = await import("hyparquet-compressors");
    const meta = parquetMetadata(bytes);
    const rows = (await parquetReadObjects({
      file: bytes,
      compressors,
      rowEnd: MAX_FILE_ROWS,
    })) as Record<string, unknown>[];
    return { rows: normRows(rows), total: Number(meta.num_rows), format };
  }
  if (format === "avro") {
    const all = await readAvro(new Uint8Array(bytes), MAX_FILE_ROWS);
    return { rows: normRows(all.rows), total: all.total, format };
  }
  const text = new TextDecoder().decode(bytes);
  if (format === "csv") {
    const r = Papa.parse<Record<string, unknown>>(text, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: true,
    });
    return { rows: normRows(r.data.slice(0, MAX_FILE_ROWS)), total: r.data.length, format };
  }
  const t = text.trim();
  let arr: unknown[];
  try {
    const document: unknown = JSON.parse(t);
    arr = Array.isArray(document)
      ? document
      : document && typeof document === "object"
        ? ((Object.values(document).find(Array.isArray) as unknown[]) ?? [document])
        : [document];
  } catch (error) {
    // Multiple top-level records are JSON Lines, regardless of the filename suffix.
    if (t && !t.includes("\n")) throw error;
    arr = t
      .split(/\r?\n/)
      .filter(Boolean)
      .map((line) => JSON.parse(line) as unknown);
  }
  const objs = arr.filter((x): x is Record<string, unknown> => !!x && typeof x === "object");
  return { rows: normRows(objs.slice(0, MAX_FILE_ROWS)), total: objs.length, format };
}

/* ---------------- Avro object container file reader ---------------- */
type Schema = string | { type: string | Schema[]; [k: string]: unknown } | Schema[];

class Reader {
  pos = 0;
  constructor(public b: Uint8Array) {}
  long(): number {
    let n = 0n,
      shift = 0n,
      byte: number;
    do {
      byte = this.b[this.pos++]!;
      n |= BigInt(byte & 0x7f) << shift;
      shift += 7n;
    } while (byte & 0x80);
    const v = (n >> 1n) ^ -(n & 1n);
    return Number(v);
  }
  bytes(): Uint8Array {
    const len = this.long();
    const out = this.b.subarray(this.pos, this.pos + len);
    this.pos += len;
    return out;
  }
  string(): string {
    return new TextDecoder().decode(this.bytes());
  }
  fixed(n: number) {
    const out = this.b.subarray(this.pos, this.pos + n);
    this.pos += n;
    return out;
  }
  float() {
    const v = new DataView(this.b.buffer, this.b.byteOffset + this.pos, 4).getFloat32(0, true);
    this.pos += 4;
    return v;
  }
  double() {
    const v = new DataView(this.b.buffer, this.b.byteOffset + this.pos, 8).getFloat64(0, true);
    this.pos += 8;
    return v;
  }
}

function decode(r: Reader, s: Schema, named: Map<string, Schema>): unknown {
  if (Array.isArray(s)) return decode(r, s[r.long()]!, named);
  if (typeof s === "string") {
    switch (s) {
      case "null":
        return null;
      case "boolean":
        return r.b[r.pos++] === 1;
      case "int":
      case "long":
        return r.long();
      case "float":
        return r.float();
      case "double":
        return r.double();
      case "bytes":
        return r.bytes();
      case "string":
        return r.string();
      default: {
        const n = named.get(s);
        if (!n) throw new Error(`Unknown Avro type ${s}`);
        return decode(r, n, named);
      }
    }
  }
  const t = s.type;
  if (Array.isArray(t)) return decode(r, t, named);
  if (typeof t === "object") return decode(r, t as Schema, named);
  if (s["name"]) named.set(String(s["name"]), s);
  switch (t) {
    case "record": {
      const o: Record<string, unknown> = {};
      for (const f of s["fields"] as { name: string; type: Schema }[])
        o[f.name] = decode(r, f.type, named);
      return o;
    }
    case "enum":
      return (s["symbols"] as string[])[r.long()];
    case "fixed":
      return r.fixed(Number(s["size"]));
    case "array": {
      const out: unknown[] = [];
      for (let n = r.long(); n !== 0; n = r.long()) {
        if (n < 0) {
          n = -n;
          r.long();
        }
        for (let i = 0; i < n; i++) out.push(decode(r, s["items"] as Schema, named));
      }
      return out;
    }
    case "map": {
      const out: Record<string, unknown> = {};
      for (let n = r.long(); n !== 0; n = r.long()) {
        if (n < 0) {
          n = -n;
          r.long();
        }
        for (let i = 0; i < n; i++) {
          const k = r.string();
          out[k] = decode(r, s["values"] as Schema, named);
        }
      }
      return out;
    }
    default: {
      const v = decode(r, t as string, named);
      if (s["logicalType"] === "timestamp-millis" && typeof v === "number")
        return new Date(v).toISOString();
      if (s["logicalType"] === "timestamp-micros" && typeof v === "number")
        return new Date(v / 1000).toISOString();
      if (s["logicalType"] === "date" && typeof v === "number")
        return new Date(v * 86400000).toISOString().slice(0, 10);
      return v;
    }
  }
}

async function inflateRaw(data: Uint8Array): Promise<Uint8Array> {
  const ds = new DecompressionStream("deflate-raw");
  const stream = new Blob([data as BlobPart]).stream().pipeThrough(ds);
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

export async function readAvro(
  bytes: Uint8Array,
  limit = Infinity,
): Promise<{ rows: Record<string, unknown>[]; total: number; schema: Schema }> {
  if (bytes[0] !== 0x4f || bytes[1] !== 0x62 || bytes[2] !== 0x6a || bytes[3] !== 1)
    throw new Error("Not an Avro container file");
  const r = new Reader(bytes);
  r.pos = 4;
  const meta: Record<string, string> = {};
  for (let n = r.long(); n !== 0; n = r.long()) {
    if (n < 0) {
      n = -n;
      r.long();
    }
    for (let i = 0; i < n; i++) {
      const k = r.string();
      meta[k] = new TextDecoder().decode(r.bytes());
    }
  }
  const schema = JSON.parse(meta["avro.schema"] ?? "null") as Schema;
  const codec = meta["avro.codec"] ?? "null";
  if (codec !== "null" && codec !== "deflate")
    throw new Error(`Avro codec "${codec}" isn't supported in the browser (use null or deflate)`);
  const sync = r.fixed(16).slice();
  const rows: Record<string, unknown>[] = [];
  let total = 0;
  while (r.pos < bytes.length) {
    const count = r.long();
    let block = r.bytes();
    r.pos += 16;
    void sync;
    total += count;
    if (rows.length >= limit) continue;
    if (codec === "deflate") block = await inflateRaw(block);
    const br = new Reader(block);
    const named = new Map<string, Schema>();
    for (let i = 0; i < count && rows.length < limit; i++) {
      const v = decode(br, schema, named);
      rows.push(
        v && typeof v === "object" && !Array.isArray(v)
          ? (v as Record<string, unknown>)
          : { value: v },
      );
    }
  }
  return { rows, total, schema };
}

/** Avro long/int zig-zag varint encoder — used by tests to build fixtures. */
export function encodeLong(n: number): number[] {
  let z = (BigInt(n) << 1n) ^ (BigInt(n) >> 63n);
  const out: number[] = [];
  do {
    let b = Number(z & 0x7fn);
    z >>= 7n;
    if (z) b |= 0x80;
    out.push(b);
  } while (z);
  return out;
}
