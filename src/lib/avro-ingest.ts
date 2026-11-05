import { sha256Hex, type Manifest, type ObjectStore, type PartResult } from "./file-ingest";
import { jsonSafe } from "./parquet-ingest";
import { sourceFingerprint, SCAN_BYTES, MAX_UNIT_BYTES } from "./ranged-source";
import {
  parseAvroSchema,
  promote,
  resolveSchemas,
  type AvroNode,
  type ResolutionPlan,
} from "./avro-schema";

/** Avro schema (subset of the spec we decode). */
type Schema =
  | string
  | {
      type: string;
      name?: string;
      fields?: { name: string; type: Schema }[];
      symbols?: string[];
      items?: Schema;
      values?: Schema;
      size?: number;
    }
  | Schema[];

class Reader {
  pos = 0;
  constructor(
    public b: Uint8Array,
    start = 0,
  ) {
    this.pos = start;
  }
  long(): number {
    let n = 0n,
      shift = 0n,
      byte: number;
    do {
      if (this.pos >= this.b.length) throw new Error("unexpected end of Avro data");
      byte = this.b[this.pos++]!;
      n |= BigInt(byte & 0x7f) << shift;
      shift += 7n;
    } while (byte & 0x80);
    const v = (n >> 1n) ^ -(n & 1n);
    return Number.isSafeInteger(Number(v))
      ? Number(v)
      : (() => {
          throw new Error("Avro long outside safe integer range");
        })();
  }
  bytes(n = this.long()) {
    if (!Number.isSafeInteger(n) || n < 0 || n > MAX_UNIT_BYTES)
      throw new Error("Avro value exceeds bounded decoder budget");
    const s = this.b.subarray(this.pos, this.pos + n);
    if (s.length !== n) throw new Error("unexpected end of Avro data");
    this.pos += n;
    return s;
  }
  string() {
    return new TextDecoder().decode(this.bytes());
  }
}

function decode(r: Reader, s: Schema, named: Map<string, Schema>): unknown {
  if (Array.isArray(s)) {
    const i = r.long();
    const t = s[i];
    if (t === undefined) throw new Error(`bad union index ${i}`);
    return decode(r, t, named);
  }
  const t = typeof s === "string" ? s : s.type;
  switch (t) {
    case "null":
      return null;
    case "boolean":
      return r.bytes(1)[0] === 1;
    case "int":
    case "long":
      return r.long();
    case "float": {
      const v = new DataView(r.b.buffer, r.b.byteOffset + r.pos, 4).getFloat32(0, true);
      r.pos += 4;
      return v;
    }
    case "double": {
      const v = new DataView(r.b.buffer, r.b.byteOffset + r.pos, 8).getFloat64(0, true);
      r.pos += 8;
      return v;
    }
    case "bytes":
      return r.bytes();
    case "string":
      return r.string();
  }
  if (typeof s === "string") {
    const n = named.get(s);
    if (!n) throw new Error(`unknown Avro type ${s}`);
    return decode(r, n, named);
  }
  if (Array.isArray(s)) throw new Error("unreachable");
  if (s.name) named.set(s.name, s);
  switch (s.type) {
    case "record":
      return Object.fromEntries((s.fields ?? []).map((f) => [f.name, decode(r, f.type, named)]));
    case "enum":
      return s.symbols![r.long()];
    case "fixed":
      return r.bytes(s.size!);
    case "array":
    case "map": {
      const out: unknown[] = [];
      const obj: Record<string, unknown> = {};
      for (let n = r.long(); n !== 0; n = r.long()) {
        if (n < 0) {
          n = -n;
          r.long();
        }
        for (let i = 0; i < n; i++) {
          if (s.type === "map") {
            const k = r.string();
            obj[k] = decode(r, s.values!, named);
          } else out.push(decode(r, s.items!, named));
        }
      }
      return s.type === "map" ? obj : out;
    }
    default:
      return decode(r, s.type as Schema, named);
  }
}

/** Decodes one value written with `node` (used to skip writer fields the reader dropped). */
function decodeNode(r: Reader, node: AvroNode): unknown {
  switch (node.t) {
    case "null":
      return null;
    case "boolean":
      return r.bytes(1)[0] === 1;
    case "int":
    case "long":
      return r.long();
    case "float":
    case "double": {
      const width = node.t === "float" ? 4 : 8;
      if (r.pos + width > r.b.length) throw new Error("unexpected end of Avro data");
      const view = new DataView(r.b.buffer, r.b.byteOffset + r.pos, width);
      r.pos += width;
      return width === 4 ? view.getFloat32(0, true) : view.getFloat64(0, true);
    }
    case "bytes":
      return r.bytes();
    case "string":
      return r.string();
    case "fixed":
      return r.bytes(node.size);
    case "enum":
      return node.symbols[r.long()];
    case "union": {
      const branch = node.branches[r.long()];
      if (!branch) throw new Error("bad union index");
      return decodeNode(r, branch);
    }
    case "record":
      return Object.fromEntries(node.fields.map((f) => [f.name, decodeNode(r, f.type)]));
    case "array":
    case "map":
      return decodeBlocks(r, node.t, (x) =>
        decodeNode(x, node.t === "array" ? node.items : node.values),
      );
  }
}

function decodeBlocks(r: Reader, kind: "array" | "map", item: (r: Reader) => unknown) {
  const out: unknown[] = [];
  const obj: Record<string, unknown> = {};
  for (let n = r.long(); n !== 0; n = r.long()) {
    if (n < 0) {
      n = -n;
      r.long();
    }
    for (let i = 0; i < n; i++) {
      if (kind === "map") {
        const k = r.string();
        obj[k] = item(r);
      } else out.push(item(r));
    }
  }
  return kind === "map" ? obj : out;
}

/** Decodes writer bytes directly into the reader schema's shape (Avro schema resolution). */
function decodeResolved(r: Reader, plan: ResolutionPlan): unknown {
  switch (plan.k) {
    case "prim":
      return promote(decodeNode(r, { t: plan.from }), plan.from, plan.to);
    case "fixed":
      return r.bytes(plan.size);
    case "enum": {
      const symbol = plan.map[r.long()];
      if (symbol == null) throw new Error("Avro enum symbol unknown to the reader schema");
      return symbol;
    }
    case "writer-union": {
      const branch = plan.branches[r.long()];
      if (!branch) throw new Error("bad union index");
      if ("error" in branch) throw new Error(`Avro value unreadable: ${branch.error}`);
      return decodeResolved(r, branch);
    }
    case "array":
    case "map":
      return decodeBlocks(r, plan.k, (x) =>
        decodeResolved(x, plan.k === "array" ? plan.items : plan.values),
      );
    case "record": {
      const values: Record<string, unknown> = {};
      for (const step of plan.steps)
        if ("skip" in step) decodeNode(r, step.skip);
        else values[step.field] = decodeResolved(r, step.plan);
      for (const d of plan.defaults) values[d.field] = structuredClone(d.value);
      return Object.fromEntries(plan.order.map((name) => [name, values[name]]));
    }
  }
}

export type AvroBlock = { idx: number; start: number; end: number; count: number };
export type AvroHeader = {
  schema: Schema;
  codec: string;
  sync: Uint8Array;
  dataStart: number;
  columns: string[];
  /** Set when rows are read through a different (evolved) reader schema. */
  plan?: ResolutionPlan;
};

/**
 * Reads the file through `readerSchema` instead of its own writer schema: added fields take their
 * defaults, removed fields are dropped, types are promoted and renamed fields follow aliases.
 * Throws, before any row is decoded, when the file's schema cannot be read that way.
 */
export function withReaderSchema(header: AvroHeader, readerSchema: unknown): AvroHeader {
  const reader = parseAvroSchema(readerSchema);
  const { plan, issues } = resolveSchemas(parseAvroSchema(header.schema), reader);
  if (issues.length)
    throw new Error(
      `Avro file schema is not compatible with the table schema: ${issues
        .slice(0, 5)
        .map((i) => `${i.path}: ${i.message}`)
        .join("; ")}`,
    );
  const columns = reader.t === "record" ? reader.fields.map((f) => f.name) : ["value"];
  return { ...header, plan, columns };
}

/** Parse the container header and walk block headers (count + size + sync) without decoding rows. */
function parseHeader(b: Uint8Array): AvroHeader {
  if (b[0] !== 0x4f || b[1] !== 0x62 || b[2] !== 0x6a || b[3] !== 1)
    throw new Error("not an Avro container file");
  const r = new Reader(b, 4);
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
  const sync = r.bytes(16).slice();
  const schema = JSON.parse(meta["avro.schema"] ?? "null") as Schema;
  const codec = meta["avro.codec"] ?? "null";
  if (codec !== "null" && codec !== "deflate")
    throw new Error(`Avro codec "${codec}" is not supported (use null or deflate)`);
  const columns =
    typeof schema === "object" && !Array.isArray(schema) && schema.type === "record"
      ? (schema.fields ?? []).map((f) => f.name)
      : ["value"];
  const header: AvroHeader = { schema, codec, sync, dataStart: r.pos, columns };
  return header;
}

export function planAvro(b: Uint8Array): { header: AvroHeader; blocks: AvroBlock[] } {
  const header = parseHeader(b);
  const { sync } = header;
  const r = new Reader(b, header.dataStart);
  const blocks: AvroBlock[] = [];
  while (r.pos < b.length) {
    const start = r.pos;
    const count = r.long();
    const size = r.long();
    if (count < 0 || size < 0) throw new Error("invalid Avro block");
    r.pos += size;
    const s = b.subarray(r.pos, r.pos + 16);
    if (s.length !== 16 || s.some((x, i) => x !== sync[i]))
      throw new Error(`Avro block ${blocks.length}: sync marker mismatch (corrupt file)`);
    r.pos += 16;
    blocks.push({ idx: blocks.length, start, end: r.pos, count });
  }
  return { header, blocks };
}

async function inflateOnce(b: Uint8Array): Promise<Uint8Array> {
  const st = new Blob([b as BlobPart]).stream().pipeThrough(new DecompressionStream("deflate-raw"));
  const reader = st.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      total += value.length;
      if (total > MAX_UNIT_BYTES) throw new Error("Avro decompressed block exceeds 8 MiB budget");
      chunks.push(value);
    }
  } finally {
    await reader.cancel();
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}
/** Raw deflate. Some writers (e.g. fastavro) leave up to 4 checksum bytes after the stream; we trim them (row counts are still verified). */
async function inflateRaw(b: Uint8Array): Promise<Uint8Array> {
  for (let trim = 0; ; trim++) {
    try {
      return await inflateOnce(trim ? b.subarray(0, b.length - trim) : b);
    } catch (e) {
      if (trim >= 4)
        throw new Error(`deflate block could not be decompressed: ${(e as Error).message}`);
    }
  }
}

/** Decode a contiguous byte range holding whole blocks. */
export async function decodeAvroRange(
  chunk: Uint8Array,
  h: AvroHeader,
): Promise<Record<string, unknown>[]> {
  const r = new Reader(chunk);
  const rows: Record<string, unknown>[] = [];
  const named = new Map<string, Schema>();
  while (r.pos < chunk.length) {
    const count = r.long();
    let data = r.bytes();
    r.bytes(16);
    if (h.codec === "deflate") data = await inflateRaw(data);
    const br = new Reader(data);
    for (let i = 0; i < count; i++) {
      const v = h.plan ? decodeResolved(br, h.plan) : decode(br, h.schema, named);
      rows.push(
        h.columns[0] === "value" && !(v && typeof v === "object")
          ? { value: v }
          : (v as Record<string, unknown>),
      );
    }
  }
  return rows;
}

/** Group blocks into at most `parts` contiguous byte ranges with roughly equal row counts. */
export function groupBlocks(blocks: AvroBlock[], parts: number): AvroBlock[] {
  if (!Number.isInteger(parts) || parts < 1) throw new Error("parts must be a positive integer");
  const total = blocks.reduce((s, x) => s + x.count, 0);
  const target = Math.max(1, Math.ceil(total / parts));
  const out: AvroBlock[] = [];
  let cur = null as AvroBlock | null;
  for (const b of blocks) {
    const prev: AvroBlock | null = cur;
    if (prev && (b.end - prev.start > MAX_UNIT_BYTES || prev.count + b.count > 10000)) {
      out.push(prev);
      cur = null;
    }
    const base = cur;
    cur = base
      ? { idx: base.idx, start: base.start, end: b.end, count: base.count + b.count }
      : { idx: out.length, start: b.start, end: b.end, count: b.count };
    if (cur.count >= target) {
      out.push(cur);
      cur = null;
    }
  }
  if (cur) out.push(cur);
  return out;
}

/** Import an Avro container file: blocks become parallel parts, same manifest format as CSV/Parquet runs. */
export async function runAvroIngest(
  store: ObjectStore,
  sourceKey: string,
  outPrefix: string,
  opts: {
    parts: number;
    concurrency: number;
    maxAttempts?: number;
    beforeWrite?: (idx: number, attempt: number) => void;
    /** Evolved table schema to read this file through (see withReaderSchema). */
    readerSchema?: unknown;
  },
): Promise<Manifest> {
  const maxAttempts = opts.maxAttempts ?? 3;
  const size = await store.size(sourceKey);
  const sourceSha256 = await sourceFingerprint(store, sourceKey);
  const parsed = parseHeader(await store.readRange(sourceKey, 0, Math.min(SCAN_BYTES, size)));
  const header = opts.readerSchema ? withReaderSchema(parsed, opts.readerSchema) : parsed;
  const blocks: AvroBlock[] = [];
  for (let start = header.dataStart; start < size;) {
    const r = new Reader(await store.readRange(sourceKey, start, Math.min(start + 20, size)));
    const count = r.long(),
      bytes = r.long();
    if (count < 1 || count > 10000 || bytes < 0 || bytes > MAX_UNIT_BYTES - 36)
      throw new Error("Avro block exceeds bounded decoder budget");
    const end = start + r.pos + bytes + 16;
    if (end > size) throw new Error("Truncated Avro block");
    const sync = await store.readRange(sourceKey, end - 16, end);
    if (sync.some((b, i) => b !== header.sync[i])) throw new Error("Avro sync marker mismatch");
    blocks.push({ idx: blocks.length, start, end, count });
    if (blocks.length > 10000) throw new Error("Avro exceeds bounded block count");
    start = end;
  }
  const ranges = groupBlocks(blocks, opts.parts);
  const results: PartResult[] = [];
  const queue = [...ranges];
  const worker = async () => {
    for (let g = queue.shift(); g; g = queue.shift()) {
      for (let attempt = 1; ; attempt++) {
        try {
          const rows = await decodeAvroRange(
            await store.readRange(sourceKey, g.start, g.end),
            header,
          );
          if (rows.length !== g.count)
            throw new Error(`part ${g.idx}: decoded ${rows.length} rows, expected ${g.count}`);
          const body = new TextEncoder().encode(
            rows.map((x) => JSON.stringify(jsonSafe(x))).join("\n") + (rows.length ? "\n" : ""),
          );
          if (body.length > MAX_UNIT_BYTES) throw new Error("Avro output exceeds 8 MiB budget");
          opts.beforeWrite?.(g.idx, attempt);
          const key = `${outPrefix}/part-${String(g.idx).padStart(5, "0")}.ndjson`;
          await store.put(key, body);
          const back = await store.get(key);
          const sha = await sha256Hex(body);
          if (!back || (await sha256Hex(back)) !== sha)
            throw new Error(`part ${g.idx}: checksum mismatch after write`);
          results.push({
            idx: g.idx,
            key,
            rows: rows.length,
            expectedRows: g.count,
            sha256: sha,
            bytes: body.length,
            attempts: attempt,
          });
          break;
        } catch (e) {
          if (attempt >= maxAttempts)
            throw new Error(
              `part ${g.idx} failed after ${attempt} attempts: ${(e as Error).message}`,
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
    columns: header.columns,
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
