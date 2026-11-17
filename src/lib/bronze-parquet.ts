// Bronze parts as Parquet (layout meridian-bronze-parquet/1, written by deploy/connect/bronze_parquet.py):
// the same meridian-connect/1 envelopes as an NDJSON part, in columns. Envelope fields that are the
// same for the whole part live in the file's `meridian.bronze` key-value metadata; each record has
// `_meridian_operation`, `_meridian_primary_key` and `_meridian_cursor` (JSON text) and one column
// per data field, typed (bool, int64, string; from the stream schema) or JSON text. Decoding checks the layout and
// gives back the envelopes, so a Parquet part is verified and read like an NDJSON one.
import { parquetMetadata, parquetReadObjects, parquetSchema } from "hyparquet";

export const BRONZE_PARQUET_LAYOUT = "meridian-bronze-parquet/1";
const OPERATION = "_meridian_operation";
const PRIMARY_KEY = "_meridian_primary_key";
const CURSOR = "_meridian_cursor";
const CONSTANT = [
  "protocol",
  "connection_id",
  "run_id",
  "stream_id",
  "schema",
  "extracted_at",
  "source_namespace",
  "source_stream",
] as const;
const PHYSICAL: Record<string, string> = {
  bool: "BOOLEAN",
  int64: "INT64",
  string: "BYTE_ARRAY",
  json: "BYTE_ARRAY",
};

export function isParquet(bytes: Uint8Array): boolean {
  return bytes.length >= 4 && String.fromCharCode(...bytes.subarray(0, 4)) === "PAR1";
}

function invalid(): never {
  throw new Error("invalid_bronze_parquet");
}

function text(v: unknown): string {
  if (typeof v !== "string") invalid();
  return v;
}

/** The envelopes of a Parquet Bronze part, in order. Throws `invalid_bronze_parquet` when the file
 * is not this layout. INT64 values become numbers, as JSON.parse of an NDJSON part would give. */
export async function bronzeParquetEnvelopes(buffer: ArrayBuffer) {
  const metadata = parquetMetadata(buffer);
  const raw = metadata.key_value_metadata?.find((kv) => kv.key === "meridian.bronze")?.value;
  if (!raw) invalid();
  const meta = JSON.parse(raw) as Record<string, unknown>;
  if (meta["layout"] !== BRONZE_PARQUET_LAYOUT || meta["protocol"] !== "meridian-connect/1")
    invalid();
  const kinds = meta["columns"];
  if (!kinds || typeof kinds !== "object" || Array.isArray(kinds)) invalid();
  const columns = Object.entries(kinds as Record<string, unknown>);
  if (columns.some(([, k]) => typeof k !== "string" || !(k in PHYSICAL))) invalid();
  const expected = [[OPERATION, "string"], [PRIMARY_KEY, "json"], [CURSOR, "json"], ...columns] as [
    string,
    string,
  ][];
  const tree = parquetSchema(metadata);
  if (tree.children.length !== expected.length) invalid();
  tree.children.forEach((c, i) => {
    const [name, kind] = expected[i]!;
    if (c.element.name !== name || c.children.length || c.element.type !== PHYSICAL[kind])
      invalid();
  });
  const rows = (await parquetReadObjects({ file: buffer })) as Record<string, unknown>[];
  if (rows.length !== Number(metadata.num_rows)) invalid();
  const constant = Object.fromEntries(CONSTANT.map((k) => [k, meta[k]]));
  return rows.map((row) => {
    const data: Record<string, unknown> = {};
    for (const [name, kind] of columns) {
      const v = row[name];
      if (v === null || v === undefined) data[name] = null;
      else if (kind === "json") data[name] = JSON.parse(text(v));
      else if (kind === "int64") {
        if (typeof v !== "bigint") invalid();
        data[name] = Number(v);
      } else if (kind === "bool") {
        if (typeof v !== "boolean") invalid();
        data[name] = v;
      } else data[name] = text(v);
    }
    return {
      ...constant,
      operation: text(row[OPERATION]),
      primary_key: JSON.parse(text(row[PRIMARY_KEY])) as unknown,
      cursor: JSON.parse(text(row[CURSOR])) as unknown,
      data,
    } as Record<string, unknown>;
  });
}
