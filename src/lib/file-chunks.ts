/**
 * Physical splitting of delimited text files into byte ranges that several workers can read independently.
 * Each range starts at the beginning of a record and ends just after a record terminator, so the union of
 * ranges is the whole file with no record split or duplicated. Quoted fields containing newlines are honoured
 * by scanning from the start (exact, O(n)); this is the correct behaviour for RFC 4180 CSV where a newline
 * inside quotes cannot be told apart from a record end by looking locally.
 */
export type ByteRange = { idx: number; start: number; end: number };

const NL = 10,
  CR = 13,
  QUOTE = 34;

/** Offsets just after each record terminator outside quotes (header excluded when `skipHeader`). */
export function recordBoundaries(bytes: Uint8Array): number[] {
  const out: number[] = [];
  let inQ = false;
  for (let i = 0; i < bytes.length; i++) {
    const b = bytes[i];
    if (b === QUOTE) inQ = !inQ;
    else if (b === NL && !inQ) out.push(i + 1);
  }
  if (bytes.length && out[out.length - 1] !== bytes.length) out.push(bytes.length);
  return out;
}

/** Split into at most `parts` ranges of roughly equal size, aligned to record boundaries. */
export function splitCsv(
  bytes: Uint8Array,
  parts: number,
  opts: { header?: boolean } = {},
): { header: Uint8Array; ranges: ByteRange[] } {
  if (!Number.isInteger(parts) || parts < 1 || parts > 10_000)
    throw new Error("parts must be an integer between 1 and 10000");
  const bounds = recordBoundaries(bytes);
  let dataStart = 0;
  if (opts.header !== false && bounds.length) dataStart = bounds.shift()!;
  const header = bytes.subarray(0, dataStart);
  const size = bytes.length - dataStart;
  const ranges: ByteRange[] = [];
  if (size <= 0) return { header, ranges };
  let start = dataStart,
    bi = 0;
  for (let p = 1; p <= parts && start < bytes.length; p++) {
    const target = p === parts ? bytes.length : dataStart + Math.ceil((size * p) / parts);
    while (bi < bounds.length && bounds[bi]! < target) bi++;
    const end = p === parts ? bytes.length : (bounds[bi] ?? bytes.length);
    if (end > start) {
      ranges.push({ idx: ranges.length, start, end });
      start = end;
    }
  }
  return { header, ranges };
}

/** Number of records in a range (terminated lines outside quotes, plus a trailing unterminated one). */
export function countRecords(bytes: Uint8Array, r: ByteRange): number {
  let n = 0,
    inQ = false,
    last = -1;
  for (let i = r.start; i < r.end; i++) {
    const b = bytes[i];
    if (b === QUOTE) inQ = !inQ;
    else if (b === NL && !inQ) {
      n++;
      last = i;
    }
  }
  if (r.end > r.start && last !== r.end - 1 && !(last === r.end - 2 && bytes[r.end - 1] === CR))
    n++;
  return n;
}

/** FNV-1a 32-bit over bytes — used to prove the ranges reassemble the exact file. */
export function fnv1a(bytes: Uint8Array): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < bytes.length; i++) {
    h ^= bytes[i]!;
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}
