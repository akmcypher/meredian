import { sha256 } from "@noble/hashes/sha2.js";

export interface RangeSource {
  size(key: string): Promise<number>;
  readRange(key: string, start: number, end: number): Promise<Uint8Array>;
}

export const SCAN_BYTES = 256 * 1024;
export const MAX_UNIT_BYTES = 8 * 1024 * 1024;

export async function sourceFingerprint(store: RangeSource, key: string): Promise<string> {
  const size = await store.size(key);
  const hash = sha256.create();
  for (let start = 0; start < size; start += SCAN_BYTES) {
    const end = Math.min(size, start + SCAN_BYTES);
    const bytes = await store.readRange(key, start, end);
    if (bytes.length !== end - start) throw new Error("Incomplete source range");
    hash.update(bytes);
  }
  return hash.digest().reduce((s, b) => s + b.toString(16).padStart(2, "0"), "");
}

/** Two bounded scans retain only the header and partition metadata, never all record boundaries. */
export async function planCsvRanges(store: RangeSource, key: string, parts: number) {
  if (!Number.isInteger(parts) || parts < 1 || parts > 10000)
    throw new Error("parts must be between 1 and 10000");
  const size = await store.size(key);
  let headerEnd = 0;
  let header: number[] = [];
  let quoted = false;
  for (let start = 0; start < size && !headerEnd; start += SCAN_BYTES) {
    const bytes = await store.readRange(key, start, Math.min(size, start + SCAN_BYTES));
    for (let i = 0; i < bytes.length; i++) {
      const b = bytes[i];
      if (b === undefined) throw new Error("Incomplete header");
      header.push(b);
      if (header.length > SCAN_BYTES) throw new Error("CSV header exceeds scan budget");
      if (b === 34) quoted = !quoted;
      if (b === 10 && !quoted) {
        headerEnd = start + i + 1;
        break;
      }
    }
  }
  if (!headerEnd) headerEnd = size;
  const ranges: { idx: number; start: number; end: number; rows: number }[] = [];
  let rangeStart = headerEnd,
    rows = 0,
    lastBoundary = headerEnd,
    lastRecord = headerEnd;
  quoted = false;
  const targetBytes = Math.min(
    MAX_UNIT_BYTES / 2,
    Math.max(1, Math.ceil((size - headerEnd) / parts)),
  );
  const boundary = (end: number) => {
    rows++;
    lastRecord = end;
    if (end - rangeStart >= targetBytes || end === size) {
      ranges.push({ idx: ranges.length, start: rangeStart, end, rows });
      if (ranges.length > 10000) throw new Error("CSV exceeds bounded part count");
      rangeStart = end;
      rows = 0;
    }
    lastBoundary = end;
  };
  for (let start = headerEnd; start < size; start += SCAN_BYTES) {
    const bytes = await store.readRange(key, start, Math.min(size, start + SCAN_BYTES));
    for (let i = 0; i < bytes.length; i++) {
      const b = bytes[i];
      if (b === 34) quoted = !quoted;
      if (b === 10 && !quoted) boundary(start + i + 1);
      if (start + i + 1 - lastRecord > SCAN_BYTES)
        throw new Error("CSV record exceeds 256 KiB budget");
    }
  }
  if (quoted) throw new Error("Unterminated CSV quote");
  if (lastBoundary < size) boundary(size);
  if (rows) ranges.push({ idx: ranges.length, start: rangeStart, end: size, rows });
  const headerBytes = new Uint8Array(header);
  header = [];
  return { header: headerBytes, ranges };
}
