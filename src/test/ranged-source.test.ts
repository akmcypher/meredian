import { describe, expect, it } from "vitest";
import { sourceFingerprint, planCsvRanges, SCAN_BYTES } from "@/lib/ranged-source";
import { MemoryStore, sha256Hex, runFileIngest } from "@/lib/file-ingest";

describe("bounded source scans", () => {
  it("hashes concatenated bytes incrementally with reads bounded at 256 KiB", async () => {
    const bytes = new Uint8Array(SCAN_BYTES * 5 + 17).map((_, i) => i % 251);
    let largest = 0;
    const store = {
      size: async () => bytes.length,
      readRange: async (_: string, start: number, end: number) => {
        largest = Math.max(largest, end - start);
        return bytes.slice(start, end);
      },
    };
    expect(await sourceFingerprint(store, "source")).toBe(await sha256Hex(bytes));
    expect(largest).toBe(SCAN_BYTES);
  });
  it("handles escaped quotes and multiline records across scan boundaries without losing rows", async () => {
    const bytes = new TextEncoder().encode(
      "id,note\n" + Array.from({ length: 40000 }, (_, i) => `${i},"hello\n""world"""\n`).join(""),
    );
    const store = new MemoryStore();
    await store.put("source", bytes);
    const plan = await planCsvRanges(store, "source", 7);
    expect(plan.ranges.reduce((n, r) => n + r.rows, 0)).toBe(40000);
    expect(plan.ranges[0]?.start).toBe(8);
    expect(plan.ranges.at(-1)?.end).toBe(bytes.length);
    const manifest = await runFileIngest(store, "source", "out", { parts: 7, concurrency: 2 });
    expect(manifest.totalRows).toBe(40000);
  });
  it("rejects a record exceeding 256 KiB rather than allocating without bound", async () => {
    const store = new MemoryStore();
    await store.put(
      "source",
      new TextEncoder().encode('id,note\n1,"' + "x".repeat(SCAN_BYTES) + '"\n'),
    );
    await expect(planCsvRanges(store, "source", 1)).rejects.toThrow(/record exceeds/);
  });
});
