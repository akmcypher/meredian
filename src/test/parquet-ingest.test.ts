// @vitest-environment node
import { describe, expect, it } from "vitest";
import { parquetWriteBuffer } from "hyparquet-writer";
import { MemoryStore, verifyManifest } from "@/lib/file-ingest";
import { planRowGroups, runParquetIngest } from "@/lib/parquet-ingest";

const N = 10_000;
function sample() {
  const id = Array.from({ length: N }, (_, i) => i);
  return new Uint8Array(
    parquetWriteBuffer({
      columnData: [
        { name: "id", data: id, type: "INT32" },
        { name: "name", data: id.map((i) => `n,"${i}"\n`) },
        { name: "big", data: id.map((i) => BigInt(i) * 1_000_000_000n), type: "INT64" },
      ],
      rowGroupSize: 1000,
    }),
  );
}

describe("parquet ingest", () => {
  it("plans contiguous row ranges covering every row", () => {
    const p = planRowGroups([1000, 1000, 1000, 500], 2);
    expect(p).toEqual([
      { idx: 0, start: 0, end: 2000 },
      { idx: 1, start: 2000, end: 3500 },
    ]);
    expect(() => planRowGroups([1], 0)).toThrow();
  });

  it("imports 10k rows across row groups with 4 workers, exactly once, and survives a crash", async () => {
    const s = new MemoryStore();
    await s.put("src.parquet", sample());
    let crashed = false;
    const m = await runParquetIngest(s, "src.parquet", "out", {
      parts: 7,
      concurrency: 4,
      beforeWrite: (i, a) => {
        if (i === 2 && a === 1) {
          crashed = true;
          throw new Error("boom");
        }
      },
    });
    expect(crashed).toBe(true);
    expect(m.totalRows).toBe(N);
    expect(m.columns).toEqual(["id", "name", "big"]);
    const ids: number[] = [];
    for (const p of m.parts)
      for (const l of new TextDecoder()
        .decode((await s.get(p.key))!)
        .split("\n")
        .filter(Boolean)) {
        const r = JSON.parse(l);
        ids.push(r.id);
        if (r.id === 9999) {
          expect(r.big).toBe(9_999_000_000_000);
          expect(r.name).toBe('n,"9999"\n');
        }
      }
    expect(new Set(ids).size).toBe(N);
    expect(ids.length).toBe(N);
    expect((await verifyManifest(s, "out")).ok).toBe(true);
  });
});
