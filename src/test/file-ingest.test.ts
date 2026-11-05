import { describe, it, expect } from "vitest";
import { MemoryStore, runFileIngest, verifyManifest, parseCsvChunk } from "@/lib/file-ingest";

function makeCsv(n: number) {
  const lines = ["id,name,note"];
  for (let i = 0; i < n; i++)
    lines.push(i % 7 === 0 ? `${i},"Name, ${i}","multi\nline ""q"""` : `${i},n${i},x`);
  return new TextEncoder().encode(lines.join("\n") + "\n");
}

describe("multi-worker file ingest", () => {
  it("splits, parses every row exactly once across 4 workers and commits a manifest", async () => {
    const s = new MemoryStore();
    await s.put("src.csv", makeCsv(5000));
    const m = await runFileIngest(s, "src.csv", "out", { parts: 16, concurrency: 4 });
    expect(m.parts).toHaveLength(16);
    expect(m.totalRows).toBe(5000);
    const ids = new Set<string>();
    for (const p of m.parts)
      for (const l of new TextDecoder()
        .decode((await s.get(p.key))!)
        .trim()
        .split("\n"))
        ids.add(JSON.parse(l).id);
    expect(ids.size).toBe(5000);
    expect((await verifyManifest(s, "out")).ok).toBe(true);
  });

  it("retries a crashed worker without duplicating the part", async () => {
    const s = new MemoryStore();
    await s.put("src.csv", makeCsv(700));
    const m = await runFileIngest(s, "src.csv", "out", {
      parts: 5,
      concurrency: 3,
      beforeWrite: (idx, a) => {
        if (idx === 2 && a === 1) throw new Error("crash");
      },
    });
    expect(m.parts[2]!.attempts).toBe(2);
    expect(m.totalRows).toBe(700);
  });

  it("does not commit a manifest when a part keeps failing", async () => {
    const s = new MemoryStore();
    await s.put("src.csv", makeCsv(100));
    await expect(
      runFileIngest(s, "src.csv", "out", {
        parts: 4,
        concurrency: 2,
        maxAttempts: 2,
        beforeWrite: (idx) => {
          if (idx === 1) throw new Error("boom");
        },
      }),
    ).rejects.toThrow(/part 1 failed/);
    expect(await s.get("out/_manifest.json")).toBeNull();
  });

  it("detects a tampered part", async () => {
    const s = new MemoryStore();
    await s.put("src.csv", makeCsv(50));
    const m = await runFileIngest(s, "src.csv", "out", { parts: 2, concurrency: 2 });
    await s.put(m.parts[0]!.key, new TextEncoder().encode("tampered"));
    expect((await verifyManifest(s, "out")).ok).toBe(false);
  });

  it("parses quoted delimiters, escaped quotes and embedded newlines", () => {
    expect(parseCsvChunk('1,"a, b","x\n""y"""\n')).toEqual([["1", "a, b", 'x\n"y"']]);
  });
});

import { toTableName } from "@/lib/file-tables";
import { readFileTablePage } from "@/lib/file-tables";
describe("file tables", () => {
  it("derives a valid table name from a file name", () => {
    expect(toTableName("Sales 2026.csv")).toBe("sales_2026");
    expect(toTableName("2026-orders.csv")).toBe("t_2026_orders");
  });
  it("pages rows across parts without gaps or duplicates", async () => {
    const s = new MemoryStore();
    const csv = "id,v\n" + Array.from({ length: 100 }, (_, i) => `${i},x${i}`).join("\n") + "\n";
    s.objects.set("src.csv", new TextEncoder().encode(csv));
    await runFileIngest(s, "src.csv", "out", { parts: 7, concurrency: 3 });
    const a = await readFileTablePage(s, "out", 0, 40),
      b = await readFileTablePage(s, "out", 40, 40),
      c = await readFileTablePage(s, "out", 80, 40);
    const ids = [...a.rows, ...b.rows, ...c.rows].map((r) => Number(r["id"]));
    expect(ids).toEqual(Array.from({ length: 100 }, (_, i) => i));
    expect(a.total).toBe(100);
  });
});
