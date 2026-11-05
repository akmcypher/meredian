import { describe, expect, it } from "vitest";
import { countRecords, fnv1a, splitCsv } from "@/lib/file-chunks";

const enc = (s: string) => new TextEncoder().encode(s);
function concat(parts: Uint8Array[]) {
  const out = new Uint8Array(parts.reduce((a, p) => a + p.length, 0));
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
}

describe("physical CSV splitting", () => {
  const rows = Array.from({ length: 10_000 }, (_, i) =>
    i % 97 === 0 ? `${i},"multi\nline, ""quoted""",${-i}` : `${i},name${i},${-i}`,
  );
  const file = enc("id,name,v\n" + rows.join("\n") + "\n");

  it.each([1, 2, 7, 16, 64])(
    "splits into %i parts that reassemble byte-for-byte with every record counted once",
    (parts) => {
      const { header, ranges } = splitCsv(file, parts);
      expect(new TextDecoder().decode(header)).toBe("id,name,v\n");
      expect(ranges.length).toBeLessThanOrEqual(parts);
      for (let i = 1; i < ranges.length; i++) expect(ranges[i]!.start).toBe(ranges[i - 1]!.end);
      expect(fnv1a(concat([header, ...ranges.map((r) => file.subarray(r.start, r.end))]))).toBe(
        fnv1a(file),
      );
      expect(ranges.reduce((a, r) => a + countRecords(file, r), 0)).toBe(10_000);
    },
  );

  it("never cuts inside a quoted multi-line field", () => {
    const { ranges } = splitCsv(file, 64);
    for (const r of ranges) {
      const text = new TextDecoder().decode(file.subarray(r.start, r.end));
      expect((text.match(/"/g) ?? []).length % 2).toBe(0);
    }
  });

  it("handles no trailing newline, header-only and invalid part counts", () => {
    const f = enc("a\n1\n2\n3");
    const { ranges } = splitCsv(f, 2);
    expect(ranges.reduce((a, r) => a + countRecords(f, r), 0)).toBe(3);
    expect(splitCsv(enc("a,b\n"), 4).ranges).toHaveLength(0);
    expect(() => splitCsv(f, 0)).toThrow();
  });
});
