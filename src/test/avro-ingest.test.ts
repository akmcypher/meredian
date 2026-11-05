// @vitest-environment node
import { describe, expect, it } from "vitest";
import { MemoryStore, verifyManifest } from "@/lib/file-ingest";
import { planAvro, runAvroIngest } from "@/lib/avro-ingest";

// Minimal independent Avro container writer used only to produce test files.
const zz = (n: number) => {
  let v = (BigInt(n) << 1n) ^ (BigInt(n) >> 63n);
  const o: number[] = [];
  while (v > 0x7fn) {
    o.push(Number((v & 0x7fn) | 0x80n));
    v >>= 7n;
  }
  o.push(Number(v));
  return o;
};
const str = (s: string) => {
  const b = new TextEncoder().encode(s);
  return [...zz(b.length), ...b];
};
const dbl = (d: number) => {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setFloat64(0, d, true);
  return [...b];
};
async function deflate(b: Uint8Array) {
  return new Uint8Array(
    await new Response(
      new Blob([b as BlobPart]).stream().pipeThrough(new CompressionStream("deflate-raw")),
    ).arrayBuffer(),
  );
}
const schema = {
  type: "record",
  name: "Sale",
  fields: [
    { name: "id", type: "long" },
    { name: "note", type: ["null", "string"] },
    { name: "amount", type: "double" },
    { name: "tier", type: { type: "enum", name: "Tier", symbols: ["gold", "silver"] } },
    { name: "tags", type: { type: "array", items: "string" } },
  ],
};
async function file(rows: number, perBlock: number, codec: "null" | "deflate") {
  const sync = Array.from({ length: 16 }, (_, i) => i * 7 + 3);
  const out: number[] = [
    0x4f,
    0x62,
    0x6a,
    1,
    ...zz(2),
    ...str("avro.schema"),
    ...str(JSON.stringify(schema)),
    ...str("avro.codec"),
    ...str(codec),
    0,
    ...sync,
  ];
  for (let s = 0; s < rows; s += perBlock) {
    const body: number[] = [];
    for (let i = s; i < Math.min(rows, s + perBlock); i++) {
      body.push(
        ...zz(i * 1_000_003),
        ...(i % 5 ? [...zz(1), ...str(`n,"${i}"\n`)] : zz(0)),
        ...dbl(i * 1.5),
        ...zz(i % 2),
        ...(i % 3 ? [...zz(1), ...str(`t${i}`), 0] : [0]),
      );
    }
    let data = new Uint8Array(body);
    if (codec === "deflate") data = await deflate(data);
    out.push(...zz(Math.min(perBlock, rows - s)), ...zz(data.length), ...data, ...sync);
  }
  return new Uint8Array(out);
}

describe("avro ingest", () => {
  for (const codec of ["null", "deflate"] as const) {
    it(`imports 6,000 rows (${codec}) across blocks with 4 workers exactly once, surviving a crash`, async () => {
      const s = new MemoryStore();
      await s.put("x.avro", await file(6000, 400, codec));
      let crashed = false;
      const m = await runAvroIngest(s, "x.avro", "out", {
        parts: 5,
        concurrency: 4,
        beforeWrite: (i, a) => {
          if (i === 1 && a === 1) {
            crashed = true;
            throw new Error("boom");
          }
        },
      });
      expect(crashed).toBe(true);
      expect(m.totalRows).toBe(6000);
      expect(m.columns).toEqual(["id", "note", "amount", "tier", "tags"]);
      const rows = (
        await Promise.all(
          m.parts.map(async (p) =>
            new TextDecoder()
              .decode((await s.get(p.key))!)
              .split("\n")
              .filter(Boolean)
              .map((l) => JSON.parse(l)),
          ),
        )
      ).flat();
      expect(new Set(rows.map((r) => r.id)).size).toBe(6000);
      expect(rows[7]).toEqual({
        id: 7_000_021,
        note: 'n,"7"\n',
        amount: 10.5,
        tier: "silver",
        tags: ["t7"],
      });
      expect(rows[0].note).toBeNull();
      expect((await verifyManifest(s, "out")).ok).toBe(true);
    });
  }
  it("rejects corrupt files and unsupported codecs", async () => {
    const f = await file(100, 50, "null");
    f[f.length - 1] = f[f.length - 1]! ^ 0xff;
    expect(() => planAvro(f)).toThrow(/sync marker/);
    expect(() => planAvro(new Uint8Array([1, 2, 3, 4]))).toThrow(/not an Avro/);
  });
});
