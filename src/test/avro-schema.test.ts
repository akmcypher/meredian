// @vitest-environment node
import { describe, expect, it } from "vitest";
import {
  avroColumns,
  checkCompatibility,
  parseAvroSchema,
  resolveSchemas,
  type CompatibilityMode,
} from "@/lib/avro-schema";
import { MemoryStore } from "@/lib/file-ingest";
import { runAvroIngest } from "@/lib/avro-ingest";

const record = (fields: object[], extra: object = {}) =>
  parseAvroSchema({ type: "record", name: "Customer", namespace: "crm", fields, ...extra });
const compatible = (next: object[], prev: object[][], mode: CompatibilityMode) =>
  checkCompatibility(
    record(next),
    prev.map((f) => record(f)),
    mode,
  ).compatible;
const id = { name: "id", type: "long" };

describe("parseAvroSchema", () => {
  it("rejects schemas the spec forbids", () => {
    expect(() => parseAvroSchema('"nope"')).toThrow(/Unknown Avro type/);
    expect(() => record([id, { name: "id", type: "int" }])).toThrow(/repeats id/);
    expect(() => record([{ name: "n", type: "int", default: "x" }])).toThrow(/Default for/);
    expect(() => parseAvroSchema([["null"], "int"])).toThrow(/unions may not contain unions/);
    expect(() => parseAvroSchema({ type: "enum", name: "E", symbols: ["a", "a"] })).toThrow(
      /repeats a symbol/,
    );
  });

  it("ties a union default to the first branch", () => {
    expect(() => record([{ name: "n", type: ["null", "string"], default: null }])).not.toThrow();
    expect(() => record([{ name: "n", type: ["null", "string"], default: "x" }])).toThrow();
  });

  it("resolves recursive named types", () => {
    const list = parseAvroSchema({
      type: "record",
      name: "Node",
      fields: [
        { name: "value", type: "int" },
        { name: "next", type: ["null", "Node"] },
      ],
    });
    expect(resolveSchemas(list, list).issues).toEqual([]);
  });
});

describe("compatibility", () => {
  it("adding a field needs a default for BACKWARD", () => {
    expect(
      compatible([id, { name: "email", type: "string", default: "" }], [[id]], "BACKWARD"),
    ).toBe(true);
    const r = checkCompatibility(
      record([id, { name: "email", type: "string" }]),
      [record([id])],
      "BACKWARD",
    );
    expect(r.compatible).toBe(false);
    expect(r.issues[0]).toMatchObject({ path: "$.email", version: 1 });
  });

  it("removing a field is BACKWARD-compatible but breaks FORWARD unless it had a default", () => {
    const old = [id, { name: "email", type: "string" }];
    expect(compatible([id], [old], "BACKWARD")).toBe(true);
    expect(compatible([id], [old], "FORWARD")).toBe(false);
    expect(compatible([id], [[id, { name: "email", type: "string", default: "" }]], "FULL")).toBe(
      true,
    );
  });

  it("allows Avro promotions only in the widening direction", () => {
    expect(
      compatible([{ name: "id", type: "long" }], [[{ name: "id", type: "int" }]], "BACKWARD"),
    ).toBe(true);
    expect(
      compatible([{ name: "id", type: "int" }], [[{ name: "id", type: "long" }]], "BACKWARD"),
    ).toBe(false);
    expect(
      compatible([{ name: "b", type: "bytes" }], [[{ name: "b", type: "string" }]], "FULL"),
    ).toBe(true);
  });

  it("follows aliases for renamed fields and records", () => {
    const renamed = [{ name: "customer_id", type: "long", aliases: ["id"] }];
    expect(compatible(renamed, [[id]], "BACKWARD")).toBe(true);
    const old = parseAvroSchema({ type: "record", name: "Client", fields: [id] });
    const next = parseAvroSchema({
      type: "record",
      name: "Customer",
      aliases: ["Client"],
      fields: [id],
    });
    expect(checkCompatibility(next, [old], "BACKWARD").compatible).toBe(true);
  });

  it("enum symbols: adding is BACKWARD-safe, FORWARD needs a reader default", () => {
    const tier = (symbols: string[], def?: string) => [
      id,
      {
        name: "tier",
        type: { type: "enum", name: "Tier", symbols, ...(def ? { default: def } : {}) },
      },
    ];
    expect(
      compatible(tier(["gold", "silver", "bronze"]), [tier(["gold", "silver"])], "BACKWARD"),
    ).toBe(true);
    expect(
      compatible(tier(["gold", "silver", "bronze"]), [tier(["gold", "silver"])], "FORWARD"),
    ).toBe(false);
    expect(
      compatible(
        tier(["gold", "silver", "bronze"]),
        [tier(["gold", "silver"], "silver")],
        "FORWARD",
      ),
    ).toBe(true);
  });

  it("making a field nullable is BACKWARD-safe, making it required is not", () => {
    const required = [id, { name: "email", type: "string" }];
    const optional = [id, { name: "email", type: ["null", "string"], default: null }];
    expect(compatible(optional, [required], "BACKWARD")).toBe(true);
    expect(compatible(required, [optional], "BACKWARD")).toBe(false);
  });

  it("transitive modes check every earlier version, not just the latest", () => {
    const v1 = [id];
    const v2 = [id, { name: "x", type: "int", default: 1 }];
    const v3 = [id, { name: "x", type: "int" }];
    expect(compatible(v3, [v1, v2], "BACKWARD")).toBe(true);
    expect(compatible(v3, [v1, v2], "BACKWARD_TRANSITIVE")).toBe(false);
    expect(compatible(v3, [v1, v2], "NONE")).toBe(true);
  });

  it("lists columns for the schema diff view", () => {
    expect(
      avroColumns(record([id, { name: "email", type: ["null", "string"], default: null }])),
    ).toEqual([
      { name: "id", type: "long", nullable: false },
      { name: "email", type: "string", nullable: true },
    ]);
  });
});

// Minimal Avro container writer for v1 files.
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
const v1 = {
  type: "record",
  name: "Customer",
  namespace: "crm",
  fields: [
    { name: "id", type: "long" },
    { name: "name", type: "string" },
    { name: "score", type: "int" },
    { name: "legacy_code", type: "string" },
    { name: "tier", type: { type: "enum", name: "Tier", symbols: ["gold", "silver"] } },
  ],
};
function v1File(rows: number) {
  const sync = Array.from({ length: 16 }, (_, i) => i * 11 + 1);
  const body: number[] = [];
  for (let i = 1; i <= rows; i++)
    body.push(...zz(i), ...str(`customer ${i}`), ...zz(i * 10), ...str(`L${i}`), ...zz(i % 2));
  return new Uint8Array([
    0x4f,
    0x62,
    0x6a,
    1,
    ...zz(2),
    ...str("avro.schema"),
    ...str(JSON.stringify(v1)),
    ...str("avro.codec"),
    ...str("null"),
    0,
    ...sync,
    ...zz(rows),
    ...zz(body.length),
    ...body,
    ...sync,
  ]);
}
const v2 = {
  type: "record",
  name: "Customer",
  namespace: "crm",
  fields: [
    { name: "id", type: "long" },
    { name: "full_name", type: "string", aliases: ["name"] },
    { name: "score", type: "double" },
    { name: "country", type: "string", default: "unknown" },
    { name: "tier", type: { type: "enum", name: "Tier", symbols: ["gold", "silver", "bronze"] } },
    { name: "email", type: ["null", "string"], default: null },
  ],
};

describe("reading old Avro files through an evolved schema", () => {
  it("renames, promotes, drops and defaults fields without rewriting the file", async () => {
    const store = new MemoryStore();
    await store.put("v1.avro", v1File(3));
    const manifest = await runAvroIngest(store, "v1.avro", "out", {
      parts: 1,
      concurrency: 1,
      readerSchema: v2,
    });
    expect(manifest.columns).toEqual(["id", "full_name", "score", "country", "tier", "email"]);
    const rows = new TextDecoder()
      .decode((await store.get(manifest.parts[0]!.key))!)
      .trim()
      .split("\n")
      .map((l) => JSON.parse(l));
    expect(rows[1]).toEqual({
      id: 2,
      full_name: "customer 2",
      score: 20,
      country: "unknown",
      tier: "gold",
      email: null,
    });
    expect(rows.map((r) => r.tier)).toEqual(["silver", "gold", "silver"]);
  });

  it("refuses an incompatible table schema before writing anything", async () => {
    const store = new MemoryStore();
    await store.put("v1.avro", v1File(3));
    const breaking = { ...v2, fields: [...v2.fields, { name: "region", type: "string" }] };
    await expect(
      runAvroIngest(store, "v1.avro", "out", { parts: 1, concurrency: 1, readerSchema: breaking }),
    ).rejects.toThrow(/\$\.region: field region is new and has no default/);
    expect(await store.get("out/_manifest.json")).toBeFalsy();
  });
});
