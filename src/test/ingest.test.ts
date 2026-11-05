import { describe, it, expect } from "vitest";
import { applyCdc, extractPath, mergeRows } from "@/lib/ingest";

describe("ingestion", () => {
  it("extracts records at a JSON path and flattens nested objects", () => {
    const r = extractPath(
      { data: { items: [{ id: 1, addr: { city: "Pune" } }] } },
      "data.items[*]",
    );
    expect(r).toEqual([{ id: 1, addr_city: "Pune" }]);
  });
  it("upsert replaces rows with the same key", () => {
    expect(
      mergeRows(
        [{ id: 1, v: "a" }],
        [
          { id: 1, v: "b" },
          { id: 2, v: "c" },
        ],
        "upsert",
        "id",
      ),
    ).toEqual([
      { id: 1, v: "b" },
      { id: 2, v: "c" },
    ]);
  });
  it("CDC applies insert, update and delete", () => {
    const out = applyCdc(
      [
        { id: 1, n: "x" },
        { id: 2, n: "y" },
      ],
      [
        { op: "u", after: { id: 1, n: "z" } },
        { op: "d", before: { id: 2 } },
        { op: "c", after: { id: 3, n: "w" } },
      ],
      "id",
    );
    expect(out).toEqual([
      { id: 1, n: "z" },
      { id: 3, n: "w" },
    ]);
  });
});
