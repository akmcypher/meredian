import { describe, expect, it } from "vitest";
import {
  checkCoverage,
  demoBatch,
  demoSample,
  demoStats,
  detectSkew,
  matches,
  nextConcurrency,
  planPartitions,
  predicateSql,
  type Strategy,
} from "@/lib/partitioning";

const TOTAL = 60_000;
const plan = (s: Strategy, col: string) =>
  planPartitions(s, "column" in s ? demoStats(col, TOTAL) : null);

describe("partition planning", () => {
  for (const s of [
    { kind: "range", column: "id", n: 7 },
    { kind: "hash", column: "customer_id", n: 9 },
    { kind: "modulo", column: "id", n: 5 },
    { kind: "date", column: "order_date", grain: "month" },
    { kind: "key", column: "region", values: ["APAC", "EU"] },
    { kind: "key", column: "coupon", values: ["C1"] },
  ] as Strategy[]) {
    it(`${s.kind} on ${"column" in s ? s.column : ""} covers every row exactly once`, () => {
      const cov = checkCoverage(plan(s, (s as { column: string }).column), demoSample(TOTAL, 3000));
      expect(cov.uncovered).toBe(0);
      expect(cov.overlapping).toBe(0);
    });
  }

  it("adds a NULL partition for nullable columns", () => {
    const parts = plan({ kind: "hash", column: "coupon", n: 4 }, "coupon");
    expect(parts.some((p) => p.predicate.kind === "null")).toBe(true);
  });

  it("keyset extraction over all range partitions returns every id once (no OFFSET)", () => {
    const parts = plan({ kind: "range", column: "id", n: 6 }, "id");
    const seen = new Set<number>();
    let count = 0;
    for (const p of parts) {
      let after = 0,
        done = false;
      while (!done) {
        const b = demoBatch(p.predicate, 12_345, after, 1000);
        b.rows.forEach((r) => {
          seen.add(r["id"] as number);
          count++;
        });
        after = b.lastId;
        done = b.done;
      }
    }
    // stats are for TOTAL ids but extraction is bounded by the source size
    expect(count).toBe(12_345);
    expect(seen.size).toBe(12_345);
  });

  it("flags the 50% APAC region as skewed", () => {
    expect(detectSkew([20, 18, 22, 19, 21]).skewed).toBe(false);
    expect(detectSkew([500, 10, 10, 10, 10]).skewed).toBe(true);
  });

  it("renders non-overlapping range SQL", () => {
    const [a, b] = plan({ kind: "range", column: "id", n: 2 }, "id");
    if (!a || !b) throw new Error("Expected two range partitions");
    expect(predicateSql(a.predicate)).toBe('"id" >= 1 AND "id" < 30001');
    expect(predicateSql(b.predicate)).toBe('"id" >= 30001 AND "id" <= 60000');
  });

  it("covers negative modulo keys and NULL exactly once", () => {
    const parts = planPartitions(
      { kind: "modulo", column: "id", n: 3 },
      { min: -7, max: 7, nullFraction: 0.1, distinct: 15, indexed: true },
    );
    const rows = Array.from({ length: 15 }, (_, i) => ({ id: i - 7 }));
    expect(checkCoverage(parts, [...rows, { id: null }])).toMatchObject({
      ok: true,
      uncovered: 0,
      overlapping: 0,
    });
    const bucket = parts.find((p) => p.predicate.kind === "modulo" && p.predicate.i === 2);
    if (!bucket) throw new Error("Expected modulo bucket 2");
    expect(matches(bucket.predicate, { id: -7 })).toBe(true);
    for (const dialect of ["postgres", "mysql", "spark"] as const) {
      const identifier = dialect === "postgres" ? '"id"' : "`id`";
      expect(predicateSql(bucket.predicate, dialect)).toBe(
        `${identifier} IS NOT NULL AND MOD(MOD(${identifier}, 3) + 3, 3) = 2`,
      );
    }
  });

  it.each([0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, 2001])(
    "rejects invalid logical partition count %s instead of an incomplete plan",
    (n) => {
      for (const kind of ["range", "hash", "modulo"] as const) {
        expect(() => planPartitions({ kind, column: "id", n }, demoStats("id", TOTAL))).toThrow(
          "Logical partition count",
        );
      }
    },
  );

  it("fails explicitly instead of truncating a long daily date plan", () => {
    expect(() =>
      planPartitions(
        { kind: "date", column: "d", grain: "day" },
        {
          min: null,
          max: null,
          minDate: "2020-01-01",
          maxDate: "2026-10-31",
          nullFraction: 0,
          distinct: 2496,
          indexed: true,
        },
      ),
    ).toThrow("exceeds 2000 partitions");
  });

  it("rejects reversed date bounds", () => {
    expect(() =>
      planPartitions(
        { kind: "date", column: "d", grain: "month" },
        {
          min: null,
          max: null,
          minDate: "2026-10-31",
          maxDate: "2020-01-01",
          nullFraction: 0,
          distinct: 2,
          indexed: true,
        },
      ),
    ).toThrow("valid, ordered dates");
  });

  it("returns no file partitions for an empty source", () => {
    expect(planPartitions({ kind: "file", files: [], n: 4 }, null)).toEqual([]);
  });

  it("rejects invalid file metadata instead of producing misleading allocation", () => {
    expect(() =>
      planPartitions({ kind: "file", files: [{ path: "broken.csv", bytes: -1 }], n: 4 }, null),
    ).toThrow("File sizes");
  });

  it("halves concurrency on errors and grows by one when fast", () => {
    expect(nextConcurrency(8, { min: 1, max: 8, latencyMs: 0, targetMs: 4000, error: true })).toBe(
      4,
    );
    expect(
      nextConcurrency(3, { min: 1, max: 8, latencyMs: 1000, targetMs: 4000, error: false }),
    ).toBe(4);
    expect(
      nextConcurrency(8, { min: 1, max: 8, latencyMs: 1000, targetMs: 4000, error: false }),
    ).toBe(8);
  });
});
