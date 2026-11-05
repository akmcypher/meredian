import { describe, expect, it } from "vitest";
import { authoritativePartitions } from "@/lib/partition-plan-validation";
import { checkCoverage, demoSample } from "@/lib/partitioning";
describe("server authoritative plans", () => {
  it("rebuilds predicates from the strategy with full demo coverage", () => {
    const parts = authoritativePartitions(
      { strategy: { kind: "range", column: "id", n: 4 } },
      100,
      true,
    );
    expect(parts).toHaveLength(4);
    expect(checkCoverage(parts, demoSample(100, 100))).toMatchObject({
      ok: true,
      uncovered: 0,
      overlapping: 0,
    });
    expect(parts[0]?.predicate).toEqual({
      kind: "range",
      column: "id",
      lo: 1,
      hi: 26,
      last: false,
    });
  });
  it("rejects arbitrary SQL, malformed columns and unbounded counts", () => {
    expect(() =>
      authoritativePartitions(
        { strategy: { kind: "sql", predicates: ["1=1; DROP TABLE orders"] } },
        10,
        true,
      ),
    ).toThrow();
    expect(() =>
      authoritativePartitions({ strategy: { kind: "hash", column: "id;drop", n: 4 } }, 10, true),
    ).toThrow();
    expect(() =>
      authoritativePartitions({ strategy: { kind: "modulo", column: "id", n: 2001 } }, 10, true),
    ).toThrow();
  });
  it("requires explicit finite source stats rather than trusting frontend partitions", () => {
    expect(() =>
      authoritativePartitions({ strategy: { kind: "range", column: "id", n: 2 } }, 10, false),
    ).toThrow();
    expect(() =>
      authoritativePartitions({ strategy: { kind: "range", column: "id", n: 2 } }, 10, false, {
        min: 0,
        max: Infinity,
        nullFraction: 0,
        distinct: 10,
        indexed: true,
      }),
    ).toThrow();
  });
});
