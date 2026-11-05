import { describe, expect, it } from "vitest";
import { referencedTables } from "@/lib/published-tables";

describe("referencedTables", () => {
  it("matches whole identifiers only, case-insensitively", () => {
    expect(
      referencedTables("SELECT * FROM Orders_E2E o JOIN pg_events e ON 1=1", [
        "orders_e2e",
        "pg_events",
        "orders",
      ]),
    ).toEqual(["orders_e2e", "pg_events"]);
    expect(referencedTables("select * from orders_e2e_old", ["orders_e2e"])).toEqual([]);
  });
});
