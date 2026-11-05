import { describe, expect, it } from "vitest";
import { workerScript } from "@/lib/ingest";

const endpoint = "http://localhost:8080/api/public/ingest";
const before = (script: string, first: string, second: string) =>
  script.indexOf(first) >= 0 && script.indexOf(first) < script.indexOf(second);

describe("workerScript", () => {
  it("database workers advance and save the cursor only after a batch lands", () => {
    for (const kind of ["mysql", "postgres"] as const) {
      const script = workerScript(kind, endpoint, { query: "select * from t", cursor: "id" });
      expect(before(script, 'await push({ rows, mode: "upsert" })', "saveCursor(last)")).toBe(true);
      expect(script).toContain("order by ${CURSOR} limit ${PAGE}");
      expect(script).toContain(kind === "mysql" ? "${CURSOR} > ?" : "${CURSOR} > $1");
    }
  });

  it("Kafka workers commit offsets manually after landing", () => {
    const script = workerScript("kafka_cdc", endpoint, { topic: "orders" });
    expect(script).toContain("autoCommit: false");
    expect(before(script, "await push({ events", "consumer.commitOffsets")).toBe(true);
    expect(script).toContain('const TOPIC = "orders"');
  });

  it("keeps one batch id across retries and stops on rejection", () => {
    const script = workerScript("mysql", endpoint, {});
    expect(script).toContain("batchId: crypto.randomUUID()");
    expect(before(script, "const payload", "for (let attempt")).toBe(true);
    expect(script).toContain("status !== 429");
  });
});
