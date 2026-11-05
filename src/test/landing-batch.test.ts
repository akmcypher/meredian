// @vitest-environment node
import { describe, expect, it } from "vitest";
import { encodeLandingBatch } from "@/lib/landing-batch";
import { sha256Hex } from "@/lib/file-ingest";
import { readFileSync } from "node:fs";

describe("analytical JSONB write retirement", () => {
  it("preserves raw CDC operations instead of rewriting a legacy row array", async () => {
    const batch = encodeLandingBatch({ events: [{ op: "d", before: { id: 1 } }] });
    expect(batch.mode).toBe("cdc");
    expect(await batch.blob.text()).toBe('{"op":"d","before":{"id":1}}\n');
    expect(batch.sha256).toBe(await sha256Hex(new Uint8Array(await batch.blob.arrayBuffer())));
  });
  it("caps raw landing rows and rejects ambiguous payloads", () => {
    expect(() =>
      encodeLandingBatch({ rows: Array.from({ length: 10001 }, () => ({ id: 1 })) }),
    ).toThrow(/10000/);
    expect(() => encodeLandingBatch({ rows: [], events: [] })).toThrow(/not both/);
  });
  it("keeps production REST and push callers off the legacy JSONB writer", () => {
    for (const path of ["src/lib/ingest.functions.ts", "src/routes/api/public/ingest.ts"]) {
      const code = readFileSync(path, "utf8");
      expect(code).not.toMatch(/writeBatch|ingest\.server/);
      expect(code).toContain("landSourceBatch");
    }
  });
});
