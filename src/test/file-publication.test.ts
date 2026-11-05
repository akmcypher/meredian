import { describe, expect, it } from "vitest";
import { MemoryStore, runFileIngest } from "@/lib/file-ingest";
import { copyVerifiedFileRun } from "@/lib/file-publication";
import { readFileTablePage } from "@/lib/file-tables";

describe("immutable publication snapshots", () => {
  it("publishes verified copies unaffected by subsequent staging mutation", async () => {
    const store = new MemoryStore();
    await store.put("source.csv", new TextEncoder().encode("id,v\n1,a\n2,b\n"));
    const source = await runFileIngest(store, "source.csv", "owner/runs/one", {
      parts: 2,
      concurrency: 2,
    });
    const { manifest } = await copyVerifiedFileRun(store, "owner/runs/one", "owner/published/two");
    const first = source.parts[0];
    if (!first) throw new Error("Missing part");
    await store.put(first.key, new TextEncoder().encode("corrupt"));
    expect(
      (await readFileTablePage(store, "owner/published/two", 0, 2)).rows.map((r) => r["id"]),
    ).toEqual(["1", "2"]);
    expect(manifest.parts[0]?.key).toBe("owner/published/two/part-00000.ndjson");
  });
  it("refuses changed staging parts and never commits their destination manifest", async () => {
    const store = new MemoryStore();
    await store.put("source.csv", new TextEncoder().encode("id\n1\n"));
    const source = await runFileIngest(store, "source.csv", "owner/runs/one", {
      parts: 1,
      concurrency: 1,
    });
    const first = source.parts[0];
    if (!first) throw new Error("Missing part");
    await store.put(first.key, new TextEncoder().encode("corrupt"));
    await expect(
      copyVerifiedFileRun(store, "owner/runs/one", "owner/published/two"),
    ).rejects.toThrow(/changed/);
    expect(await store.get("owner/published/two/_manifest.json")).toBeNull();
  });
});
