import { describe, expect, it } from "vitest";
import { MemoryStore, runFileIngest, parseManifest, verifyManifest } from "@/lib/file-ingest";
import { readFileTablePage } from "@/lib/file-tables";
import { FILE_READ_LIMITS, validateObjectPrefix } from "@/lib/file-manifest";

const bytes = (v: unknown) => new TextEncoder().encode(JSON.stringify(v));
async function fixture() {
  const store = new MemoryStore();
  await store.put("source.csv", new TextEncoder().encode("id,name\n1,a\n2,b\n"));
  const manifest = await runFileIngest(store, "source.csv", "owner/run", {
    parts: 2,
    concurrency: 2,
  });
  return { store, manifest };
}

describe("manifest authorization and bounds", () => {
  it.each([
    "../other/part-00000.ndjson",
    "/other/part-00000.ndjson",
    "owner/run/%2e%2e/part-00000.ndjson",
    "other/run/part-00000.ndjson",
    "owner/run/part-00000.ndjson/extra",
  ])("rejects forged key %s before reading parts", async (key) => {
    const { store, manifest } = await fixture();
    const first = manifest.parts[0];
    if (!first) throw new Error("Missing part");
    first.key = key;
    await store.put("owner/run/_manifest.json", bytes(manifest));
    const reads: string[] = [];
    const get = store.get.bind(store);
    store.get = async (k) => {
      reads.push(k);
      return get(k);
    };
    await expect(readFileTablePage(store, "owner/run", 0, 1)).rejects.toThrow();
    expect(reads).toEqual(["owner/run/_manifest.json"]);
    expect((await verifyManifest(store, "owner/run")).ok).toBe(false);
  });
  it.each(["/owner/run", "owner//run", "owner/../run", "owner/%252e/run", "owner\\run"])(
    "rejects ambiguous prefix %s",
    (prefix) => {
      expect(() => validateObjectPrefix(prefix)).toThrow();
    },
  );
  it("rejects duplicate indices, wrong versions and invented totals", async () => {
    const { manifest } = await fixture();
    expect(() => parseManifest(bytes({ ...manifest, version: 99 }), "owner/run")).toThrow();
    expect(() => parseManifest(bytes({ ...manifest, totalRows: 999 }), "owner/run")).toThrow();
    expect(() =>
      parseManifest(
        bytes({ ...manifest, parts: [manifest.parts[0], manifest.parts[0]] }),
        "owner/run",
      ),
    ).toThrow();
  });
  it("checks size and fingerprint on page reads", async () => {
    const { store, manifest } = await fixture();
    const first = manifest.parts[0];
    if (!first) throw new Error("Missing part");
    await store.put(first.key, new TextEncoder().encode('{"id":"9","name":"z"}\n'));
    await expect(readFileTablePage(store, "owner/run", 0, 1)).rejects.toThrow(/checksum or size/);
  });
  it("rejects registry mismatch and oversized pages", async () => {
    const { store, manifest } = await fixture();
    await expect(
      readFileTablePage(store, "owner/run", 0, 1, {
        columns: manifest.columns,
        row_count: 999,
        part_count: 2,
        source_sha256: manifest.sourceSha256,
      }),
    ).rejects.toThrow(/registry/);
    await expect(
      readFileTablePage(store, "owner/run", 0, FILE_READ_LIMITS.pageRows + 1),
    ).rejects.toThrow(/bounds/);
    await expect(readFileTablePage(store, "owner/run", -1, 1)).rejects.toThrow(/bounds/);
  });
});
