// @vitest-environment node
import { describe, expect, it } from "vitest";
import { verifyBatchPreview } from "@/lib/connect-inspection";

async function fixture() {
  const body = new Blob(['{"id":1}\n{"id":2}\n']);
  const sha256 = Array.from(
    new Uint8Array(await crypto.subtle.digest("SHA-256", await body.arrayBuffer())),
    (b) => b.toString(16).padStart(2, "0"),
  ).join("");
  const batch = {
    batch_id: "batch",
    manifest_key: "owner/manifest.json",
    sha256,
    row_count: 2,
    byte_count: body.size,
  };
  return { body, batch, manifest: { batch_id: batch.batch_id, sha256, rows: 2, bytes: body.size } };
}

describe("committed Bronze inspection", () => {
  it("shows verified records and manifest metadata", async () => {
    const f = await fixture();
    expect((await verifyBatchPreview(f.batch, f.manifest, f.body)).records).toEqual([
      { id: 1 },
      { id: 2 },
    ]);
  });
  it("rejects corruption even when byte length stays unchanged", async () => {
    const f = await fixture();
    await expect(
      verifyBatchPreview(f.batch, f.manifest, new Blob(['{"id":9}\n{"id":2}\n'])),
    ).rejects.toThrow("checksum");
  });
  it("rejects a manifest from a different batch", async () => {
    const f = await fixture();
    await expect(
      verifyBatchPreview(f.batch, { ...f.manifest, batch_id: "other" }, f.body),
    ).rejects.toThrow("metadata");
  });
});
