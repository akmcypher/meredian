// @vitest-environment node
// Bronze Parquet parts (S6): the server decodes a part written by deploy/connect/bronze_parquet.py to
// the same envelopes the writer encoded (fixture shared with deploy/connect/test_bronze_parquet.py),
// and refuses files that are not that layout.
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { parquetWriteBuffer } from "hyparquet-writer";
import { bronzeParquetEnvelopes, isParquet } from "@/lib/bronze-parquet";

const dir = join(__dirname, "fixtures");
const bytes = new Uint8Array(readFileSync(join(dir, "bronze-part.parquet")));
const expected = JSON.parse(readFileSync(join(dir, "bronze-part.json"), "utf8")) as unknown[];

describe("Bronze Parquet parts", () => {
  it("decodes a part to the envelopes it was written from", async () => {
    expect(isParquet(bytes)).toBe(true);
    expect(await bronzeParquetEnvelopes(bytes.slice().buffer)).toEqual(expected);
  });

  it("refuses Parquet without the Bronze layout", async () => {
    const plain = parquetWriteBuffer({ columnData: [{ name: "a", data: [1, 2], type: "INT32" }] });
    await expect(bronzeParquetEnvelopes(plain)).rejects.toThrow("invalid_bronze_parquet");
  });

  it("refuses NDJSON bytes", () => {
    expect(isParquet(new TextEncoder().encode('{"protocol":"meridian-connect/1"}\n'))).toBe(false);
  });
});
