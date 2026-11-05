import { describe, it, expect } from "vitest";
import { parseFile } from "@/lib/file-formats";
const bytes = (s: string) => new TextEncoder().encode(s).buffer;
// Regression: valid pretty-printed JSON objects were mistaken for NDJSON.
// Value: protects=JSON object uploads; fails_when=every newline forces line parsing;
describe("JSON upload documents", () => {
  it.each([
    {
      name: "wrapped pretty JSON",
      text: JSON.stringify(
        {
          items: [
            { id: 1, name: "Ω" },
            { id: 2, name: null },
          ],
        },
        null,
        2,
      ),
      rows: [
        { id: 1, name: "Ω" },
        { id: 2, name: null },
      ],
    },
    {
      name: "pretty single record",
      text: JSON.stringify({ id: 1, name: "one" }, null, 2),
      rows: [{ id: 1, name: "one" }],
    },
    { name: "JSON lines", text: '{"id":1}\n{"id":2}\n', rows: [{ id: 1 }, { id: 2 }] },
    { name: "empty document", text: "", rows: [] },
  ])("parses $name", async ({ text, rows }) => {
    expect((await parseFile("records.json", bytes(text))).rows).toEqual(rows);
  });
  it("rejects malformed JSON instead of creating a table", async () => {
    await expect(parseFile("broken.json", bytes("{invalid"))).rejects.toThrow();
  });
});
