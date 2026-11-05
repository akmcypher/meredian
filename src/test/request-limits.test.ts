// @vitest-environment node
import { describe, expect, it } from "vitest";
import { rateLimit, readBodyCapped, RequestLimitError } from "@/lib/request-limits.server";

const streamed = (chunks: number[]) =>
  new Request("http://x/", {
    method: "POST",
    body: new ReadableStream({
      start(c) {
        for (const n of chunks) c.enqueue(new Uint8Array(n));
        c.close();
      },
    }),
    // @ts-expect-error Node needs duplex for streamed request bodies
    duplex: "half",
  });

// Bodies are capped while reading, before parsing.
describe("readBodyCapped", () => {
  it("rejects a declared oversized body without reading it", async () => {
    const r = new Request("http://x/", {
      method: "POST",
      body: "x".repeat(20),
      headers: { "content-length": "20" },
    });
    await expect(readBodyCapped(r, 10)).rejects.toMatchObject({ status: 413 });
  });
  it("rejects a streamed body that grows past the cap", async () => {
    await expect(readBodyCapped(streamed([6, 6]), 10)).rejects.toBeInstanceOf(RequestLimitError);
  });
  it("returns bodies within the cap", async () => {
    expect((await readBodyCapped(streamed([4, 5]), 10)).length).toBe(9);
  });
  it("reads limits from the environment with safe defaults", () => {
    expect(rateLimit("chat", {})).toBe(20);
    expect(rateLimit("worker", { RATE_LIMIT_WORKER_PER_MINUTE: "50" })).toBe(50);
    expect(rateLimit("worker", { RATE_LIMIT_WORKER_PER_MINUTE: "-1" })).toBe(600);
  });
});
