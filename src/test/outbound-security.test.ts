// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";
import { publicHttpsUrl, isPublicAddress } from "@/lib/outbound-policy";
import { safeOutboundFetch, boundedResponseBytes, publicOnlyLookup } from "@/lib/outbound.server";

afterEach(() => vi.unstubAllGlobals());
describe("outbound request safety", () => {
  it.each([
    "http://public.example.com",
    "https://127.1",
    "https://2130706433",
    "https://0x7f000001",
    "https://10.0.0.1",
    "https://169.254.169.254/latest",
    "https://[::1]",
    "https://[::ffff:127.0.0.1]",
    "https://localhost",
    "https://service.internal",
    "https://user:password@public.example.com",
    "https://public.example.com:8998",
  ])("blocks unsafe URL %s", (url) => {
    expect(() => publicHttpsUrl(url)).toThrow();
  });
  it("rejects private and mixed DNS answers before sending credentials", async () => {
    const fetch = vi.fn().mockResolvedValue(
      Response.json({
        Status: 0,
        Answer: [
          { type: 1, data: "8.8.8.8" },
          { type: 1, data: "10.0.0.1" },
        ],
      }),
    );
    vi.stubGlobal("fetch", fetch);
    await expect(
      safeOutboundFetch("https://public.example.com", { headers: { Authorization: "test-token" } }),
    ).rejects.toThrow(/private/);
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch.mock.calls[0]?.[1]?.headers).toEqual({ Accept: "application/dns-json" });
  });
  it("refuses redirects without requesting their destination", async () => {
    const fetch = vi
      .fn()
      .mockImplementation(async (url: string) =>
        url.startsWith("https://cloudflare-dns.com/")
          ? Response.json({ Status: 0, Answer: [{ type: 1, data: "8.8.8.8" }] })
          : new Response(null, { status: 302, headers: { Location: "https://10.0.0.1" } }),
      );
    vi.stubGlobal("fetch", fetch);
    await expect(safeOutboundFetch("https://public.example.com")).rejects.toThrow(/redirects/);
    expect(fetch).toHaveBeenCalledTimes(3);
    expect(fetch.mock.calls[2]?.[1]?.redirect).toBe("manual");
  });
  it("cancels oversized streamed responses without Content-Length", async () => {
    const cancel = vi.fn();
    const stream = new ReadableStream({
      start(c) {
        c.enqueue(new Uint8Array(6));
        c.enqueue(new Uint8Array(6));
      },
      cancel,
    });
    await expect(boundedResponseBytes(new Response(stream), 10)).rejects.toThrow(/byte limit/);
    expect(cancel).toHaveBeenCalledTimes(1);
  });
  it("allows public HTTPS and rejects reserved IP ranges", () => {
    expect(publicHttpsUrl("https://warehouse.example.com/api").hostname).toBe(
      "warehouse.example.com",
    );
    expect(isPublicAddress("8.8.8.8")).toBe(true);
    expect(isPublicAddress("100.64.0.1")).toBe(false);
    expect(isPublicAddress("fc00::1")).toBe(false);
  });

  // Transition prefixes that embed an IPv4 address, in every spelling.
  it.each([
    "2002:7f00:1::",
    "2002:a9fe:a9fe::1",
    "2001:0:4136:e378:8000:63bf:80ff:fffe",
    "2001:0000:4136:e378:8000:63bf:80ff:fffe",
    "2001:db8::1",
    "2001:0db8::1",
    "2001:10::1",
    "64:ff9b::7f00:1",
    "64:ff9b:1::a9fe:a9fe",
    "::ffff:127.0.0.1",
    "::127.0.0.1",
    "3fff::1",
    "fe80::1%eth0",
    "2001:4860::1::1",
    "192.88.99.1",
    "192.0.0.170",
  ])("classifies %s as not public", (address) => {
    expect(isPublicAddress(address)).toBe(false);
  });
  it.each(["2001:4860:4860::8888", "2606:4700:4700::1111", "2a00:1450:4001:80b::200e"])(
    "classifies global unicast %s as public",
    (address) => {
      expect(isPublicAddress(address)).toBe(true);
    },
  );

  it("refuses at connect time when a name resolves to a private address", async () => {
    const answer = (addresses: { address: string; family: number }[]) =>
      publicOnlyLookup((_host, _opts, cb) => cb(null, addresses));
    const run = (lookup: ReturnType<typeof answer>, all: boolean) =>
      new Promise<{ err: Error | null; value: unknown }>((resolve) =>
        lookup("rebind.example.com", { all }, (err, value) => resolve({ err, value })),
      );
    expect((await run(answer([{ address: "10.0.0.1", family: 4 }]), false)).err?.message).toMatch(
      /private/,
    );
    const mixed = answer([
      { address: "8.8.8.8", family: 4 },
      { address: "2002:7f00:1::", family: 6 },
    ]);
    expect((await run(mixed, true)).err?.message).toMatch(/private/);
    const ok = await run(answer([{ address: "8.8.8.8", family: 4 }]), false);
    expect(ok).toEqual({ err: null, value: "8.8.8.8" });
  });

  it("sends the request through the pinned dispatcher on Node", async () => {
    const fetch = vi
      .fn()
      .mockImplementation(async (url: string) =>
        url.startsWith("https://cloudflare-dns.com/")
          ? Response.json({ Status: 0, Answer: [{ type: 1, data: "8.8.8.8" }] })
          : new Response("ok"),
      );
    vi.stubGlobal("fetch", fetch);
    await safeOutboundFetch("https://public.example.com");
    expect(fetch.mock.calls[2]?.[1]?.dispatcher).toBeDefined();
  });
});
