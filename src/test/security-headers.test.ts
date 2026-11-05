import { describe, expect, it } from "vitest";
import { contentSecurityPolicy, securityHeaders } from "@/lib/security-headers";

const base = { nonce: "abc123", supabaseUrl: "https://db.example.com", https: true, dev: false };

// Every response carries browser hardening headers.
describe("security headers", () => {
  it("enforces a nonce-based CSP that forbids framing and plugins", () => {
    const h = securityHeaders(base);
    const csp = h["content-security-policy"]!;
    expect(csp).toContain("script-src 'self' 'nonce-abc123'");
    expect(csp).not.toMatch(/script-src[^;]*'unsafe-inline'/);
    expect(csp).toContain("frame-ancestors 'none'");
    expect(csp).toContain("object-src 'none'");
    expect(csp).toContain("base-uri 'none'");
    expect(csp).toContain("connect-src 'self' https://db.example.com wss://db.example.com");
    expect(h["x-frame-options"]).toBe("DENY");
    expect(h["x-content-type-options"]).toBe("nosniff");
    expect(h["referrer-policy"]).toBe("strict-origin-when-cross-origin");
    expect(h["strict-transport-security"]).toMatch(/max-age=31536000/);
  });

  it("only reports in development and skips HSTS on plain HTTP", () => {
    const h = securityHeaders({ ...base, dev: true, https: false });
    expect(h["content-security-policy"]).toBeUndefined();
    expect(h["content-security-policy-report-only"]).toBeDefined();
    expect(h["strict-transport-security"]).toBeUndefined();
    expect(contentSecurityPolicy({ ...base, dev: true })).toContain("ws://localhost:*");
  });
});
