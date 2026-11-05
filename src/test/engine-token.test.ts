// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ENGINE_TOKEN_TTL_SECONDS,
  engineTokenFor,
  importEngineSigningKey,
  mintEngineToken,
  verifyEngineToken,
} from "@/lib/engine-token.server";

// Test-only Ed25519 key pair; never configured anywhere. The same vector is verified by
// deploy/datafusion/test_service.py, which proves the TS signer and the Python verifier agree.
const FIXTURE_ED25519_PKCS8 = "MC4CAQAwBQYDK2VwBCIEIKIeAnIBUGfbINuhNUXeJMkhE4C2Bu/MeoKckHr6jVzv";
const FIXTURE_PUBLIC_RAW = "OWb_yN4j_8kSRKv02XfaoBHkUVn6QXt5fHjeZe0jsig";
const GOLDEN =
  "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJtZXJpZGlhbiIsImF1ZCI6Im1lcmlkaWFuLWVuZ2luZTo2ZjFjMmE4ZS0wMDAwLTQwMDAtODAwMC0wMDAwMDAwMDAwMDEiLCJzdWIiOiIxMTExMTExMS0xMTExLTQxMTEtODExMS0xMTExMTExMTExMTEiLCJpYXQiOjE4MDAwMDAwMDAsImV4cCI6MTgwMDAwMDMwMCwianRpIjoiMDAwMDAwMDAtMDAwMC00MDAwLTgwMDAtMDAwMDAwMDAwMGFhIn0.fUafLd903l-Q8soKfe2cpLh9Oq6Z2py2A5xpbHYI4ITfXPAxSxEGtd6Nu12_y5mFYMc_CWRY4dhMnLftyyB6Dw";
const engineId = "6f1c2a8e-0000-4000-8000-000000000001";
const userId = "11111111-1111-4111-8111-111111111111";

const fromB64url = (s: string) =>
  Uint8Array.from(atob(s.replace(/-/g, "+").replace(/_/g, "/")), (c) => c.charCodeAt(0));
const claimsOf = (token: string) =>
  JSON.parse(new TextDecoder().decode(fromB64url(token.split(".")[1]!))) as Record<string, unknown>;

afterEach(() => vi.unstubAllEnvs());

// Engines receive an engine-bound token, never the Supabase session.
describe("engine tokens", () => {
  it("matches the cross-language golden vector", async () => {
    const key = await importEngineSigningKey(FIXTURE_ED25519_PKCS8);
    expect(
      await mintEngineToken(key, {
        engineId,
        userId,
        now: 1_800_000_000_000,
        jti: "00000000-0000-4000-8000-0000000000aa",
      }),
    ).toBe(GOLDEN);
  });

  it("binds the token to one engine and one user for five minutes, verifiably", async () => {
    vi.stubEnv("ENGINE_TOKEN_SIGNING_KEY", FIXTURE_ED25519_PKCS8);
    const token = await engineTokenFor(engineId, userId);
    const claims = claimsOf(token);
    expect(claims).toMatchObject({
      iss: "meridian",
      aud: `meridian-engine:${engineId}`,
      sub: userId,
    });
    expect(Number(claims["exp"]) - Number(claims["iat"])).toBe(ENGINE_TOKEN_TTL_SECONDS);
    const [h, p, s] = token.split(".");
    const pub = await crypto.subtle.importKey(
      "raw",
      fromB64url(FIXTURE_PUBLIC_RAW),
      { name: "Ed25519" },
      false,
      ["verify"],
    );
    const ok = await crypto.subtle.verify(
      { name: "Ed25519" },
      pub,
      fromB64url(s!),
      new TextEncoder().encode(`${h}.${p}`),
    );
    expect(ok).toBe(true);
  });

  it("fails closed without a signing key instead of forwarding the session", async () => {
    vi.stubEnv("ENGINE_TOKEN_SIGNING_KEY", "");
    await expect(engineTokenFor(engineId, userId)).rejects.toThrow(/not configured/);
  });
});

// The Serve manifest route verifies tokens engines send back to the control plane.
describe("verifyEngineToken", () => {
  const now = 1_800_000_100_000;
  it("accepts a token this control plane signed and returns its engine and user", async () => {
    vi.stubEnv("ENGINE_TOKEN_SIGNING_KEY", FIXTURE_ED25519_PKCS8);
    expect(await verifyEngineToken(GOLDEN, now)).toEqual({ engineId, userId });
  });

  it("refuses expired, tampered, foreign-key and unconfigured cases", async () => {
    vi.stubEnv("ENGINE_TOKEN_SIGNING_KEY", FIXTURE_ED25519_PKCS8);
    expect(await verifyEngineToken(GOLDEN, 1_800_000_301_000)).toBeNull();
    const [h, , sig] = GOLDEN.split(".");
    const forged = btoa(JSON.stringify({ ...claimsOf(GOLDEN), sub: crypto.randomUUID() }))
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=+$/, "");
    expect(await verifyEngineToken(`${h}.${forged}.${sig}`, now)).toBeNull();
    expect(await verifyEngineToken("not.a.token", now)).toBeNull();
    const { generateKeyPairSync } = await import("node:crypto");
    const other = generateKeyPairSync("ed25519")
      .privateKey.export({ type: "pkcs8", format: "der" })
      .toString("base64");
    vi.stubEnv("ENGINE_TOKEN_SIGNING_KEY", other);
    expect(await verifyEngineToken(GOLDEN, now)).toBeNull();
    vi.stubEnv("ENGINE_TOKEN_SIGNING_KEY", "");
    expect(await verifyEngineToken(GOLDEN, now)).toBeNull();
  });
});
