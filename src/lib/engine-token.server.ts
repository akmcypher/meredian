/**
 * Engine tokens. The control plane never forwards a user's Supabase
 * session to an execution engine: it signs a short-lived Ed25519 token bound to one engine
 * (`aud`) and one user (`sub`). The engine verifies it locally with the public key, so a
 * captured token is useless against Supabase and against every other engine.
 *
 * ENGINE_TOKEN_SIGNING_KEY holds the PKCS#8 private key (base64). Engines are configured with
 * the matching raw public key (base64url) and their own engine id; `node
 * scripts/engine-keys.mjs` prints both halves of a new key pair.
 */
export const ENGINE_TOKEN_TTL_SECONDS = 300;
export const engineAudience = (engineId: string) => `meridian-engine:${engineId}`;

export type EngineTokenClaims = {
  iss: "meridian";
  aud: string;
  sub: string;
  iat: number;
  exp: number;
  jti: string;
};

const base64url = (bytes: Uint8Array) =>
  btoa(String.fromCharCode(...bytes))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
const utf8 = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));

export async function importEngineSigningKey(pkcs8Base64: string): Promise<CryptoKey> {
  const der = Uint8Array.from(atob(pkcs8Base64.trim()), (c) => c.charCodeAt(0));
  return crypto.subtle.importKey("pkcs8", der, { name: "Ed25519" }, false, ["sign"]);
}

export async function mintEngineToken(
  key: CryptoKey,
  input: { engineId: string; userId: string; now?: number; jti?: string },
): Promise<string> {
  const iat = Math.floor((input.now ?? Date.now()) / 1000);
  const claims: EngineTokenClaims = {
    iss: "meridian",
    aud: engineAudience(input.engineId),
    sub: input.userId,
    iat,
    exp: iat + ENGINE_TOKEN_TTL_SECONDS,
    jti: input.jti ?? crypto.randomUUID(),
  };
  const signingInput = `${base64url(utf8({ alg: "EdDSA", typ: "JWT" }))}.${base64url(utf8(claims))}`;
  const signature = new Uint8Array(
    await crypto.subtle.sign({ name: "Ed25519" }, key, new TextEncoder().encode(signingInput)),
  );
  return `${signingInput}.${base64url(signature)}`;
}

let cached: Promise<CryptoKey> | undefined;
/** Fails closed: without a signing key no engine request is made (there is no JWT fallback). */
export async function engineTokenFor(engineId: string, userId: string): Promise<string> {
  const raw = process.env["ENGINE_TOKEN_SIGNING_KEY"];
  if (!raw)
    throw new Error(
      "Engine access is not configured: set ENGINE_TOKEN_SIGNING_KEY (see scripts/engine-keys.mjs)",
    );
  cached ??= importEngineSigningKey(raw);
  return mintEngineToken(await cached, { engineId, userId });
}

let verifier: { raw: string; key: Promise<import("node:crypto").KeyObject> } | undefined;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Verifies a token this control plane minted, for engines calling back into it (the Serve
 * manifest). Checks the Ed25519 signature against the public half of ENGINE_TOKEN_SIGNING_KEY,
 * the issuer, an engine audience, the subject and a lifetime of at most ENGINE_TOKEN_TTL_SECONDS.
 * Returns null for anything else, including when no signing key is configured.
 */
export async function verifyEngineToken(
  token: string,
  nowMs = Date.now(),
): Promise<{ engineId: string; userId: string } | null> {
  const raw = process.env["ENGINE_TOKEN_SIGNING_KEY"]?.trim();
  const parts = token.split(".");
  if (!raw || parts.length !== 3) return null;
  const { createPrivateKey, createPublicKey, verify } = await import("node:crypto");
  if (verifier?.raw !== raw)
    verifier = {
      raw,
      key: Promise.resolve(
        createPublicKey(
          createPrivateKey({ key: Buffer.from(raw, "base64"), format: "der", type: "pkcs8" }),
        ),
      ),
    };
  let header: Record<string, unknown>, claims: Record<string, unknown>;
  try {
    header = JSON.parse(Buffer.from(parts[0]!, "base64url").toString("utf8"));
    claims = JSON.parse(Buffer.from(parts[1]!, "base64url").toString("utf8"));
  } catch {
    return null;
  }
  if (header["alg"] !== "EdDSA") return null;
  const signed = Buffer.from(`${parts[0]}.${parts[1]}`);
  if (!verify(null, signed, await verifier.key, Buffer.from(parts[2]!, "base64url"))) return null;
  const now = Math.floor(nowMs / 1000);
  const aud = typeof claims["aud"] === "string" ? claims["aud"] : "";
  const engineId = aud.startsWith("meridian-engine:") ? aud.slice("meridian-engine:".length) : "";
  const { sub, iat, exp } = claims;
  if (
    claims["iss"] !== "meridian" ||
    !UUID.test(engineId) ||
    typeof sub !== "string" ||
    !UUID.test(sub) ||
    !Number.isInteger(iat) ||
    !Number.isInteger(exp) ||
    (exp as number) <= now ||
    (iat as number) > now + 60 ||
    (exp as number) - (iat as number) > ENGINE_TOKEN_TTL_SECONDS
  )
    return null;
  return { engineId, userId: sub };
}
