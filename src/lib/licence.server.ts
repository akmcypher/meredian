/**
 * Licences. A licence is `ml1.<payload>.<signature>`: a
 * base64url JSON payload signed with the vendor's Ed25519 key over `ml1.<payload>`. Only the
 * public key ships (MERIDIAN_LICENCE_PUBLIC_KEY, raw base64url); licences are issued outside
 * this repository. A verified licence grants entitlements to one organization, server-side.
 */
export type Licence = {
  licenceId: string;
  organizationId: string;
  features: string[];
  expiresAt: string | null;
};

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const FEATURE = /^[a-z][a-z0-9_.-]{1,63}$/;

const fromB64url = (s: string) =>
  Uint8Array.from(atob(s.replace(/-/g, "+").replace(/_/g, "/")), (c) => c.charCodeAt(0));

export async function verifyLicence(
  token: string,
  publicKeyB64url: string,
  now = Date.now(),
): Promise<Licence> {
  const parts = token.trim().split(".");
  if (parts.length !== 3 || parts[0] !== "ml1") throw new Error("Not a Meridian licence");
  const key = await crypto.subtle.importKey(
    "raw",
    fromB64url(publicKeyB64url),
    { name: "Ed25519" },
    false,
    ["verify"],
  );
  let valid = false;
  try {
    valid = await crypto.subtle.verify(
      { name: "Ed25519" },
      key,
      fromB64url(parts[2]!),
      new TextEncoder().encode(`ml1.${parts[1]}`),
    );
  } catch {
    valid = false;
  }
  if (!valid) throw new Error("Licence signature is invalid");
  const p = JSON.parse(new TextDecoder().decode(fromB64url(parts[1]!))) as Record<string, unknown>;
  const features = p["features"];
  const expiresAt = p["expires_at"] ?? null;
  if (
    typeof p["licence_id"] !== "string" ||
    !p["licence_id"] ||
    p["licence_id"].length > 100 ||
    typeof p["organization_id"] !== "string" ||
    !UUID.test(p["organization_id"]) ||
    !Array.isArray(features) ||
    !features.length ||
    features.length > 100 ||
    !features.every((f) => typeof f === "string" && FEATURE.test(f)) ||
    (expiresAt !== null && (typeof expiresAt !== "string" || Number.isNaN(Date.parse(expiresAt))))
  )
    throw new Error("Licence payload is invalid");
  if (expiresAt !== null && Date.parse(expiresAt) <= now) throw new Error("Licence has expired");
  return {
    licenceId: p["licence_id"],
    organizationId: p["organization_id"],
    features: features as string[],
    expiresAt,
  };
}
