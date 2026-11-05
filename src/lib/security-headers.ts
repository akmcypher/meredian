/**
 * HTTP security headers for every response.
 *
 * Scripts need a per-request nonce (TanStack Start's SSR scripts carry it through the router's
 * `ssr.nonce`). 'unsafe-eval' stays because the browser demo engine (AlaSQL) compiles SQL with
 * the Function constructor; the policy still blocks injected <script> elements, inline event
 * handlers and foreign script origins. Inline style attributes (React, Radix, charts) need
 * 'unsafe-inline' in style-src, which cannot run script.
 */
export type SecurityHeaderInput = {
  nonce: string;
  supabaseUrl?: string | undefined;
  https: boolean;
  /** Vite's dev preamble is inline without a nonce: development only reports violations. */
  dev: boolean;
};

function origins(url: string | undefined): string[] {
  if (!url) return [];
  try {
    const u = new URL(url);
    const ws = `${u.protocol === "https:" ? "wss:" : "ws:"}//${u.host}`;
    return [u.origin, ws];
  } catch {
    return [];
  }
}

export function contentSecurityPolicy(input: SecurityHeaderInput): string {
  return [
    "default-src 'self'",
    `script-src 'self' 'nonce-${input.nonce}' 'unsafe-eval' 'wasm-unsafe-eval'`,
    // The root layout loads its typefaces from Google Fonts.
    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
    "img-src 'self' data: blob:",
    "font-src 'self' data: https://fonts.gstatic.com",
    // Supabase (REST, auth, realtime) and Local Spark, which talks to Livy on the user's machine.
    [
      "connect-src 'self'",
      ...origins(input.supabaseUrl),
      "http://localhost:*",
      "http://127.0.0.1:*",
    ]
      .concat(input.dev ? ["ws://localhost:*", "ws://127.0.0.1:*"] : [])
      .join(" "),
    // Embedded Superset dashboards live on the customer's own HTTPS host.
    "frame-src 'self' https:",
    "worker-src 'self' blob:",
    "object-src 'none'",
    "base-uri 'none'",
    "form-action 'self'",
    "frame-ancestors 'none'",
  ].join("; ");
}

export function securityHeaders(input: SecurityHeaderInput): Record<string, string> {
  return {
    [input.dev ? "content-security-policy-report-only" : "content-security-policy"]:
      contentSecurityPolicy(input),
    "x-content-type-options": "nosniff",
    "x-frame-options": "DENY",
    "referrer-policy": "strict-origin-when-cross-origin",
    "permissions-policy": "camera=(), microphone=(), geolocation=(), payment=(), usb=()",
    "cross-origin-opener-policy": "same-origin",
    ...(input.https ? { "strict-transport-security": "max-age=31536000; includeSubDomains" } : {}),
  };
}

export function withSecurityHeaders(response: Response, input: SecurityHeaderInput): Response {
  const headers = new Headers(response.headers);
  for (const [name, value] of Object.entries(securityHeaders(input))) headers.set(name, value);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

const NONCE_STORE = Symbol.for("meridian.cspNonce");
type NonceStore = { getStore(): string | undefined };

/** Server only: the server entry registers the per-request store; browsers get undefined. */
export function registerNonceStore(store: NonceStore) {
  (globalThis as Record<symbol, unknown>)[NONCE_STORE] = store;
}
export function currentNonce(): string | undefined {
  if (typeof window !== "undefined") return undefined;
  return (
    (globalThis as Record<symbol, unknown>)[NONCE_STORE] as NonceStore | undefined
  )?.getStore();
}
