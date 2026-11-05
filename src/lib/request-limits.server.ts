/**
 * Server-only request limits for public endpoints: bodies are
 * read with a byte cap before parsing, and each caller is held to a fixed-window request budget
 * kept in Postgres (consume_rate_limit, service role only).
 */
export class RequestLimitError extends Error {
  constructor(
    readonly status: 413 | 429 | 503,
    message: string,
  ) {
    super(message);
  }
}

/** Reads at most maxBytes; a larger body (declared or streamed) is rejected without buffering it. */
export async function readBodyCapped(
  request: Pick<Request, "headers" | "body">,
  maxBytes: number,
): Promise<Uint8Array> {
  const declared = Number(request.headers.get("content-length"));
  if (declared > maxBytes) throw new RequestLimitError(413, "request_too_large");
  const reader = request.body?.getReader();
  if (!reader) return new Uint8Array();
  const chunks: Uint8Array[] = [];
  let size = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    size += value.length;
    if (size > maxBytes) {
      await reader.cancel();
      throw new RequestLimitError(413, "request_too_large");
    }
    chunks.push(value);
  }
  const out = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

/** JSON body under a byte cap; null when the body is not valid JSON. */
export async function readJsonCapped(request: Request, maxBytes: number): Promise<unknown> {
  const text = new TextDecoder().decode(await readBodyCapped(request, maxBytes));
  try {
    return JSON.parse(text || "null");
  } catch {
    return null;
  }
}

/** Requests allowed per window for each limit, overridable through the environment. */
export function rateLimit(name: "chat" | "worker", env = process.env) {
  const defaults = { chat: 20, worker: 600 };
  const raw = Number(env[`RATE_LIMIT_${name.toUpperCase()}_PER_MINUTE`]);
  return Number.isInteger(raw) && raw > 0 ? raw : defaults[name];
}

/** Counts one request for key; throws 429 over the limit and 503 (fail closed) on errors. */
export async function enforceRateLimit(key: string, limit: number, windowSeconds = 60) {
  const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
  const { data, error } = await supabaseAdmin.rpc("consume_rate_limit", {
    _key: key,
    _limit: limit,
    _window_seconds: windowSeconds,
  });
  if (error) throw new RequestLimitError(503, "rate_limit_unavailable");
  if (!data) throw new RequestLimitError(429, "rate_limited");
}

export function limitResponse(e: RequestLimitError) {
  return Response.json(
    { error: e.message },
    { status: e.status, headers: e.status === 429 ? { "retry-after": "60" } : {} },
  );
}
