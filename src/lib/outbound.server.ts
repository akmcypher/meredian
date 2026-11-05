import { isPublicAddress, publicHttpsUrl } from "./outbound-policy";

export const OUTBOUND_MAX_BYTES = 8 * 1024 * 1024;

/** Bounded reads also enforce the limit when a provider omits or lies about Content-Length. */
export async function boundedResponseBytes(
  response: Response,
  maxBytes = OUTBOUND_MAX_BYTES,
): Promise<Uint8Array> {
  const declared = Number(response.headers.get("content-length"));
  if (declared > maxBytes) {
    await response.body?.cancel();
    throw new Error("Provider response exceeds byte limit");
  }
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.length;
      if (size > maxBytes) {
        await reader.cancel();
        throw new Error("Provider response exceeds byte limit");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const result = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }
  return result;
}

async function verifyPublicDns(host: string, signal: AbortSignal) {
  if (/^[\d.]+$/.test(host) || host.includes(":")) return;
  let found = false;
  for (const type of ["A", "AAAA"]) {
    const response = await fetch(
      `https://cloudflare-dns.com/dns-query?name=${encodeURIComponent(host)}&type=${type}`,
      { headers: { Accept: "application/dns-json" }, redirect: "error", signal },
    );
    if (!response.ok) throw new Error("Connector DNS could not be verified");
    const answer = JSON.parse(
      new TextDecoder().decode(await boundedResponseBytes(response, 64 * 1024)),
    ) as { Status?: number; Answer?: { type: number; data: string }[] };
    if (answer.Status !== 0) throw new Error("Connector DNS could not be verified");
    for (const record of answer.Answer ?? []) {
      if (record.type !== 1 && record.type !== 28) continue;
      found = true;
      if (!isPublicAddress(record.data))
        throw new Error("Connector DNS resolves to a private or reserved address");
    }
  }
  if (!found) throw new Error("Connector DNS has no public address");
}

type LookupAddress = { address: string; family: number };
type LookupCallback = (
  err: Error | null,
  address: string | LookupAddress[],
  family?: number,
) => void;
type DnsLookup = (
  host: string,
  options: { all: true; family?: number | "IPv4" | "IPv6" | undefined },
  callback: (err: Error | null, addresses: LookupAddress[]) => void,
) => void;

/**
 * Connect-time address check: the socket only opens to an address that was validated in the
 * same resolution, which closes the DNS-rebinding window between verifyPublicDns and the
 * request.
 */
export function publicOnlyLookup(dnsLookup: DnsLookup) {
  return (
    host: string,
    options: { all?: boolean | undefined; family?: number | "IPv4" | "IPv6" | undefined },
    callback: LookupCallback,
  ) => {
    dnsLookup(host, { family: options.family, all: true }, (err, addresses) => {
      if (err) return callback(err, "");
      if (!addresses.length || addresses.some((a) => !isPublicAddress(a.address)))
        return callback(new Error("Connector DNS resolves to a private or reserved address"), "");
      if (options.all) callback(null, addresses);
      else callback(null, addresses[0]!.address, addresses[0]!.family);
    });
  };
}

// Node runtimes (node-server preset, tests) pin through undici. Other runtimes (Cloudflare
// Workers) cannot hook DNS: they keep the DoH check and need an egress proxy for rebinding.
let dispatcher: Promise<unknown> | undefined;
function pinnedDispatcher(): Promise<unknown> {
  const isNode =
    typeof process !== "undefined" &&
    !!process.versions?.node &&
    globalThis.navigator?.userAgent !== "Cloudflare-Workers";
  dispatcher ??= isNode
    ? Promise.all([import("undici"), import("node:dns")]).then(
        ([{ Agent }, dns]) =>
          new Agent({ connect: { lookup: publicOnlyLookup(dns.lookup as unknown as DnsLookup) } }),
      )
    : Promise.resolve(null);
  return dispatcher;
}

/** No redirects: credentials can never follow a provider-controlled Location header. */
export async function safeOutboundFetch(
  raw: string,
  init: RequestInit = {},
  timeoutMs = 15000,
  maxBytes = OUTBOUND_MAX_BYTES,
): Promise<Response> {
  const url = publicHttpsUrl(raw);
  const signal = init.signal
    ? AbortSignal.any([init.signal, AbortSignal.timeout(timeoutMs)])
    : AbortSignal.timeout(timeoutMs);
  await verifyPublicDns(url.hostname, signal);
  const pinned = await pinnedDispatcher();
  const response = await fetch(url.href, {
    ...init,
    redirect: "manual",
    signal,
    ...(pinned ? { dispatcher: pinned } : {}),
  } as RequestInit);
  if (response.status >= 300 && response.status < 400) {
    await response.body?.cancel();
    throw new Error("Connector redirects are not allowed");
  }
  const bytes = await boundedResponseBytes(response, maxBytes);
  return new Response(bytes as BodyInit, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
}
