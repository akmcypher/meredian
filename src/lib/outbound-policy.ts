/** Expands an IPv6 literal (including `::` and an embedded IPv4 tail) to 8 hextets, or null. */
function ipv6Hextets(value: string): number[] | null {
  let text = value.replace(/%.*$/, "");
  const v4 = /(\d+\.\d+\.\d+\.\d+)$/.exec(text);
  if (v4) {
    const o = v4[1]!.split(".").map(Number);
    if (o.length !== 4 || o.some((n) => !Number.isInteger(n) || n < 0 || n > 255)) return null;
    text =
      text.slice(0, -v4[1]!.length) +
      `${((o[0]! << 8) | o[1]!).toString(16)}:${((o[2]! << 8) | o[3]!).toString(16)}`;
  }
  const halves = text.split("::");
  if (halves.length > 2) return null;
  const part = (s: string) => (s ? s.split(":") : []);
  const head = part(halves[0]!),
    tail = halves.length === 2 ? part(halves[1]!) : [];
  const fill = halves.length === 2 ? 8 - head.length - tail.length : 0;
  if (fill < 0 || (halves.length === 1 && head.length !== 8)) return null;
  const all = [...head, ...Array<string>(fill).fill("0"), ...tail];
  if (all.length !== 8 || all.some((h) => !/^[0-9a-f]{1,4}$/.test(h))) return null;
  return all.map((h) => parseInt(h, 16));
}

/** Lexical URL checks shared by all direct server connector calls. */
export function isPublicAddress(address: string): boolean {
  const value = address.toLowerCase().replace(/^\[|\]$/g, "");
  if (value.includes(":")) {
    // Only global unicast 2000::/3 is public. Fail closed for mapped, translated, link-local,
    // ULA, multicast and reserved IPv6, and for transition prefixes that embed an IPv4 address
    // (6to4 2002::/16, Teredo and the other IETF assignments in 2001::/23), plus documentation.
    const h = ipv6Hextets(value);
    if (!h) return false;
    const [h0 = 0, h1 = 0] = h;
    return (
      (h0 & 0xe000) === 0x2000 &&
      h0 !== 0x2002 &&
      !(h0 === 0x2001 && (h1 < 0x200 || h1 === 0xdb8)) &&
      !(h0 === 0x3fff && h1 < 0x1000)
    );
  }
  const octets = value.split(".").map(Number);
  if (octets.length !== 4 || octets.some((n) => !Number.isInteger(n) || n < 0 || n > 255))
    return false;
  const [a = 0, b = 0, c = 0] = octets;
  return !(
    a === 0 ||
    a === 10 ||
    a === 127 ||
    a >= 224 ||
    (a === 169 && b === 254) ||
    (a === 172 && b >= 16 && b <= 31) ||
    (a === 192 && (b === 168 || b === 0 || (b === 88 && c === 99))) ||
    (a === 100 && b >= 64 && b <= 127) ||
    (a === 198 && (b === 18 || b === 19 || b === 51)) ||
    (a === 203 && b === 0 && c === 113)
  );
}

export function publicHttpsUrl(raw: string): URL {
  const url = new URL(raw);
  if (url.protocol !== "https:" || url.username || url.password || url.hash)
    throw new Error("Connector URLs require HTTPS without embedded credentials or fragments");
  const host = url.hostname.toLowerCase();
  if (url.port && url.port !== "443")
    throw new Error("Server connectors require the standard HTTPS port");
  if (
    host.endsWith(".") ||
    !host.includes(".") ||
    /(?:^|\.)(?:localhost|local|internal|lan|home|test|invalid)$/.test(host)
  )
    throw new Error("Private or reserved connector host is not allowed");
  if (/^[\d.]+$/.test(host) || host.includes(":")) {
    if (!isPublicAddress(host))
      throw new Error("Private or reserved connector address is not allowed");
  }
  return url;
}
