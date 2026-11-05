/**
 * Partition planning for scalable ingestion. Pure functions shared by the browser planner,
 * the in-app extractor (server functions) and the self-host worker/Spark kit.
 *
 * Three distinct layers:
 *  - logical partition: a non-overlapping predicate over the source (what a worker owns)
 *  - extraction batch: a keyset page inside one partition (what one request moves)
 *  - spark partition: how the target engine splits work when writing (sized by rows)
 */
export type Strategy =
  | { kind: "range"; column: string; n: number }
  | { kind: "hash"; column: string; n: number }
  | { kind: "modulo"; column: string; n: number }
  | { kind: "date"; column: string; grain: "day" | "month" | "quarter" }
  | { kind: "key"; column: string; values: string[] }
  | { kind: "file"; files: { path: string; bytes: number }[]; n: number }
  | { kind: "sql"; predicates: string[] };

export type Predicate =
  | { kind: "range"; column: string; lo: number; hi: number; last: boolean }
  | { kind: "null"; column: string }
  | { kind: "hash"; column: string; n: number; i: number }
  | { kind: "modulo"; column: string; n: number; i: number }
  | { kind: "date"; column: string; from: string; to: string }
  | { kind: "key"; column: string; values: string[] }
  | { kind: "key_other"; column: string; exclude: string[] }
  | { kind: "file"; files: string[] }
  | { kind: "sql"; where: string };

export interface ColumnStats {
  min: number | null;
  max: number | null;
  minDate?: string | null;
  maxDate?: string | null;
  nullFraction: number;
  distinct: number;
  indexed: boolean;
}
export interface PlannedPartition {
  idx: number;
  label: string;
  predicate: Predicate;
  estRows: number;
}

export type Row = Record<string, unknown>;

/** Browser/demo FNV-1a over UTF-16 code units. Native source SQL hashes differ;
 * do not treat a demo hash coverage preview as source-engine verification. */
export function fnv1a(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

function addMonths(d: Date, m: number) {
  const x = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + m, 1));
  return x;
}
function iso(d: Date) {
  return d.toISOString().slice(0, 10);
}

/** Build non-overlapping predicates. Nullable columns always get a dedicated NULL partition. */
export function planPartitions(
  s: Strategy,
  stats: ColumnStats | null,
): Omit<PlannedPartition, "estRows">[] {
  if ("n" in s && (!Number.isSafeInteger(s.n) || s.n < 1 || s.n > 2000)) {
    throw new Error("Logical partition count must be an integer between 1 and 2000");
  }
  const out: Omit<PlannedPartition, "estRows">[] = [];
  const push = (label: string, predicate: Predicate) =>
    out.push({ idx: out.length, label, predicate });
  const withNulls = (col: string) => {
    if (stats && stats.nullFraction > 0) push(`${col} IS NULL`, { kind: "null", column: col });
  };
  switch (s.kind) {
    case "range": {
      if (!stats || stats.min == null || stats.max == null)
        throw new Error("Range partitioning needs min/max of " + s.column);
      if (!Number.isFinite(stats.min) || !Number.isFinite(stats.max) || stats.max < stats.min) {
        throw new Error("Range partitioning needs finite, ordered min/max values");
      }
      const n = Math.max(1, Math.min(s.n, stats.max - stats.min + 1));
      const step = (stats.max - stats.min + 1) / n;
      for (let i = 0; i < n; i++) {
        const lo = Math.floor(stats.min + i * step);
        const hi = i === n - 1 ? stats.max : Math.floor(stats.min + (i + 1) * step);
        push(i === n - 1 ? `${s.column} ∈ [${lo}, ${hi}]` : `${s.column} ∈ [${lo}, ${hi})`, {
          kind: "range",
          column: s.column,
          lo,
          hi,
          last: i === n - 1,
        });
      }
      withNulls(s.column);
      break;
    }
    case "hash":
    case "modulo":
      for (let i = 0; i < s.n; i++)
        push(`${s.kind}(${s.column}) % ${s.n} = ${i}`, {
          kind: s.kind,
          column: s.column,
          n: s.n,
          i,
        });
      withNulls(s.column);
      break;
    case "date": {
      if (!stats?.minDate || !stats.maxDate)
        throw new Error("Date partitioning needs min/max dates of " + s.column);
      const end = new Date(stats.maxDate + "T00:00:00Z");
      let cur = new Date(stats.minDate + "T00:00:00Z");
      if (!Number.isFinite(cur.getTime()) || !Number.isFinite(end.getTime()) || cur > end) {
        throw new Error("Date partitioning needs valid, ordered dates");
      }
      if (s.grain !== "day") cur = addMonths(cur, 0);
      if (s.grain === "quarter")
        cur = new Date(Date.UTC(cur.getUTCFullYear(), Math.floor(cur.getUTCMonth() / 3) * 3, 1));
      let guard = 0;
      while (cur <= end) {
        if (guard++ >= 2000)
          throw new Error("Date plan exceeds 2000 partitions; choose month or quarter grain");
        const next =
          s.grain === "day"
            ? new Date(cur.getTime() + 86400000)
            : addMonths(cur, s.grain === "month" ? 1 : 3);
        push(`${s.column} ∈ [${iso(cur)}, ${iso(next)})`, {
          kind: "date",
          column: s.column,
          from: iso(cur),
          to: iso(next),
        });
        cur = next;
      }
      withNulls(s.column);
      break;
    }
    case "key": {
      const vals = [...new Set(s.values.map((v) => v.trim()).filter(Boolean))];
      for (const v of vals)
        push(`${s.column} = '${v}'`, { kind: "key", column: s.column, values: [v] });
      push(`${s.column} not in listed keys`, {
        kind: "key_other",
        column: s.column,
        exclude: vals,
      });
      withNulls(s.column);
      break;
    }
    case "file": {
      if (!s.files.length) break;
      if (s.files.some((f) => !Number.isFinite(f.bytes) || f.bytes < 0)) {
        throw new Error("File sizes must be finite and non-negative");
      }
      // Largest-first bin packing so each partition carries a similar number of bytes.
      const bins = Array.from({ length: Math.max(1, Math.min(s.n, s.files.length)) }, () => ({
        bytes: 0,
        files: [] as string[],
      }));
      for (const f of [...s.files].sort((a, b) => b.bytes - a.bytes)) {
        const b = bins.reduce((m, x) => (x.bytes < m.bytes ? x : m));
        b.bytes += f.bytes;
        b.files.push(f.path);
      }
      bins.forEach((b, i) =>
        push(`files #${i} (${b.files.length})`, { kind: "file", files: b.files }),
      );
      break;
    }
    case "sql":
      s.predicates
        .map((p) => p.trim())
        .filter(Boolean)
        .forEach((w) => push(w, { kind: "sql", where: w }));
      break;
  }
  return out;
}

const q = (c: string) => `"${c.replace(/"/g, '""')}"`;
const lit = (v: string) => `'${v.replace(/'/g, "''")}'`;

/** Render a predicate as a WHERE clause for Postgres/MySQL/Spark SQL. */
export function predicateSql(
  p: Predicate,
  dialect: "postgres" | "mysql" | "spark" = "postgres",
): string {
  const col = (c: string) =>
    dialect === "mysql" || dialect === "spark" ? "`" + c.replace(/`/g, "``") + "`" : q(c);
  switch (p.kind) {
    case "range":
      return `${col(p.column)} >= ${p.lo} AND ${col(p.column)} ${p.last ? "<=" : "<"} ${p.hi}`;
    case "null":
      return `${col(p.column)} IS NULL`;
    case "modulo":
      return `${col(p.column)} IS NOT NULL AND MOD(MOD(${col(p.column)}, ${p.n}) + ${p.n}, ${p.n}) = ${p.i}`;
    case "hash":
      return dialect === "postgres"
        ? `${col(p.column)} IS NOT NULL AND MOD(ABS(hashtext(${col(p.column)}::text)), ${p.n}) = ${p.i}`
        : dialect === "mysql"
          ? `${col(p.column)} IS NOT NULL AND MOD(CRC32(${col(p.column)}), ${p.n}) = ${p.i}`
          : `${col(p.column)} IS NOT NULL AND pmod(hash(${col(p.column)}), ${p.n}) = ${p.i}`;
    case "date":
      return `${col(p.column)} >= ${lit(p.from)} AND ${col(p.column)} < ${lit(p.to)}`;
    case "key":
      return `${col(p.column)} IN (${p.values.map(lit).join(", ")})`;
    case "key_other":
      return p.exclude.length
        ? `${col(p.column)} IS NOT NULL AND ${col(p.column)} NOT IN (${p.exclude.map(lit).join(", ")})`
        : `${col(p.column)} IS NOT NULL`;
    case "file":
      return `input_file_name() IN (${p.files.map(lit).join(", ")})`;
    case "sql":
      return `(${p.where})`;
  }
}

/** Evaluate a predicate against a row (used by the in-app extractor and for coverage checks). */
export function matches(p: Predicate, row: Row): boolean {
  const v = "column" in p ? row[p.column] : undefined;
  switch (p.kind) {
    case "null":
      return v == null;
    case "range":
      return typeof v === "number" && v >= p.lo && (p.last ? v <= p.hi : v < p.hi);
    case "modulo":
      return typeof v === "number" && ((v % p.n) + p.n) % p.n === p.i;
    case "hash":
      return v != null && fnv1a(String(v)) % p.n === p.i;
    case "date":
      return typeof v === "string" && v >= p.from && v < p.to;
    case "key":
      return v != null && p.values.includes(String(v));
    case "key_other":
      return v != null && !p.exclude.includes(String(v));
    case "file":
      return typeof row["__file"] === "string" && p.files.includes(row["__file"]);
    case "sql":
      return false; // custom SQL runs only at the source
  }
}

/** Every sampled row must fall in exactly one partition. Returns counts + violations. */
export function checkCoverage(parts: { predicate: Predicate }[], sample: Row[]) {
  let uncovered = 0,
    overlapping = 0;
  const counts = parts.map(() => 0);
  for (const r of sample) {
    let hits = 0;
    parts.forEach((p, i) => {
      if (matches(p.predicate, r)) {
        hits++;
        counts[i] = (counts[i] ?? 0) + 1;
      }
    });
    if (hits === 0) uncovered++;
    else if (hits > 1) overlapping++;
  }
  return { counts, uncovered, overlapping, ok: uncovered === 0 && overlapping === 0 };
}

/** Skew = largest partition vs. mean. > 2 means one worker will dominate the run. */
export function detectSkew(est: number[]) {
  const nonEmpty = est.filter((x) => x > 0);
  const mean = est.reduce((a, b) => a + b, 0) / Math.max(1, est.length);
  const max = Math.max(0, ...est);
  const ratio = mean > 0 ? max / mean : 0;
  const hot = est.map((x, i) => (mean > 0 && x / mean > 2 ? i : -1)).filter((i) => i >= 0);
  return {
    ratio: Math.round(ratio * 100) / 100,
    skewed: ratio > 2,
    hot,
    empty: est.length - nonEmpty.length,
  };
}

/** Suggest a strategy from column stats: prefer indexed numeric range, else hash for high cardinality. */
export function recommend(
  column: string,
  stats: ColumnStats,
  totalRows: number,
  workers: number,
): { strategy: Strategy; reason: string } {
  const n = Math.max(workers, Math.min(256, Math.ceil(totalRows / 50_000)) || 1);
  if (stats.min != null && stats.max != null && stats.indexed)
    return {
      strategy: { kind: "range", column, n },
      reason: `${column} is indexed and numeric: range predicates use the index (no full scans, no OFFSET).`,
    };
  if (stats.distinct > n * 10)
    return {
      strategy: { kind: "hash", column, n },
      reason: `${column} has high cardinality (~${stats.distinct}); hashing spreads rows evenly.`,
    };
  return {
    strategy: { kind: "key", column, values: [] },
    reason: `${column} has few distinct values; split by key and isolate the large ones.`,
  };
}

export function sparkPartitions(totalRows: number, rowsPerSparkPartition = 100_000) {
  return Math.max(1, Math.ceil(totalRows / rowsPerSparkPartition));
}

/** Additive-increase / multiplicative-decrease concurrency controller (source throttling). */
export function nextConcurrency(
  cur: number,
  opts: { min: number; max: number; latencyMs: number; targetMs: number; error: boolean },
) {
  if (opts.error || opts.latencyMs > opts.targetMs * 1.5)
    return Math.max(opts.min, Math.floor(cur / 2));
  if (opts.latencyMs < opts.targetMs) return Math.min(opts.max, cur + 1);
  return cur;
}

// ---------- Demo source: deterministic synthetic "orders" table (any size, generated on the server) ----------
const REGIONS = ["EU", "NA", "LATAM", "MEA"];
export function demoRow(id: number): Row {
  const day = id % 365;
  return {
    id,
    customer_id: (id * 7919) % 50_000,
    region: id % 10 < 5 ? "APAC" : REGIONS[id % 4], // 50% APAC: intentional skew
    amount: Math.round(((id * 37) % 100_000) / 10) / 10,
    order_date: iso(new Date(Date.UTC(2025, 0, 1) + day * 86400000)),
    coupon: id % 13 === 0 ? null : `C${id % 97}`,
  };
}
export const DEMO_COLUMNS = ["id", "customer_id", "region", "amount", "order_date", "coupon"];

export function demoStats(column: string, total: number): ColumnStats {
  const sample = demoSample(total, 2000);
  const vals = sample.map((r) => r[column]);
  const nn = vals.filter((v) => v != null);
  const nums = nn.filter((v): v is number => typeof v === "number");
  const strs = nn.filter((v): v is string => typeof v === "string").sort();
  return {
    min: column === "id" ? 1 : nums.length ? Math.min(...nums) : null,
    max: column === "id" ? total : nums.length ? Math.max(...nums) : null,
    minDate: column === "order_date" ? (strs[0] ?? null) : null,
    maxDate: column === "order_date" ? (strs[strs.length - 1] ?? null) : null,
    nullFraction: (vals.length - nn.length) / Math.max(1, vals.length),
    distinct: column === "id" ? total : new Set(nn.map(String)).size,
    indexed: column === "id",
  };
}
export function demoSample(total: number, n: number): Row[] {
  const out: Row[] = [];
  for (let i = 0; i < n; i++) out.push(demoRow(1 + Math.floor(((i + 0.5) / n) * total)));
  return out;
}

/** Keyset extraction over the demo source: ids strictly after `afterId`, matching the predicate. */
export function demoBatch(p: Predicate, total: number, afterId: number, batchSize: number) {
  let id = afterId + 1;
  const end =
    p.kind === "range" && p.column === "id" ? Math.min(total, p.last ? p.hi : p.hi - 1) : total;
  if (p.kind === "range" && p.column === "id") id = Math.max(id, p.lo);
  const rows: Row[] = [];
  for (; id <= end && rows.length < batchSize; id++) {
    const r = demoRow(id);
    if (matches(p, r)) rows.push(r);
  }
  return { rows, lastId: id - 1, done: id > end };
}
