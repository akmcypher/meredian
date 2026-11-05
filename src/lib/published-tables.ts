import { supabase } from "@/integrations/supabase/client";
import type { Row, Warehouse } from "./rows";
import { readFileTablePage } from "./file-tables";
import { AppStorageStore } from "./storage-store";
import { readSharedFileTable } from "./file-tables.functions";
import { FILE_READ_LIMITS } from "./file-manifest";

/** Owners read their own parts directly; everyone else goes through the server, which applies row rules and masks. */
export async function readAnyFileTable(
  t: { id: string; manifest_prefix: string },
  offset: number,
  limit: number,
) {
  const { data } = await supabase.auth.getSession();
  const uid = data.session?.user.id;
  if (uid && t.manifest_prefix.startsWith(`${uid}/`))
    return readFileTablePage(new AppStorageStore(), t.manifest_prefix, offset, limit);
  return readSharedFileTable({ data: { id: t.id, offset, limit } });
}

/** Most rows one query may pull into the in-browser engine; larger tables need a real engine. */
export const MAX_LOAD_ROWS = 200_000;

/** Names from `candidates` that the SQL mentions as whole identifiers (case-insensitive). */
export function referencedTables(sql: string, candidates: string[]): string[] {
  const words = new Set(sql.toLowerCase().match(/[a-z_][a-z0-9_]*/g) ?? []);
  return [...new Set(candidates.filter((c) => words.has(c.toLowerCase())))];
}

async function loadIngest(name: string, total: number): Promise<Row[]> {
  if (total > MAX_LOAD_ROWS)
    throw new Error(`${name} has ${total} rows; the in-app engine loads at most ${MAX_LOAD_ROWS}`);
  const out: Row[] = [];
  let after: string | undefined;
  for (;;) {
    const { data, error } = await supabase.rpc(
      "read_ingest_table",
      after ? { _name: name, _after: after, _limit: 1000 } : { _name: name, _limit: 1000 },
    );
    if (error) throw new Error(`${name}: ${error.message}`);
    const page = (data ?? []) as { pk: string; data: Row }[];
    for (const r of page) out.push(r.data);
    if (page.length < 1000) return out;
    after = page[page.length - 1]!.pk;
  }
}

async function loadFile(
  name: string,
  t: { id: string; manifest_prefix: string },
  total: number,
): Promise<Row[]> {
  if (total > MAX_LOAD_ROWS)
    throw new Error(`${name} has ${total} rows; the in-app engine loads at most ${MAX_LOAD_ROWS}`);
  const rows: Row[] = [];
  for (let offset = 0; offset < total; offset += FILE_READ_LIMITS.pageRows) {
    const page = await readAnyFileTable(
      t,
      offset,
      Math.min(FILE_READ_LIMITS.pageRows, total - offset),
    );
    rows.push(...page.rows);
  }
  return rows;
}

/**
 * Add published ingest/file tables that the SQL references to a copy of the warehouse.
 * Only tables the caller can see (RLS) are found; ingest rows come back masked by the database.
 */
export async function withPublishedTables(
  sql: string,
  wh: Warehouse,
): Promise<{ wh: Warehouse; loaded: string[] }> {
  const [ing, files] = await Promise.all([
    supabase.from("ingest_tables").select("name,row_count"),
    supabase.from("file_tables").select("id,name,manifest_prefix,row_count"),
  ]);
  const ingest = (ing.data ?? []).filter((t) => !(t.name in wh));
  const file = (files.data ?? []).filter(
    (t) => !(t.name in wh) && !ingest.some((i) => i.name === t.name),
  );
  const want = referencedTables(sql, [...ingest.map((t) => t.name), ...file.map((t) => t.name)]);
  if (!want.length) return { wh, loaded: [] };
  const next: Warehouse = { ...wh };
  await Promise.all(
    want.map(async (n) => {
      const i = ingest.find((t) => t.name === n);
      if (i) {
        next[n] = await loadIngest(n, Number(i.row_count));
        return;
      }
      const f = file.find((t) => t.name === n)!;
      next[n] = await loadFile(n, f, Number(f.row_count));
    }),
  );
  return { wh: next, loaded: want };
}
