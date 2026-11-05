/** Lakehouse table writes with version history (time travel) and row-level security. */
import { myOrgId } from "./org";
import { supabase } from "@/integrations/supabase/client";
import type { Json } from "@/integrations/supabase/types";
import type { Row, Warehouse } from "./rows";
import { inferSchema, type Field } from "./schema";

export function fieldsOf(rows: Row[]): Field[] {
  const sample = rows
    .slice(0, 200)
    .map((r) =>
      Object.fromEntries(Object.entries(r).map(([k, v]) => [k, v == null ? "" : String(v)])),
    );
  return inferSchema(sample);
}

/** Create or overwrite a table, recording a new version each time. Returns the new version number. */
export async function writeTable(
  name: string,
  rows: Row[],
  operation: string,
  fields = fieldsOf(rows),
  expectedTableId?: string,
): Promise<number> {
  const result = await supabase.rpc("write_native_table", {
    _name: name,
    _rows: rows as unknown as NonNullable<Json>,
    _columns: fields as unknown as NonNullable<Json>,
    _operation: operation,
    ...(expectedTableId ? { _table_id: expectedTableId } : {}),
  });
  if (result.error) throw new Error(result.error.message);
  return result.data;
}

const TT = /\b([a-z_][\w.]*)\s+(?:VERSION\s+AS\s+OF\s+(\d+)|TIMESTAMP\s+AS\s+OF\s+'([^']+)')/gi;

/** `FROM t VERSION AS OF 3` / `FROM t TIMESTAMP AS OF '2026-01-01'` → loads that snapshot and rewrites the SQL. */
export async function resolveTimeTravel(
  sql: string,
  wh: Warehouse,
): Promise<{ sql: string; wh: Warehouse; used: string[] }> {
  const matches = [...sql.matchAll(TT)];
  if (!matches.length) return { sql, wh, used: [] };
  const next: Warehouse = { ...wh };
  const used: string[] = [];
  let out = sql;
  for (const m of matches) {
    const table = m[1]!,
      v = m[2],
      ts = m[3];
    const org = await myOrgId();
    const current = await supabase
      .from("user_tables")
      .select("id")
      .eq("name", table)
      .eq("organization_id", org!)
      .maybeSingle();
    if (current.error || !current.data)
      throw new Error(`No authorized table ${table} in this workspace`);
    let q = supabase.from("table_versions").select("version,rows").eq("table_id", current.data.id);
    q = v ? q.eq("version", Number(v)) : q.lte("created_at", new Date(ts!).toISOString());
    const { data } = await q.order("version", { ascending: false }).limit(1).maybeSingle();
    if (!data) throw new Error(`No version of ${table} ${v ? `#${v}` : `at ${ts}`}`);
    const alias = `${table.replace(/\./g, "_")}__v${data.version}`;
    next[alias] = (data.rows as unknown as Row[]) ?? [];
    out = out.replace(m[0], alias);
    used.push(`${table}@v${data.version}`);
  }
  return { sql: out, wh: next, used };
}

// Row policies are enforced only in the database (structured column/op/value in
// secure_tables() and read_ingest_table). row_policies.filter_sql is display text: never
// evaluate it in a browser, where steward-written SQL would run in other users' sessions.
