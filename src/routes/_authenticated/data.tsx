import { createFileRoute } from "@tanstack/react-router";
import { useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import Papa from "papaparse";
import { toast } from "sonner";
import { Trash2, Upload } from "lucide-react";
import { supabase } from "@/integrations/supabase/client";
import { PageHeader } from "@/components/PageHeader";
import { ResultTable } from "@/components/ResultTable";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useAuth } from "@/hooks/use-auth";
import { useUserTables } from "@/hooks/use-user-tables";
import { inferSchema, type Field } from "@/lib/schema";
import { audit } from "@/lib/audit";
import type { Row } from "@/lib/rows";
import { parseFile } from "@/lib/file-formats";
import { fieldsOf, writeTable } from "@/lib/tables";

export const Route = createFileRoute("/_authenticated/data")({
  head: () => ({
    meta: [
      { title: "Data upload — Meridian" },
      {
        name: "description",
        content: "Upload bounded file previews as legacy user tables.",
      },
      { property: "og:title", content: "Data upload — Meridian" },
      { property: "og:description", content: "Upload bounded files as legacy user tables." },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary" },
    ],
  }),
  component: DataPage,
});

const MAX_ROWS = 5000;
const tableName = (s: string) =>
  s
    .toLowerCase()
    .replace(/\.(csv|tsv|parquet|parq|avro|json|jsonl|ndjson)$/, "")
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^(\d)/, "t_$1")
    .slice(0, 60);

function coerce(rows: Record<string, string>[], fields: Field[]): Row[] {
  return rows.map((r) =>
    Object.fromEntries(
      fields.map((f) => {
        const v = (r[f.name] ?? "").trim();
        if (v === "") return [f.name, null];
        if (
          f.type === "int" ||
          f.type === "bigint" ||
          f.type === "decimal" ||
          f.type === "double"
        ) {
          const n = Number(v);
          return [f.name, Number.isNaN(n) ? v : n];
        }
        if (f.type === "boolean") return [f.name, v.toLowerCase() === "true"];
        return [f.name, v];
      }),
    ),
  );
}

function DataPage() {
  const { user } = useAuth();
  const qc = useQueryClient();
  const { data: tables = [] } = useUserTables();
  const [name, setName] = useState("");
  const [pending, setPending] = useState<{ fields: Field[]; rows: Row[] } | null>(null);
  const [open, setOpen] = useState<string | null>(null);

  const selection = useRef(0);
  const onFile = async (f: File) => {
    const generation = ++selection.current;
    setPending(null);
    if (!/\.(csv|tsv)$/i.test(f.name)) {
      try {
        const r = await parseFile(f.name, await f.arrayBuffer());
        if (generation !== selection.current) return;
        if (!r.rows.length) {
          toast.error("That file has no rows");
          return;
        }
        setName(tableName(f.name));
        setPending({ fields: fieldsOf(r.rows), rows: r.rows });
        if (r.total > r.rows.length)
          toast.warning(
            `${r.format} file has ${r.total} rows — only the first ${r.rows.length} are kept`,
          );
      } catch (e) {
        if (generation === selection.current) toast.error((e as Error).message);
      }
      return;
    }
    Papa.parse<Record<string, string>>(f, {
      header: true,
      skipEmptyLines: true,
      complete: (r) => {
        if (generation !== selection.current) return;
        const raw = r.data.slice(0, MAX_ROWS);
        if (!raw.length) {
          toast.error("That file has no rows");
          return;
        }
        const fields = inferSchema(raw);
        setName(tableName(f.name));
        setPending({ fields, rows: coerce(raw, fields) });
        if (r.data.length > MAX_ROWS) toast.warning(`Only the first ${MAX_ROWS} rows will be kept`);
      },
    });
  };

  const save = async () => {
    if (!pending || !user) return;
    const n = tableName(name);
    if (!n) {
      toast.error("Give the table a name");
      return;
    }
    let version: number;
    try {
      version = await writeTable(n, pending.rows, "upload", pending.fields);
    } catch (e) {
      toast.error((e as Error).message);
      return;
    }
    if (version > 1)
      toast.info(
        `Overwrote ${n} — version ${version}. Older versions stay available for time travel.`,
      );
    await audit("table.upload", n, {
      rows: pending.rows.length,
      columns: pending.fields.map((f) => f.name),
    });
    toast.success(`Table ${n} is ready — query it from notebooks`);
    setPending(null);
    qc.invalidateQueries({ queryKey: ["user_tables"] });
  };

  const remove = async (id: string, n: string) => {
    const { error } = await supabase.from("user_tables").delete().eq("id", id);
    if (error) {
      toast.error(error.message);
      return;
    }
    await audit("table.delete", n);
    qc.invalidateQueries({ queryKey: ["user_tables"] });
  };

  return (
    <div>
      <PageHeader
        kicker="catalog"
        title="Data upload"
        desc={`Upload CSV, Parquet, Avro or JSON (up to ${MAX_ROWS} rows). Uploading the same name again makes a new version you can time-travel to. It creates a legacy user table under tenant access policies, separate from Connect Bronze publication.`}
      />
      <section className="mb-8 border bg-card p-4">
        <label className="flex cursor-pointer items-center gap-2 text-sm">
          <Upload className="h-4 w-4" />{" "}
          <span className="underline">Choose a file — CSV, Parquet, Avro, JSON</span>
          <input
            type="file"
            accept=".csv,.tsv,.parquet,.parq,.avro,.json,.jsonl,.ndjson"
            className="hidden"
            onChange={(e) => e.target.files?.[0] && onFile(e.target.files[0])}
          />
        </label>
        {pending && (
          <div className="mt-4 space-y-3">
            <div className="flex gap-2">
              <Input
                className="max-w-xs font-mono"
                value={name}
                onChange={(e) => setName(e.target.value)}
              />
              <Button onClick={save}>Create table</Button>
              <Button variant="ghost" onClick={() => setPending(null)}>
                Cancel
              </Button>
            </div>
            <div className="flex flex-wrap gap-1">
              {pending.fields.map((f) => (
                <Badge key={f.name} variant="outline" className="font-mono text-[10px]">
                  {f.name}: {f.type}
                </Badge>
              ))}
            </div>
            <ResultTable
              columns={pending.fields.map((f) => f.name)}
              rows={pending.rows.slice(0, 10)}
              masked={[]}
            />
          </div>
        )}
      </section>
      <div className="label-mono mb-2">uploaded tables · {tables.length}</div>
      <div className="divide-y border bg-card">
        {tables.length === 0 && (
          <div className="p-4 text-sm text-muted-foreground">No uploads yet.</div>
        )}
        {tables.map((t) => {
          const cols = (t.columns as unknown as Field[]) ?? [];
          return (
            <div key={t.id} className="p-3">
              <div className="flex items-center gap-3">
                <button
                  className="font-mono text-sm font-semibold underline-offset-2 hover:underline"
                  onClick={() => setOpen(open === t.id ? null : t.id)}
                >
                  {t.name}
                </button>
                <span className="text-xs text-muted-foreground">
                  {t.row_count} rows · {cols.length} columns ·{" "}
                  {new Date(t.created_at).toLocaleDateString()}
                </span>
                {t.owner_id === user?.id && (
                  <Button
                    size="sm"
                    variant="ghost"
                    className="ml-auto"
                    aria-label="Delete table"
                    onClick={() => remove(t.id, t.name)}
                  >
                    <Trash2 className="h-3 w-3" />
                  </Button>
                )}
              </div>
              {open === t.id && (
                <div className="mt-3 space-y-3">
                  <ResultTable
                    columns={cols.map((c) => c.name)}
                    rows={((t.rows as unknown as Row[]) ?? []).slice(0, 20)}
                    masked={[]}
                  />
                  <VersionHistory table={t.name} tableId={t.id} />
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function VersionHistory({ table, tableId }: { table: string; tableId: string }) {
  const qc = useQueryClient();
  const {
    data: versions = [],
    error: historyError,
    isPending,
  } = useQuery({
    queryKey: ["table_versions", tableId],
    queryFn: async () => {
      const result = await supabase
        .from("table_versions")
        .select("id,version,operation,row_count,created_at")
        .eq("table_id", tableId)
        .order("version", { ascending: false });
      if (result.error) throw new Error("Version history unavailable");
      return result.data;
    },
  });
  const restore = async (v: number) => {
    try {
      const { data, error } = await supabase
        .from("table_versions")
        .select("rows,columns")
        .eq("table_id", tableId)
        .eq("version", v)
        .single();
      if (error || !data) throw new Error("This version is unavailable to your account");
      const nv = await writeTable(
        table,
        (data.rows as unknown as Row[]) ?? [],
        `restore v${v}`,
        (data.columns as unknown as Field[]) ?? undefined,
        tableId,
      );
      await audit("table.restore", table, { from_version: v, new_version: nv });
      toast.success(`Restored version ${v} as version ${nv}`);
      qc.invalidateQueries({ queryKey: ["user_tables"] });
      qc.invalidateQueries({ queryKey: ["table_versions", tableId] });
    } catch (e) {
      toast.error((e as Error).message);
    }
  };
  return (
    <div>
      {isPending && <p role="status">Loading version history…</p>}
      {historyError && <p role="alert">{historyError.message}</p>}
      <div className="label-mono mb-1">
        time travel · {versions.length} versions · query with{" "}
        <span className="normal-case">SELECT * FROM {table} VERSION AS OF n</span>
      </div>
      <div className="divide-y border">
        {!versions.length && (
          <div className="p-2 text-xs text-muted-foreground">
            No history yet — versions are recorded from now on.
          </div>
        )}
        {versions.map((v, i) => (
          <div key={v.id} className="flex flex-wrap items-center gap-3 p-2 font-mono text-xs">
            <Badge variant={i === 0 ? "default" : "outline"}>v{v.version}</Badge>
            <span>{v.operation}</span>
            <span>{v.row_count} rows</span>
            <span className="text-muted-foreground">{new Date(v.created_at).toLocaleString()}</span>
            {i > 0 && (
              <Button
                size="sm"
                variant="outline"
                className="ml-auto h-6"
                onClick={() => restore(v.version)}
              >
                Restore
              </Button>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
