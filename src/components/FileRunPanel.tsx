import { useState } from "react";
import { toast } from "sonner";
import { supabase } from "@/integrations/supabase/client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { runFileIngest, verifyManifest, type Manifest } from "@/lib/file-ingest";
import { AppStorageStore } from "@/lib/storage-store";
import { publishFileRun, toTableName } from "@/lib/file-tables";
import { readAnyFileTable } from "@/lib/published-tables";
import { runParquetIngest } from "@/lib/parquet-ingest";
import { runAvroIngest } from "@/lib/avro-ingest";
import { useQuery, useQueryClient } from "@tanstack/react-query";

/** Upload a CSV to file storage, then import it in parallel byte ranges with checksums and a manifest-last commit. */
export function FileRunPanel() {
  const [file, setFile] = useState<File | null>(null);
  const [parts, setParts] = useState(8);
  const [workers, setWorkers] = useState(4);
  const [busy, setBusy] = useState(false);
  const [m, setM] = useState<Manifest | null>(null);
  const [check, setCheck] = useState<string>("");
  const [stats, setStats] = useState<{ ms: number; reads: number } | null>(null);
  const [prefix, setPrefix] = useState("");
  const [name, setName] = useState("");
  const [published, setPublished] = useState(false);
  const [view, setView] = useState<{
    id: string;
    name: string;
    columns: string[];
    rows: Record<string, unknown>[];
    total: number;
    offset: number;
    prefix: string;
  } | null>(null);
  const qc = useQueryClient();
  const { data: tables = [] } = useQuery({
    queryKey: ["file_tables"],
    queryFn: async () =>
      (
        await supabase
          .from("file_tables")
          .select("id,name,manifest_prefix,row_count,part_count,columns,published_at")
          .order("published_at", { ascending: false })
      ).data ?? [],
  });

  async function publish() {
    setBusy(true);
    try {
      await publishFileRun(new AppStorageStore(), prefix, name);
      setPublished(true);
      toast.success(`Published ${name}`);
      qc.invalidateQueries({ queryKey: ["file_tables"] });
      qc.invalidateQueries({ queryKey: ["file_tables_catalog"] });
    } catch (e) {
      toast.error((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  async function open(t: { id: string; name: string; manifest_prefix: string }, offset = 0) {
    try {
      const r = await readAnyFileTable(t, offset, 50);
      setView({ id: t.id, name: t.name, ...r, offset, prefix: t.manifest_prefix });
    } catch (e) {
      toast.error((e as Error).message);
    }
  }

  async function run() {
    if (!file) return;
    setBusy(true);
    setM(null);
    setCheck("");
    setPublished(false);
    try {
      const { data: u } = await supabase.auth.getUser();
      if (!u.user) throw new Error("Sign in first");
      const id = crypto.randomUUID();
      const src = `${u.user.id}/sources/${id}/${file.name.replace(/[^\w.-]/g, "_")}`;
      const out = `${u.user.id}/runs/${id}`;
      const store = new AppStorageStore();
      await store.uploadFile(src, file);
      const t = performance.now();
      const isParquet = /\.parquet$/i.test(file.name);
      const isAvro = /\.avro$/i.test(file.name);
      const res = await (isParquet ? runParquetIngest : isAvro ? runAvroIngest : runFileIngest)(
        store,
        src,
        out,
        { parts, concurrency: workers },
      );
      setStats({ ms: Math.round(performance.now() - t), reads: store.rangeReads });
      setM(res);
      setPrefix(out);
      setName(toTableName(file.name));
      const v = await verifyManifest(store, out);
      setCheck(v.ok ? "All parts verified against their fingerprints" : v.problems.join("; "));
      toast.success(`Imported ${res.totalRows.toLocaleString()} rows in ${res.parts.length} parts`);
    } catch (e) {
      setCheck((e as Error).message);
      toast.error((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className="space-y-3 rounded-lg border bg-card p-4">
      <h2 className="font-semibold">File import · browser-local execution</h2>
      <div className="flex flex-wrap items-end gap-3">
        <Input
          type="file"
          accept=".csv,text/csv,.parquet,.avro"
          aria-label="Data file"
          className="max-w-xs"
          onChange={(e) => setFile(e.target.files?.[0] ?? null)}
        />
        <label className="text-sm">
          Parts
          <Input
            type="number"
            min={1}
            max={200}
            value={parts}
            onChange={(e) => setParts(Number(e.target.value))}
            className="w-24"
          />
        </label>
        <label className="text-sm">
          Workers
          <Input
            type="number"
            min={1}
            max={16}
            value={workers}
            onChange={(e) => setWorkers(Number(e.target.value))}
            className="w-24"
          />
        </label>
        <Button onClick={run} disabled={!file || busy}>
          {busy ? "Importing…" : "Upload & import"}
        </Button>
      </div>
      {!m && check && <p className="text-sm text-destructive">{check}</p>}
      {m && (
        <div className="space-y-2 text-sm">
          <p>
            <b>{m.totalRows.toLocaleString()}</b> rows · {m.parts.length} parts · {m.columns.length}{" "}
            columns{stats && ` · ${stats.ms} ms · ${stats.reads} slice reads`}
          </p>
          <p className={check.startsWith("All") ? "text-primary" : "text-destructive"}>{check}</p>
          {check.startsWith("All") && !published && (
            <div className="flex flex-wrap items-end gap-2">
              <label className="text-sm">
                Table name
                <Input value={name} onChange={(e) => setName(e.target.value)} className="w-56" />
              </label>
              <Button onClick={publish} disabled={busy || !/^[a-z_][a-z0-9_]{0,62}$/.test(name)}>
                Publish as table
              </Button>
            </div>
          )}
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-left text-muted-foreground">
                  <th>Part</th>
                  <th>Rows</th>
                  <th>Bytes</th>
                  <th>Attempts</th>
                  <th>Fingerprint</th>
                </tr>
              </thead>
              <tbody>
                {m.parts.map((p) => (
                  <tr key={p.idx} className="border-t">
                    <td>{p.idx}</td>
                    <td>{p.rows}</td>
                    <td>{p.bytes}</td>
                    <td>{p.attempts}</td>
                    <td className="font-mono">{p.sha256.slice(0, 12)}…</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
      {tables.length > 0 && (
        <div className="space-y-2 border-t pt-3">
          <h3 className="text-sm font-semibold">Published file tables</h3>
          <div className="flex flex-wrap gap-2">
            {tables.map((t) => (
              <Button
                key={t.id}
                size="sm"
                variant={view?.name === t.name ? "default" : "outline"}
                onClick={() => open(t)}
              >
                {t.name} · {Number(t.row_count).toLocaleString()} rows
              </Button>
            ))}
          </div>
          {view && (
            <div className="space-y-2">
              <div className="flex items-center gap-2 text-xs text-muted-foreground">
                Rows {view.offset + 1}–{view.offset + view.rows.length} of{" "}
                {view.total.toLocaleString()}
                <Button
                  size="sm"
                  variant="outline"
                  disabled={view.offset === 0}
                  onClick={() =>
                    open(
                      { id: view.id, name: view.name, manifest_prefix: view.prefix },
                      Math.max(0, view.offset - 50),
                    )
                  }
                >
                  Prev
                </Button>
                <Button
                  size="sm"
                  variant="outline"
                  disabled={view.offset + 50 >= view.total}
                  onClick={() =>
                    open(
                      { id: view.id, name: view.name, manifest_prefix: view.prefix },
                      view.offset + 50,
                    )
                  }
                >
                  Next
                </Button>
              </div>
              <div className="max-h-96 overflow-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="text-left text-muted-foreground">
                      {view.columns.map((c) => (
                        <th key={c} className="pr-3">
                          {c}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {view.rows.map((r, i) => (
                      <tr key={i} className="border-t">
                        {view.columns.map((c) => (
                          <td key={c} className="pr-3 whitespace-pre-wrap">
                            {String(r[c] ?? "")}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}
    </section>
  );
}
