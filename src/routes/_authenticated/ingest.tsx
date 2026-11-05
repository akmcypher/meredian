import { createFileRoute, Link } from "@tanstack/react-router";
import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useServerFn } from "@tanstack/react-start";
import { toast } from "sonner";
import { Copy, Play, Trash2 } from "lucide-react";
import { supabase } from "@/integrations/supabase/client";
import { PageHeader } from "@/components/PageHeader";
import { ResultTable } from "@/components/ResultTable";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { createSource, previewRest, runRestSource } from "@/lib/ingest.functions";
import { CDC_COMPOSE, workerScript, type IngestKind } from "@/lib/ingest";

export const Route = createFileRoute("/_authenticated/ingest")({
  head: () => ({
    meta: [
      { title: "Ingestion — Meridian" },
      {
        name: "description",
        content:
          "Pull data from REST APIs, Postgres, MySQL and Kafka/Debezium CDC into lakehouse tables.",
      },
      { property: "og:title", content: "Ingestion — Meridian" },
      {
        property: "og:description",
        content: "REST API, database and CDC ingestion into Meridian tables.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary" },
    ],
  }),
  component: IngestPage,
});

const KINDS: { k: IngestKind; label: string }[] = [
  { k: "rest", label: "REST API" },
  { k: "postgres", label: "Postgres" },
  { k: "mysql", label: "MySQL" },
  { k: "kafka_cdc", label: "Kafka raw events" },
];

function copy(s: string) {
  void navigator.clipboard.writeText(s);
  toast.success("Copied");
}

function IngestPage() {
  const qc = useQueryClient();
  const preview = useServerFn(previewRest);
  const create = useServerFn(createSource);
  const run = useServerFn(runRestSource);
  const [kind, setKind] = useState<IngestKind>("rest");
  const [name, setName] = useState("");
  const [target, setTarget] = useState("");
  const [url, setUrl] = useState("https://jsonplaceholder.typicode.com/users");
  const [headers, setHeaders] = useState("");
  const [path, setPath] = useState("");
  const [mode, setMode] = useState<"replace" | "append" | "upsert">("replace");
  const [key, setKey] = useState("id");
  const [query, setQuery] = useState("select * from customers");
  const [cursor, setCursor] = useState("updated_at");
  const [topic, setTopic] = useState("dbserver1.public.customers");
  const [rows, setRows] = useState<Record<string, unknown>[] | null>(null);
  const [created, setCreated] = useState<{ token: string; script: string } | null>(null);
  const endpoint =
    typeof window !== "undefined"
      ? `${window.location.origin}/api/public/ingest`
      : "/api/public/ingest";

  const { data: sources = [] } = useQuery({
    queryKey: ["ingest_sources"],
    queryFn: async () =>
      (await supabase.from("ingest_sources").select("*").order("created_at", { ascending: false }))
        .data ?? [],
  });

  const restConfig = () => {
    const h: Record<string, string> = {};
    headers.split("\n").forEach((l) => {
      const i = l.indexOf(":");
      if (i > 0) h[l.slice(0, i).trim()] = l.slice(i + 1).trim();
    });
    return { url, method: "GET" as const, headers: h, path, mode, key: key || undefined };
  };

  const doPreview = async () => {
    try {
      const r = await preview({ data: restConfig() });
      setRows(r.rows);
      toast.success(`${r.count} records found`);
    } catch (e) {
      toast.error((e as Error).message);
    }
  };

  const save = async () => {
    if (!name || !target) {
      toast.error("Give it a name and a target table");
      return;
    }
    const config =
      kind === "rest"
        ? restConfig()
        : kind === "kafka_cdc"
          ? { topic, key }
          : { query, cursor, key };
    try {
      const r = await create({ data: { name, kind, target, config } });
      if (r.token)
        setCreated({
          token: r.token,
          script: workerScript(kind, endpoint, { query, cursor, key, topic }),
        });
      else {
        await run({ data: { id: r.id } });
        toast.success("Source saved and first load done");
      }
      qc.invalidateQueries({ queryKey: ["ingest_sources"] });
      qc.invalidateQueries({ queryKey: ["user_tables"] });
    } catch (e) {
      toast.error((e as Error).message);
    }
  };

  const runNow = async (id: string) => {
    try {
      const r = await run({ data: { id } });
      toast.success(`Landed ${r.fetched} records · table-format commit pending`);
    } catch (e) {
      toast.error((e as Error).message);
    }
    qc.invalidateQueries({ queryKey: ["ingest_sources"] });
    qc.invalidateQueries({ queryKey: ["user_tables"] });
  };

  const remove = async (id: string) => {
    await supabase.from("ingest_sources").delete().eq("id", id);
    qc.invalidateQueries({ queryKey: ["ingest_sources"] });
  };

  return (
    <div>
      <PageHeader
        kicker="catalog"
        title="Ingestion"
        desc="Raw source batches land in immutable file storage. Iceberg/Delta commits are pending; existing legacy tables remain readable."
        actions={
          <Link to="/ingest/connectors">
            <Button variant="outline">Connectors catalog</Button>
          </Link>
        }
      />
      <section className="mb-8 space-y-4 border bg-card p-4">
        <div className="flex flex-wrap gap-2">
          {KINDS.map((x) => (
            <Button
              key={x.k}
              size="sm"
              variant={kind === x.k ? "default" : "outline"}
              onClick={() => {
                setKind(x.k);
                setCreated(null);
                setRows(null);
              }}
            >
              {x.label}
            </Button>
          ))}
        </div>
        <div className="grid gap-3 sm:grid-cols-2 [&>*]:min-w-0">
          <Input placeholder="Source name" value={name} onChange={(e) => setName(e.target.value)} />
          <Input
            placeholder="Target table (e.g. crm_users)"
            className="font-mono"
            value={target}
            onChange={(e) => setTarget(e.target.value)}
          />
        </div>
        {kind === "rest" && (
          <div className="space-y-3">
            <Input
              className="font-mono"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              placeholder="https://api.example.com/items"
            />
            <Textarea
              className="font-mono text-xs"
              rows={2}
              value={headers}
              onChange={(e) => setHeaders(e.target.value)}
              placeholder={"Headers, one per line\nAuthorization: Bearer ..."}
            />
            <div className="grid gap-3 sm:grid-cols-3 [&>*]:min-w-0">
              <Input
                className="font-mono"
                value={path}
                onChange={(e) => setPath(e.target.value)}
                placeholder="JSON path (e.g. data.items)"
              />
              <select
                className="h-9 border bg-background px-2 text-sm"
                value={mode}
                onChange={(e) => setMode(e.target.value as typeof mode)}
              >
                <option value="replace">Replace intent (raw batches retained)</option>
                <option value="append">Append</option>
                <option value="upsert">Upsert intent (no applied merge)</option>
              </select>
              <Input
                className="font-mono"
                value={key}
                onChange={(e) => setKey(e.target.value)}
                placeholder="Key column"
              />
            </div>
            <div className="flex gap-2">
              <Button variant="outline" onClick={doPreview}>
                Preview
              </Button>
              <Button onClick={save}>Save & load</Button>
            </div>
            {rows && rows.length > 0 && (
              <ResultTable columns={Object.keys(rows[0]!)} rows={rows as never} masked={[]} />
            )}
          </div>
        )}
        {(kind === "postgres" || kind === "mysql") && (
          <div className="space-y-3">
            <Textarea
              className="font-mono text-xs"
              rows={2}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />
            <div className="grid gap-3 sm:grid-cols-2 [&>*]:min-w-0">
              <Input
                className="font-mono"
                value={cursor}
                onChange={(e) => setCursor(e.target.value)}
                placeholder="Incremental column (blank = full load)"
              />
              <Input
                className="font-mono"
                value={key}
                onChange={(e) => setKey(e.target.value)}
                placeholder="Primary key"
              />
            </div>
            <p className="text-xs text-muted-foreground">
              Your database stays private: a small worker you run next to it reads new rows and
              pushes them here.
            </p>
            <Button onClick={save}>Create source & get worker</Button>
          </div>
        )}
        {kind === "kafka_cdc" && (
          <div className="space-y-3">
            <div className="grid gap-3 sm:grid-cols-2 [&>*]:min-w-0">
              <Input
                className="font-mono"
                value={topic}
                onChange={(e) => setTopic(e.target.value)}
                placeholder="Kafka topic"
              />
              <Input
                className="font-mono"
                value={key}
                onChange={(e) => setKey(e.target.value)}
                placeholder="Primary key"
              />
            </div>
            <p className="text-xs text-muted-foreground">
              Debezium envelopes are retained as append-only raw evidence. Updates and deletes are
              not applied as table merges; this is not a materialized CDC replica.
            </p>
            <Button onClick={save}>Create source & get worker</Button>
            <details>
              <summary className="cursor-pointer text-sm underline">
                Kafka + Debezium setup file
              </summary>
              <Button size="sm" variant="ghost" onClick={() => copy(CDC_COMPOSE)}>
                <Copy className="h-3 w-3" /> Copy
              </Button>
              <pre className="max-h-72 overflow-auto bg-muted p-3 text-[11px]">{CDC_COMPOSE}</pre>
            </details>
          </div>
        )}
        {created && (
          <div className="space-y-2 border border-primary p-3">
            <div className="text-sm font-semibold">
              Save this token now — it is shown only once.
            </div>
            <div className="flex gap-2">
              <code className="min-w-0 flex-1 break-all bg-muted p-2 text-xs">{created.token}</code>
              <Button size="sm" variant="outline" onClick={() => copy(created.token)}>
                <Copy className="h-3 w-3" />
              </Button>
            </div>
            <div className="flex items-center justify-between">
              <span className="label-mono">worker.mjs</span>
              <Button size="sm" variant="ghost" onClick={() => copy(created.script)}>
                <Copy className="h-3 w-3" /> Copy
              </Button>
            </div>
            <pre className="max-h-72 overflow-auto bg-muted p-3 text-[11px]">{created.script}</pre>
            <p className="text-xs text-muted-foreground">
              Run: <code>INGEST_TOKEN=… DATABASE_URL=… node worker.mjs</code>{" "}
              {kind === "kafka_cdc"
                ? "(set KAFKA_BROKERS instead of DATABASE_URL). Offsets are committed only after a batch lands."
                : "The cursor is saved to meridian-cursor.json (STATE_FILE) only after a batch lands; ONCE=1 runs a single sync."}
            </p>
          </div>
        )}
      </section>
      <div className="label-mono mb-2">sources · {sources.length}</div>
      <div className="divide-y border bg-card">
        {sources.length === 0 && (
          <div className="p-4 text-sm text-muted-foreground">No sources yet.</div>
        )}
        {sources.map((s) => (
          <div key={s.id} className="flex flex-wrap items-center gap-3 p-3">
            <span className="font-semibold">{s.name}</span>
            <Badge variant="outline">{KINDS.find((k) => k.k === s.kind)?.label ?? s.kind}</Badge>
            <span className="font-mono text-xs">→ {s.target_table}</span>
            <span className="text-xs text-muted-foreground">
              {s.last_run_at
                ? `${new Date(s.last_run_at).toLocaleString()} · ${s.last_status} · ${s.last_rows} rows`
                : "waiting for first batch"}
            </span>
            <div className="ml-auto flex gap-1">
              {s.kind === "rest" && (
                <Button size="sm" variant="outline" onClick={() => runNow(s.id)}>
                  <Play className="h-3 w-3" /> Run
                </Button>
              )}
              <Button
                size="sm"
                variant="ghost"
                aria-label="Delete source"
                onClick={() => remove(s.id)}
              >
                <Trash2 className="h-3 w-3" />
              </Button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
