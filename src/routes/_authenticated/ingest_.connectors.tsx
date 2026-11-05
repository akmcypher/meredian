import { createFileRoute, Link, useRouter } from "@tanstack/react-router";
import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useServerFn } from "@tanstack/react-start";
import { toast } from "sonner";
import { Play, Pause, Plug, RefreshCw } from "lucide-react";
import { supabase } from "@/integrations/supabase/client";
import { CONNECTORS, filterConnectors } from "@/lib/connector-registry";
import { testIngestSource, manageIngestSource, runRestSource } from "@/lib/ingest.functions";
import { ConnectWorkspace } from "@/components/ConnectWorkspace";
import { PageHeader } from "@/components/PageHeader";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
export const Route = createFileRoute("/_authenticated/ingest_/connectors")({
  head: () => ({
    meta: [
      { title: "Ingestion connectors — Meridian" },
      {
        name: "description",
        content: "Discover truthful source capabilities and manage durable ingestion connections.",
      },
      { property: "og:title", content: "Ingestion connectors — Meridian" },
      {
        property: "og:description",
        content: "Source adapters, configuration requirements and ingestion history.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary" },
    ],
  }),
  component: ConnectorsPage,
});
function ConnectorsPage() {
  const { routesByPath } = useRouter();
  const [q, setQ] = useState(""),
    [category, setCategory] = useState("all"),
    [status, setStatus] = useState("all"),
    [selected, setSelected] = useState("rest"),
    [sourceId, setSourceId] = useState("");
  const [headers, setHeaders] = useState(""),
    [busy, setBusy] = useState(false),
    [discovery, setDiscovery] = useState<string[]>([]);
  const qc = useQueryClient();
  const test = useServerFn(testIngestSource),
    manage = useServerFn(manageIngestSource),
    run = useServerFn(runRestSource);
  const sources = useQuery({
    queryKey: ["connector-sources"],
    queryFn: async () => {
      const r = await supabase
        .from("ingest_sources")
        .select(
          "id,name,kind,target_table,last_run_at,last_status,last_rows,enabled,adapter_version",
        )
        .order("created_at", { ascending: false });
      if (r.error) throw new Error("Unable to load connections");
      return r.data;
    },
  });
  const history = useQuery({
    queryKey: ["connector-history", sourceId],
    enabled: !!sourceId,
    queryFn: async () => {
      const r = await supabase
        .from("ingest_landing_batches")
        .select("id,row_count,mode,created_at,sha256")
        .eq("source_id", sourceId)
        .order("created_at", { ascending: false })
        .limit(30);
      if (r.error) throw new Error("Unable to load landing history");
      return r.data;
    },
  });
  const entry = CONNECTORS.find((c) => c.id === selected);
  const src = sources.data?.find((s) => s.id === sourceId);
  async function action(kind: "test" | "run" | "pause" | "rotate") {
    if (!src) return;
    setBusy(true);
    try {
      if (kind === "test") {
        const r = await test({ data: { id: src.id } });
        setDiscovery(r.columns);
        toast.success(`Endpoint returned ${r.count} records`);
      }
      if (kind === "run") {
        const r = await run({ data: { id: src.id } });
        toast.success(`${r.fetched} records durably landed; native table commit pending`);
      }
      if (kind === "pause") await manage({ data: { id: src.id, enabled: !src.enabled } });
      if (kind === "rotate") {
        const parsed = Object.fromEntries(
          headers
            .split("\n")
            .filter(Boolean)
            .map((line) => {
              const i = line.indexOf(":");
              if (i < 1) throw new Error("Each header needs a name and value");
              return [line.slice(0, i).trim(), line.slice(i + 1).trim()];
            }),
        );
        await manage({ data: { id: src.id, headers: parsed } });
        setHeaders("");
        toast.success("Credentials replaced securely");
      }
      await qc.invalidateQueries({ queryKey: ["connector-sources"] });
      await qc.invalidateQueries({ queryKey: ["connector-history"] });
    } catch (e) {
      toast.error((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="min-w-0">
      <PageHeader
        kicker="Data engineering / Ingestion"
        title="Connectors"
        desc="Meridian Connect is the primary database/topic replication workflow. Legacy REST and push landing are managed separately on Ingestion."
        actions={
          <Link to="/ingest">
            <Button variant="outline">
              <Plug />
              Add supported source
            </Button>
          </Link>
        }
      />
      <ConnectWorkspace />
      <div className="mb-5 flex flex-wrap gap-3">
        <Input
          aria-label="Search connectors"
          className="max-w-sm"
          placeholder="Search sources or formats"
          value={q}
          onChange={(e) => setQ(e.target.value)}
        />
        <select
          aria-label="Connector category"
          className="max-w-full border bg-background p-2"
          value={category}
          onChange={(e) => setCategory(e.target.value)}
        >
          <option value="all">All categories</option>
          {[...new Set(CONNECTORS.map((c) => c.category))].map((c) => (
            <option key={c}>{c}</option>
          ))}
        </select>
        <select
          aria-label="Connector status"
          className="border bg-background p-2"
          value={status}
          onChange={(e) => setStatus(e.target.value)}
        >
          {["all", ...new Set(CONNECTORS.map((connector) => connector.status))].map((s) => (
            <option key={s} value={s}>
              {s === "all" ? "All statuses" : s}
            </option>
          ))}
        </select>
      </div>
      <div className="grid gap-6 lg:grid-cols-[minmax(250px,1fr)_minmax(0,1fr)]">
        <div className="max-h-[600px] overflow-y-auto divide-y border">
          {filterConnectors(q, category, status).map((c) => (
            <Button
              variant="ghost"
              className="h-auto w-full justify-between gap-3 rounded-none p-3 text-left whitespace-normal"
              key={c.id}
              onClick={() => setSelected(c.id)}
            >
              <span>
                <strong>{c.name}</strong>
                <span className="block text-xs text-muted-foreground">{c.category}</span>
              </span>
              <Badge variant="outline">{c.status}</Badge>
            </Button>
          ))}
        </div>
        {entry && (
          <section className="min-w-0 space-y-4">
            <h2 className="text-xl font-semibold">{entry.name}</h2>
            <Badge variant="secondary">{entry.status}</Badge>
            <dl className="grid gap-2 text-sm">
              <dt className="font-semibold">Adapter</dt>
              <dd>
                {entry.adapter} · version {entry.version}
              </dd>
              <dt className="font-semibold">Modes / formats</dt>
              <dd>
                {entry.modes.join(", ") || "None implemented"} /{" "}
                {entry.formats.join(", ") || "None"}
              </dd>
              <dt className="font-semibold">Authentication</dt>
              <dd>{entry.auth.join(", ") || "Not implemented"}</dd>
              <dt className="font-semibold">Discovery</dt>
              <dd>{entry.discovery}</dd>
              {entry.capabilities && (
                <>
                  <dt className="font-semibold">Implemented and tested</dt>
                  <dd>{entry.capabilities}</dd>
                </>
              )}
              {entry.certification && (
                <>
                  <dt className="font-semibold">Certification evidence</dt>
                  <dd>{entry.certification}</dd>
                </>
              )}
              <dt className="font-semibold">Limitations</dt>
              <dd>{entry.limitations}</dd>
            </dl>
            {entry.destination && entry.destination in routesByPath && (
              <Link to={entry.destination as "/"}>
                <Button variant="outline">
                  Open existing {entry.destination.slice(1)} workflow
                </Button>
              </Link>
            )}
          </section>
        )}
      </div>
      <h2 className="mb-3 mt-8 text-lg font-semibold">Managed ingestion sources</h2>
      {sources.isLoading && <p>Loading connections…</p>}
      {sources.error && <p role="alert">{sources.error.message}</p>}
      {sources.data?.length === 0 && <p>No ingestion sources registered.</p>}
      <div className="flex flex-wrap gap-2">
        {sources.data?.map((s) => (
          <Button
            key={s.id}
            variant={sourceId === s.id ? "default" : "outline"}
            onClick={() => {
              setSourceId(s.id);
              setDiscovery([]);
            }}
          >
            {s.name} · {s.enabled ? "Enabled" : "Paused"}
          </Button>
        ))}
      </div>
      {src && (
        <section className="mt-5 space-y-4">
          <h3 className="font-semibold">
            {src.name} → {src.target_table}
          </h3>
          <p className="text-sm">
            {src.kind} · adapter v{src.adapter_version} ·{" "}
            {src.last_run_at ? new Date(src.last_run_at).toLocaleString() : "No sync yet"} ·{" "}
            {src.last_status ?? "Untested"} · {src.last_rows} records
          </p>
          <div className="flex flex-wrap gap-2">
            <Button
              disabled={busy || src.kind !== "rest"}
              variant="outline"
              onClick={() => void action("test")}
            >
              <Plug />
              Test & discover fields
            </Button>
            <Button
              disabled={busy || src.kind !== "rest" || !src.enabled}
              onClick={() => void action("run")}
            >
              <Play />
              Run initial / next sync
            </Button>
            <Button disabled={busy} variant="outline" onClick={() => void action("pause")}>
              <Pause />
              {src.enabled ? "Pause" : "Resume"}
            </Button>
          </div>
          {src.kind !== "rest" && (
            <p className="text-sm text-muted-foreground">
              Connection testing runs at the independent worker; no hosted success is simulated.
            </p>
          )}
          {discovery.length > 0 && (
            <p className="text-sm">Discovered response fields: {discovery.join(", ")}</p>
          )}
          {src.kind === "rest" && (
            <details>
              <summary className="cursor-pointer text-sm">Rotate HTTP credentials</summary>
              <Textarea
                className="my-3"
                aria-label="Replacement HTTP headers"
                placeholder="Header: value"
                value={headers}
                onChange={(e) => setHeaders(e.target.value)}
              />
              <Button variant="outline" disabled={busy} onClick={() => void action("rotate")}>
                <RefreshCw />
                Replace secret headers
              </Button>
            </details>
          )}
          <h3 className="font-semibold">Durable landing history</h3>
          {history.error && <p role="alert">{history.error.message}</p>}
          {history.data?.map((h) => (
            <div className="border-b py-2 text-sm" key={h.id}>
              {new Date(h.created_at).toLocaleString()} · {h.row_count} records · {h.mode} ·
              checksum {h.sha256.slice(0, 12)}
            </div>
          ))}
        </section>
      )}
    </div>
  );
}
