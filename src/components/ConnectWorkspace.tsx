import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useServerFn } from "@tanstack/react-start";
import { toast } from "sonner";
import { Play, Plug, Search, Pause, X } from "lucide-react";
import { supabase } from "@/integrations/supabase/client";
import {
  createConnect,
  configureConnect,
  enqueueConnect,
  cancelConnect,
} from "@/lib/connect.functions";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Badge } from "./ui/badge";
import { inspectCommittedBatch, type CommittedBatch } from "@/lib/connect-inspection";
export function ConnectWorkspace() {
  const [inspected, setInspected] = useState<CommittedBatch | null>(null);
  const inspection = useQuery({
    queryKey: ["connect-batch-inspection", inspected?.batch_id],
    enabled: !!inspected,
    retry: false,
    queryFn: () => inspectCommittedBatch(inspected!),
  });
  const [selected, setSelected] = useState(""),
    [source, setSource] = useState(""),
    [name, setName] = useState(""),
    [ref, setRef] = useState("postgres"),
    [stream, setStream] = useState(""),
    [cursor, setCursor] = useState("updated_at"),
    [interval, setInterval] = useState("0"),
    [mode, setMode] = useState<"incremental" | "full_refresh">("incremental"),
    [busy, setBusy] = useState(false);
  const qc = useQueryClient();
  const create = useServerFn(createConnect),
    configure = useServerFn(configureConnect),
    enqueue = useServerFn(enqueueConnect),
    cancel = useServerFn(cancelConnect);
  const query = useQuery({
    queryKey: ["connect-workspace"],
    refetchInterval: 5000,
    queryFn: async () => {
      const [c, s, r] = await Promise.all([
        supabase.from("connect_connections").select("*"),
        supabase
          .from("ingest_sources")
          .select("id,name,kind")
          .in("kind", ["postgres", "mysql", "kafka"]),
        supabase
          .from("connect_runs")
          .select(
            "id,connection_id,operation,status,attempts,rows_committed,bytes_committed,error_code,created_at,finished_at",
          )
          .order("created_at", { ascending: false })
          .limit(50),
      ]);
      if (c.error || s.error || r.error) throw new Error("Connect workspace unavailable");
      return { connections: c.data, sources: s.data, runs: r.data };
    },
  });
  const details = useQuery({
    queryKey: ["connect-details", selected],
    enabled: !!selected,
    refetchInterval: 5000,
    queryFn: async () => {
      const [s, c, e] = await Promise.all([
        supabase.from("connect_streams").select("*").eq("connection_id", selected),
        supabase
          .from("connect_commits")
          .select("*")
          .in(
            "run_id",
            query.data?.runs.filter((r) => r.connection_id === selected).map((r) => r.id) ?? [],
          ),
        supabase
          .from("connect_events")
          .select("*")
          .eq("connection_id", selected)
          .order("created_at", { ascending: false })
          .limit(50),
      ]);
      if (s.error || c.error || e.error) throw new Error("Connect run details unavailable");
      return { streams: s.data, commits: c.data, events: e.data };
    },
  });
  const c = query.data?.connections.find((c) => c.id === selected);
  const catalog = (c?.discovered_catalog ?? []) as {
    name: string;
    namespace: string;
    primary_key: string | string[];
  }[];
  async function action(fn: () => Promise<unknown>) {
    setBusy(true);
    try {
      await fn();
      await qc.invalidateQueries({ queryKey: ["connect-workspace"] });
      await qc.invalidateQueries({ queryKey: ["connect-details"] });
      toast.success("Connect operation saved");
    } catch (e) {
      toast.error((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  async function queue(operation: "check" | "discover" | "sync" | "reset" | "backfill") {
    if (
      ["reset", "backfill"].includes(operation) &&
      !window.confirm(
        "Replay the stream from the beginning? Raw history is retained; this is not an overwrite.",
      )
    )
      return;
    await action(() =>
      enqueue({ data: { id: selected, requestId: crypto.randomUUID(), operation } }),
    );
  }
  return (
    <section className="mb-8 min-w-0 border-b pb-8">
      <div className="mb-4 flex flex-wrap items-center gap-3">
        <h2 className="text-xl font-semibold">Meridian Connect</h2>
        <Badge variant="outline">PostgreSQL · MySQL — preview</Badge>
        <Badge variant="outline">Kafka: Available — Certified Preview</Badge>
      </div>
      <div className="grid gap-3 sm:grid-cols-4">
        <select
          aria-label="Connect source"
          className="min-w-0 border bg-background p-2"
          value={source}
          onChange={(e) => setSource(e.target.value)}
        >
          <option value="">Existing PostgreSQL, MySQL or Kafka source</option>
          {query.data?.sources.map((s) => (
            <option value={s.id} key={s.id}>
              {s.name} ({s.kind})
            </option>
          ))}
        </select>
        <Input
          aria-label="Replication connection name"
          placeholder="Replication connection name"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
        <Input
          aria-label="Agent credential reference"
          placeholder="Agent credential reference"
          value={ref}
          onChange={(e) => setRef(e.target.value)}
        />
        <Button
          disabled={busy}
          onClick={() =>
            void action(() => create({ data: { sourceId: source, name, credentialRef: ref } }))
          }
        >
          <Plug />
          Register replication
        </Button>
      </div>
      {query.error && <p role="alert">{query.error.message}</p>}
      <div className="my-4 flex flex-wrap gap-2">
        {query.data?.connections.map((c) => (
          <Button
            key={c.id}
            variant={selected === c.id ? "default" : "outline"}
            onClick={() => {
              setSelected(c.id);
              setInspected(null);
            }}
          >
            {c.name} · {c.health}
          </Button>
        ))}
      </div>
      {c && (
        <>
          <div className="flex flex-wrap gap-2">
            <Button
              disabled={busy || !c.enabled}
              variant="outline"
              onClick={() => void queue("check")}
            >
              <Plug />
              Check credentials
            </Button>
            <Button
              disabled={busy || !c.enabled}
              variant="outline"
              onClick={() => void queue("discover")}
            >
              <Search />
              Discover streams
            </Button>
            <Button disabled={busy || !c.enabled} onClick={() => void queue("sync")}>
              <Play />
              Sync now
            </Button>
            <Button
              disabled={busy}
              variant="outline"
              onClick={() =>
                void action(() => configure({ data: { id: c.id, enabled: !c.enabled } }))
              }
            >
              <Pause />
              {c.enabled ? "Pause" : "Resume"}
            </Button>
            <Button disabled={busy} variant="outline" onClick={() => void queue("reset")}>
              Reset / replay
            </Button>
          </div>
          <div className="my-4 grid gap-3 sm:grid-cols-4">
            <select
              aria-label="Discovered Connect stream"
              className="min-w-0 border bg-background p-2"
              value={stream}
              onChange={(e) => setStream(e.target.value)}
            >
              <option value="">Select discovered stream</option>
              {catalog.map((s) => (
                <option key={s.namespace + "." + s.name}>{s.namespace + "." + s.name}</option>
              ))}
            </select>
            <Input
              aria-label="Replication cursor"
              value={cursor}
              onChange={(e) => setCursor(e.target.value)}
            />
            <select
              aria-label="Replication mode"
              className="border bg-background p-2"
              value={mode}
              onChange={(e) => setMode(e.target.value as typeof mode)}
            >
              <option value="incremental">Incremental cursor</option>
              <option value="full_refresh">Full refresh snapshot</option>
            </select>
            <Button
              disabled={busy}
              variant="outline"
              onClick={() => {
                const s = catalog.find((s) => s.namespace + "." + s.name === stream);
                if (s)
                  void action(() =>
                    configure({
                      data: {
                        id: c.id,
                        stream: {
                          namespace: s.namespace,
                          name: s.name,
                          primaryKey: s.primary_key,
                          cursor,
                          mode,
                        },
                      },
                    }),
                  );
              }}
            >
              Save stream
            </Button>
          </div>
          <div className="my-4 flex flex-wrap items-center gap-3">
            <label>
              Schedule minutes (0 = manual)
              <Input
                aria-label="Connect schedule minutes"
                type="number"
                value={interval}
                onChange={(e) => setInterval(e.target.value)}
              />
            </label>
            <Button
              disabled={busy}
              variant="outline"
              onClick={() =>
                void action(() => configure({ data: { id: c.id, interval: Number(interval) } }))
              }
            >
              Save schedule
            </Button>
            <span className="text-sm text-muted-foreground">
              {c.next_run_at ? `Next due ${new Date(c.next_run_at).toLocaleString()}` : "Manual"}
            </span>
          </div>
          {details.data?.streams.map((s) => (
            <div className="border-b py-2 text-sm" key={s.id}>
              <strong>
                {s.namespace}.{s.name}
              </strong>{" "}
              · {s.sync_mode} · {s.last_success_at ?? "No committed batch"}
              <details>
                <summary>Durable checkpoint</summary>
                <pre className="overflow-auto text-xs">{JSON.stringify(s.state, null, 2)}</pre>
              </details>
            </div>
          ))}
          <h3 className="my-3 font-semibold">Replication runs</h3>
          <div className="overflow-x-auto">
            <table className="w-full min-w-[580px] text-sm">
              <thead>
                <tr>
                  <th className="text-left">Operation / status</th>
                  <th>Rows</th>
                  <th>Bytes</th>
                  <th>Attempts</th>
                  <th>Failure</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {query.data?.runs
                  .filter((r) => r.connection_id === c.id)
                  .map((r) => (
                    <tr className="border-b" key={r.id}>
                      <td className="py-3">
                        {r.operation} · {r.status}
                        <div className="text-xs">{new Date(r.created_at).toLocaleString()}</div>
                      </td>
                      <td>{r.rows_committed}</td>
                      <td>{r.bytes_committed}</td>
                      <td>{r.attempts}</td>
                      <td>{r.error_code}</td>
                      <td>
                        {["queued", "running", "retry_wait"].includes(r.status) && (
                          <Button
                            aria-label="Cancel replication run"
                            size="icon"
                            variant="ghost"
                            onClick={() => void action(() => cancel({ data: { id: r.id } }))}
                          >
                            <X />
                          </Button>
                        )}
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
          <h3 className="my-3 font-semibold">Committed bronze batches</h3>
          {details.data?.commits.map((b) => (
            <div key={b.id} className="border-b py-2 text-sm">
              {b.row_count} rows · {b.byte_count} bytes · batch {b.batch_id}
              <Button size="sm" variant="outline" onClick={() => setInspected(b)}>
                Inspect batch
              </Button>
              <div className="text-xs">
                Downstream handoff uses committed batch ID; records require source mapping.
              </div>
            </div>
          ))}
          {inspected && (
            <section aria-label="Committed batch inspection" className="my-4 space-y-2">
              <h3 className="font-semibold">Batch {inspected.batch_id}</h3>
              <Button variant="ghost" onClick={() => setInspected(null)}>
                Close inspection
              </Button>
              {inspection.isPending && <p role="status">Verifying stored output…</p>}
              {inspection.error && <p role="alert">{inspection.error.message}</p>}
              {inspection.data && (
                <>
                  <p>
                    Checksum verified. Showing up to100 of {inspection.data.total} raw records;
                    operations are not applied warehouse merges.
                  </p>
                  <details>
                    <summary>Manifest and checkpoint</summary>
                    <pre className="max-h-80 overflow-auto text-xs">
                      {JSON.stringify(inspection.data.manifest, null, 2)}
                    </pre>
                  </details>
                  <pre className="max-h-96 overflow-auto text-xs">
                    {JSON.stringify(inspection.data.records, null, 2)}
                  </pre>
                </>
              )}
            </section>
          )}
          <details className="mt-4">
            <summary>Structured operational events</summary>
            {details.data?.events.map((e) => (
              <div key={e.id} className="border-b py-2 text-xs">
                {e.created_at} · {e.level} · {e.code}
              </div>
            ))}
          </details>
          {details.error && <p role="alert">{details.error.message}</p>}
        </>
      )}
    </section>
  );
}
