import { createFileRoute } from "@tanstack/react-router";
import { useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useServerFn } from "@tanstack/react-start";
import { toast } from "sonner";
import { supabase } from "@/integrations/supabase/client";
import { PageHeader } from "@/components/PageHeader";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import {
  checkCoverage,
  demoSample,
  demoStats,
  detectSkew,
  nextConcurrency,
  planPartitions,
  predicateSql,
  recommend,
  sparkPartitions,
  type Predicate,
  type Strategy,
} from "@/lib/partitioning";
import { SourceRunPanel } from "@/components/SourceRunPanel";
import { PublishedTables } from "@/components/PublishedTables";
import { FileRunPanel } from "@/components/FileRunPanel";
import {
  createPartitionRun,
  processPartition,
  publishPartitionRun,
} from "@/lib/partition-ingest.functions";

export const Route = createFileRoute("/_authenticated/partitions")({
  head: () => ({
    meta: [
      { title: "Partitioned ingestion — Meridian" },
      {
        name: "description",
        content:
          "Plan, preview and run partitioned, parallel, resumable ingestion with live partition-level monitoring.",
      },
      { property: "og:title", content: "Partitioned ingestion — Meridian" },
      {
        property: "og:description",
        content: "Visual partition planner with parallel workers, checkpoints and atomic publish.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary" },
    ],
  }),
  component: PartitionsPage,
});

type Kind = Strategy["kind"];
const KINDS: Kind[] = ["range", "hash", "modulo", "date", "key", "sql"];
const COLS = ["id", "customer_id", "region", "amount", "order_date", "coupon"];

interface PartRow {
  idx: number;
  label: string;
  status: string;
  rows_written: number;
  est_rows: number;
  attempts: number;
  worker: string | null;
  error: string | null;
  batches: number;
}

function PartitionsPage() {
  const [sourceId, setSourceId] = useState("demo");
  const { data: sources = [] } = useQuery({
    queryKey: ["ingest_sources_min"],
    queryFn: async () =>
      (
        await supabase
          .from("ingest_sources")
          .select("id,name,kind,target_table,pk_columns")
          .order("created_at", { ascending: false })
      ).data ?? [],
  });
  const src = sources.find((s) => s.id === sourceId);
  return (
    <div className="space-y-6">
      <PageHeader
        kicker="Ingestion"
        title="Partitioned ingestion"
        desc="Split a source into non-overlapping partitions, preview the plan, then extract in parallel with leases, checkpoints, retries and an atomic publish."
      />
      <label className="block max-w-md text-sm">
        Source
        <select
          className="mt-1 w-full rounded-md border bg-background p-2"
          value={sourceId}
          onChange={(e) => setSourceId(e.target.value)}
          aria-label="Source"
        >
          <option value="demo">Built-in demo generator (runs in Meridian)</option>
          {sources.map((s) => (
            <option key={s.id} value={s.id}>
              {s.name} ({s.kind}) → {s.target_table}
            </option>
          ))}
        </select>
      </label>
      {src ? <SourceRunPanel key={src.id} source={src} /> : <DemoPlanner />}
      <FileRunPanel />
      <PublishedTables />
    </div>
  );
}

function DemoPlanner() {
  const create = useServerFn(createPartitionRun);
  const process = useServerFn(processPartition);
  const publish = useServerFn(publishPartitionRun);
  const [total, setTotal] = useState(50_000);
  const [kind, setKind] = useState<Kind>("range");
  const [column, setColumn] = useState("id");
  const [n, setN] = useState(16);
  const [grain, setGrain] = useState<"day" | "month" | "quarter">("month");
  const [keys, setKeys] = useState("APAC, EU");
  const [sqlPreds, setSqlPreds] = useState("amount < 5000\namount >= 5000");
  const [batchSize, setBatchSize] = useState(1000);
  const [maxWorkers, setMaxWorkers] = useState(8);
  const [connLimit, setConnLimit] = useState(8);
  const [failRate, setFailRate] = useState(0);
  const [target, setTarget] = useState("orders_partitioned");
  const [runId, setRunId] = useState<string | null>(null);
  const [live, setLive] = useState({
    workers: 0,
    conc: 0,
    rows: 0,
    started: 0,
    retries: 0,
    running: false,
  });
  const stop = useRef(false);

  const strategy: Strategy =
    kind === "date"
      ? { kind, column, grain }
      : kind === "key"
        ? { kind, column, values: keys.split(",") }
        : kind === "sql"
          ? { kind, predicates: sqlPreds.split("\n") }
          : kind === "file"
            ? { kind, files: [], n }
            : { kind, column, n };
  const preview = useMemo(() => {
    try {
      const stats = kind === "sql" ? null : demoStats(column, total);
      const parts = planPartitions(strategy, stats);
      const sample = demoSample(total, 4000);
      const cov = checkCoverage(parts, sample);
      const scale = total / sample.length;
      const est = cov.counts.map((c) => Math.round(c * scale));
      return {
        parts: parts.map((p, i) => ({ ...p, estRows: est[i] ?? 0 })),
        cov,
        skew: detectSkew(est),
        stats,
        rec: stats ? recommend(column, stats, total, maxWorkers) : null,
        error: null as string | null,
      };
    } catch (e) {
      return {
        parts: [],
        cov: null,
        skew: null,
        stats: null,
        rec: null,
        error: (e as Error).message,
      };
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [total, kind, column, n, grain, keys, sqlPreds, maxWorkers]);

  const { data: parts = [], refetch: refetchParts } = useQuery({
    queryKey: ["partitions", runId],
    enabled: !!runId,
    refetchInterval: live.running ? 1000 : false,
    queryFn: async () =>
      ((
        await supabase
          .from("ingest_partitions")
          .select("idx,label,status,rows_written,est_rows,attempts,worker,error,batches")
          .eq("run_id", runId!)
          .order("idx")
      ).data as PartRow[]) ?? [],
  });
  const { data: published, refetch: refetchPublished } = useQuery({
    queryKey: ["ingest_tables", target],
    queryFn: async () =>
      (await supabase.from("ingest_tables").select("*").eq("name", target).maybeSingle()).data,
  });

  async function execute(id: string): Promise<void> {
    stop.current = false;
    const { data: todo } = await supabase
      .from("ingest_partitions")
      .select("idx,status")
      .eq("run_id", id)
      .neq("status", "done")
      .order("idx");
    const queue = (todo ?? []).map((p) => p.idx);
    const tries = new Map<number, number>();
    const cap = Math.min(maxWorkers, connLimit);
    const free = Array.from({ length: cap }, (_, i) => cap - i);
    let conc = Math.max(1, Math.ceil(cap / 2)),
      active = 0,
      rows = 0,
      retries = 0;
    const t0 = performance.now();
    setLive({ workers: 0, conc, rows: 0, started: t0, retries: 0, running: true });
    await new Promise<void>((resolve) => {
      const pump = () => {
        if (stop.current && active === 0) return resolve();
        if (!queue.length && active === 0) return resolve();
        while (!stop.current && active < conc && queue.length) {
          const idx = queue.shift()!;
          const slot = free.pop() ?? active + 1;
          const worker = `w${slot}`;
          active++;
          const s = performance.now();
          process({ data: { runId: id, idx, worker, maxBatches: 3, failRate } })
            .then((r) => {
              rows += r.rows;
              if (!r.done) queue.unshift(idx);
              conc = nextConcurrency(conc, {
                min: 1,
                max: cap,
                latencyMs: performance.now() - s,
                targetMs: 12000,
                error: false,
              });
            })
            .catch(() => {
              const t = (tries.get(idx) ?? 0) + 1;
              tries.set(idx, t);
              retries++;
              conc = nextConcurrency(conc, {
                min: 1,
                max: cap,
                latencyMs: 0,
                targetMs: 12000,
                error: true,
              });
              if (t <= 5)
                setTimeout(
                  () => {
                    queue.push(idx);
                    pump();
                  },
                  300 * 2 ** t,
                );
            })
            .finally(() => {
              free.push(slot);
              active--;
              setLive((l) => ({ ...l, workers: active, conc, rows, retries }));
              pump();
            });
        }
        setLive((l) => ({ ...l, workers: active, conc }));
      };
      pump();
    });
    setLive((l) => ({ ...l, running: false, workers: 0 }));
    await refetchParts();
    const { data: left } = await supabase
      .from("ingest_partitions")
      .select("idx")
      .eq("run_id", id)
      .neq("status", "done");
    if (stop.current) {
      toast.message("Paused — progress is checkpointed. Press Resume.");
      return;
    }
    if (left?.length) {
      toast.error(`${left.length} partitions still failing. Press Resume to retry them.`);
      return;
    }
    const res = await publish({ data: { runId: id } });
    await refetchPublished();
    toast.success(
      `Published ${res.published.toLocaleString()} rows atomically in ${((performance.now() - t0) / 1000).toFixed(1)}s`,
    );
  }

  async function start(): Promise<void> {
    if (preview.error || !preview.parts.length) return;
    if (preview.cov && !preview.cov.ok && kind !== "sql") {
      toast.error("Partitions overlap or leave gaps — fix the plan first.");
      return;
    }
    const { id } = await create({
      data: {
        target,
        total,
        batchSize,
        concurrency: Math.min(maxWorkers, connLimit),
        plan: { strategy, sparkPartitions: sparkPartitions(total) } as Record<string, unknown>,
        partitions: preview.parts.map((p) => ({
          idx: p.idx,
          label: p.label,
          predicate: p.predicate as unknown as Record<string, unknown>,
          estRows: p.estRows,
        })),
      },
    });
    setRunId(id);
    await execute(id);
  }

  const done = parts.filter((p) => p.status === "done").length;
  const elapsed = live.started ? (performance.now() - live.started) / 1000 : 0;
  const tput = elapsed > 0 ? Math.round(live.rows / elapsed) : 0;

  return (
    <div className="space-y-6">
      <section className="grid gap-4 rounded-lg border bg-card p-4 md:grid-cols-4">
        <label className="text-sm">
          Source rows
          <Input
            type="number"
            value={total}
            onChange={(e) => setTotal(Math.max(1, Number(e.target.value)))}
          />
        </label>
        <label className="text-sm">
          Strategy
          <select
            className="mt-1 w-full rounded-md border bg-background p-2"
            value={kind}
            onChange={(e) => setKind(e.target.value as Kind)}
          >
            {KINDS.map((k) => (
              <option key={k}>{k}</option>
            ))}
          </select>
        </label>
        {kind !== "sql" && (
          <label className="text-sm">
            Column
            <select
              className="mt-1 w-full rounded-md border bg-background p-2"
              value={column}
              onChange={(e) => setColumn(e.target.value)}
            >
              {COLS.map((c) => (
                <option key={c}>{c}</option>
              ))}
            </select>
          </label>
        )}
        {(kind === "range" || kind === "hash" || kind === "modulo") && (
          <label className="text-sm">
            Partitions
            <Input
              type="number"
              value={n}
              onChange={(e) => setN(Math.max(1, Math.min(512, Number(e.target.value))))}
            />
          </label>
        )}
        {kind === "date" && (
          <label className="text-sm">
            Grain
            <select
              className="mt-1 w-full rounded-md border bg-background p-2"
              value={grain}
              onChange={(e) => setGrain(e.target.value as typeof grain)}
            >
              <option>day</option>
              <option>month</option>
              <option>quarter</option>
            </select>
          </label>
        )}
        {kind === "key" && (
          <label className="text-sm">
            Keys (comma)
            <Input value={keys} onChange={(e) => setKeys(e.target.value)} />
          </label>
        )}
        {kind === "sql" && (
          <label className="text-sm md:col-span-2">
            Custom WHERE predicates (one per line, run at source by the worker kit)
            <textarea
              className="mt-1 w-full rounded-md border bg-background p-2 font-mono text-xs"
              rows={3}
              value={sqlPreds}
              onChange={(e) => setSqlPreds(e.target.value)}
            />
          </label>
        )}
        <label className="text-sm">
          Batch size
          <Input
            type="number"
            value={batchSize}
            onChange={(e) => setBatchSize(Math.max(100, Math.min(5000, Number(e.target.value))))}
          />
        </label>
        <label className="text-sm">
          Max workers
          <Input
            type="number"
            value={maxWorkers}
            onChange={(e) => setMaxWorkers(Math.max(1, Math.min(32, Number(e.target.value))))}
          />
        </label>
        <label className="text-sm">
          Source connection limit
          <Input
            type="number"
            value={connLimit}
            onChange={(e) => setConnLimit(Math.max(1, Math.min(32, Number(e.target.value))))}
          />
        </label>
        <label className="text-sm">
          Inject failures (%)
          <Input
            type="number"
            value={failRate * 100}
            onChange={(e) => setFailRate(Math.max(0, Math.min(50, Number(e.target.value))) / 100)}
          />
        </label>
        <label className="text-sm">
          Target table
          <Input value={target} onChange={(e) => setTarget(e.target.value)} />
        </label>
      </section>

      <section className="rounded-lg border bg-card p-4 space-y-3">
        <h2 className="font-semibold">Plan preview</h2>
        {preview.error ? (
          <p className="text-sm text-destructive">{preview.error}</p>
        ) : (
          <>
            <div className="flex flex-wrap gap-2 text-sm">
              <Badge variant="secondary">{preview.parts.length} logical partitions</Badge>
              <Badge variant="secondary">
                ~{Math.ceil(total / batchSize).toLocaleString()} extraction batches
              </Badge>
              <Badge variant="secondary">{sparkPartitions(total)} Spark write partitions</Badge>
              <Badge variant="secondary">
                {Math.min(maxWorkers, connLimit)} max concurrent workers
              </Badge>
              {preview.cov && kind !== "sql" && (
                <Badge variant={preview.cov.ok ? "default" : "destructive"}>
                  {preview.cov.ok
                    ? "Coverage OK: no gaps, no overlaps"
                    : `${preview.cov.uncovered} uncovered / ${preview.cov.overlapping} overlapping sample rows`}
                </Badge>
              )}
              {preview.skew && (
                <Badge variant={preview.skew.skewed ? "destructive" : "default"}>
                  Skew {preview.skew.ratio}×{" "}
                  {preview.skew.skewed ? `(hot: #${preview.skew.hot.slice(0, 5).join(", #")})` : ""}
                </Badge>
              )}
            </div>
            {preview.rec && (
              <p className="text-xs text-muted-foreground">Suggestion: {preview.rec.reason}</p>
            )}
            <div className="max-h-64 overflow-auto rounded border">
              <table className="w-full text-xs">
                <thead className="sticky top-0 bg-muted">
                  <tr>
                    <th className="p-2 text-left">#</th>
                    <th className="p-2 text-left">Predicate (Postgres)</th>
                    <th className="p-2 text-right">Est. rows</th>
                  </tr>
                </thead>
                <tbody>
                  {preview.parts.map((p) => (
                    <tr key={p.idx} className="border-t">
                      <td className="p-2">{p.idx}</td>
                      <td className="p-2 font-mono">{predicateSql(p.predicate as Predicate)}</td>
                      <td className="p-2 text-right">{p.estRows.toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
        <div className="flex flex-wrap gap-2">
          <Button
            onClick={() => void start().catch((error: Error) => toast.error(error.message))}
            disabled={live.running || !!preview.error || kind === "sql"}
          >
            Start ingestion
          </Button>
          {runId && (
            <Button
              variant="outline"
              disabled={live.running}
              onClick={() =>
                void execute(runId).catch((error: Error) => toast.error(error.message))
              }
            >
              Resume
            </Button>
          )}
          {live.running && (
            <Button
              variant="outline"
              onClick={() => {
                stop.current = true;
              }}
            >
              Pause
            </Button>
          )}
        </div>
        {kind === "sql" && (
          <p className="text-xs text-muted-foreground">
            Custom SQL partitions execute on your database through the worker kit (deploy/ingest),
            not in the demo.
          </p>
        )}
      </section>

      {runId && (
        <section className="rounded-lg border bg-card p-4 space-y-3">
          <h2 className="font-semibold">Live execution</h2>
          <div className="grid grid-cols-2 gap-2 text-sm md:grid-cols-6">
            <Stat k="Partitions done" v={`${done}/${parts.length}`} />
            <Stat k="Active workers" v={String(live.workers)} />
            <Stat k="Concurrency limit" v={String(live.conc)} />
            <Stat
              k="Rows written"
              v={parts.reduce((a, p) => a + Number(p.rows_written), 0).toLocaleString()}
            />
            <Stat k="Throughput" v={`${tput.toLocaleString()} rows/s`} />
            <Stat k="Retries" v={String(live.retries)} />
          </div>
          <div
            className="grid gap-1"
            style={{ gridTemplateColumns: "repeat(auto-fill, minmax(150px, 1fr))" }}
          >
            {parts.map((p) => {
              const pct = p.est_rows
                ? Math.min(100, (Number(p.rows_written) / Number(p.est_rows)) * 100)
                : p.status === "done"
                  ? 100
                  : 0;
              return (
                <div
                  key={p.idx}
                  title={p.error ?? p.label}
                  className="rounded border p-2 text-xs"
                  data-status={p.status}
                >
                  <div className="flex justify-between">
                    <span>
                      #{p.idx} {p.worker ?? ""}
                    </span>
                    <span
                      className={
                        p.status === "failed"
                          ? "text-destructive"
                          : p.status === "done"
                            ? "text-primary"
                            : "text-muted-foreground"
                      }
                    >
                      {p.status}
                    </span>
                  </div>
                  <div className="mt-1 h-1.5 rounded bg-muted">
                    <div
                      className="h-1.5 rounded bg-primary"
                      style={{ width: `${p.status === "done" ? 100 : pct}%` }}
                    />
                  </div>
                  <div className="mt-1 text-muted-foreground">
                    {Number(p.rows_written).toLocaleString()} rows · {p.attempts} calls
                  </div>
                </div>
              );
            })}
          </div>
        </section>
      )}

      {published && (
        <section className="rounded-lg border bg-card p-4 text-sm" data-testid="published">
          Published table <b>{published.name}</b>: {Number(published.row_count).toLocaleString()}{" "}
          rows (snapshot from run {String(published.current_run_id).slice(0, 8)},{" "}
          {new Date(published.published_at).toLocaleString()}).
        </section>
      )}
    </div>
  );
}

function Stat({ k, v }: { k: string; v: string }) {
  return (
    <div className="rounded border p-2">
      <div className="text-xs text-muted-foreground">{k}</div>
      <div className="font-semibold">{v}</div>
    </div>
  );
}
