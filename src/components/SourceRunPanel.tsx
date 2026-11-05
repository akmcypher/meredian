import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useServerFn } from "@tanstack/react-start";
import { toast } from "sonner";
import { supabase } from "@/integrations/supabase/client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { planPartitions, predicateSql, sparkPartitions, type Strategy } from "@/lib/partitioning";
import { cancelRun, createSourceRun, publishPartitionRun } from "@/lib/partition-ingest.functions";

type Kind = "range" | "hash" | "modulo" | "sql";

/** Plan + monitor a run for a registered source. Execution happens in the source's worker (deploy/ingest). */
export function SourceRunPanel({
  source,
}: {
  source: { id: string; name: string; kind: string; target_table: string; pk_columns: string[] };
}) {
  const create = useServerFn(createSourceRun);
  const publish = useServerFn(publishPartitionRun);
  const cancel = useServerFn(cancelRun);
  const [pk, setPk] = useState(source.pk_columns.join(", "));
  const [kind, setKind] = useState<Kind>("range");
  const [column, setColumn] = useState(source.pk_columns[0] ?? "id");
  const [min, setMin] = useState(1);
  const [max, setMax] = useState(100_000);
  const [nullable, setNullable] = useState(false);
  const [n, setN] = useState(16);
  const [preds, setPreds] = useState("");
  const [batch, setBatch] = useState(5000);
  const [threads, setThreads] = useState(4);
  const [est, setEst] = useState(100_000);

  const strategy: Strategy =
    kind === "sql" ? { kind, predicates: preds.split("\n") } : { kind, column, n };
  const plan = useMemo(() => {
    try {
      const parts = planPartitions(strategy, {
        min,
        max,
        nullFraction: nullable ? 0.01 : 0,
        distinct: max - min + 1,
        indexed: true,
      });
      return {
        parts: parts.map((p) => ({ ...p, estRows: Math.round(est / Math.max(1, parts.length)) })),
        error: null as string | null,
      };
    } catch (e) {
      return { parts: [], error: (e as Error).message };
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [kind, column, n, min, max, nullable, preds, est]);

  const { data: run, refetch } = useQuery({
    queryKey: ["source_run", source.id],
    refetchInterval: 2000,
    queryFn: async () =>
      (
        await supabase
          .from("ingest_runs")
          .select("id,status,target_table,snapshot,created_at,written_rows")
          .eq("source_id", source.id)
          .order("created_at", { ascending: false })
          .limit(1)
          .maybeSingle()
      ).data,
  });
  const { data: parts = [] } = useQuery({
    queryKey: ["source_run_parts", run?.id],
    enabled: !!run,
    refetchInterval: run?.status === "running" || run?.status === "planned" ? 1500 : false,
    queryFn: async () =>
      (
        await supabase
          .from("ingest_partitions")
          .select("idx,status,rows_written,attempts,worker,error,heartbeat_at,lease_until")
          .eq("run_id", run!.id)
          .order("idx")
      ).data ?? [],
  });
  const { data: workers = [] } = useQuery({
    queryKey: ["source_workers", source.id],
    refetchInterval: 3000,
    queryFn: async () =>
      (
        await supabase
          .from("ingest_workers")
          .select("id,host,heartbeat_at")
          .eq("source_id", source.id)
          .order("heartbeat_at", { ascending: false })
          .limit(50)
      ).data ?? [],
  });

  const open = run && (run.status === "planned" || run.status === "running");
  const done = parts.filter((p) => p.status === "done").length;
  const rows = parts.reduce((s, p) => s + Number(p.rows_written), 0);
  const live = workers.filter((w) => Date.now() - new Date(w.heartbeat_at).getTime() < 30_000);

  async function start() {
    try {
      await create({
        data: {
          sourceId: source.id,
          pkColumns: pk
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean),
          batchSize: batch,
          concurrency: threads,
          expectedRows: est,
          plan: {
            strategy,
            stats: {
              min,
              max,
              nullFraction: nullable ? 0.01 : 0,
              distinct: max - min + 1,
              indexed: true,
            },
            sparkPartitions: sparkPartitions(est),
          } as Record<string, unknown>,
          partitions: plan.parts.map((p) => ({
            idx: p.idx,
            label: p.label,
            predicate: p.predicate as unknown as Record<string, unknown>,
            estRows: p.estRows,
          })),
        },
      });
      toast.success("Run planned. Start the worker for this source to execute it.");
      await refetch();
    } catch (e) {
      toast.error((e as Error).message);
    }
  }

  return (
    <div className="space-y-4">
      <section className="grid gap-4 rounded-lg border bg-card p-4 md:grid-cols-4">
        <label className="text-sm">
          Primary key column(s)
          <Input value={pk} onChange={(e) => setPk(e.target.value)} placeholder="region, id" />
        </label>
        <label className="text-sm">
          Strategy
          <select
            className="mt-1 w-full rounded-md border bg-background p-2"
            value={kind}
            onChange={(e) => setKind(e.target.value as Kind)}
          >
            <option>range</option>
            <option>hash</option>
            <option>modulo</option>
            <option>sql</option>
          </select>
        </label>
        {kind !== "sql" && (
          <label className="text-sm">
            Partition column
            <Input value={column} onChange={(e) => setColumn(e.target.value)} />
          </label>
        )}
        {kind !== "sql" && (
          <label className="text-sm">
            Partitions
            <Input
              type="number"
              value={n}
              onChange={(e) => setN(Math.max(1, Math.min(2000, Number(e.target.value))))}
            />
          </label>
        )}
        {kind === "range" && (
          <label className="text-sm">
            Column min
            <Input type="number" value={min} onChange={(e) => setMin(Number(e.target.value))} />
          </label>
        )}
        {kind === "range" && (
          <label className="text-sm">
            Column max
            <Input type="number" value={max} onChange={(e) => setMax(Number(e.target.value))} />
          </label>
        )}
        {kind !== "sql" && (
          <label className="flex items-center gap-2 text-sm">
            <input
              type="checkbox"
              checked={nullable}
              onChange={(e) => setNullable(e.target.checked)}
            />{" "}
            Column can be NULL
          </label>
        )}
        {kind === "sql" && (
          <label className="text-sm md:col-span-2">
            WHERE predicates (one per line, must not overlap)
            <textarea
              className="mt-1 w-full rounded-md border bg-background p-2 font-mono text-xs"
              rows={3}
              value={preds}
              onChange={(e) => setPreds(e.target.value)}
            />
          </label>
        )}
        <label className="text-sm">
          Batch size
          <Input
            type="number"
            value={batch}
            onChange={(e) => setBatch(Math.max(100, Math.min(10_000, Number(e.target.value))))}
          />
        </label>
        <label className="text-sm">
          Worker threads (≤ connection limit)
          <Input
            type="number"
            value={threads}
            onChange={(e) => setThreads(Math.max(1, Math.min(64, Number(e.target.value))))}
          />
        </label>
        <label className="text-sm">
          Estimated rows
          <Input
            type="number"
            value={est}
            onChange={(e) => setEst(Math.max(0, Number(e.target.value)))}
          />
        </label>
      </section>

      <section className="rounded-lg border bg-card p-4 space-y-3">
        <h2 className="font-semibold">
          Plan for {source.name} → {source.target_table}
        </h2>
        {plan.error ? (
          <p className="text-sm text-destructive">{plan.error}</p>
        ) : (
          <>
            <div className="flex flex-wrap gap-2">
              <Badge variant="secondary">{plan.parts.length} partitions</Badge>
              <Badge variant="secondary">~{Math.ceil(est / batch).toLocaleString()} batches</Badge>
              <Badge variant="secondary">{sparkPartitions(est)} Spark write partitions</Badge>
            </div>
            <div className="max-h-48 overflow-auto rounded border font-mono text-xs">
              {plan.parts.map((p) => (
                <div key={p.idx} className="border-t px-2 py-1">
                  #{p.idx}{" "}
                  {predicateSql(p.predicate, source.kind === "mysql" ? "mysql" : "postgres")}
                </div>
              ))}
            </div>
          </>
        )}
        <div className="flex flex-wrap gap-2">
          <Button onClick={() => void start()} disabled={!!plan.error || !!open}>
            Plan run
          </Button>
          {open && (
            <Button
              variant="outline"
              onClick={() => void cancel({ data: { runId: run.id } }).then(() => refetch())}
            >
              Cancel run
            </Button>
          )}
        </div>
        <pre className="overflow-auto rounded bg-muted p-2 text-xs">{`MERIDIAN_URL=${typeof window !== "undefined" ? window.location.origin : ""} MERIDIAN_TOKEN=<this source's token> \\
  node deploy/ingest/partitioned_worker.mjs --kind ${source.kind} --table <schema.table> --url <connection-url> --threads ${threads}`}</pre>
      </section>

      {run && (
        <section className="rounded-lg border bg-card p-4 space-y-3" data-testid="source-run">
          <div className="flex flex-wrap items-center gap-2">
            <h2 className="font-semibold">Run {run.id.slice(0, 8)}</h2>
            <Badge>{run.status}</Badge>
            <Badge variant="secondary">
              {done}/{parts.length} partitions
            </Badge>
            <Badge variant="secondary">{rows.toLocaleString()} rows staged</Badge>
            <Badge variant="secondary">{live.length} live workers</Badge>
            {run.snapshot && (
              <Badge variant="outline">
                snapshot {String((run.snapshot as { lsn?: string }).lsn ?? "")}
              </Badge>
            )}
            {open && parts.length > 0 && done === parts.length && (
              <Button
                size="sm"
                onClick={() =>
                  void publish({ data: { runId: run.id } })
                    .then((r) => {
                      toast.success(`Published ${r.published.toLocaleString()} rows`);
                      void refetch();
                    })
                    .catch((e: Error) => toast.error(e.message))
                }
              >
                Publish
              </Button>
            )}
          </div>
          <div
            className="grid gap-1"
            style={{ gridTemplateColumns: "repeat(auto-fill, minmax(140px, 1fr))" }}
          >
            {parts.map((p) => (
              <div key={p.idx} className="rounded border p-2 text-xs" title={p.error ?? ""}>
                <div className="flex justify-between">
                  <span>#{p.idx}</span>
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
                <div className="truncate text-muted-foreground">{p.worker ?? "—"}</div>
                <div>
                  {Number(p.rows_written).toLocaleString()} rows · {p.attempts} claims
                </div>
              </div>
            ))}
          </div>
        </section>
      )}
    </div>
  );
}
