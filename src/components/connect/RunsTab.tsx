import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { X } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { guidance, knownGuidance, type GuidanceAction } from "@/lib/connect-guidance";
import { inspectCommittedBatch, type CommittedBatch } from "@/lib/connect-inspection";
import { ErrorGuidance } from "./ErrorGuidance";
import { streamKey } from "./connect-view";
import type { ConnectDetails, ConnectRun, useConnectActions } from "./use-connect";

const ACTIVE = ["queued", "running", "retry_wait"];

/** Runs with failure guidance, committed bronze batches and durable checkpoints. */
export function RunsTab({
  runs,
  details,
  actions,
  onGuidance,
}: {
  runs: ConnectRun[];
  details: ConnectDetails | undefined;
  actions: ReturnType<typeof useConnectActions>;
  onGuidance: (a: GuidanceAction) => void;
}) {
  const [openRun, setOpenRun] = useState<string | null>(null);
  const [inspected, setInspected] = useState<CommittedBatch | null>(null);
  const inspection = useQuery({
    queryKey: ["connect-batch-inspection", inspected?.batch_id],
    enabled: !!inspected,
    retry: false,
    queryFn: () => inspectCommittedBatch(inspected!),
  });
  const open = runs.find((r) => r.id === openRun);
  const events = (details?.events ?? []).filter((e) => !open || e.run_id === open.id);
  return (
    <div className="space-y-6">
      <section className="space-y-2">
        <h4 className="font-semibold">Runs</h4>
        {runs.length === 0 && <p className="text-sm text-muted-foreground">No runs yet.</p>}
        {runs.length > 0 && (
          <div className="overflow-x-auto">
            <Table className="min-w-[620px]">
              <TableHeader>
                <TableRow>
                  <TableHead>Operation</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Rows</TableHead>
                  <TableHead className="text-right">Bytes</TableHead>
                  <TableHead className="text-right">Attempts</TableHead>
                  <TableHead />
                </TableRow>
              </TableHeader>
              <TableBody>
                {runs.map((r) => (
                  <TableRow key={r.id} data-state={openRun === r.id ? "selected" : undefined}>
                    <TableCell>
                      {r.operation}
                      <div className="text-xs text-muted-foreground">
                        {new Date(r.created_at).toLocaleString()}
                      </div>
                    </TableCell>
                    <TableCell>
                      {r.status}
                      {r.error_code && (
                        <div className="text-xs text-destructive">
                          {guidance(r.error_code)?.title}
                        </div>
                      )}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">{r.rows_committed}</TableCell>
                    <TableCell className="text-right tabular-nums">{r.bytes_committed}</TableCell>
                    <TableCell className="text-right tabular-nums">{r.attempts}</TableCell>
                    <TableCell className="text-right">
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => setOpenRun(openRun === r.id ? null : r.id)}
                      >
                        {openRun === r.id ? "Hide" : "Details"}
                      </Button>
                      {ACTIVE.includes(r.status) && (
                        <Button
                          aria-label="Cancel replication run"
                          size="icon"
                          variant="ghost"
                          onClick={() =>
                            void actions.run(
                              () => actions.cancel({ data: { id: r.id } }),
                              "Run cancelled",
                            )
                          }
                        >
                          <X />
                        </Button>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
        {open && (
          <div className="space-y-2" aria-label="Run report">
            <ErrorGuidance code={open.error_code} onAction={onGuidance} disabled={actions.busy} />
          </div>
        )}
      </section>
      <section className="space-y-2">
        <h4 className="font-semibold">Committed bronze batches</h4>
        {!details?.commits.length && (
          <p className="text-sm text-muted-foreground">No committed batches yet.</p>
        )}
        {details?.commits.map((b) => (
          <div key={b.id} className="flex flex-wrap items-center gap-2 border-b py-2 text-sm">
            <span>
              {b.row_count} rows · {b.byte_count} bytes · batch <code>{b.batch_id}</code>
              {b.shard ? ` · partition ${b.shard}` : ""}
            </span>
            <Button size="sm" variant="outline" onClick={() => setInspected(b)}>
              Inspect batch
            </Button>
          </div>
        ))}
        {inspected && (
          <section aria-label="Committed batch inspection" className="space-y-2">
            <div className="flex items-center gap-2">
              <h5 className="font-medium">Batch {inspected.batch_id}</h5>
              <Button variant="ghost" size="sm" onClick={() => setInspected(null)}>
                Close inspection
              </Button>
            </div>
            {inspection.isPending && <p role="status">Verifying stored output…</p>}
            {inspection.error && <p role="alert">{inspection.error.message}</p>}
            {inspection.data && (
              <>
                <p className="text-sm">
                  Checksum verified. Showing up to 100 of {inspection.data.total} raw records;
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
      </section>
      <section className="space-y-2">
        <h4 className="font-semibold">Checkpoints</h4>
        {details?.streams.map((s) => (
          <details key={s.id} className="border-b py-2 text-sm">
            <summary>
              {streamKey(s)} · generation {s.state_generation}
            </summary>
            <pre className="overflow-auto text-xs">{JSON.stringify(s.state, null, 2)}</pre>
          </details>
        ))}
      </section>
      <section className="space-y-2">
        <h4 className="font-semibold">Events{open ? " for this run" : ""}</h4>
        {events.length === 0 && <p className="text-sm text-muted-foreground">No events.</p>}
        {events.map((e) => {
          const g = knownGuidance(e.code);
          return (
            <div key={e.id} className="border-b py-1 text-xs">
              {new Date(e.created_at).toLocaleString()} · {e.level} · <code>{e.code}</code>
              {g && (
                <span className="text-muted-foreground">
                  {" "}
                  — {g.title}. {g.next}
                </span>
              )}
              {e.detail && (
                <pre className="mt-1 overflow-auto text-xs">
                  {JSON.stringify(e.detail, null, 2)}
                </pre>
              )}
            </div>
          );
        })}
      </section>
    </div>
  );
}
