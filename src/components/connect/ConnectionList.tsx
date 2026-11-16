import { Plus } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { relativeTime } from "@/lib/connect-health";
import { formatBytes, healthByConnection, isCdc, sourceKind } from "./connect-view";
import { guidance } from "@/lib/connect-guidance";
import { HealthBadge } from "./ErrorGuidance";
import type { ConnectList } from "./use-connect";

export function ConnectionList({
  data,
  onOpen,
  onNew,
}: {
  data: ConnectList;
  onOpen: (id: string) => void;
  onNew: () => void;
}) {
  const health = healthByConnection(data);
  const attention = [...health.values()].filter((h) => h.state === "attention").length;
  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="text-sm text-muted-foreground">
          {data.connections.length} connection{data.connections.length === 1 ? "" : "s"}
          {attention ? ` · ${attention} need${attention === 1 ? "s" : ""} attention` : ""}
        </p>
        <Button onClick={onNew}>
          <Plus />
          New connection
        </Button>
      </div>
      {data.connections.length === 0 ? (
        <div className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
          No Connect replications yet. Create one to land PostgreSQL, MySQL or Kafka data in bronze.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <Table className="min-w-[640px]">
            <TableHeader>
              <TableRow>
                <TableHead>Connection</TableHead>
                <TableHead>Health</TableHead>
                <TableHead>Last successful sync</TableHead>
                <TableHead className="text-right">Total lag</TableHead>
                <TableHead>Schedule</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.connections.map((c) => {
                const h = health.get(c.id)!;
                const g = guidance(h.errorCode);
                return (
                  <TableRow key={c.id} className="cursor-pointer" onClick={() => onOpen(c.id)}>
                    <TableCell>
                      <button
                        type="button"
                        className="text-left font-medium underline-offset-4 hover:underline"
                        onClick={(e) => {
                          e.stopPropagation();
                          onOpen(c.id);
                        }}
                      >
                        {c.name}
                      </button>
                      <div className="text-xs text-muted-foreground">
                        {sourceKind(c.source_version)}
                      </div>
                    </TableCell>
                    <TableCell>
                      <HealthBadge state={h.state} />
                      {(g || h.openDeadLetters > 0) && (
                        <div className="mt-1 text-xs text-muted-foreground">
                          {g?.title ??
                            `${h.openDeadLetters} open dead letter${h.openDeadLetters === 1 ? "" : "s"}`}
                        </div>
                      )}
                    </TableCell>
                    <TableCell
                      title={
                        h.lastSuccessAt ? new Date(h.lastSuccessAt).toLocaleString() : undefined
                      }
                    >
                      {relativeTime(h.lastSuccessAt)}
                    </TableCell>
                    <TableCell
                      className="text-right tabular-nums"
                      title={
                        h.lagObservedAt
                          ? `Observed ${new Date(h.lagObservedAt).toLocaleString()}`
                          : undefined
                      }
                    >
                      {h.totalLag === null
                        ? "—"
                        : isCdc(c.source_version)
                          ? formatBytes(h.totalLag)
                          : h.totalLag.toLocaleString()}
                    </TableCell>
                    <TableCell>
                      {c.interval_minutes ? `Every ${c.interval_minutes} min` : "Manual"}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}
