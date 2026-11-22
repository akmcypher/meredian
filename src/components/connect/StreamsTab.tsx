import { useState } from "react";
import { Button } from "@/components/ui/button";
import { relativeTime, totalLag } from "@/lib/connect-health";
import { StreamPicker } from "./StreamPicker";
import { formatBytes, isCdc, readCatalog, saveStreams, streamKey } from "./connect-view";
import type { ConnectConnection, ConnectDetails, useConnectActions } from "./use-connect";

type Actions = ReturnType<typeof useConnectActions>;
export function StreamsTab({
  connection: c,
  details,
  actions,
}: {
  connection: ConnectConnection;
  details: ConnectDetails | undefined;
  actions: Actions;
}) {
  const [adding, setAdding] = useState(false);
  const kafka = /^(spark-)?kafka-v[0-9]+$/.test(c.source_version);
  const cdc = isCdc(c.source_version);
  const catalog = readCatalog(c.discovered_catalog);
  const streams = details?.streams ?? [];
  const retire = (id: string) =>
    void actions.run(
      () => actions.retireStream({ data: { id } }),
      "Stream retired; CDC cleanup queued if needed",
    );
  const retirementButton = (s: (typeof streams)[number]) => {
    const status = (s.state as { retirement?: { status?: string } } | null)?.retirement?.status;
    return status === "done" ? (
      <span className="ml-2 text-muted-foreground">Retired</span>
    ) : (
      <Button
        className="ml-2"
        variant="outline"
        size="sm"
        disabled={actions.busy}
        onClick={() => retire(s.id)}
      >
        {status === "pending" ? "Retry source cleanup" : "Retire stream"}
      </Button>
    );
  };
  return (
    <div className="space-y-4">
      {streams.length === 0 && (
        <p className="text-sm text-muted-foreground">
          No streams configured. Add discovered streams to sync them.
        </p>
      )}
      <ul className="divide-y">
        {streams.map((s) => {
          const lag = (details?.lag ?? []).filter((l) => l.stream_id === s.id);
          const { total } = totalLag(lag);
          const worst = lag.reduce<(typeof lag)[number] | null>(
            (a, b) => (!a || Number(b.lag ?? 0) > Number(a.lag ?? 0) ? b : a),
            null,
          );
          if (cdc) {
            const slot = (s.state as { cdc?: { slot?: string } } | null)?.cdc?.slot;
            return (
              <li key={s.id} className="py-2 text-sm">
                <strong>{streamKey(s)}</strong> · Change data capture
                {slot ? (
                  <>
                    {" "}
                    · slot <code>{slot}</code>
                  </>
                ) : null}{" "}
                · last commit {relativeTime(s.last_success_at)}
                {retirementButton(s)}
                {total !== null && (
                  <span className="text-muted-foreground">
                    {" "}
                    · {formatBytes(total)} of log behind the source
                    {lag[0]?.observed_at
                      ? ` (observed ${new Date(lag[0].observed_at).toLocaleString()})`
                      : ""}
                  </span>
                )}
              </li>
            );
          }
          return (
            <li key={s.id} className="py-2 text-sm">
              <strong>{streamKey(s)}</strong> ·{" "}
              {s.sync_mode === "full_refresh" ? "Full refresh" : "Incremental"} · cursor{" "}
              <code>{s.cursor_field}</code> · last commit {relativeTime(s.last_success_at)}
              {retirementButton(s)}
              {total !== null && (
                <span className="text-muted-foreground">
                  {" "}
                  · Lag {total.toLocaleString()} across {lag.length} partition
                  {lag.length === 1 ? "" : "s"}
                  {total > 0 && worst
                    ? ` (max ${Number(worst.lag).toLocaleString()} on partition ${worst.partition})`
                    : ""}
                </span>
              )}
            </li>
          );
        })}
      </ul>
      {adding ? (
        <div className="space-y-2">
          <StreamPicker
            key={c.id}
            catalog={catalog}
            kafka={kafka}
            cdc={cdc}
            configured={streams.map(streamKey)}
            busy={actions.busy}
            onSave={async (chosen) => {
              const ok = await actions.run(
                () => saveStreams(actions, c.id, chosen),
                "Streams saved",
              );
              if (ok !== undefined) setAdding(false);
            }}
          />
          <Button variant="ghost" onClick={() => setAdding(false)}>
            Cancel
          </Button>
        </div>
      ) : (
        <Button variant="outline" disabled={!catalog.length} onClick={() => setAdding(true)}>
          {catalog.length ? "Add or change streams" : "Discover streams first (Overview)"}
        </Button>
      )}
    </div>
  );
}
