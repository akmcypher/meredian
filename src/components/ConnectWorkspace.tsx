import { useState } from "react";
import { Badge } from "./ui/badge";
import { ConnectionList } from "./connect/ConnectionList";
import { healthByConnection } from "./connect/connect-view";
import { ConnectionDetail } from "./connect/ConnectionDetail";
import { NewConnectionWizard } from "./connect/NewConnectionWizard";
import { PoolsPanel } from "./connect/PoolsPanel";
import { useConnectList } from "./connect/use-connect";

type View = { kind: "list" } | { kind: "detail"; id: string } | { kind: "wizard" };

/** Meridian Connect: connection list → detail (Overview/Streams/Runs/Settings) or setup wizard. */
export function ConnectWorkspace() {
  const [view, setView] = useState<View>({ kind: "list" });
  const query = useConnectList();
  const data = query.data;
  const selected =
    view.kind === "detail" ? data?.connections.find((c) => c.id === view.id) : undefined;
  return (
    <section className="mb-8 min-w-0 space-y-4 border-b pb-8">
      <div className="flex flex-wrap items-center gap-3">
        <h2 className="text-xl font-semibold">Meridian Connect</h2>
        <Badge variant="outline">PostgreSQL · MySQL — preview</Badge>
        <Badge variant="outline">Kafka: Available — Certified Preview</Badge>
      </div>
      {query.error && <p role="alert">{query.error.message}</p>}
      {view.kind === "wizard" && (
        <NewConnectionWizard
          data={data}
          onClose={() => setView({ kind: "list" })}
          onDone={(id) => setView({ kind: "detail", id })}
        />
      )}
      {view.kind === "detail" && data && selected && (
        <ConnectionDetail
          data={data}
          connection={selected}
          health={healthByConnection(data).get(selected.id)!}
          onBack={() => setView({ kind: "list" })}
        />
      )}
      {(view.kind === "list" || (view.kind === "detail" && data && !selected)) && data && (
        <>
          <ConnectionList
            data={data}
            onOpen={(id) => setView({ kind: "detail", id })}
            onNew={() => setView({ kind: "wizard" })}
          />
          {data.connections.length > 0 && <PoolsPanel data={data} />}
        </>
      )}
      {query.isPending && <p role="status">Loading connections…</p>}
    </section>
  );
}
