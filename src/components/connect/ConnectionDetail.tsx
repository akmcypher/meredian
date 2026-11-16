import { useState } from "react";
import { ArrowLeft, Pause, Play, Plug, RotateCcw, Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import type { GuidanceAction } from "@/lib/connect-guidance";
import { relativeTime, type ConnectionHealth } from "@/lib/connect-health";
import { ErrorGuidance, HealthBadge } from "./ErrorGuidance";
import { formatBytes, isCdc, sourceKind } from "./connect-view";
import { StreamsTab } from "./StreamsTab";
import { RunsTab } from "./RunsTab";
import { SettingsTab } from "./SettingsTab";
import {
  useConnectActions,
  useConnectDetails,
  type ConnectList,
  type ConnectConnection,
} from "./use-connect";

export type DetailTab = "overview" | "streams" | "runs" | "settings";

export function ConnectionDetail({
  data,
  connection: c,
  health,
  onBack,
}: {
  data: ConnectList;
  connection: ConnectConnection;
  health: ConnectionHealth;
  onBack: () => void;
}) {
  const [tab, setTab] = useState<DetailTab>("overview");
  const runs = data.runs.filter((r) => r.connection_id === c.id);
  const details = useConnectDetails(
    c.id,
    runs.map((r) => r.id),
  );
  const actions = useConnectActions();
  const queue = (operation: "check" | "discover" | "sync" | "reset") => {
    if (
      operation === "reset" &&
      !window.confirm(
        "Replay the streams from the beginning? Raw history is retained; this is not an overwrite.",
      )
    )
      return;
    void actions.run(() => actions.queue(c.id, operation), "Run queued");
  };
  const onGuidance = (a: GuidanceAction) => {
    if (a === "check") queue("check");
    else if (a === "discover") queue("discover");
    else if (a === "retry") queue("sync");
    else if (a === "settings") setTab("settings");
    else if (a === "runs") setTab("runs");
  };
  const disabled = actions.busy || !c.enabled;
  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-3">
        <Button variant="ghost" size="sm" onClick={onBack}>
          <ArrowLeft />
          Connections
        </Button>
        <h3 className="text-lg font-semibold">{c.name}</h3>
        <HealthBadge state={health.state} />
        <span className="text-sm text-muted-foreground">{sourceKind(c.source_version)}</span>
      </div>
      <Tabs value={tab} onValueChange={(v) => setTab(v as DetailTab)}>
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="streams">Streams</TabsTrigger>
          <TabsTrigger value="runs">Runs</TabsTrigger>
          <TabsTrigger value="settings">Settings</TabsTrigger>
        </TabsList>
        <TabsContent value="overview" className="space-y-4">
          <ErrorGuidance code={health.errorCode} onAction={onGuidance} disabled={actions.busy} />
          {health.openDeadLetters > 0 && (
            <p className="text-sm">
              {health.openDeadLetters} unreadable record
              {health.openDeadLetters === 1 ? " is" : "s are"} held as dead letters; replay or
              discard them once the cause is fixed.
            </p>
          )}
          <dl className="grid gap-3 sm:grid-cols-4">
            <Stat label="Last successful sync" value={relativeTime(health.lastSuccessAt)} />
            <Stat
              label="Total lag"
              value={
                health.totalLag === null
                  ? "—"
                  : isCdc(c.source_version)
                    ? `${formatBytes(health.totalLag)} of log`
                    : `${health.totalLag.toLocaleString()} records`
              }
            />
            <Stat label="Streams" value={String(details.data?.streams.length ?? "…")} />
            <Stat
              label="Schedule"
              value={
                c.interval_minutes
                  ? `Every ${c.interval_minutes} min${c.next_run_at ? ` · next ${relativeTime(c.next_run_at)}` : ""}`
                  : "Manual"
              }
            />
          </dl>
          <div className="flex flex-wrap gap-2">
            <Button disabled={disabled} variant="outline" onClick={() => queue("check")}>
              <Plug />
              Test connection
            </Button>
            <Button disabled={disabled} variant="outline" onClick={() => queue("discover")}>
              <Search />
              Discover streams
            </Button>
            <Button disabled={disabled} onClick={() => queue("sync")}>
              <Play />
              Sync now
            </Button>
            <Button
              disabled={actions.busy}
              variant="outline"
              onClick={() =>
                void actions.run(
                  () => actions.configure({ data: { id: c.id, enabled: !c.enabled } }),
                  c.enabled ? "Connection paused" : "Connection resumed",
                )
              }
            >
              <Pause />
              {c.enabled ? "Pause" : "Resume"}
            </Button>
            <Button disabled={actions.busy} variant="outline" onClick={() => queue("reset")}>
              <RotateCcw />
              Reset / replay
            </Button>
          </div>
          {!c.enabled && (
            <p className="text-sm text-muted-foreground">
              Paused: no runs are queued until resumed.
            </p>
          )}
        </TabsContent>
        <TabsContent value="streams">
          <StreamsTab connection={c} details={details.data} actions={actions} />
        </TabsContent>
        <TabsContent value="runs">
          <RunsTab runs={runs} details={details.data} actions={actions} onGuidance={onGuidance} />
        </TabsContent>
        <TabsContent value="settings">
          <SettingsTab
            connection={c}
            source={data.sources.find((s) => s.id === c.source_id)}
            actions={actions}
          />
        </TabsContent>
      </Tabs>
      {details.error && <p role="alert">{details.error.message}</p>}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border p-3">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="font-medium">{value}</dd>
    </div>
  );
}
