// Connection health for the Connect list and overview, derived from what the control plane records
// (connect_connections.health only distinguishes "unchecked" from "reachable"): the latest finished
// run, open dead letters, pause state, last successful commit and Kafka partition lag.

export interface HealthRun {
  connection_id: string;
  operation: string;
  status: string;
  error_code: string | null;
  created_at: string;
  finished_at: string | null;
}
export interface HealthStream {
  id: string;
  connection_id: string;
  last_success_at: string | null;
}
export interface HealthLag {
  stream_id: string;
  partition: number;
  lag: number | null;
  observed_at: string;
}
export interface HealthConnection {
  id: string;
  enabled: boolean;
  health: string;
}

export type HealthState = "healthy" | "attention" | "untested" | "paused" | "running";

export interface ConnectionHealth {
  state: HealthState;
  /** Failure code of the latest finished run, when it failed. */
  errorCode: string | null;
  openDeadLetters: number;
  lastSuccessAt: string | null;
  /** Records behind the end offset, summed over partitions; null when no lag was observed. */
  totalLag: number | null;
  lagObservedAt: string | null;
  activeRuns: number;
}

const ACTIVE = new Set(["queued", "running", "retry_wait"]);

export function totalLag(rows: HealthLag[]) {
  if (!rows.length) return { total: null, observedAt: null };
  return {
    total: rows.reduce((sum, r) => sum + Number(r.lag ?? 0), 0),
    observedAt: rows.reduce((a, b) => (b.observed_at > a.observed_at ? b : a)).observed_at,
  };
}

export function connectionHealth(
  c: HealthConnection,
  runs: HealthRun[],
  streams: HealthStream[],
  lag: HealthLag[],
  openDeadLetters: number,
): ConnectionHealth {
  const mine = runs
    .filter((r) => r.connection_id === c.id)
    .sort((a, b) => (a.created_at < b.created_at ? 1 : -1));
  const activeRuns = mine.filter((r) => ACTIVE.has(r.status)).length;
  const finished = mine.find((r) => r.status === "succeeded" || r.status === "failed");
  const ids = new Set(streams.filter((s) => s.connection_id === c.id).map((s) => s.id));
  const lastSuccessAt = streams
    .filter((s) => ids.has(s.id) && s.last_success_at)
    .reduce<string | null>((a, s) => (!a || s.last_success_at! > a ? s.last_success_at : a), null);
  const { total, observedAt } = totalLag(lag.filter((l) => ids.has(l.stream_id)));
  const errorCode = finished?.status === "failed" ? (finished.error_code ?? "failed") : null;
  let state: HealthState;
  if (!c.enabled) state = "paused";
  else if (errorCode || openDeadLetters > 0) state = "attention";
  else if (activeRuns) state = "running";
  else if (c.health === "unchecked" && !finished) state = "untested";
  else state = "healthy";
  return {
    state,
    errorCode,
    openDeadLetters,
    lastSuccessAt,
    totalLag: total,
    lagObservedAt: observedAt,
    activeRuns,
  };
}

export const HEALTH_LABEL: Record<HealthState, string> = {
  healthy: "Healthy",
  attention: "Needs attention",
  untested: "Not tested",
  paused: "Paused",
  running: "Running",
};

export function relativeTime(iso: string | null, now = Date.now()) {
  if (!iso) return "Never";
  const diff = Math.round((now - new Date(iso).getTime()) / 1000);
  const s = Math.abs(diff);
  const span =
    s < 60
      ? null
      : s < 3600
        ? `${Math.round(s / 60)} min`
        : s < 48 * 3600
          ? `${Math.round(s / 3600)} h`
          : `${Math.round(s / 86400)} d`;
  if (!span) return diff >= 0 ? "just now" : "in under a minute";
  return diff >= 0 ? `${span} ago` : `in ${span}`;
}
