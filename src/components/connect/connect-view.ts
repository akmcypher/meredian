// Pure helpers shared by the Connect components (kept out of component files for fast refresh).
import { connectionHealth, type ConnectionHealth } from "@/lib/connect-health";
import type { SetupState } from "@/lib/connect-setup";
import type { ConnectList, useConnectActions } from "./use-connect";

type Actions = ReturnType<typeof useConnectActions>;

export function healthByConnection(data: ConnectList) {
  return new Map<string, ConnectionHealth>(
    data.connections.map((c) => [
      c.id,
      connectionHealth(c, data.runs, data.streams, data.lag, data.deadLetters.get(c.id) ?? 0),
    ]),
  );
}

export function sourceKind(sourceVersion: string) {
  const kind = sourceVersion.split("-")[0];
  const name =
    kind === "postgres"
      ? "PostgreSQL"
      : kind === "mysql"
        ? "MySQL"
        : kind === "kafka"
          ? "Kafka"
          : kind;
  return isCdc(sourceVersion) ? `${name} CDC` : name;
}

export type WizardStep =
  "type" | "security" | "credentials" | "test" | "discover" | "streams" | "schedule";

export function wizardSteps(kind: SetupState["kind"]): WizardStep[] {
  return [
    "type",
    ...(kind === "kafka" ? (["security"] as const) : []),
    "credentials",
    "test",
    "discover",
    "streams",
    "schedule",
  ];
}

export interface CatalogStream {
  name: string;
  namespace: string;
  primary_key: string | string[];
  json_schema?: { properties?: Record<string, unknown> };
}

export interface StreamChoice {
  namespace: string;
  name: string;
  primaryKey: string | string[];
  cursor: string;
  mode: "incremental" | "full_refresh" | "cdc";
}

/** CDC connections (`postgres-cdc-v1`, `mysql-cdc-v1`) read the change log; their streams use cdc. */
export const isCdc = (sourceVersion: string) => /-cdc-v[0-9]+$/.test(sourceVersion);

/** CDC lag is how far the landed position is behind the source's log, in bytes. */
export function formatBytes(n: number) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = n;
  let u = 0;
  while (v >= 1024 && u < units.length - 1) {
    v /= 1024;
    u++;
  }
  return `${u ? v.toFixed(1) : v} ${units[u]}`;
}

export function readCatalog(value: unknown): CatalogStream[] {
  return Array.isArray(value) ? (value as CatalogStream[]) : [];
}

export const streamKey = (s: { namespace: string; name: string }) => `${s.namespace}.${s.name}`;

/** Cursor candidates: declared columns. Kafka streams are always read by offset. */
export function cursorOptions(s: CatalogStream, kafka: boolean) {
  if (kafka) return ["_offset"];
  const cols = Object.keys(s.json_schema?.properties ?? {});
  const preferred = ["updated_at", "modified_at", "updated", "id"];
  return [
    ...preferred.filter((c) => cols.includes(c)),
    ...cols.filter((c) => !preferred.includes(c)),
  ];
}

/** Saves each chosen stream through configureConnect (it re-checks the discovered catalog). */
export async function saveStreams(actions: Actions, id: string, streams: StreamChoice[]) {
  for (const stream of streams) await actions.configure({ data: { id, stream } });
  return streams.length;
}
