import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useServerFn } from "@tanstack/react-start";
import { toast } from "sonner";
import { supabase } from "@/integrations/supabase/client";
import {
  cancelConnect,
  configureConnect,
  createConnect,
  enqueueConnect,
  retireConnectStream,
} from "@/lib/connect.functions";
import { assignConnectionPool, createPool, updatePool } from "@/lib/connect-pools.functions";
import { createSource } from "@/lib/ingest.functions";
import { PUBLIC_CONFIG_KEYS } from "@/lib/connect-setup";

export const CONNECT_KINDS = ["postgres", "mysql", "kafka"] as const;
const SOURCE_SELECT = [
  "id",
  "name",
  "kind",
  ...[...PUBLIC_CONFIG_KEYS].map((k) => `${k}:config->>${k}`),
].join(",");
export type ConnectSourceRow = { id: string; name: string; kind: string } & Partial<
  Record<string, string | null>
>;

/** Every Connect list read is RLS-scoped to what the signed-in user may see. */
export function useConnectList() {
  return useQuery({
    queryKey: ["connect-workspace"],
    refetchInterval: 5000,
    queryFn: async () => {
      const [c, s, r, st, l, d] = await Promise.all([
        supabase
          .from("connect_connections")
          .select(
            "id,name,source_id,source_version,credential_ref,enabled,health,interval_minutes,next_run_at,discovered_catalog,created_at,organization_id,pool_id,max_parallel_tasks",
          )
          .order("created_at", { ascending: false }),
        supabase
          .from("ingest_sources")
          // Only allowlisted, non-secret keys are selected, so nothing else in a source's config
          // (for example from an older registration) is ever read back into the browser.
          .select(SOURCE_SELECT)
          .in("kind", [...CONNECT_KINDS]),
        supabase
          .from("connect_runs")
          .select(
            "id,connection_id,operation,status,attempts,rows_committed,bytes_committed,error_code,created_at,finished_at",
          )
          .order("created_at", { ascending: false })
          .limit(200),
        supabase.from("connect_streams").select("id,connection_id,last_success_at"),
        supabase.from("connect_partition_lag").select("stream_id,partition,lag,observed_at"),
        supabase
          .from("connect_dead_letters")
          .select("connection_id")
          .eq("status", "open")
          .limit(1000),
      ]);
      if (c.error || s.error || r.error || st.error || l.error || d.error)
        throw new Error("Connect workspace unavailable");
      const deadLetters = new Map<string, number>();
      for (const x of d.data)
        deadLetters.set(x.connection_id, (deadLetters.get(x.connection_id) ?? 0) + 1);
      return {
        connections: c.data,
        sources: s.data as unknown as ConnectSourceRow[],
        runs: r.data,
        streams: st.data,
        lag: l.data,
        deadLetters,
      };
    },
  });
}
/** Worker pools of the workspaces the user can see, with the agents that last claimed for them. */
export function usePools() {
  return useQuery({
    queryKey: ["connect-pools"],
    refetchInterval: 10000,
    queryFn: async () => {
      const [p, w] = await Promise.all([
        supabase
          .from("connect_pools")
          .select("id,owner_id,organization_id,name,enabled,created_at")
          .order("created_at"),
        supabase.from("connect_workers").select("pool_id,agent,version,slots,last_seen_at"),
      ]);
      if (p.error || w.error) throw new Error("Worker pools unavailable");
      return { pools: p.data, workers: w.data };
    },
  });
}
export type ConnectPools = NonNullable<ReturnType<typeof usePools>["data"]>;
export type ConnectList = NonNullable<ReturnType<typeof useConnectList>["data"]>;
export type ConnectConnection = ConnectList["connections"][number];
export type ConnectRun = ConnectList["runs"][number];

export function useConnectDetails(id: string, runIds: string[]) {
  return useQuery({
    queryKey: ["connect-details", id, runIds.join(",")],
    refetchInterval: 5000,
    queryFn: async () => {
      const s = await supabase.from("connect_streams").select("*").eq("connection_id", id);
      if (s.error) throw new Error("Connect run details unavailable");
      const [c, e, l] = await Promise.all([
        supabase
          .from("connect_commits")
          .select("*")
          .in("run_id", runIds)
          .order("created_at", { ascending: false })
          .limit(100),
        supabase
          .from("connect_events")
          .select("*")
          .eq("connection_id", id)
          .order("created_at", { ascending: false })
          .limit(50),
        supabase
          .from("connect_partition_lag")
          .select("stream_id,partition,lag,observed_at")
          .in(
            "stream_id",
            s.data.map((x) => x.id),
          ),
      ]);
      if (c.error || e.error || l.error) throw new Error("Connect run details unavailable");
      return { streams: s.data, commits: c.data, events: e.data, lag: l.data };
    },
  });
}
export type ConnectDetails = NonNullable<ReturnType<typeof useConnectDetails>["data"]>;

/** The existing Connect and ingestion server functions, with one busy flag and query refresh. */
export function useConnectActions() {
  const qc = useQueryClient();
  const [busy, setBusy] = useState(false);
  const fns = {
    createSource: useServerFn(createSource),
    create: useServerFn(createConnect),
    configure: useServerFn(configureConnect),
    enqueue: useServerFn(enqueueConnect),
    cancel: useServerFn(cancelConnect),
    retireStream: useServerFn(retireConnectStream),
    createPool: useServerFn(createPool),
    updatePool: useServerFn(updatePool),
    assignPool: useServerFn(assignConnectionPool),
  };
  async function run<T>(fn: () => Promise<T>, success?: string): Promise<T | undefined> {
    setBusy(true);
    try {
      const out = await fn();
      await qc.invalidateQueries({ queryKey: ["connect-workspace"] });
      await qc.invalidateQueries({ queryKey: ["connect-details"] });
      await qc.invalidateQueries({ queryKey: ["connect-pools"] });
      if (success) toast.success(success);
      return out;
    } catch (e) {
      toast.error((e as Error).message);
      return undefined;
    } finally {
      setBusy(false);
    }
  }
  const queue = (id: string, operation: "check" | "discover" | "sync" | "reset" | "backfill") =>
    fns.enqueue({ data: { id, requestId: crypto.randomUUID(), operation } });
  return { busy, run, queue, ...fns };
}

/** Polls one run until it finishes (the agent claims and executes it out of band). */
export function useRunStatus(runId: string | null) {
  return useQuery({
    queryKey: ["connect-run", runId],
    enabled: !!runId,
    refetchInterval: (q) =>
      q.state.data && ["succeeded", "failed", "cancelled"].includes(q.state.data.status)
        ? false
        : 2000,
    queryFn: async () => {
      const r = await supabase
        .from("connect_runs")
        .select("id,status,error_code,attempts,max_attempts,finished_at")
        .eq("id", runId!)
        .maybeSingle();
      if (r.error || !r.data) throw new Error("Run status unavailable");
      return r.data;
    },
  });
}
