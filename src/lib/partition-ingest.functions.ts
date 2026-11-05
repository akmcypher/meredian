import { createServerFn } from "@tanstack/react-start";
import { z } from "zod";
import { requireSupabaseAuth } from "@/integrations/supabase/auth-middleware";
import { demoBatch, type Predicate } from "./partitioning";
import { safeTableName } from "./ingest";
import { authoritativePartitions } from "./partition-plan-validation";
import type { Json } from "@/integrations/supabase/types";

const MAX_DEMO_ROWS = 2_000_000;

/** Persist a planned run and its logical partitions. */
export const createPartitionRun = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) =>
    z
      .object({
        target: z.string().min(1).max(60),
        total: z.number().int().min(1).max(MAX_DEMO_ROWS),
        batchSize: z.number().int().min(100).max(5000),
        concurrency: z.number().int().min(1).max(32),
        plan: z.record(z.string(), z.unknown()),
        partitions: z
          .array(
            z.object({
              idx: z.number().int(),
              label: z.string().max(300),
              predicate: z.record(z.string(), z.unknown()),
              estRows: z.number(),
            }),
          )
          .min(1)
          .max(2000),
      })
      .parse(d),
  )
  .handler(async ({ data, context }) => {
    const parts = authoritativePartitions(data.plan, data.total, true);
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    const result = await supabaseAdmin.rpc("create_demo_ingest_run_verified", {
      _actor: context.userId,
      _target: safeTableName(data.target),
      _total: data.total,
      _batch: data.batchSize,
      _concurrency: data.concurrency,
      _plan: { ...data.plan, total: data.total } as Json,
      _partitions: parts as unknown as Json,
    });
    if (result.error) throw new Error(result.error.message);
    return { id: result.data };
  });

/**
 * Process up to `maxBatches` keyset batches of one partition, checkpointing after each batch.
 * Rows are upserted on (run_id, pk) so a retry after a crash never duplicates data.
 */
export const processPartition = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) =>
    z
      .object({
        runId: z.string().uuid(),
        idx: z.number().int().min(0),
        worker: z.string().max(40),
        maxBatches: z.number().int().min(1).max(20).default(4),
        failRate: z.number().min(0).max(0.5).default(0),
      })
      .parse(d),
  )
  .handler(async ({ data, context }) => {
    const { data: run } = await context.supabase
      .from("ingest_runs")
      .select("id, plan, batch_size, status, owner_id, source_kind, source_id")
      .eq("id", data.runId)
      .maybeSingle();
    // Org members can read a run, but only its owner may execute it, and only demo runs execute in-app.
    if (!run || run.owner_id !== context.userId) throw new Error("Run not found");
    if (run.source_kind !== "demo_orders" || run.source_id)
      throw new Error("Runs for registered sources are executed by the source worker");
    if (run.status === "cancelled") throw new Error("Run was cancelled");
    if (run.status === "published") return { done: true, rows: 0, skipped: true };
    const { data: existing } = await context.supabase
      .from("ingest_partitions")
      .select("status")
      .eq("run_id", data.runId)
      .eq("idx", data.idx)
      .maybeSingle();
    if (!existing) throw new Error("Partition not found");
    if (existing.status === "done") return { done: true, rows: 0, skipped: true };
    const total = Number((run.plan as { total?: number }).total ?? 0);
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    // Atomic lease: only one live worker may hold a partition; expired leases can be taken over.
    const { data: claimed, error: claimErr } = await supabaseAdmin.rpc("claim_specific_partition", {
      _run: run.id,
      _idx: data.idx,
      _worker: data.worker,
    });
    if (claimErr) throw new Error(claimErr.message);
    const part = (
      claimed as unknown as
        | {
            id: string;
            idx: number;
            predicate: unknown;
            checkpoint: unknown;
            lease_token: string;
          }[]
        | null
    )?.[0];
    if (!part) return { done: false, rows: 0, skipped: true };
    const pred = part.predicate as unknown as Predicate;
    let after = Number((part.checkpoint as { last_id?: number } | null)?.last_id ?? 0);
    let moved = 0,
      done = false;
    try {
      for (let b = 0; b < data.maxBatches && !done; b++) {
        const batch = demoBatch(pred, total, after, run.batch_size);
        // Simulated worker crash before commit: nothing from this batch is kept, the retry resumes at the checkpoint.
        if (data.failRate > 0 && Math.random() < data.failRate)
          throw new Error("injected worker failure");
        const { error } = await supabaseAdmin.rpc("write_ingest_batch_fenced", {
          _run: run.id,
          _idx: part.idx,
          _worker: data.worker,
          _lease_token: part.lease_token,
          _rows: batch.rows as unknown as Json,
          _checkpoint: { last_id: batch.lastId } as Json,
          _done: batch.done,
        });
        if (error) throw new Error(error.message);
        after = batch.lastId;
        done = batch.done;
        moved += batch.rows.length;
      }
      // Bounded demo requests hand their live lease back; the next request must claim a new token.
      if (!done) {
        const yielded = await supabaseAdmin.rpc("yield_ingest_partition_fenced", {
          _run: run.id,
          _idx: part.idx,
          _worker: data.worker,
          _lease_token: part.lease_token,
        });
        if (yielded.error || !yielded.data)
          throw new Error("Partition lease could not be released");
      }
      return { done, rows: moved, skipped: false };
    } catch (e) {
      await supabaseAdmin.rpc("fail_ingest_partition_fenced", {
        _run: run.id,
        _idx: part.idx,
        _worker: data.worker,
        _lease_token: part.lease_token,
        _error: "Partition execution failed",
      });
      throw e;
    }
  });

/** Plan a run for a registered source (Postgres/MySQL/Kafka/S3/ADLS/REST). The source's worker executes it. */
export const createSourceRun = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) =>
    z
      .object({
        sourceId: z.string().uuid(),
        pkColumns: z
          .array(
            z
              .string()
              .min(1)
              .max(63)
              .regex(/^[A-Za-z_][\w]*$/),
          )
          .min(1)
          .max(8),
        batchSize: z.number().int().min(100).max(10_000),
        concurrency: z.number().int().min(1).max(64),
        expectedRows: z.number().int().min(0).default(0),
        plan: z.record(z.string(), z.unknown()),
        partitions: z
          .array(
            z.object({
              idx: z.number().int(),
              label: z.string().max(300),
              predicate: z.record(z.string(), z.unknown()),
              estRows: z.number(),
            }),
          )
          .min(1)
          .max(5000),
      })
      .parse(d),
  )
  .handler(async ({ data, context }) => {
    const { data: src } = await context.supabase
      .from("ingest_sources")
      .select("id, owner_id, kind, target_table, organization_id")
      .eq("id", data.sourceId)
      .single();
    if (!src || src.owner_id !== context.userId) throw new Error("Source not found");
    const parts = authoritativePartitions(data.plan, data.expectedRows, false, data.plan["stats"]);
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    const result = await supabaseAdmin.rpc("create_ingest_run_verified", {
      _actor: context.userId,
      _source: src.id,
      _target: src.target_table,
      _total: data.expectedRows,
      _batch: data.batchSize,
      _concurrency: data.concurrency,
      _plan: data.plan as Json,
      _partitions: parts as unknown as Json,
      _pk: data.pkColumns,
    });
    if (result.error) throw new Error(result.error.message);
    return { id: result.data };
  });

export const cancelRun = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) => z.object({ runId: z.string().uuid() }).parse(d))
  .handler(async ({ data, context }) => {
    const { data: cancelled, error } = await context.supabase.rpc("cancel_ingest_run", {
      _run: data.runId,
    });
    if (error) throw new Error(error.message);
    if (!cancelled) throw new Error("Run not found or not active");
    return { ok: true };
  });

/** Count staged rows, then atomically point the table at this run (snapshot swap). */
export const publishPartitionRun = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) => z.object({ runId: z.string().uuid() }).parse(d))
  .handler(async ({ data, context }) => {
    const { data: n, error } = await context.supabase.rpc("publish_ingest_run", {
      _run: data.runId,
    });
    if (error) throw new Error(error.message);
    const { data: dup } = await context.supabase
      .from("ingest_partitions")
      .select("rows_written")
      .eq("run_id", data.runId);
    const counted = (dup ?? []).reduce((a, p) => a + Number(p.rows_written), 0);
    return { published: Number(n), counterTotal: counted };
  });
