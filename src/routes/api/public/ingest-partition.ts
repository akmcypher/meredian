import { createFileRoute } from "@tanstack/react-router";
import { z } from "zod";
import { sha256 } from "@/lib/ingest";
import { predicateSql, type Predicate } from "@/lib/partitioning";

const worker = z
  .string()
  .min(1)
  .max(60)
  .regex(/^[\w.:@-]+$/);
const body = z.discriminatedUnion("action", [
  z.object({ action: z.literal("register"), worker, host: z.string().max(120).optional() }),
  z.object({ action: z.literal("plan"), worker }),
  z.object({
    action: z.literal("snapshot"),
    worker,
    run_id: z.string().uuid(),
    snapshot: z.record(z.string(), z.unknown()),
  }),
  z.object({
    action: z.literal("claim"),
    worker,
    run_id: z.string().uuid(),
    lease_seconds: z.number().int().min(10).max(600).default(120),
  }),
  z.object({
    action: z.literal("heartbeat"),
    worker,
    run_id: z.string().uuid(),
    idx: z.number().int().min(0),
    lease_token: z.string().uuid(),
  }),
  z.object({
    action: z.literal("write"),
    worker,
    run_id: z.string().uuid(),
    idx: z.number().int().min(0),
    lease_token: z.string().uuid(),
    rows: z.array(z.record(z.string(), z.unknown())).max(10_000), // per-request batch; tables are unbounded
    checkpoint: z.record(z.string(), z.unknown()),
    done: z.boolean(),
  }),
  z.object({
    action: z.literal("fail"),
    worker,
    run_id: z.string().uuid(),
    idx: z.number().int().min(0),
    lease_token: z.string().uuid(),
    error: z.string().max(500),
  }),
]);

const json = (b: unknown, status = 200) => Response.json(b, { status });
// write_ingest_batch_fenced accepts at most 8 MiB of rows; allow envelope overhead.
const MAX_BODY_BYTES = 10 * 1024 * 1024;

/**
 * Worker protocol for real sources (deploy/ingest/partitioned_worker.mjs).
 * Caller = per-source token (only its SHA-256 is stored). Every run touched must belong to that source.
 * Claims are atomic (FOR UPDATE SKIP LOCKED) with leases; writes + checkpoints commit in one transaction.
 */
export const Route = createFileRoute("/api/public/ingest-partition")({
  server: {
    handlers: {
      POST: async ({ request }) => {
        const token = request.headers.get("x-ingest-token") ?? "";
        if (!/^mi_[a-f0-9]{64}$/.test(token)) return json({ error: "unauthorized" }, 401);
        const { supabaseAdmin: db } = await import("@/integrations/supabase/client.server");
        const { data: src } = await db
          .from("ingest_sources")
          .select("id, owner_id, kind, config, pk_columns, enabled")
          .eq("token_hash", await sha256(token))
          .maybeSingle();
        // Disabling a source is how its token is revoked.
        if (!src || !src.enabled) return json({ error: "unauthorized" }, 401);
        const limits = await import("@/lib/request-limits.server");
        let payload: unknown;
        try {
          await limits.enforceRateLimit(`partition:${src.id}`, limits.rateLimit("worker"));
          payload = await limits.readJsonCapped(request, MAX_BODY_BYTES);
        } catch (e) {
          if (e instanceof limits.RequestLimitError) return limits.limitResponse(e);
          throw e;
        }
        const parsed = body.safeParse(payload);
        if (!parsed.success)
          return json({ error: "bad body", issues: parsed.error.issues.slice(0, 3) }, 400);
        const b = parsed.data;

        if (b.action === "register") {
          await db.from("ingest_workers").upsert(
            {
              id: b.worker,
              source_id: src.id,
              owner_id: src.owner_id,
              host: b.host ?? null,
              heartbeat_at: new Date().toISOString(),
            },
            { onConflict: "source_id,id" },
          );
          return json({ ok: true, source: { kind: src.kind, pk: src.pk_columns } });
        }

        if (b.action === "plan") {
          const { data: run } = await db
            .from("ingest_runs")
            .select("id, target_table, batch_size, concurrency, pk_columns, snapshot, status")
            .eq("source_id", src.id)
            .in("status", ["planned", "running"])
            .order("created_at", { ascending: false })
            .limit(1)
            .maybeSingle();
          if (!run) return json({ run: null });
          const { data: parts } = await db
            .from("ingest_partitions")
            .select("idx, label, predicate, status, checkpoint, rows_written")
            .eq("run_id", run.id)
            .order("idx");
          const dialect = src.kind === "mysql" ? "mysql" : "postgres";
          return json({
            run,
            partitions: (parts ?? []).map((p) => ({
              ...p,
              where: predicateSql(p.predicate as unknown as Predicate, dialect),
            })),
          });
        }

        // All remaining actions target a run that must belong to this source.
        const { data: run } = await db
          .from("ingest_runs")
          .select("id, source_id, status")
          .eq("id", b.run_id)
          .maybeSingle();
        if (!run || run.source_id !== src.id) return json({ error: "run not found" }, 404);
        if (run.status === "published") return json({ error: "run already published" }, 409);
        await db
          .from("ingest_workers")
          .update({ heartbeat_at: new Date().toISOString() })
          .eq("source_id", src.id)
          .eq("id", b.worker);

        if (b.action === "snapshot") {
          // The first worker's snapshot is kept; a worker resuming partitions after a restart
          // marks the run "mixed" (see the consistency contract in partitioned_worker.mjs).
          if (b.snapshot["consistency"] === "mixed") {
            const { data: cur } = await db
              .from("ingest_runs")
              .select("snapshot")
              .eq("id", run.id)
              .single();
            const first = (cur?.snapshot ?? {}) as Record<string, unknown>;
            await db
              .from("ingest_runs")
              .update({
                snapshot: { ...first, consistency: "mixed", resumed: b.snapshot } as never,
              })
              .eq("id", run.id);
          } else {
            const first = await db
              .from("ingest_runs")
              .update({ snapshot: b.snapshot as never })
              .eq("id", run.id)
              .is("snapshot", null)
              .select("id");
            // Another worker process already recorded its own snapshot: two processes read two
            // points in time, so the run is mixed even though neither resumed.
            if (!first.data?.length) {
              const { data: cur } = await db
                .from("ingest_runs")
                .select("snapshot")
                .eq("id", run.id)
                .single();
              const prior = (cur?.snapshot ?? {}) as Record<string, unknown>;
              if (prior["lsn"] !== b.snapshot["lsn"] || prior["at"] !== b.snapshot["at"])
                await db
                  .from("ingest_runs")
                  .update({
                    snapshot: { ...prior, consistency: "mixed", concurrent: b.snapshot } as never,
                  })
                  .eq("id", run.id);
            }
          }
          return json({ ok: true });
        }
        if (b.action === "claim") {
          const { data, error } = await db.rpc("claim_ingest_partition", {
            _run: run.id,
            _worker: b.worker,
            _lease_seconds: b.lease_seconds,
          });
          if (error) return json({ error: error.message }, 500);
          const p = (
            data as
              | {
                  idx: number;
                  predicate: unknown;
                  checkpoint: unknown;
                  attempts: number;
                  lease_token: string;
                  claim_version: number;
                }[]
              | null
          )?.[0];
          if (!p) return json({ partition: null });
          return json({
            partition: {
              idx: p.idx,
              checkpoint: p.checkpoint,
              attempts: p.attempts,
              lease_token: p.lease_token,
              claim_version: p.claim_version,
              where: predicateSql(
                p.predicate as Predicate,
                src.kind === "mysql" ? "mysql" : "postgres",
              ),
            },
          });
        }
        if (b.action === "heartbeat") {
          const { data } = await db.rpc("heartbeat_ingest_partition_fenced", {
            _run: run.id,
            _idx: b.idx,
            _worker: b.worker,
            _lease_token: b.lease_token,
            _lease_seconds: 120,
          });
          return data ? json({ ok: true }) : json({ error: "lease lost" }, 409);
        }
        if (b.action === "write") {
          const { data, error } = await db.rpc("write_ingest_batch_fenced", {
            _run: run.id,
            _idx: b.idx,
            _worker: b.worker,
            _lease_token: b.lease_token,
            _rows: b.rows as never,
            _checkpoint: b.checkpoint as never,
            _done: b.done,
            _lease_seconds: 120,
          });
          if (error)
            return json({ error: error.message }, error.message.includes("lease lost") ? 409 : 500);
          return json({ ok: true, rows_written: data });
        }
        // fail
        const failed = await db.rpc("fail_ingest_partition_fenced", {
          _run: run.id,
          _idx: b.idx,
          _worker: b.worker,
          _lease_token: b.lease_token,
          _error: b.error,
        });
        if (failed.error) return json({ error: "failure transition unavailable" }, 503);
        return failed.data ? json({ ok: true }) : json({ error: "lease lost" }, 409);
      },
    },
  },
});
