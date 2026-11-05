import { createFileRoute } from "@tanstack/react-router";
import { z } from "zod";
import { sha256 } from "@/lib/ingest";
import { ingestDeadLetter } from "@/lib/ingest-dead-letters";

const body = z.object({
  rows: z.array(z.record(z.string(), z.unknown())).max(5000).optional(),
  events: z
    .array(
      z.object({
        op: z.enum(["c", "r", "u", "d"]),
        before: z.record(z.string(), z.unknown()).nullish(),
        after: z.record(z.string(), z.unknown()).nullish(),
      }),
    )
    .max(5000)
    .optional(),
  mode: z.enum(["replace", "append", "upsert"]).optional(),
  batchId: z.string().uuid().optional(),
  // Records the worker could not use; stored before it advances its offset.
  deadLetters: z.array(ingestDeadLetter).max(1000).optional(),
});

// 5,000 rows per request; the cap is checked while reading, before JSON parsing.
const MAX_BODY_BYTES = 8 * 1024 * 1024;

// Push endpoint for database / Kafka CDC workers. Caller is verified by a per-source token (hash lookup).
export const Route = createFileRoute("/api/public/ingest")({
  server: {
    handlers: {
      POST: async ({ request }) => {
        const token = request.headers.get("x-ingest-token") ?? "";
        if (!/^mi_[a-f0-9]{64}$/.test(token))
          return Response.json({ error: "unauthorized" }, { status: 401 });
        const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
        const { data: src } = await supabaseAdmin
          .from("ingest_sources")
          .select("id, owner_id, organization_id, target_table, config, enabled")
          .eq("token_hash", await sha256(token))
          .maybeSingle();
        if (!src || !src.enabled) return Response.json({ error: "unauthorized" }, { status: 401 });
        const limits = await import("@/lib/request-limits.server");
        let payload: unknown;
        try {
          await limits.enforceRateLimit(`ingest:${src.id}`, limits.rateLimit("worker"));
          payload = await limits.readJsonCapped(request, MAX_BODY_BYTES);
        } catch (e) {
          if (e instanceof limits.RequestLimitError) return limits.limitResponse(e);
          throw e;
        }
        const parsed = body.safeParse(payload);
        if (!parsed.success) return Response.json({ error: "bad body" }, { status: 400 });
        const cfg = (src.config ?? {}) as { key?: string; cursor?: string };
        const { deadLetters, ...batch } = parsed.data;
        if (deadLetters?.length && !batch.batchId)
          return Response.json({ error: "batchId required with deadLetters" }, { status: 400 });
        try {
          let stored = 0;
          if (deadLetters?.length) {
            const { recordIngestDeadLetters } = await import("@/lib/ingest-dead-letters.server");
            stored = await recordIngestDeadLetters(supabaseAdmin, src, batch.batchId!, deadLetters);
            // A dead-letter-only request lands no rows.
            if (!batch.rows?.length && !batch.events?.length)
              return Response.json({ ok: true, deadLetters: stored }, { status: 202 });
          }
          const { landSourceBatch } = await import("@/lib/landing-batch.server");
          const landed = await landSourceBatch(src.id, src.owner_id, {
            ...batch,
            key: cfg.key || "id",
          });
          return Response.json({ ok: true, ...landed, deadLetters: stored }, { status: 202 });
        } catch {
          await supabaseAdmin
            .from("ingest_sources")
            .update({
              last_run_at: new Date().toISOString(),
              last_status: "Landing failed; reconciliation required",
            })
            .eq("id", src.id);
          return Response.json({ error: "write failed" }, { status: 500 });
        }
      },
    },
  },
});
