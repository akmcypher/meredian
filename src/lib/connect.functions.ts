import { createServerFn } from "@tanstack/react-start";
import { requireSupabaseAuth } from "@/integrations/supabase/auth-middleware";
import { z } from "zod";
import { identifier, primaryKeySpec } from "./connect-protocol";
const CONNECT_SOURCES = ["postgres", "mysql", "kafka"];
export const createConnect = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((v: unknown) =>
    z
      .object({
        sourceId: z.string().uuid(),
        name: z.string().min(1).max(80),
        credentialRef: z.string().regex(/^[a-zA-Z0-9_-]{1,80}$/),
      })
      .parse(v),
  )
  .handler(async ({ context, data }) => {
    const src = await context.supabase
      .from("ingest_sources")
      .select("*")
      .eq("id", data.sourceId)
      .maybeSingle();
    // One actor per certified source kind (deploy/connect/agent.py SOURCES).
    const kind = src.data?.kind ?? "";
    if (!src.data || src.data.owner_id !== context.userId || !CONNECT_SOURCES.includes(kind))
      throw new Error("Choose your PostgreSQL, MySQL or Kafka ingestion source");
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    const r = await supabaseAdmin
      .from("connect_connections")
      .insert({
        source_id: src.data.id,
        owner_id: context.userId,
        organization_id: src.data.organization_id,
        name: data.name,
        credential_ref: data.credentialRef,
        source_version: `${kind}-v1`,
      })
      .select("id")
      .single();
    if (r.error) throw new Error("Connect registration failed; source may already be registered");
    return r.data;
  });
export const enqueueConnect = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((v: unknown) =>
    z
      .object({
        id: z.string().uuid(),
        requestId: z.string().uuid(),
        operation: z.enum(["check", "discover", "sync", "reset", "backfill"]),
      })
      .parse(v),
  )
  .handler(async ({ context, data }) => {
    const r = await context.supabase.rpc("connect_enqueue", {
      _connection: data.id,
      _request: data.requestId,
      _operation: data.operation,
    });
    if (r.error) throw new Error(r.error.message);
    return { id: r.data };
  });
export const configureConnect = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((v: unknown) =>
    z
      .object({
        id: z.string().uuid(),
        enabled: z.boolean().optional(),
        interval: z.number().int().min(0).max(10080).optional(),
        stream: z
          .object({
            namespace: identifier,
            name: identifier,
            primaryKey: primaryKeySpec,
            cursor: identifier,
            mode: z.enum(["incremental", "full_refresh"]),
            // Kafka Avro streams: pin the latest version of a Meridian schema subject as the
            // reader schema, so writers that no longer resolve to it become dead letters.
            readerSchemaSubjectId: z.string().uuid().optional(),
          })
          .optional(),
      })
      .parse(v),
  )
  .handler(async ({ context, data }) => {
    const c = await context.supabase
      .from("connect_connections")
      .select("*")
      .eq("id", data.id)
      .maybeSingle();
    if (!c.data || c.data.owner_id !== context.userId) throw new Error("Connection owner required");
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    if (data.stream) {
      const active = await supabaseAdmin
        .from("connect_runs")
        .select("id")
        .eq("connection_id", data.id)
        .in("status", ["queued", "running", "retry_wait"]);
      if (active.data?.length)
        throw new Error("Stop active run before changing stream configuration");
      const catalog = z
        .array(
          z.object({
            name: identifier,
            namespace: identifier,
            json_schema: z.record(z.string(), z.unknown()),
            primary_key: primaryKeySpec,
          }),
        )
        .parse(c.data.discovered_catalog);
      const keys = [data.stream.primaryKey].flat();
      const found = catalog.find(
        (s) => s.name === data.stream?.name && s.namespace === data.stream.namespace,
      );
      if (
        !found ||
        [found.primary_key].flat().join(",") !== keys.join(",") ||
        !(found.json_schema["properties"] as Record<string, unknown>)?.[data.stream.cursor]
      )
        throw new Error("Select a discovered stream, cursor and its declared primary key");
      let schemaJson = found.json_schema;
      if (data.stream.readerSchemaSubjectId) {
        if (typeof found.json_schema["x-kafka-topic"] !== "string")
          throw new Error("Reader schemas apply to Kafka streams only");
        const { latestSchema } = await import("./schema-registry.server");
        // Read with the caller's rights: only subjects the caller can see can be pinned.
        const pinned = await latestSchema(context.supabase, data.stream.readerSchemaSubjectId);
        schemaJson = {
          ...found.json_schema,
          "x-avro-reader-schema": pinned.schema,
          "x-avro-reader-version": {
            subject_id: data.stream.readerSchemaSubjectId,
            version: pinned.version,
          },
        };
      }
      const r = await supabaseAdmin.from("connect_streams").upsert(
        {
          connection_id: data.id,
          namespace: data.stream.namespace,
          name: data.stream.name,
          primary_key: keys.join(","),
          key_columns: keys,
          cursor_field: data.stream.cursor,
          sync_mode: data.stream.mode,
          schema_json: schemaJson as never,
        },
        { onConflict: "connection_id,namespace,name" },
      );
      if (r.error) throw new Error("Stream configuration failed");
    }
    if (data.enabled !== undefined || data.interval !== undefined) {
      const r = await supabaseAdmin
        .from("connect_connections")
        .update({
          ...(data.enabled === undefined ? {} : { enabled: data.enabled }),
          ...(data.interval === undefined
            ? {}
            : { interval_minutes: data.interval, next_run_at: new Date().toISOString() }),
        })
        .eq("id", data.id);
      if (r.error) throw new Error("Connection configuration failed");
    }
    return { saved: true };
  });
export const cancelConnect = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((v: unknown) => z.object({ id: z.string().uuid() }).parse(v))
  .handler(async ({ context, data }) => {
    const r = await context.supabase.rpc("connect_cancel", { _run: data.id });
    if (r.error) throw new Error("Cancellation failed");
    return { cancelled: r.data };
  });
