import { createServerFn } from "@tanstack/react-start";
import { z } from "zod";
import { requireSupabaseAuth } from "@/integrations/supabase/auth-middleware";
import { extractPath, safeTableName, sha256, type Row } from "./ingest";
import { publicHttpsUrl } from "./outbound-policy";

const restCfg = z.object({
  url: z.string().url(),
  method: z.enum(["GET", "POST"]).default("GET"),
  headers: z.record(z.string(), z.string()).default({}),
  body: z.string().max(10000).optional(),
  path: z.string().max(200).default(""),
  mode: z.enum(["replace", "append", "upsert"]).default("replace"),
  key: z.string().max(80).optional(),
});

async function callRest(cfg: z.infer<typeof restCfg>): Promise<Row[]> {
  const { safeOutboundFetch } = await import("./outbound.server");
  const r = await safeOutboundFetch(cfg.url, {
    method: cfg.method,
    headers: cfg.headers,
    body: cfg.method === "POST" ? (cfg.body ?? null) : null,
  });
  if (!r.ok) throw new Error(`API answered ${r.status}`);
  const json = await r.json().catch(() => {
    throw new Error("API did not return JSON");
  });
  return extractPath(json, cfg.path);
}

export const previewRest = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) => restCfg.parse(d))
  .handler(async ({ data }) => {
    const rows = await callRest(data);
    return {
      count: rows.length,
      rows: JSON.parse(JSON.stringify(rows.slice(0, 20))) as Record<
        string,
        string | number | boolean | null
      >[],
    };
  });

/** Create a source. For pushed kinds returns a one-time token (only its hash is stored). */
export const createSource = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) =>
    z
      .object({
        name: z.string().min(1).max(80),
        kind: z.enum(["rest", "postgres", "mysql", "kafka_cdc", "kafka", "s3", "adls"]),
        target: z.string().min(1).max(60),
        config: z.record(z.string(), z.unknown()),
      })
      .parse(d),
  )
  .handler(async ({ data, context }) => {
    if (data.kind === "rest") publicHttpsUrl(restCfg.parse(data.config).url);
    const cfg = data.kind === "rest" ? restCfg.parse(data.config) : data.config;
    const headers = data.kind === "rest" ? restCfg.parse(data.config).headers : {};
    const safeConfig = { ...cfg } as Record<string, unknown>;
    delete safeConfig["headers"];
    const token = `mi_${crypto.randomUUID().replace(/-/g, "")}${crypto.randomUUID().replace(/-/g, "")}`;
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    const { data: id, error } = await supabaseAdmin.rpc("create_ingest_source_verified", {
      _actor: context.userId,
      _name: data.name,
      _kind: data.kind,
      _target: safeTableName(data.target),
      _config: safeConfig as never,
      _headers: headers,
      _hash: data.kind === "rest" ? "" : await sha256(token),
    });
    if (error)
      throw new Error(
        "Source registration failed; keep credentials out of URLs and public configuration",
      );
    return { id, token: data.kind === "rest" ? "" : token };
  });

export const runRestSource = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) => z.object({ id: z.string().uuid() }).parse(d))
  .handler(async ({ data, context }) => {
    const { data: src } = await context.supabase
      .from("ingest_sources")
      .select("*")
      .eq("id", data.id)
      .single();
    if (!src || src.kind !== "rest" || src.owner_id !== context.userId || !src.enabled)
      throw new Error("Source not found");
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    const secrets = await supabaseAdmin
      .from("ingest_source_secrets")
      .select("headers")
      .eq("source_id", src.id)
      .maybeSingle();
    if (secrets.error) throw new Error("Source credentials unavailable");
    const cfg = restCfg.parse({ ...(src.config as object), headers: secrets.data?.headers ?? {} });
    try {
      const rows = await callRest(cfg);
      const { landSourceBatch } = await import("./landing-batch.server");
      const landed = await landSourceBatch(src.id, context.userId, {
        rows,
        mode: cfg.mode,
        key: cfg.key,
      });
      return { fetched: rows.length, total: landed.rows, status: landed.status };
    } catch {
      await context.supabase
        .from("ingest_sources")
        .update({
          last_run_at: new Date().toISOString(),
          last_status: "REST extraction failed; check credentials and endpoint",
        })
        .eq("id", src.id);
      throw new Error("REST extraction failed; check credentials, endpoint and response format");
    }
  });

export const testIngestSource = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) => z.object({ id: z.string().uuid() }).parse(d))
  .handler(async ({ context, data }) => {
    const src = await context.supabase
      .from("ingest_sources")
      .select("*")
      .eq("id", data.id)
      .maybeSingle();
    if (!src.data || src.data.owner_id !== context.userId) throw new Error("Source unavailable");
    if (src.data.kind !== "rest")
      throw new Error(
        "Connectivity must be tested by the independent source worker; no hosted probe adapter exists",
      );
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    const secrets = await supabaseAdmin
      .from("ingest_source_secrets")
      .select("headers")
      .eq("source_id", data.id)
      .maybeSingle();
    if (secrets.error) throw new Error("Source credentials unavailable");
    try {
      const rows = await callRest(
        restCfg.parse({ ...(src.data.config as object), headers: secrets.data?.headers ?? {} }),
      );
      return {
        ok: true,
        count: rows.length,
        columns: [...new Set(rows.slice(0, 20).flatMap(Object.keys))],
      };
    } catch {
      throw new Error("Connection test failed; check endpoint, credentials and JSON path");
    }
  });

export const manageIngestSource = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) =>
    z
      .object({
        id: z.string().uuid(),
        enabled: z.boolean().optional(),
        headers: z.record(z.string(), z.string().max(4000)).optional(),
      })
      .parse(d),
  )
  .handler(async ({ context, data }) => {
    const src = await context.supabase
      .from("ingest_sources")
      .select("owner_id,kind")
      .eq("id", data.id)
      .maybeSingle();
    if (!src.data || src.data.owner_id !== context.userId) throw new Error("Source unavailable");
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    if (data.headers) {
      if (src.data.kind !== "rest")
        throw new Error("Rotate database and broker credentials at the independent worker");
      const r = await supabaseAdmin.from("ingest_source_secrets").upsert({
        source_id: data.id,
        headers: data.headers,
        updated_at: new Date().toISOString(),
      });
      if (r.error) throw new Error("Credential rotation failed");
    }
    if (data.enabled !== undefined) {
      const r = await context.supabase
        .from("ingest_sources")
        .update({ enabled: data.enabled })
        .eq("id", data.id);
      if (r.error) throw new Error("Source state update failed");
    }
    return { saved: true };
  });
