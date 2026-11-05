import { createServerFn } from "@tanstack/react-start";
import { z } from "zod";
import { requireSupabaseAuth } from "@/integrations/supabase/auth-middleware";
import { splitConfig } from "./provisioning";
import { publicHttpsUrl } from "./outbound-policy";
import {
  authorizeConnection,
  base,
  fetchT,
  livyHeaders,
  loadConnection,
  requireRole,
  type Ctx,
} from "./connection-access";

export const saveConnection = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) =>
    z
      .object({
        id: z.string().uuid().optional(),
        kind: z.string().min(1).max(40),
        name: z.string().min(1).max(80),
        values: z.record(z.string(), z.string().max(4000)),
      })
      .parse(d),
  )
  .handler(async ({ data, context }) => {
    await requireRole(context as Ctx, ["admin", "engineer"]);
    const { config, secrets } = splitConfig(data.kind, data.values);
    if (config["url"]) publicHttpsUrl(config["url"]);
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    let id = data.id;
    if (id) {
      await authorizeConnection(context as Ctx, id, true);
      const { error } = await supabaseAdmin
        .from("connections")
        .update({ name: data.name, config, status: "untested" })
        .eq("id", id);
      if (error) throw new Error(error.message);
    } else {
      const { data: row, error } = await supabaseAdmin
        .from("connections")
        .insert({ kind: data.kind, name: data.name, config, created_by: context.userId })
        .select("id")
        .single();
      if (error) throw new Error(error.message);
      id = row.id;
    }
    if (Object.keys(secrets).length) {
      if (!id) throw new Error("Connection was not saved");
      const { data: prev } = await supabaseAdmin
        .from("connection_secrets")
        .select("secrets")
        .eq("connection_id", id)
        .maybeSingle();
      const saved = await supabaseAdmin.from("connection_secrets").upsert({
        connection_id: id,
        secrets: { ...((prev?.secrets ?? {}) as object), ...secrets },
        updated_at: new Date().toISOString(),
      });
      if (saved.error) throw new Error("Connection credentials could not be saved");
    }
    if (!id) throw new Error("Connection was not saved");
    return { id, secretKeys: Object.keys(secrets) };
  });

export const testConnection = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) => z.object({ id: z.string().uuid() }).parse(d))
  .handler(async ({ data, context }) => {
    await requireRole(context as Ctx, ["admin", "engineer"]);
    const { c, all, admin } = await loadConnection(data.id, context as Ctx);
    let ok = false,
      message = "";
    try {
      const { SIGNED_REMOTE_KINDS, testRemoteConnection } = await import("./remote.server");
      const u = base(all["url"] ?? "");
      if (SIGNED_REMOTE_KINDS.has(c.kind)) {
        message = await testRemoteConnection(c.kind, all);
        ok = true;
      } else if (c.kind === "postgres" || c.kind === "mysql") {
        message = "Runs through the Ingestion worker — set it up on the Ingestion page";
        ok = true;
      } else if (!u) throw new Error("No URL configured");
      else if (c.kind === "emr" || c.kind === "livy") {
        const r = await fetchT(`${u}/sessions`, { headers: livyHeaders(all) });
        const j = (await r.json().catch(() => ({}))) as { total?: number };
        ok = r.ok;
        message = r.ok ? `Livy reachable · ${j.total ?? 0} sessions` : `HTTP ${r.status}`;
      } else if (c.kind === "databricks") {
        const r = await fetchT(`${u}/api/2.0/sql/warehouses/${all["warehouse_id"] ?? ""}`, {
          headers: { Authorization: `Bearer ${all["token"] ?? ""}` },
        });
        const j = (await r.json().catch(() => ({}))) as { state?: string; message?: string };
        ok = r.ok;
        message = r.ok ? `Warehouse ${j.state ?? "found"}` : (j.message ?? `HTTP ${r.status}`);
      } else if (c.kind === "superset") {
        const r = await fetchT(`${u}/api/v1/security/login`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            username: all["username"],
            password: all["password"],
            provider: "db",
            refresh: false,
          }),
        });
        ok = r.ok;
        message = r.ok ? "Logged in to Superset" : `Login failed (HTTP ${r.status})`;
      } else if (c.kind === "cube") {
        const r = await fetchT(`${u}/v1/meta`, { headers: { Authorization: all["token"] ?? "" } });
        ok = r.ok;
        message = r.ok ? "Cube API reachable" : `HTTP ${r.status}`;
      } else if (c.kind === "iceberg_rest") {
        const q = all["warehouse"] ? `?warehouse=${encodeURIComponent(all["warehouse"])}` : "";
        const r = await fetchT(`${u}/v1/config${q}`, {
          headers: all["token"] ? { Authorization: `Bearer ${all["token"]}` } : {},
        });
        ok = r.ok;
        message = r.ok ? "Catalog reachable" : `HTTP ${r.status}`;
      } else message = "No test available for this type";
    } catch (e) {
      message = (e as Error).message;
    }
    await admin
      .from("connections")
      .update({
        status: ok ? "ok" : "error",
        status_message: message.slice(0, 300),
        last_tested_at: new Date().toISOString(),
      })
      .eq("id", data.id);
    return { ok, message };
  });

type RemoteResult = {
  state: string;
  handle: string;
  columns: string[];
  rows: Record<string, string | null>[];
  text: string;
};

/** Submit code to a remote Spark cluster (EMR/Livy) or Databricks SQL. Returns a handle to poll. */
export const submitRemote = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) =>
    z
      .object({
        id: z.string().uuid(),
        code: z.string().min(1).max(50000),
        language: z.enum(["sql", "pyspark"]),
      })
      .parse(d),
  )
  .handler(async ({ data, context }): Promise<RemoteResult> => {
    await requireRole(context as Ctx, ["admin", "engineer"]);
    const { c, all, admin } = await loadConnection(data.id, context as Ctx);
    const u = base(all["url"] ?? "");
    const { WAREHOUSE_KINDS, runWarehouseSql } = await import("./remote.server");
    if ((WAREHOUSE_KINDS as readonly string[]).includes(c.kind)) {
      if (data.language !== "sql") throw new Error(`${c.name} runs SQL cells only`);
      const out = await runWarehouseSql(c.kind, all, data.code);
      return { state: "FINISHED", handle: "", columns: out.columns, rows: out.rows, text: "" };
    }
    if (c.kind !== "emr" && c.kind !== "livy") throw new Error("This connection can't run code");
    let session = Number(all["session_id"] ?? NaN);
    const h = livyHeaders(all);
    const alive =
      Number.isFinite(session) &&
      ((await fetchT(`${u}/sessions/${session}/state`, { headers: h })
        .then((r) => (r.ok ? r.json() : null))
        .catch(() => null)) as { state?: string } | null);
    if (!alive || ["dead", "error", "killed", "success"].includes(alive.state ?? "")) {
      const r = await fetchT(`${u}/sessions`, {
        method: "POST",
        headers: h,
        body: JSON.stringify({
          kind: all["session_kind"] || "pyspark",
          name: `meridian-${Date.now()}`,
        }),
      });
      if (!r.ok) throw new Error(`Could not start Spark session (HTTP ${r.status})`);
      session = ((await r.json()) as { id: number }).id;
      await admin
        .from("connections")
        .update({ config: { ...(c.config as object), session_id: String(session) } })
        .eq("id", data.id);
      return {
        state: "starting",
        handle: `${session}:`,
        columns: [],
        rows: [],
        text: "Spark session is starting on the cluster — run again in ~30s.",
      };
    }
    const code =
      data.language === "sql"
        ? `spark.sql("""${data.code.replace(/"""/g, '\\"\\"\\"')}""").show(100, False)`
        : data.code;
    const r = await fetchT(`${u}/sessions/${session}/statements`, {
      method: "POST",
      headers: h,
      body: JSON.stringify({ code, kind: "pyspark" }),
    });
    if (!r.ok) throw new Error(`Submit failed (HTTP ${r.status})`);
    const st = (await r.json()) as { id: number };
    return { state: "running", handle: `${session}:${st.id}`, columns: [], rows: [], text: "" };
  });

export const pollRemote = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) =>
    z.object({ id: z.string().uuid(), handle: z.string().regex(/^\d+:\d+$/) }).parse(d),
  )
  .handler(async ({ data, context }) => {
    await requireRole(context as Ctx, ["admin", "engineer"]);
    const { all } = await loadConnection(data.id, context as Ctx);
    const [s, st] = data.handle.split(":");
    const r = await fetchT(`${base(all["url"] ?? "")}/sessions/${s}/statements/${st}`, {
      headers: livyHeaders(all),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = (await r.json()) as {
      state: string;
      output?: { status: string; data?: Record<string, unknown>; evalue?: string };
    };
    const text =
      j.output?.status === "error"
        ? `Error: ${j.output.evalue}`
        : String(j.output?.data?.["text/plain"] ?? "");
    return { state: j.state, text };
  });

async function s3Conn(id: string, ctx: Ctx) {
  const { c, all } = await loadConnection(id, ctx);
  if (c.kind !== "s3") throw new Error("Not an S3 connection");
  return all;
}

export const s3List = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) =>
    z
      .object({
        id: z.string().uuid(),
        prefix: z.string().max(1024),
        token: z.string().max(2048).optional(),
      })
      .parse(d),
  )
  .handler(async ({ data, context }) => {
    await requireRole(context as Ctx, ["admin", "engineer", "steward"]);
    await s3Conn(data.id, context as Ctx);
    const { listS3 } = await import("./remote.server");
    return listS3(data.prefix, data.token);
  });

/** Returns the object as base64; the browser parses Parquet/Avro/CSV/JSON. */
export const s3Read = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) =>
    z.object({ id: z.string().uuid(), key: z.string().min(1).max(1024) }).parse(d),
  )
  .handler(async ({ data, context }) => {
    await requireRole(context as Ctx, ["admin", "engineer", "steward"]);
    await s3Conn(data.id, context as Ctx);
    const { readS3 } = await import("./remote.server");
    return { base64: await readS3(data.key) };
  });
