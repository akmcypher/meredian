// Worker pools (migration 0108, deploy/connect/README.md "Worker pools"): a named group of agents in
// one workspace with its own token. Agents holding the pool token claim tasks of every connection
// assigned to the pool. The token is shown once and stored as a SHA-256 hash only.
import { createServerFn } from "@tanstack/react-start";
import { requireSupabaseAuth } from "@/integrations/supabase/auth-middleware";
import { z } from "zod";
import { sha256 } from "./ingest";

function poolToken() {
  const hex = () => crypto.randomUUID().replace(/-/g, "");
  return `mp_${hex()}${hex()}`;
}

export const createPool = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((v: unknown) =>
    z
      .object({
        name: z.string().trim().min(1).max(80),
        organizationId: z.string().uuid().nullable(),
      })
      .parse(v),
  )
  .handler(async ({ context, data }) => {
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    if (data.organizationId) {
      const member = await supabaseAdmin
        .from("org_members")
        .select("user_id")
        .eq("org_id", data.organizationId)
        .eq("user_id", context.userId)
        .maybeSingle();
      if (!member.data) throw new Error("Organization membership required");
    }
    const token = poolToken();
    const r = await supabaseAdmin
      .from("connect_pools")
      .insert({
        owner_id: context.userId,
        organization_id: data.organizationId,
        name: data.name,
        token_hash: await sha256(token),
      })
      .select("id")
      .single();
    if (r.error) throw new Error("Pool creation failed; the name may already be in use");
    return { id: r.data.id, token };
  });

export const updatePool = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((v: unknown) =>
    z
      .object({
        id: z.string().uuid(),
        enabled: z.boolean().optional(),
        // A new token replaces the old one at once; agents with the old token stop claiming.
        rotate: z.boolean().optional(),
      })
      .parse(v),
  )
  .handler(async ({ context, data }) => {
    // The caller must own the pool, read through their own RLS client first.
    const p = await context.supabase
      .from("connect_pools")
      .select("id,owner_id")
      .eq("id", data.id)
      .maybeSingle();
    if (!p.data || p.data.owner_id !== context.userId) throw new Error("Pool owner required");
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    const token = data.rotate ? poolToken() : null;
    const r = await supabaseAdmin
      .from("connect_pools")
      .update({
        ...(data.enabled === undefined ? {} : { enabled: data.enabled }),
        ...(token ? { token_hash: await sha256(token) } : {}),
      })
      .eq("id", data.id);
    if (r.error) throw new Error("Pool update failed");
    return { token };
  });

export const assignConnectionPool = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((v: unknown) =>
    z
      .object({
        connectionId: z.string().uuid(),
        poolId: z.string().uuid().nullable().optional(),
        // Live tasks at once for this connection, to protect its source (null = no limit).
        maxParallelTasks: z.number().int().min(1).max(64).nullable().optional(),
      })
      .parse(v),
  )
  .handler(async ({ context, data }) => {
    const c = await context.supabase
      .from("connect_connections")
      .select("id,owner_id")
      .eq("id", data.connectionId)
      .maybeSingle();
    if (!c.data || c.data.owner_id !== context.userId) throw new Error("Connection owner required");
    if (data.poolId) {
      // The pool must be visible to the caller; migration 0108's trigger also checks that it is in
      // the connection's workspace and that no other pool connection uses the same credential file.
      const p = await context.supabase
        .from("connect_pools")
        .select("id")
        .eq("id", data.poolId)
        .maybeSingle();
      if (!p.data) throw new Error("Pool not found");
    }
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    const r = await supabaseAdmin
      .from("connect_connections")
      .update({
        ...(data.poolId === undefined ? {} : { pool_id: data.poolId }),
        ...(data.maxParallelTasks === undefined
          ? {}
          : { max_parallel_tasks: data.maxParallelTasks }),
      })
      .eq("id", data.connectionId);
    if (r.error)
      throw new Error(
        /credential file|workspace/.test(r.error.message)
          ? r.error.message
          : "Connection update failed",
      );
    return { saved: true };
  });
