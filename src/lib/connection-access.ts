/** Shared checks for connection server functions: role, tenant-scoped read, then the secret load. */
export type Ctx = { supabase: any; userId: string }; // eslint-disable-line @typescript-eslint/no-explicit-any

export async function requireRole(ctx: Ctx, roles: string[]) {
  for (const r of roles) {
    const { data } = await ctx.supabase.rpc("has_role", { _user_id: ctx.userId, _role: r });
    if (data) return;
  }
  throw new Error(`Requires role: ${roles.join(" or ")}`);
}

/** Tenant check through the caller's own RLS view before any privileged read. */
export async function authorizeConnection(ctx: Ctx, id: string, manage = false) {
  const { data } = await ctx.supabase
    .from("connections")
    .select("id, created_by")
    .eq("id", id)
    .maybeSingle();
  if (!data) throw new Error("Connection not found");
  if (manage && data.created_by !== ctx.userId) {
    const { data: isAdmin } = await ctx.supabase.rpc("has_role", {
      _user_id: ctx.userId,
      _role: "admin",
    });
    if (!isAdmin) throw new Error("Only the connection owner or an admin can change it");
  }
}

export async function loadConnection(id: string, ctx: Ctx) {
  await authorizeConnection(ctx, id);
  const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
  const { data: c, error } = await supabaseAdmin
    .from("connections")
    .select("*")
    .eq("id", id)
    .single();
  if (error || !c) throw new Error("Connection not found");
  const { data: s } = await supabaseAdmin
    .from("connection_secrets")
    .select("secrets")
    .eq("connection_id", id)
    .maybeSingle();
  const all = {
    ...(c.config as Record<string, string>),
    ...((s?.secrets ?? {}) as Record<string, string>),
  };
  return { c, all, admin: supabaseAdmin };
}

export const base = (u: string) => u.replace(/\/+$/, "");
export function livyHeaders(all: Record<string, string>) {
  const h: Record<string, string> = {
    "Content-Type": "application/json",
    "X-Requested-By": "meridian",
  };
  if (all["username"])
    h["Authorization"] = "Basic " + btoa(`${all["username"]}:${all["password"] ?? ""}`);
  return h;
}
export async function fetchT(url: string, init: RequestInit = {}, ms = 10000) {
  const { safeOutboundFetch } = await import("./outbound.server");
  return safeOutboundFetch(url, init, ms);
}
