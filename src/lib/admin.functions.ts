import { createServerFn } from "@tanstack/react-start";
import { z } from "zod";
import { requireSupabaseAuth } from "@/integrations/supabase/auth-middleware";
import type { SupabaseClient } from "@supabase/supabase-js";
import type { Database } from "@/integrations/supabase/types";

export type Ctx = { supabase: SupabaseClient<Database>; userId: string };
const ROLE = z.enum(["admin", "steward", "engineer", "analyst"]);

/** Every admin operation re-checks the caller's role in the database; the UI gate is cosmetic. */
export async function requireAdmin(ctx: Ctx) {
  const { data, error } = await ctx.supabase.rpc("has_role", {
    _user_id: ctx.userId,
    _role: "admin",
  });
  if (error || !data) throw new Error("Admins only");
}

async function logAdmin(
  ctx: Ctx,
  action: string,
  target: string,
  detail: Record<string, unknown> = {},
) {
  const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
  await supabaseAdmin
    .from("admin_audit")
    .insert({ actor: ctx.userId, action, target, detail: detail as never });
}

export const adminOverview = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .handler(async ({ context }) => {
    await requireAdmin(context);
    const { supabaseAdmin: db } = await import("@/integrations/supabase/client.server");
    const count = async (t: "organizations" | "connections" | "ingest_sources") => {
      const { count: n } = await db.from(t).select("*", { count: "exact", head: true });
      return n ?? 0;
    };
    const { data: users } = await db.auth.admin.listUsers({ perPage: 1000 });
    const { data: tables } = await db.from("user_tables").select("row_count, rows");
    const storageBytes = (tables ?? []).reduce(
      (a, t) => a + JSON.stringify(t.rows ?? []).length,
      0,
    );
    return {
      organizations: await count("organizations"),
      users: users?.users.length ?? 0,
      tables: tables?.length ?? 0,
      rows: (tables ?? []).reduce((a, t) => a + (t.row_count ?? 0), 0),
      storageMb: +(storageBytes / 1048576).toFixed(2),
      connections: await count("connections"),
      ingestSources: await count("ingest_sources"),
    };
  });

export const adminListUsers = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .handler(async ({ context }) => {
    await requireAdmin(context);
    const { supabaseAdmin: db } = await import("@/integrations/supabase/client.server");
    const { data, error } = await db.auth.admin.listUsers({ perPage: 1000 });
    if (error) throw new Error(error.message);
    const { data: roles } = await db.from("user_roles").select("user_id, role");
    const { data: members } = await db.from("org_members").select("user_id, org_id");
    return data.users.map((u) => ({
      id: u.id,
      email: u.email ?? "",
      createdAt: u.created_at,
      lastSignIn: u.last_sign_in_at ?? null,
      provider: (u.app_metadata?.provider as string) ?? "email",
      banned:
        !!(u as { banned_until?: string | null }).banned_until &&
        new Date((u as { banned_until?: string }).banned_until!) > new Date(),
      roles: (roles ?? []).filter((r) => r.user_id === u.id).map((r) => r.role as string),
      orgs: (members ?? []).filter((m) => m.user_id === u.id).map((m) => m.org_id),
    }));
  });

export const adminSetRole = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) =>
    z.object({ userId: z.string().uuid(), role: ROLE, grant: z.boolean() }).parse(d),
  )
  .handler(async ({ data, context }) => {
    await requireAdmin(context);
    if (!data.grant && data.role === "admin" && data.userId === context.userId)
      throw new Error("You can't remove your own admin role");
    // Runs as the caller so RLS + the audit trigger see who did it.
    const q = data.grant
      ? context.supabase.from("user_roles").insert({ user_id: data.userId, role: data.role })
      : context.supabase
          .from("user_roles")
          .delete()
          .eq("user_id", data.userId)
          .eq("role", data.role);
    const { error } = await q;
    if (error && !String(error.message).includes("duplicate")) throw new Error(error.message);
    return { ok: true };
  });

export const adminSetUserActive = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) =>
    z.object({ userId: z.string().uuid(), active: z.boolean() }).parse(d),
  )
  .handler(async ({ data, context }) => {
    await requireAdmin(context);
    if (data.userId === context.userId) throw new Error("You can't deactivate yourself");
    const { supabaseAdmin: db } = await import("@/integrations/supabase/client.server");
    const { error } = await db.auth.admin.updateUserById(data.userId, {
      ban_duration: data.active ? "none" : "876000h",
    });
    if (error) throw new Error(error.message);
    await logAdmin(context, data.active ? "user.reactivate" : "user.deactivate", data.userId);
    return { ok: true };
  });

export const adminInviteUser = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) =>
    z.object({ email: z.string().email().max(255), role: ROLE }).parse(d),
  )
  .handler(async ({ data, context }) => {
    await requireAdmin(context);
    const { supabaseAdmin: db } = await import("@/integrations/supabase/client.server");
    const password = crypto.randomUUID() + "Aa1!";
    const { data: created, error } = await db.auth.admin.createUser({
      email: data.email,
      password,
      email_confirm: true,
    });
    if (error) throw new Error(error.message);
    await context.supabase.from("user_roles").insert({ user_id: created.user.id, role: data.role });
    await logAdmin(context, "user.create", data.email, { role: data.role });
    return {
      ok: true,
      note: "Account created. They sign in with Google using this email, or use 'forgot password'.",
    };
  });

export const adminSetOrgMember = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) =>
    z.object({ orgId: z.string().uuid(), userId: z.string().uuid(), member: z.boolean() }).parse(d),
  )
  .handler(async ({ data, context }) => {
    await requireAdmin(context);
    const q = data.member
      ? context.supabase.from("org_members").insert({ org_id: data.orgId, user_id: data.userId })
      : context.supabase
          .from("org_members")
          .delete()
          .eq("org_id", data.orgId)
          .eq("user_id", data.userId);
    const { error } = await q;
    if (error && !String(error.message).includes("duplicate")) throw new Error(error.message);
    return { ok: true };
  });

export const adminVerifyAudit = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .handler(async ({ context }) => {
    await requireAdmin(context);
    const { data, error } = await context.supabase.rpc("verify_admin_audit");
    if (error) throw new Error(error.message);
    return (data?.[0] ?? { ok: true, checked: 0, broken_at: null }) as {
      ok: boolean;
      checked: number;
      broken_at: number | null;
    };
  });

/** Applies a signed enterprise licence to its organization (server-side entitlements only). */
export const adminApplyLicence = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) => z.object({ licence: z.string().min(10).max(20000) }).parse(d))
  .handler(async ({ data, context }) => {
    await requireAdmin(context);
    const publicKey = process.env["MERIDIAN_LICENCE_PUBLIC_KEY"];
    if (!publicKey) throw new Error("Licence verification is not configured on this server");
    const { verifyLicence } = await import("./licence.server");
    const licence = await verifyLicence(data.licence, publicKey);
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    const { data: count, error } = await supabaseAdmin.rpc("apply_licence_entitlements", {
      _actor: context.userId,
      _licence: licence.licenceId,
      _org: licence.organizationId,
      _features: licence.features,
      _expires: licence.expiresAt as string,
    });
    if (error) throw new Error("Licence could not be applied");
    await logAdmin(context, "licence.apply", licence.organizationId, {
      licence: licence.licenceId,
      features: licence.features,
      expires_at: licence.expiresAt,
    });
    return { organizationId: licence.organizationId, features: licence.features, count };
  });
