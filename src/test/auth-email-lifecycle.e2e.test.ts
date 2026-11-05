// @vitest-environment node
// Real local Auth + Mailpit, including the same PKCE exchanges as the browser callback.
import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import { afterAll, describe, expect, it } from "vitest";
const url = process.env["SUPABASE_URL"],
  key = process.env["SUPABASE_PUBLISHABLE_KEY"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"],
  mailpit = process.env["MAILPIT_URL"];
const enabled = !!url && !!key && !!service && !!mailpit && process.env["RUN_TENANT_E2E"] !== "0";
const options = {
  auth: { flowType: "pkce" as const, persistSession: false, autoRefreshToken: false },
};
const ids: string[] = [];
async function messages(email: string) {
  const r = await fetch(`${mailpit}/api/v1/search?query=${encodeURIComponent(`to:${email}`)}`);
  if (!r.ok) throw new Error("Local Mailpit search unavailable");
  return (await r.json()).messages as { ID: string }[];
}
async function emailLink(email: string, previous: Set<string> = new Set()) {
  for (let attempt = 0; attempt < 50; attempt++) {
    const found = (await messages(email)).find((m) => !previous.has(m.ID));
    if (found) {
      const r = await fetch(`${mailpit}/api/v1/message/${found.ID}`);
      const html = (await r.json()).HTML as string;
      const href = html.match(/href="([^"]+)"/)?.[1]?.replaceAll("&amp;", "&");
      if (!href) throw new Error("Auth email has no verification link");
      const link = new URL(href);
      if (!["127.0.0.1", "localhost", "[::1]"].includes(link.hostname))
        throw new Error("Auth test refuses non-local email links");
      return link.href;
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error("Auth email was not delivered to local Mailpit");
}
async function exchange(client: Pick<SupabaseClient, "auth">, link: string) {
  const r = await fetch(link, { redirect: "manual" });
  expect(r.status).toBe(303);
  const location = new URL(r.headers.get("location")!);
  expect(location.searchParams.has("error")).toBe(false);
  const code = location.searchParams.get("code");
  expect(Boolean(code)).toBe(true);
  const result = await client.auth.exchangeCodeForSession(code!);
  expect(result.error).toBeNull();
  expect(result.data.user?.email_confirmed_at).toBeTruthy();
}
afterAll(async () => {
  if (!enabled) return;
  const db = createClient(url!, service!, options);
  for (const id of ids) {
    await db.from("organizations").delete().eq("created_by", id);
    await db.auth.admin.deleteUser(id);
  }
});
describe.skipIf(!enabled)("actual email registration and recovery", { timeout: 60000 }, () => {
  it("enforces confirmation, provisions once, refreshes and resets through delivered email", async () => {
    const db = createClient(url!, service!, options),
      client = createClient(url!, key!, options);
    const settings = await (
      await fetch(url + "/auth/v1/settings", { headers: { apikey: key! } })
    ).json();
    expect(settings.disable_signup).toBe(false);
    expect(settings.mailer_autoconfirm).toBe(false);
    const email = `public-lifecycle-${crypto.randomUUID()}@example.test`,
      password = `Local-A9-${crypto.randomUUID()}`;
    const redirectTo = (process.env["APP_URL"] ?? "http://localhost:8080") + "/auth/callback";
    const signup = await client.auth.signUp({
      email,
      password,
      options: {
        emailRedirectTo: redirectTo,
        data: { full_name: "Public developer", role: "admin" },
      },
    });
    expect(signup.error).toBeNull();
    expect(signup.data.session).toBeNull();
    const id = signup.data.user!.id;
    ids.push(id);
    expect((await db.from("org_members").select("org_id").eq("user_id", id)).data).toEqual([]);
    expect((await client.auth.signInWithPassword({ email, password })).error?.code).toBe(
      "email_not_confirmed",
    );
    const link = await emailLink(email);
    await exchange(client, link);
    const workspace = await client.rpc("ensure_workspace");
    expect(workspace.error).toBeNull();
    expect(workspace.data.workspaceRole).toBe("owner");
    expect((await db.from("org_members").select("org_id").eq("user_id", id)).data).toHaveLength(1);
    expect((await db.from("user_roles").select("role").eq("user_id", id)).data).toEqual([]);
    expect((await client.auth.refreshSession()).error).toBeNull();
    expect((await client.rpc("ensure_workspace")).data.workspaceId).toBe(
      workspace.data.workspaceId,
    );
    const replay = await fetch(link, { redirect: "manual" });
    expect(new URL(replay.headers.get("location")!).searchParams.has("error")).toBe(true);
    const duplicate = createClient(url!, key!, options);
    await duplicate.auth.signUp({ email, password });
    expect((await db.from("profiles").select("id").eq("email", email)).data).toHaveLength(1);
    expect((await db.from("org_members").select("org_id").eq("user_id", id)).data).toHaveLength(1);
    expect((await client.auth.signOut()).error).toBeNull();
    expect((await client.rpc("ensure_workspace")).error).toBeTruthy();
    const previous = new Set((await messages(email)).map((m) => m.ID));
    expect(
      (
        await client.auth.resetPasswordForEmail(email, {
          redirectTo: redirectTo + "?flow=recovery",
        })
      ).error,
    ).toBeNull();
    await exchange(client, await emailLink(email, previous));
    const nextPassword = password + "-New";
    expect((await client.auth.updateUser({ password: nextPassword })).error).toBeNull();
    expect((await client.auth.signOut({ scope: "global" })).error).toBeNull();
    expect((await client.auth.signInWithPassword({ email, password })).error?.code).toBe(
      "invalid_credentials",
    );
    expect(
      (await client.auth.signInWithPassword({ email, password: nextPassword })).error,
    ).toBeNull();
    expect((await client.rpc("ensure_workspace")).data.workspaceId).toBe(
      workspace.data.workspaceId,
    );
  });
  it("rejects invalid email and weak passwords at the actual Auth service", async () => {
    const client = createClient(url!, key!, options);
    expect(
      (await client.auth.signUp({ email: "invalid", password: "Local-A9-123456" })).error,
    ).toBeTruthy();
    expect(
      (
        await client.auth.signUp({
          email: `weak-${crypto.randomUUID()}@example.test`,
          password: "weak",
        })
      ).error,
    ).toBeTruthy();
  });
});
