/** Server-only email helpers shared by server functions and the scheduler route. */
import { requireSingleEmailAddress } from "./email-address";
import { getEmailProvider, type EmailMessage } from "./email-provider.server";

/** Validates one alert message: exactly one recipient and a single-line subject. */
export function buildEmail(to: string, subject: string, body: string): EmailMessage {
  const recipient = requireSingleEmailAddress(to);
  if (/[\r\n]/.test(subject)) throw new Error("Email subject must be a single line");
  return { to: recipient, subject, text: body };
}

/** Sends through the configured provider (EMAIL_PROVIDER); fails loudly when none is set. */
export async function sendEmail(to: string, subject: string, body: string): Promise<void> {
  const message = buildEmail(to, subject, body);
  const provider = getEmailProvider();
  if (!provider)
    throw new Error(
      "Email alerts are not configured: set EMAIL_PROVIDER=smtp with SMTP_HOST and EMAIL_FROM.",
    );
  await provider.send(message);
}

export type AlertKind = "job_failed" | "check_failed" | "approval_pending" | "approval_decided";

const SUBJECTS: Record<AlertKind, string> = {
  job_failed: "Meridian: a scheduled job failed",
  check_failed: "Meridian: a data quality check is failing",
  approval_pending: "Meridian: a change is waiting for your approval",
  approval_decided: "Meridian: your change was reviewed",
};

export async function recipients(kind: AlertKind, targetUserId?: string) {
  // No unscoped system fan-out: each event must identify its owning user.
  if (!targetUserId) return [];
  const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
  const { data: settings } = await supabaseAdmin.from("alert_settings").select("*");
  const { data: profiles } = await supabaseAdmin.from("profiles").select("id, email");
  const emailOf = new Map((profiles ?? []).map((p) => [p.id, p.email as string]));
  let rows = (settings ?? []).filter((s) =>
    kind === "job_failed" || kind === "check_failed" ? s.notify_job_failures : s.notify_approvals,
  );
  if (kind === "approval_pending") {
    const { data: roles } = await supabaseAdmin
      .from("user_roles")
      .select("user_id, role")
      .in("role", ["steward", "admin"]);
    const stewards = new Set((roles ?? []).map((r) => r.user_id));
    const { data: memberships } = await supabaseAdmin
      .from("org_members")
      .select("org_id")
      .eq("user_id", targetUserId);
    const orgIds = (memberships ?? []).map((m) => m.org_id);
    const peers = orgIds.length
      ? await supabaseAdmin.from("org_members").select("user_id").in("org_id", orgIds)
      : { data: [] };
    const permitted = new Set((peers.data ?? []).map((m) => m.user_id));
    rows = rows.filter(
      (s) => stewards.has(s.user_id) && permitted.has(s.user_id) && s.user_id !== targetUserId,
    );
  }
  if (targetUserId && kind !== "approval_pending")
    rows = rows.filter((s) => s.user_id === targetUserId);
  const seen = new Set<string>();
  return rows
    .map((s) => (s.alert_email || emailOf.get(s.user_id) || "").trim())
    .filter((e) => e && !seen.has(e) && seen.add(e));
}

export async function fanOut(
  kind: AlertKind,
  title: string,
  detail: string,
  targetUserId?: string,
) {
  const to = await recipients(kind, targetUserId);
  let sent = 0;
  for (const addr of to) {
    try {
      await sendEmail(addr, SUBJECTS[kind], `${title}\n\n${detail}\n\n— Meridian alerts`);
      sent++;
    } catch (e) {
      console.error(`Alert to recipient failed: ${(e as Error).message}`);
    }
  }
  return { sent };
}
