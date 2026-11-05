// @vitest-environment node
// Real delivery through the SMTP email provider into the local Mailpit server that ships with
// the Supabase CLI stack (config.toml [inbucket] smtp_port). scripts/e2e-local.mjs sets
// MAILPIT_URL / SMTP_* only when Mailpit answers.
import { createClient } from "@supabase/supabase-js";
import { afterAll, describe, expect, it, vi } from "vitest";

const url = process.env["SUPABASE_URL"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"],
  mailpit = process.env["MAILPIT_URL"],
  smtpPort = process.env["MAILPIT_SMTP_PORT"];

type Found = { ID: string; Subject: string; To: { Address: string }[]; Cc: unknown[] | null };
async function inbox(address: string): Promise<Found[]> {
  for (let i = 0; i < 50; i++) {
    const r = await fetch(`${mailpit}/api/v1/search?query=${encodeURIComponent(`to:${address}`)}`);
    const found = ((await r.json()) as { messages: Found[] }).messages;
    if (found.length) return found;
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  return [];
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let db: any;
let uid = "";
afterAll(async () => {
  vi.unstubAllEnvs();
  if (uid) await db.auth.admin.deleteUser(uid);
});

describe.skipIf(!url || !service || !mailpit || !smtpPort)(
  "email alerts through the SMTP provider",
  { timeout: 60000 },
  () => {
    it("delivers a job-failure alert to the owner's configured address only", async () => {
      vi.stubEnv("EMAIL_PROVIDER", "smtp");
      vi.stubEnv("SMTP_HOST", "127.0.0.1");
      vi.stubEnv("SMTP_PORT", smtpPort!);
      vi.stubEnv("SMTP_SECURITY", "none");
      vi.stubEnv("EMAIL_FROM", "alerts@meridian.test");
      db = createClient(url!, service!, { auth: { persistSession: false } });
      const u = await db.auth.admin.createUser({
        email: `alert-owner-${crypto.randomUUID()}@example.test`,
        password: `P-${crypto.randomUUID()}`,
        email_confirm: true,
      });
      if (u.error) throw u.error;
      uid = u.data.user.id;
      const recipient = `alerts-${crypto.randomUUID()}@example.test`;
      const s = await db
        .from("alert_settings")
        .upsert({ user_id: uid, alert_email: recipient, notify_job_failures: true });
      if (s.error) throw s.error;

      const { fanOut } = await import("@/lib/alerts.server");
      const result = await fanOut("job_failed", "Nightly load failed", "Step 3 timed out", uid);
      expect(result).toEqual({ sent: 1 });

      const messages = await inbox(recipient);
      expect(messages).toHaveLength(1);
      expect(messages[0]!.Subject).toBe("Meridian: a scheduled job failed");
      expect(messages[0]!.To.map((t) => t.Address)).toEqual([recipient]);
      expect(messages[0]!.Cc ?? []).toEqual([]);
      const full = await (await fetch(`${mailpit}/api/v1/message/${messages[0]!.ID}`)).json();
      expect(full.Text).toContain("Nightly load failed");
      expect(full.From.Address).toBe("alerts@meridian.test");
    });

    it("refuses to send when no provider is configured", async () => {
      vi.stubEnv("EMAIL_PROVIDER", "");
      const { sendEmail } = await import("@/lib/alerts.server");
      await expect(sendEmail("me@example.com", "Subject", "Body")).rejects.toThrow(
        /not configured/,
      );
    });
  },
);
