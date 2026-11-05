import { createServerFn } from "@tanstack/react-start";
import { z } from "zod";
import { requireSupabaseAuth } from "@/integrations/supabase/auth-middleware";
import { authorizeAlertTarget } from "./alert-authorization";

/** Fan out an alert email to every user whose settings subscribe to this kind of event. */
export const notifyEvent = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) =>
    z
      .object({
        kind: z.enum(["job_failed", "check_failed", "approval_pending", "approval_decided"]),
        title: z.string().min(1).max(200),
        detail: z.string().max(2000).default(""),
        targetUserId: z.string().uuid().optional(),
        requestId: z.string().uuid().optional(),
      })
      .parse(d),
  )
  .handler(async ({ data, context }) => {
    let request;
    if (data.kind === "approval_pending" || data.kind === "approval_decided") {
      if (!data.requestId) throw new Error("Approval request is required");
      const row = await context.supabase
        .from("change_requests")
        .select("submitted_by,reviewed_by,status,title,review_note")
        .eq("id", data.requestId)
        .maybeSingle();
      if (row.error || !row.data) throw new Error("Approval request not found");
      request = row.data;
    }
    const target = authorizeAlertTarget(data.kind, context.userId, data.targetUserId, request);
    const { fanOut } = await import("./alerts.server");
    const title = request
      ? `"${request.title}" ${request.status === "pending" ? "needs approval" : `was ${request.status}`}`
      : data.title;
    return fanOut(data.kind, title, request ? (request.review_note ?? "") : data.detail, target);
  });

/** Send a test alert to the caller's own configured address. */
export const sendTestAlert = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .handler(async ({ context }) => {
    const { data: s } = await context.supabase
      .from("alert_settings")
      .select("*")
      .eq("user_id", context.userId)
      .maybeSingle();
    const { data: p } = await context.supabase
      .from("profiles")
      .select("email")
      .eq("id", context.userId)
      .maybeSingle();
    const to = (s?.alert_email || p?.email || "").trim();
    if (!to) throw new Error("Set an alert email address first");
    const { sendEmail } = await import("./alerts.server");
    await sendEmail(
      to,
      "Meridian: test alert",
      "This is a test alert from Meridian. If you can read this, email alerts are working.\n\n— Meridian alerts",
    );
    return { sent: 1, to };
  });
