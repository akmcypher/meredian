import { supabase } from "@/integrations/supabase/client";
import type { Json } from "@/integrations/supabase/types";

/** Append an entry to the audit log as the signed-in user. Never throws. */
export async function audit(
  action: string,
  target: string | null,
  details: Record<string, unknown> = {},
) {
  const { data } = await supabase.auth.getUser();
  if (!data.user) return;
  await supabase.from("audit_log").insert({
    actor_id: data.user.id,
    actor_email: data.user.email ?? null,
    action,
    target,
    details: details as NonNullable<Json>,
  });
}
