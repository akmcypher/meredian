import { z } from "zod";

const scheduleResult = z.object({ queued: z.number().int().min(0).max(1000) });
type ScheduleRpc = () => PromiseLike<{ data: unknown; error: unknown }>;

/** Queue due runs transactionally; agents acquire their normal fenced leases later. */
export async function scheduleConnectRuns(rpc?: ScheduleRpc): Promise<{ queued: number }> {
  if (!rpc) {
    const { supabaseAdmin: db } = await import("@/integrations/supabase/client.server");
    rpc = () => db.rpc("connect_schedule_due");
  }
  const result = await rpc();
  if (result.error) throw new Error("Unable to schedule Connect runs");
  return scheduleResult.parse(result.data);
}
