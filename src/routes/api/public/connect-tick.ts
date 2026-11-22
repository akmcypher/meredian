import { createFileRoute } from "@tanstack/react-router";
import { authenticateCronRequest } from "@/integrations/supabase/cron-auth";

/** Community ingestion scheduler: queues durable work without warehouse dependencies. */
export const Route = createFileRoute("/api/public/connect-tick")({
  server: {
    handlers: {
      POST: async ({ request }) => {
        const authFailure = await authenticateCronRequest(request);
        if (authFailure) return authFailure;
        const { scheduleConnectRuns } = await import("@/lib/connect-scheduler.server");
        try {
          const { queued } = await scheduleConnectRuns();
          return Response.json({ connectQueued: queued });
        } catch {
          return Response.json({ error: "Unable to schedule Connect runs" }, { status: 503 });
        }
      },
    },
  },
});
