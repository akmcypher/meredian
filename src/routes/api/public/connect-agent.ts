import { createFileRoute } from "@tanstack/react-router";
export const Route = createFileRoute("/api/public/connect-agent")({
  server: {
    handlers: {
      POST: async ({ request }) => {
        const { handleConnectAgent } = await import("@/lib/connect-agent.server");
        return handleConnectAgent(request);
      },
    },
  },
});
