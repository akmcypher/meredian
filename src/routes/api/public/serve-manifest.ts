import { createFileRoute } from "@tanstack/react-router";
export const Route = createFileRoute("/api/public/serve-manifest")({
  server: {
    handlers: {
      POST: async ({ request }) => {
        const { handleServeManifest } = await import("@/lib/serve-manifest.server");
        return handleServeManifest(request);
      },
    },
  },
});
