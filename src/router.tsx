import { QueryClient } from "@tanstack/react-query";
import { createRouter } from "@tanstack/react-router";
import { routeTree } from "./routeTree.gen";
import { currentNonce } from "./lib/security-headers";

export const getRouter = () => {
  const queryClient = new QueryClient();
  const nonce = currentNonce();

  const router = createRouter({
    routeTree,
    context: { queryClient },
    scrollRestoration: true,
    defaultPreloadStaleTime: 0,
    // SSR scripts carry the per-request CSP nonce (src/server.ts).
    ...(nonce ? { ssr: { nonce } } : {}),
  });

  return router;
};
