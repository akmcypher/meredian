// Vite config for the TanStack Start app: Tailwind, path aliases, SSR via Nitro on build.
import { fileURLToPath } from "node:url";
import { defineConfig, type PluginOption, type UserConfig } from "vite";
import tailwindcss from "@tailwindcss/vite";
import tsConfigPaths from "vite-tsconfig-paths";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import viteReact from "@vitejs/plugin-react";

const src = (path: string) => fileURLToPath(new URL(`./src/${path}`, import.meta.url));
// @supabase/* optionally reference react-native; stub it out for web builds.
const emptyModule = src("lib/empty-module.ts");

export default defineConfig(async ({ command }): Promise<UserConfig> => {
  const plugins: PluginOption[] = [
    tailwindcss(),
    tsConfigPaths({ projects: ["./tsconfig.json"] }),
    tanstackStart({
      importProtection: {
        behavior: "error",
        client: { files: ["**/server/**"], specifiers: ["server-only"] },
      },
      // Redirect TanStack Start's bundled server entry to src/server.ts (our SSR error wrapper).
      server: { entry: "server" },
    }),
  ];
  if (command === "build") {
    const { nitro } = await import("nitro/vite");
    // Hosting-neutral: a plain Node server by default (`bun run start`, any VM or container);
    // set NITRO_PRESET (e.g. cloudflare-module, vercel, netlify) to target a platform instead.
    plugins.push(nitro({ defaultPreset: "node-server" }));
  }
  plugins.push(viteReact());

  return {
    plugins,
    css: { transformer: "lightningcss" },
    resolve: {
      alias: {
        "@": src(""),
        "react-native": emptyModule,
        "react-native-fs": emptyModule,
        "react-native-fetch-blob": emptyModule,
      },
      dedupe: [
        "react",
        "react-dom",
        "react/jsx-runtime",
        "react/jsx-dev-runtime",
        "@tanstack/react-query",
        "@tanstack/query-core",
      ],
    },
    optimizeDeps: {
      include: [
        "react",
        "react-dom",
        "react-dom/client",
        "react/jsx-runtime",
        "react/jsx-dev-runtime",
      ],
      exclude: ["react-native", "react-native-fs", "react-native-fetch-blob"],
      ignoreOutdatedRequests: true,
    },
    server: { host: "::", port: 8080, watch: { ignored: ["**/.venv/**"] } },
  };
});
