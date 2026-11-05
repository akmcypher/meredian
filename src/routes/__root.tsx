import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  Outlet,
  Link,
  createRootRouteWithContext,
  useRouter,
  HeadContent,
  Scripts,
  type ErrorComponentProps,
} from "@tanstack/react-router";
import { useEffect, useState, type ReactNode } from "react";
import { Menu, LogOut, X } from "lucide-react";
import * as DialogPrimitive from "@radix-ui/react-dialog";
import { Button } from "@/components/ui/button";
import { PlatformNavigation } from "@/components/PlatformNavigation";
import { activeNavigationSection } from "@/components/navigation-items";
import { useRouterState } from "@tanstack/react-router";

import appCss from "../styles.css?url";
import { MODULES } from "../lib/modules";
import { Toaster } from "../components/ui/sonner";
import { AuthProvider, useAuth } from "../hooks/use-auth";
import { supabase } from "../integrations/supabase/client";

function NotFoundComponent() {
  return (
    <div className="flex min-h-screen items-center justify-center bg-background px-4">
      <div className="max-w-md text-center">
        <h1 className="text-7xl font-bold text-foreground">404</h1>
        <h2 className="mt-4 text-xl font-semibold text-foreground">Page not found</h2>
        <p className="mt-2 text-sm text-muted-foreground">
          The page you're looking for doesn't exist or has been moved.
        </p>
        <div className="mt-6">
          <Link
            to="/"
            className="inline-flex items-center justify-center rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground transition-colors hover:bg-primary/90"
          >
            Go home
          </Link>
        </div>
      </div>
    </div>
  );
}

function ErrorComponent({ error, reset }: ErrorComponentProps) {
  console.error(error);
  const router = useRouter();

  return (
    <div className="flex min-h-screen items-center justify-center bg-background px-4">
      <div className="max-w-md text-center">
        <h1 className="text-xl font-semibold tracking-tight text-foreground">
          This page didn't load
        </h1>
        <p className="mt-2 text-sm text-muted-foreground">
          Something went wrong on our end. You can try refreshing or head back home.
        </p>
        <div className="mt-6 flex flex-wrap justify-center gap-2">
          <button
            onClick={() => {
              router.invalidate();
              reset();
            }}
            className="inline-flex items-center justify-center rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground transition-colors hover:bg-primary/90"
          >
            Try again
          </button>
          <a
            href="/"
            className="inline-flex items-center justify-center rounded-md border border-input bg-background px-4 py-2 text-sm font-medium text-foreground transition-colors hover:bg-accent"
          >
            Go home
          </a>
        </div>
      </div>
    </div>
  );
}

export const Route = createRootRouteWithContext<{ queryClient: QueryClient }>()({
  head: () => ({
    meta: [
      { charSet: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1" },
      { title: "Meridian — Data control plane" },
      {
        name: "description",
        content: "Bring data in, see where it landed, and trust what you publish.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
    links: [
      {
        rel: "stylesheet",
        href: appCss,
      },
      { rel: "icon", href: "/favicon.svg", type: "image/svg+xml" },
      { rel: "preconnect", href: "https://fonts.googleapis.com" },
      {
        rel: "stylesheet",
        href: "https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Space+Grotesk:wght@400;500;600;700&display=swap",
      },
    ],
  }),
  shellComponent: RootShell,
  component: RootComponent,
  notFoundComponent: NotFoundComponent,
  errorComponent: ErrorComponent,
});

function RootShell({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        {children}
        <Scripts />
      </body>
    </html>
  );
}

function RootComponent() {
  const { queryClient } = Route.useRouteContext();

  return (
    <QueryClientProvider client={queryClient}>
      {/* Required: nested routes render here. Removing <Outlet /> breaks all child routes. */}
      <AuthProvider>
        <IdentityWorkspace />
      </AuthProvider>
    </QueryClientProvider>
  );
}

function IdentityWorkspace() {
  const { user } = useAuth();
  const authTransition = useRouterState({
    select: (state) =>
      state.matches.some(
        (match) => match.routeId === "/auth_/callback" || match.routeId === "/account_/recovery",
      ),
  });
  // Exchanging a one-use code changes identity. Keep that callback mounted while
  // private module providers are reset for the newly authenticated account.
  if (authTransition) return <Outlet />;
  return <ModuleWorkspace key={user?.id ?? "signed-out"} />;
}

/** Installed modules wrap the workspace; the key above remounts them for every identity. */
function ModuleWorkspace() {
  const content = (
    <>
      <Shell />
      <Toaster />
      {MODULES.map((m) => m.Background && <m.Background key={m.id} />)}
    </>
  );
  return MODULES.reduceRight<ReactNode>(
    (children, m) => (m.Provider ? <m.Provider key={m.id}>{children}</m.Provider> : children),
    content,
  );
}

function SidebarBody({
  path,
  expanded,
  onToggle,
  onNavigate,
}: {
  path: string;
  expanded: Record<string, boolean>;
  onToggle: (id: string) => void;
  onNavigate?: (() => void) | undefined;
}) {
  return (
    <>
      <div className="shrink-0 px-5 py-6">
        <div className="text-lg font-bold">
          Meridian<span className="text-primary">.</span>
        </div>
        <div className="label-mono mt-1">data control plane</div>
      </div>
      <PlatformNavigation
        path={path}
        expanded={expanded}
        onToggle={onToggle}
        onNavigate={onNavigate}
      />
      <AccountBox />
    </>
  );
}

function Shell() {
  const [open, setOpen] = useState(false);
  const path = useRouterState({ select: (s) => s.location.pathname });
  const { user } = useAuth();
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  useEffect(() => {
    setOpen(false);
    const section = activeNavigationSection(path);
    if (section) setExpanded((previous) => ({ ...previous, [section]: true }));
  }, [path]);
  const toggle = (section: string) =>
    setExpanded((previous) => ({ ...previous, [section]: !previous[section] }));
  return (
    <div className="flex min-h-screen flex-col md:flex-row">
      <DialogPrimitive.Root open={open} onOpenChange={setOpen}>
        <header className="sticky top-0 z-40 flex items-center justify-between gap-3 bg-ink px-4 py-3 text-ink-foreground md:hidden">
          <div className="text-lg font-bold">
            Meridian<span className="text-primary">.</span>
          </div>
          <div className="flex items-center gap-1">
            {!user && (
              <Link
                to="/auth"
                className="rounded-sm bg-primary px-3 py-1.5 text-sm text-primary-foreground"
              >
                Sign in
              </Link>
            )}
            <DialogPrimitive.Trigger asChild>
              <Button
                variant="ghost"
                size="icon"
                aria-label="Open menu"
                className="text-ink-foreground hover:bg-ink-foreground/10 hover:text-ink-foreground"
              >
                <Menu className="size-5" />
              </Button>
            </DialogPrimitive.Trigger>
          </div>
        </header>
        <DialogPrimitive.Portal>
          <DialogPrimitive.Overlay className="fixed inset-0 z-50 bg-foreground/40 md:hidden" />
          <DialogPrimitive.Content
            aria-describedby={undefined}
            className="fixed inset-y-0 left-0 z-50 flex w-72 max-w-[calc(100vw-2rem)] flex-col bg-ink text-ink-foreground focus:outline-none md:hidden"
          >
            <DialogPrimitive.Title className="sr-only">Meridian navigation</DialogPrimitive.Title>
            <DialogPrimitive.Close asChild>
              <Button
                aria-label="Close menu"
                variant="ghost"
                size="icon"
                className="absolute right-2 top-4 text-ink-foreground hover:bg-ink-foreground/10 hover:text-ink-foreground"
              >
                <X className="size-4" />
              </Button>
            </DialogPrimitive.Close>
            <SidebarBody
              path={path}
              expanded={expanded}
              onToggle={toggle}
              onNavigate={() => setOpen(false)}
            />
          </DialogPrimitive.Content>
        </DialogPrimitive.Portal>
      </DialogPrimitive.Root>
      <aside
        aria-label="Desktop sidebar"
        className="sticky top-0 hidden h-dvh w-72 shrink-0 flex-col bg-ink text-ink-foreground md:flex"
      >
        <SidebarBody path={path} expanded={expanded} onToggle={toggle} />
      </aside>
      <main className="min-w-0 flex-1 px-4 py-6 md:px-8 md:py-8">
        <Outlet />
      </main>
    </div>
  );
}

function AccountBox() {
  const { user, roles, workspace } = useAuth();
  const { queryClient } = Route.useRouteContext();
  const router = useRouter();
  if (!user)
    return (
      <Link
        to="/auth"
        className="mx-3 mb-5 mt-auto rounded-sm bg-primary px-3 py-2 text-center text-sm text-primary-foreground"
      >
        Sign in
      </Link>
    );
  const out = async () => {
    await queryClient.cancelQueries();
    queryClient.clear();
    await supabase.auth.signOut();
    router.navigate({ to: "/auth", replace: true });
  };
  return (
    <div className="shrink-0 border-t border-ink-foreground/10 px-5 py-4">
      <div className="truncate text-xs">{user.email}</div>
      {workspace && (
        <div className="mt-1 text-xs">
          <span className="block truncate">{workspace.name}</span>
          <span className="text-ink-foreground/70">workspace {workspace.workspaceRole}</span>
        </div>
      )}
      <Link to="/account" className="mt-2 block text-xs underline">
        Your account
      </Link>
      <div className="label-mono mt-1">{roles.join(" · ")}</div>
      <Button
        variant="ghost"
        onClick={out}
        className="mt-2 h-8 justify-start px-0 text-xs text-ink-foreground/70 hover:bg-transparent hover:text-ink-foreground"
      >
        <LogOut className="h-3 w-3" />
        Sign out
      </Button>
    </div>
  );
}
