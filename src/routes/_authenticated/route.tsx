import { createFileRoute, Outlet, redirect } from "@tanstack/react-router";
import { ensureWorkspace } from "@/lib/workspace";
import { supabase } from "@/integrations/supabase/client";

export const Route = createFileRoute("/_authenticated")({
  ssr: false,
  beforeLoad: async () => {
    const { data } = await supabase.auth.getSession();
    if (!data.session?.user?.email_confirmed_at) throw redirect({ to: "/auth" });
    const workspace = await ensureWorkspace();
    if (!workspace.onboardingCompleted) throw redirect({ to: "/onboarding" });
  },
  component: () => <Outlet />,
});
