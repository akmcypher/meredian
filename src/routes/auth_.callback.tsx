import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { useEffect, useRef, useState } from "react";
import { supabase } from "@/integrations/supabase/client";
import { ensureWorkspace } from "@/lib/workspace";
import { authErrorMessage } from "@/lib/auth-input";
import { Button } from "@/components/ui/button";
export const Route = createFileRoute("/auth_/callback")({ ssr: false, component: AuthCallback });
async function completeCallback() {
  const query = new URLSearchParams(window.location.search),
    fragment = new URLSearchParams(window.location.hash.slice(1));
  const code = query.get("code"),
    hash = query.get("token_hash"),
    type = query.get("type"),
    recovery = query.get("flow") === "recovery" || type === "recovery";
  const failed = query.has("error") || fragment.has("error");
  window.history.replaceState(window.history.state, "", "/auth/callback");
  if (failed) throw new Error("This link is invalid or expired. Request a new email.");
  let result;
  if (code) result = await supabase.auth.exchangeCodeForSession(code);
  else if (hash && (type === "email" || type === "signup" || type === "recovery"))
    result = await supabase.auth.verifyOtp({ token_hash: hash, type });
  else throw new Error("This link is invalid or expired. Request a new email.");
  if (result.error) throw new Error(authErrorMessage(result.error));
  const current = await supabase.auth.getUser();
  if (current.error || !current.data.user?.email_confirmed_at)
    throw new Error("Verify your email before continuing.");
  return { recovery, workspace: recovery ? null : await ensureWorkspace() };
}
function AuthCallback() {
  const navigate = useNavigate(),
    task = useRef<ReturnType<typeof completeCallback> | null>(null),
    [error, setError] = useState("");
  useEffect(() => {
    let active = true;
    task.current ??= completeCallback();
    void task.current.then(
      (result) => {
        if (active)
          void navigate({
            to: result.recovery
              ? "/account/recovery"
              : result.workspace?.onboardingCompleted
                ? "/"
                : "/onboarding",
            replace: true,
          });
      },
      (reason) => {
        if (active)
          setError(
            reason instanceof Error
              ? reason.message
              : "This link could not be verified. Request a new email.",
          );
      },
    );
    return () => {
      active = false;
    };
  }, [navigate]);
  return (
    <section className="mx-auto my-10 max-w-md border bg-card p-6">
      <div className="label-mono text-primary">meridian</div>
      <h1 className="mt-2 text-2xl font-bold">
        {error ? "Verification needs attention" : "Verifying your email"}
      </h1>
      <p className="mt-4" role={error ? "alert" : "status"}>
        {error || "Confirming your account and preparing your workspace…"}
      </p>
      {error && (
        <Link to="/auth">
          <Button className="mt-5">Return to sign in</Button>
        </Link>
      )}
    </section>
  );
}
