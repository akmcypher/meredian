import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useEffect, useRef, useState } from "react";
import { supabase, loadSignupPolicy } from "@/integrations/supabase/client";
import { useAuth } from "@/hooks/use-auth";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { registrationInput, emailInput, authCallbackUrl, authErrorMessage } from "@/lib/auth-input";
import { MODULES } from "@/lib/modules";
const GOOGLE_ENABLED = import.meta.env["VITE_AUTH_GOOGLE_ENABLED"] === "true";
const HOME = MODULES.find((m) => m.home)?.home ?? "/";
const RESEND_KEY = "meridian-auth-resend-until";
export const Route = createFileRoute("/auth")({
  head: () => ({
    meta: [
      { title: "Sign in — Meridian" },
      {
        name: "description",
        content: "Create your verified Meridian workspace or sign in to your account.",
      },
    ],
  }),
  component: AuthPage,
});
function AuthPage() {
  const { user, loading, workspace, workspaceError, refreshWorkspace } = useAuth(),
    navigate = useNavigate();
  const [mode, setMode] = useState<"in" | "up" | "verify" | "recover">("in"),
    [email, setEmail] = useState(""),
    [password, setPassword] = useState(""),
    [confirmation, setConfirmation] = useState(""),
    [fullName, setFullName] = useState(""),
    [busy, setBusy] = useState(false),
    [signupEnabled, setSignupEnabled] = useState(false),
    [error, setError] = useState(""),
    [notice, setNotice] = useState(""),
    [resendAt, setResendAt] = useState(0),
    [now, setNow] = useState(Date.now());
  const pending = useRef(false);
  useEffect(() => {
    let active = true;
    loadSignupPolicy().then(
      (enabled) => {
        if (active) setSignupEnabled(enabled);
      },
      () => {
        if (active) setSignupEnabled(false);
      },
    );
    setResendAt(Number(sessionStorage.getItem(RESEND_KEY) ?? 0));
    return () => {
      active = false;
    };
  }, []);
  useEffect(() => {
    if (!resendAt) return;
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [resendAt]);
  useEffect(() => {
    if (user?.email_confirmed_at && workspace && !loading && mode !== "recover")
      void navigate({
        to: (workspace.onboardingCompleted ? HOME : "/onboarding") as "/",
        replace: true,
      });
  }, [user, workspace, loading, mode, navigate]);
  const cooldown = () => {
    const until = Date.now() + 60000;
    sessionStorage.setItem(RESEND_KEY, String(until));
    setResendAt(until);
    setNow(Date.now());
  };
  const changeMode = (next: typeof mode) => {
    setMode(next);
    setError("");
    setNotice("");
    setPassword("");
    setConfirmation("");
  };
  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (pending.current) return;
    setError("");
    setNotice("");
    const address = emailInput.safeParse(email);
    if (!address.success) {
      setError("Enter a valid email address");
      return;
    }
    let signup: ReturnType<typeof registrationInput.parse> | null = null;
    if (mode === "up") {
      if (!signupEnabled) return;
      const checked = registrationInput.safeParse({
        fullName,
        email: address.data,
        password,
        confirmation,
      });
      if (!checked.success) {
        setError(checked.error.issues[0]?.message ?? "Check the registration details");
        return;
      }
      signup = checked.data;
    }
    pending.current = true;
    setBusy(true);
    try {
      if (mode === "recover") {
        const r = await supabase.auth.resetPasswordForEmail(address.data, {
          redirectTo: authCallbackUrl(window.location.origin, true),
        });
        if (r.error) {
          setError(authErrorMessage(r.error));
          return;
        }
        cooldown();
        setNotice(
          "If an account exists for this address, check your email for a password reset link.",
        );
        return;
      }
      if (signup) {
        const r = await supabase.auth.signUp({
          email: signup.email,
          password: signup.password,
          options: {
            data: { full_name: signup.fullName },
            emailRedirectTo: authCallbackUrl(window.location.origin),
          },
        });
        if (r.error) {
          setError(authErrorMessage(r.error));
          return;
        }
        setPassword("");
        setConfirmation("");
        cooldown();
        setMode("verify");
        setNotice(
          "If this address can be registered, a verification email is on its way. Verify it before entering your workspace.",
        );
        return;
      }
      const r = await supabase.auth.signInWithPassword({ email: address.data, password });
      if (r.error) {
        if (r.error.code === "email_not_confirmed") setMode("verify");
        setError(authErrorMessage(r.error));
      }
    } catch {
      setError("We could not reach the authentication service. Please try again.");
    } finally {
      pending.current = false;
      setBusy(false);
    }
  };
  const resend = async () => {
    if (pending.current || Date.now() < resendAt) return;
    const address = emailInput.safeParse(email);
    if (!address.success) {
      setError("Enter your email on the sign-in form first.");
      return;
    }
    pending.current = true;
    setBusy(true);
    setError("");
    try {
      const r = await supabase.auth.resend({
        type: "signup",
        email: address.data,
        options: { emailRedirectTo: authCallbackUrl(window.location.origin) },
      });
      if (r.error) setError(authErrorMessage(r.error));
      else {
        cooldown();
        setNotice("If verification is pending, a new email is on its way.");
      }
    } catch {
      setError("We could not reach the authentication service. Please try again.");
    } finally {
      pending.current = false;
      setBusy(false);
    }
  };
  if (user?.email_confirmed_at && mode !== "recover")
    return (
      <div className="mx-auto mt-16 max-w-md border bg-card p-6">
        <h1 className="text-2xl font-bold">Preparing your workspace</h1>
        <p className="mt-3" role={workspaceError ? "alert" : "status"}>
          {workspaceError ?? "Checking your verified account and workspace…"}
        </p>
        {workspaceError && (
          <Button className="mt-4" onClick={() => void refreshWorkspace()}>
            Retry
          </Button>
        )}
      </div>
    );
  return (
    <div className="mx-auto my-10 w-full max-w-md border bg-card p-6">
      <div className="label-mono text-primary">meridian</div>
      <h1 className="mt-1 text-2xl font-bold">
        {mode === "up"
          ? "Create account"
          : mode === "verify"
            ? "Check your email"
            : mode === "recover"
              ? "Reset your password"
              : "Sign in"}
      </h1>
      <p className="mt-2 text-sm text-muted-foreground">
        {signupEnabled
          ? "Verify your email to receive your own workspace. Workspace ownership never grants platform-wide administration."
          : "Contact an administrator to create an account."}
      </p>
      {error && (
        <p id="auth-error" role="alert" className="mt-4 text-sm text-destructive">
          {error}
        </p>
      )}
      {notice && (
        <p role="status" className="mt-4 text-sm">
          {notice}
        </p>
      )}
      {mode === "verify" ? (
        <div className="mt-5 space-y-3">
          <p className="text-sm">
            Check the inbox for {email || "your email address"}. You can return here after
            verification.
          </p>
          <Button disabled={busy || now < resendAt} onClick={() => void resend()}>
            Resend verification email
          </Button>
          {now < resendAt && (
            <p className="text-xs text-muted-foreground" aria-live="polite">
              Try again in {Math.ceil((resendAt - now) / 1000)} seconds.
            </p>
          )}
          <Button variant="ghost" onClick={() => changeMode("in")}>
            Back to sign in
          </Button>
        </div>
      ) : (
        <>
          <form onSubmit={submit} className="mt-6 space-y-4">
            {mode === "up" && (
              <div>
                <Label htmlFor="full-name">Full name</Label>
                <Input
                  id="full-name"
                  disabled={loading || busy}
                  autoComplete="name"
                  required
                  maxLength={80}
                  value={fullName}
                  onChange={(e) => setFullName(e.target.value)}
                  aria-describedby={error ? "auth-error" : undefined}
                />
              </div>
            )}
            <div>
              <Label htmlFor="em">Email</Label>
              <Input
                id="em"
                disabled={loading || busy}
                type="email"
                autoComplete="email"
                required
                maxLength={254}
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                aria-describedby={error ? "auth-error" : undefined}
              />
            </div>
            {mode !== "recover" && (
              <div>
                <Label htmlFor="pw">Password</Label>
                <Input
                  id="pw"
                  disabled={loading || busy}
                  type="password"
                  autoComplete={mode === "up" ? "new-password" : "current-password"}
                  required
                  minLength={mode === "up" ? 12 : undefined}
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  aria-describedby={
                    mode === "up" ? "password-help" : error ? "auth-error" : undefined
                  }
                />
                {mode === "up" && (
                  <p id="password-help" className="mt-1 text-xs text-muted-foreground">
                    At least 12 characters, including uppercase, lowercase and a number.
                  </p>
                )}
              </div>
            )}
            {mode === "up" && (
              <div>
                <Label htmlFor="confirm-password">Confirm password</Label>
                <Input
                  id="confirm-password"
                  disabled={loading || busy}
                  type="password"
                  autoComplete="new-password"
                  required
                  value={confirmation}
                  onChange={(e) => setConfirmation(e.target.value)}
                  aria-describedby={error ? "auth-error" : undefined}
                />
              </div>
            )}
            <Button className="w-full" disabled={busy || loading}>
              {busy
                ? "Please wait…"
                : mode === "up"
                  ? "Create account"
                  : mode === "recover"
                    ? "Send reset email"
                    : "Sign in"}
            </Button>
          </form>
          {mode === "in" && (
            <>
              <button
                type="button"
                className="mt-3 text-sm underline"
                onClick={() => changeMode("recover")}
              >
                Forgot password?
              </button>
              {GOOGLE_ENABLED && (
                <Button
                  variant="outline"
                  className="mt-3 w-full"
                  disabled={busy}
                  onClick={() =>
                    void supabase.auth
                      .signInWithOAuth({
                        provider: "google",
                        options: { redirectTo: authCallbackUrl(window.location.origin) },
                      })
                      .then((r) => {
                        if (r.error) setError(authErrorMessage(r.error));
                      })
                  }
                >
                  Continue with Google
                </Button>
              )}
            </>
          )}
          {signupEnabled && mode !== "recover" && (
            <button
              type="button"
              className="mt-4 block text-sm text-muted-foreground underline"
              onClick={() => changeMode(mode === "in" ? "up" : "in")}
            >
              {mode === "in" ? "No account? Sign up" : "Have an account? Sign in"}
            </button>
          )}
          {mode === "recover" && (
            <button
              type="button"
              className="mt-4 text-sm underline"
              onClick={() => changeMode("in")}
            >
              Back to sign in
            </button>
          )}
        </>
      )}
    </div>
  );
}
