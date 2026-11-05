import { createFileRoute, Link } from "@tanstack/react-router";
import { useRef, useState } from "react";
import { z } from "zod";
import { supabase } from "@/integrations/supabase/client";
import { useAuth } from "@/hooks/use-auth";
import { passwordInput, authErrorMessage } from "@/lib/auth-input";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
export const Route = createFileRoute("/account_/recovery")({ ssr: false, component: Recovery });
const schema = z
  .object({ password: passwordInput, confirmation: z.string() })
  .refine((x) => x.password === x.confirmation, { message: "Passwords do not match" });
function Recovery() {
  const { user, loading } = useAuth(),
    [password, setPassword] = useState(""),
    [confirmation, setConfirmation] = useState(""),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false),
    [done, setDone] = useState(false),
    pending = useRef(false);
  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (pending.current) return;
    const parsed = schema.safeParse({ password, confirmation });
    if (!parsed.success) {
      setError(parsed.error.issues[0]?.message ?? "Check your password");
      return;
    }
    pending.current = true;
    setBusy(true);
    setError("");
    try {
      const r = await supabase.auth.updateUser({ password: parsed.data.password });
      if (r.error) {
        setError(authErrorMessage(r.error));
        return;
      }
      const signedOut = await supabase.auth.signOut({ scope: "global" });
      if (signedOut.error) {
        setError("Password updated. Please sign out and sign in again.");
        return;
      }
      setPassword("");
      setConfirmation("");
      setDone(true);
    } catch {
      setError("The password could not be updated. Request a new recovery email.");
    } finally {
      pending.current = false;
      setBusy(false);
    }
  };
  return (
    <section className="mx-auto my-10 w-full max-w-md border bg-card p-6">
      <div className="label-mono text-primary">meridian</div>
      <h1 className="mt-2 text-2xl font-bold">Set a new password</h1>
      {done ? (
        <>
          <p role="status" className="my-4">
            Your password is updated. Sign in with your new password.
          </p>
          <Link to="/auth">Return to sign in</Link>
        </>
      ) : loading ? (
        <p role="status" className="mt-4">
          Checking your recovery session…
        </p>
      ) : !user?.email_confirmed_at ? (
        <>
          <p role="alert" className="my-4">
            This recovery session is unavailable or expired. Request a new password reset from sign
            in.
          </p>
          <Link to="/auth">Return to sign in</Link>
        </>
      ) : (
        <form onSubmit={submit} className="mt-5 space-y-4">
          <p className="text-sm text-muted-foreground">
            Use at least 12 characters, with uppercase, lowercase and a number.
          </p>
          <div>
            <Label htmlFor="new-password">New password</Label>
            <Input
              id="new-password"
              type="password"
              autoComplete="new-password"
              required
              minLength={12}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />
          </div>
          <div>
            <Label htmlFor="confirm-new-password">Confirm new password</Label>
            <Input
              id="confirm-new-password"
              type="password"
              autoComplete="new-password"
              required
              value={confirmation}
              onChange={(e) => setConfirmation(e.target.value)}
            />
          </div>
          {error && (
            <p role="alert" className="text-sm text-destructive">
              {error}
            </p>
          )}
          <Button disabled={busy}>{busy ? "Updating…" : "Update password"}</Button>
        </form>
      )}
    </section>
  );
}
