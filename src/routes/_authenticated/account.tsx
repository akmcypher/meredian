import { createFileRoute, Link } from "@tanstack/react-router";
import { useEffect, useRef, useState } from "react";
import { useAuth } from "@/hooks/use-auth";
import { supabase } from "@/integrations/supabase/client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { PageHeader } from "@/components/PageHeader";
export const Route = createFileRoute("/_authenticated/account")({ ssr: false, component: Account });
function Account() {
  const { user, workspace, refreshWorkspace } = useAuth(),
    [name, setName] = useState(workspace?.displayName ?? ""),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [notice, setNotice] = useState(""),
    pending = useRef(false);
  useEffect(() => {
    setName(workspace?.displayName ?? "");
  }, [workspace?.displayName]);
  const save = async (e: React.FormEvent) => {
    e.preventDefault();
    if (pending.current) return;
    pending.current = true;
    setBusy(true);
    setError("");
    setNotice("");
    try {
      const r = await supabase.rpc("update_account_profile", { _name: name });
      if (r.error) {
        setError("Enter a name of 2 to 80 characters and try again.");
        return;
      }
      await refreshWorkspace();
      setNotice("Profile saved.");
    } catch {
      setError("Profile could not be saved. Please retry.");
    } finally {
      pending.current = false;
      setBusy(false);
    }
  };
  return (
    <section className="max-w-2xl">
      <PageHeader
        kicker="account"
        title="Your account"
        desc="Your verified identity and workspace access."
      />
      <form onSubmit={save} className="space-y-4 border bg-card p-5">
        <div>
          <Label htmlFor="account-name">Full name</Label>
          <Input
            id="account-name"
            required
            minLength={2}
            maxLength={80}
            value={name}
            onChange={(e) => setName(e.target.value)}
          />
        </div>
        <div>
          <Label htmlFor="account-email">Verified email</Label>
          <Input id="account-email" readOnly value={user?.email ?? ""} />
        </div>
        {error && (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
        )}
        {notice && (
          <p role="status" className="text-sm">
            {notice}
          </p>
        )}
        <Button disabled={busy}>{busy ? "Saving…" : "Save profile"}</Button>
      </form>
      <div className="mt-5 space-y-3 border bg-card p-5">
        <h2 className="font-semibold">Workspace</h2>
        <p>
          {workspace?.name} · {workspace?.workspaceRole}
        </p>
        <p className="break-all font-mono text-xs text-muted-foreground">
          {workspace?.workspaceId}
        </p>
        <p className="text-sm">Workspace privileges do not grant platform administration.</p>
        <Link to="/account/recovery" className="block text-sm underline">
          Change password
        </Link>
        <p className="text-sm text-muted-foreground">
          For account deactivation or deletion, contact a platform administrator. Workspace data is
          retained; this page never deletes another member’s data.
        </p>
      </div>
    </section>
  );
}
