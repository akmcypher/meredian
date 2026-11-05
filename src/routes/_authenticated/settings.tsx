import { createFileRoute } from "@tanstack/react-router";
import { useEffect, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { MailCheck } from "lucide-react";
import { supabase } from "@/integrations/supabase/client";
import { PageHeader } from "@/components/PageHeader";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { useAuth } from "@/hooks/use-auth";
import { sendTestAlert } from "@/lib/alerts.functions";
import { isSingleEmailAddress } from "@/lib/email-address";

export const Route = createFileRoute("/_authenticated/settings")({
  head: () => ({
    meta: [
      { title: "Alert settings — Meridian" },
      {
        name: "description",
        content:
          "Choose where Meridian emails you about failed jobs, failing checks and approval requests.",
      },
      { property: "og:title", content: "Alert settings — Meridian" },
      { property: "og:description", content: "Email alert preferences." },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary" },
    ],
  }),
  component: Settings,
});

function Settings() {
  const { user, workspace, refreshWorkspace } = useAuth();
  const [switching, setSwitching] = useState(false);
  const workspaces = useQuery({
    queryKey: ["workspace-options", user?.id],
    enabled: !!user,
    queryFn: async () => {
      const memberships = await supabase
        .from("org_members")
        .select("org_id")
        .eq("user_id", user!.id);
      if (memberships.error) throw new Error("Workspace list unavailable");
      const result = await supabase
        .from("organizations")
        .select("id,name")
        .in(
          "id",
          memberships.data.map((m) => m.org_id),
        )
        .eq("status", "active");
      if (result.error) throw new Error("Workspace list unavailable");
      return result.data;
    },
  });
  async function switchWorkspace(id: string) {
    setSwitching(true);
    try {
      const result = await supabase.rpc("select_workspace", { _workspace: id });
      if (result.error || !(await refreshWorkspace())) throw new Error("Workspace switch failed");
      toast.success("Workspace switched");
    } catch {
      toast.error("Workspace switch failed. Your membership may have changed.");
    } finally {
      setSwitching(false);
    }
  }
  const qc = useQueryClient();
  const { data } = useQuery({
    queryKey: ["alert_settings", user?.id],
    enabled: !!user,
    queryFn: async () =>
      (await supabase.from("alert_settings").select("*").eq("user_id", user!.id).maybeSingle())
        .data,
  });
  const [email, setEmail] = useState("");
  const [jobFailures, setJobFailures] = useState(true);
  const [approvals, setApprovals] = useState(true);
  const [testing, setTesting] = useState(false);

  useEffect(() => {
    setEmail(data?.alert_email ?? user?.email ?? "");
    setJobFailures(data?.notify_job_failures ?? true);
    setApprovals(data?.notify_approvals ?? true);
  }, [data, user?.email]);

  const save = async (): Promise<boolean> => {
    if (!user) return false;
    const address = email.trim();
    if (address && !isSingleEmailAddress(address)) {
      toast.error("Enter a single email address");
      return false;
    }
    const { error } = await supabase.from("alert_settings").upsert({
      user_id: user.id,
      alert_email: address || null,
      notify_job_failures: jobFailures,
      notify_approvals: approvals,
      updated_at: new Date().toISOString(),
    });
    if (error) {
      toast.error(error.message);
      return false;
    }
    toast.success("Alert settings saved");
    qc.invalidateQueries({ queryKey: ["alert_settings", user.id] });
    return true;
  };

  const test = async () => {
    setTesting(true);
    try {
      if (!(await save())) return;
      const r = await sendTestAlert();
      toast.success(`Test email sent to ${r.to}`);
    } catch (e) {
      toast.error((e as Error).message);
    } finally {
      setTesting(false);
    }
  };

  return (
    <div>
      <PageHeader
        kicker="account"
        title="Alert settings"
        desc="Meridian emails you when a scheduled job fails, a data quality check breaks, or a change needs your steward approval. Email is sent through the email provider your administrator configured (SMTP); until one is set, alerts and test emails report that email is not configured."
      />
      <section className="mb-5 max-w-xl border bg-card p-5">
        <label htmlFor="active-workspace" className="label-mono mb-2 block">
          Active workspace
        </label>
        <select
          id="active-workspace"
          className="w-full border bg-background p-2"
          value={workspace?.workspaceId ?? ""}
          disabled={switching || workspaces.isPending}
          onChange={(e) => void switchWorkspace(e.target.value)}
        >
          {workspaces.data?.map((w) => (
            <option key={w.id} value={w.id}>
              {w.name}
            </option>
          ))}
        </select>
        {workspaces.error && <p role="alert">{workspaces.error.message}</p>}
        <p className="mt-2 text-sm text-muted-foreground">
          Personal legacy files are retained in a separate owner-only workspace.
        </p>
      </section>
      <section className="max-w-xl space-y-5 border bg-card p-5">
        <div>
          <label className="label-mono mb-1 block" htmlFor="alert-email">
            send alerts to
          </label>
          <Input
            id="alert-email"
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            placeholder="you@example.com"
          />
          <p className="mt-1 text-xs text-muted-foreground">
            Leave empty to use your sign-in email.
          </p>
        </div>
        <div className="flex items-center justify-between gap-4 border-t pt-4">
          <div>
            <div className="text-sm font-medium">Job & check failures</div>
            <div className="text-xs text-muted-foreground">
              Email me when a scheduled job fails or a hop-to-hop check breaks.
            </div>
          </div>
          <Switch
            checked={jobFailures}
            onCheckedChange={setJobFailures}
            aria-label="Job and check failure alerts"
          />
        </div>
        <div className="flex items-center justify-between gap-4 border-t pt-4">
          <div>
            <div className="text-sm font-medium">Approvals</div>
            <div className="text-xs text-muted-foreground">
              Email stewards when a change waits for review, and the author when it's approved or
              rejected.
            </div>
          </div>
          <Switch checked={approvals} onCheckedChange={setApprovals} aria-label="Approval alerts" />
        </div>
        <div className="flex gap-2 border-t pt-4">
          <Button onClick={save}>Save</Button>
          <Button variant="outline" onClick={test} disabled={testing}>
            <MailCheck className="mr-1 h-4 w-4" />
            {testing ? "Sending…" : "Send test email"}
          </Button>
        </div>
      </section>
    </div>
  );
}
