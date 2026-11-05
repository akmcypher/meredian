import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { useEffect, useRef, useState } from "react";
import { useAuth } from "@/hooks/use-auth";
import { saveOnboarding, type Workspace } from "@/lib/workspace";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
export const Route = createFileRoute("/onboarding")({ ssr: false, component: Onboarding });
function Onboarding() {
  const { user, workspace, workspaceError, loading, refreshWorkspace } = useAuth(),
    navigate = useNavigate();
  useEffect(() => {
    if (!loading && !user) void navigate({ to: "/auth", replace: true });
    else if (!loading && workspace?.onboardingCompleted) void navigate({ to: "/", replace: true });
  }, [loading, user, workspace, navigate]);
  if (loading)
    return (
      <p role="status" className="p-6">
        Preparing your verified workspace…
      </p>
    );
  if (!user?.email_confirmed_at)
    return (
      <section className="mx-auto my-10 max-w-lg border bg-card p-6">
        <h1 className="text-2xl font-bold">Verify your email first</h1>
        <p className="my-3">Open the verification email before setting up a workspace.</p>
        <Link to="/auth">Return to sign in</Link>
      </section>
    );
  if (!workspace)
    return (
      <section className="mx-auto my-10 max-w-lg border bg-card p-6">
        <h1 className="text-2xl font-bold">Workspace setup</h1>
        <p role="alert" className="my-3">
          {workspaceError ?? "Workspace information is unavailable."}
        </p>
        <Button onClick={() => void refreshWorkspace()}>Retry</Button>
      </section>
    );
  return <Setup key={workspace.workspaceId} workspace={workspace} />;
}
function Setup({ workspace }: { workspace: Workspace }) {
  const { refreshWorkspace } = useAuth(),
    navigate = useNavigate(),
    [name, setName] = useState(workspace.name),
    [description, setDescription] = useState(workspace.description),
    [source, setSource] = useState(workspace.firstSource ?? "skip"),
    [busy, setBusy] = useState(false),
    [error, setError] = useState("");
  const pending = useRef(false);
  const step = workspace.onboardingStep;
  const canManage = ["owner", "admin"].includes(workspace.workspaceRole);
  const advance = async (e: React.FormEvent) => {
    e.preventDefault();
    if (pending.current) return;
    pending.current = true;
    setBusy(true);
    setError("");
    try {
      const next = await saveOnboarding(
        step + 1,
        step === 0 && canManage ? { name, description } : step === 1 ? { firstSource: source } : {},
      );
      await refreshWorkspace();
      if (next.onboardingCompleted) void navigate({ to: "/", replace: true });
    } catch (e) {
      setError(e instanceof Error ? e.message : "Workspace setup could not be saved.");
    } finally {
      pending.current = false;
      setBusy(false);
    }
  };
  return (
    <section className="mx-auto my-8 w-full max-w-2xl border bg-card p-6 sm:p-8">
      <div className="label-mono text-primary">your workspace</div>
      <h1 className="mt-2 text-3xl font-bold">Welcome to Meridian</h1>
      <p className="mt-3 text-sm text-muted-foreground">
        {workspace.displayName ?? "Your account"} · workspace {workspace.workspaceRole}. Your role
        applies to this workspace, not the whole platform.
      </p>
      <ol aria-label="Onboarding progress" className="my-6 flex flex-wrap gap-3 text-sm">
        {["Workspace", "First source", "Team & finish"].map((label, i) => (
          <li
            key={label}
            aria-current={i === step ? "step" : undefined}
            className={i === step ? "font-semibold text-primary" : "text-muted-foreground"}
          >
            {i + 1}. {label}
            {i < step ? " ✓" : ""}
          </li>
        ))}
      </ol>
      <form onSubmit={advance} className="space-y-5">
        {step === 0 ? (
          <>
            <h2 className="text-xl font-semibold">
              {canManage ? "Name your workspace" : "Your workspace"}
            </h2>
            <div>
              <Label htmlFor="workspace-name">Workspace name</Label>
              <Input
                id="workspace-name"
                required
                minLength={2}
                maxLength={80}
                value={name}
                readOnly={!canManage}
                onChange={(e) => setName(e.target.value)}
              />
            </div>
            <div>
              <Label htmlFor="workspace-description">Description (optional)</Label>
              <textarea
                id="workspace-description"
                className="mt-1 w-full rounded-md border bg-background p-3 text-sm"
                maxLength={500}
                rows={3}
                value={description}
                readOnly={!canManage}
                onChange={(e) => setDescription(e.target.value)}
              />
            </div>
          </>
        ) : step === 1 ? (
          <>
            <h2 className="text-xl font-semibold">Choose a starting point</h2>
            <p className="text-sm text-muted-foreground">
              This saves your preference. A real connection is created later when you configure the
              source and run its worker.
            </p>
            <div>
              <Label htmlFor="first-source">First source (optional)</Label>
              <select
                id="first-source"
                className="mt-1 w-full rounded-md border bg-background p-3"
                value={source}
                onChange={(e) => setSource(e.target.value)}
              >
                <option value="skip">Skip for now</option>
                <option value="postgres">PostgreSQL — Connect preview</option>
                <option value="mysql">MySQL — Connect preview</option>
                <option value="kafka">Kafka — Available · Certified Preview</option>
              </select>
            </div>
            <p className="text-sm">
              You can also upload CSV, TSV, Parquet, Avro or JSON after setup.
            </p>
          </>
        ) : (
          <>
            <h2 className="text-xl font-semibold">Your workspace is ready</h2>
            <p className="text-sm">
              Team invitations from workspace accounts are not supported in this preview. A platform
              administrator can manage membership; no shared account is needed.
            </p>
            <p className="text-sm text-muted-foreground">
              You can start using your own workspace now. No global administrator role has been
              assigned.
            </p>
          </>
        )}
        {error && (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
        )}
        <Button disabled={busy}>
          {busy
            ? "Saving…"
            : step === 2
              ? "Enter Meridian"
              : step === 1 && source === "skip"
                ? "Skip and continue"
                : "Save and continue"}
        </Button>
      </form>
      <p className="mt-5 text-xs text-muted-foreground">
        Your progress is saved after each step. Refresh to resume where you left off.
      </p>
    </section>
  );
}
