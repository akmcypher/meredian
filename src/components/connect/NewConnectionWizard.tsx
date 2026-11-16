import { useState } from "react";
import { ArrowLeft, Loader2, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  buildConfig,
  initialSetup,
  scheduleProblem,
  secretFields,
  setupProblems,
  targetTable,
  type SetupState,
} from "@/lib/connect-setup";
import { AgentFileTemplate, AgentToken } from "./AgentFileTemplate";
import { ErrorGuidance } from "./ErrorGuidance";
import { StreamPicker } from "./StreamPicker";
import { readCatalog, saveStreams, wizardSteps, type WizardStep } from "./connect-view";
import { CredentialsStep, KafkaSecurityStep, SourceTypeStep } from "./wizard-steps";
import { useConnectActions, useRunStatus, type ConnectList } from "./use-connect";

const LABEL: Record<WizardStep, string> = {
  type: "Source",
  security: "Security",
  credentials: "Settings",
  test: "Test",
  discover: "Discover",
  streams: "Streams",
  schedule: "Schedule",
};

export function NewConnectionWizard({
  data,
  onClose,
  onDone,
}: {
  data: ConnectList | undefined;
  onClose: () => void;
  onDone: (connectionId: string) => void;
}) {
  const [setup, setSetup] = useState<SetupState>(initialSetup("kafka"));
  const [step, setStep] = useState<WizardStep>("type");
  const [sourceId, setSourceId] = useState<string | null>(null);
  const [connectionId, setConnectionId] = useState<string | null>(null);
  const [checkRun, setCheckRun] = useState<string | null>(null);
  const [discoverRun, setDiscoverRun] = useState<string | null>(null);
  // The source's agent token: returned once by createSource (only its hash is stored), kept in
  // memory for this wizard only.
  const [agentToken, setAgentToken] = useState<string | null>(null);
  const [interval, setInterval] = useState("0");
  const actions = useConnectActions();
  const check = useRunStatus(checkRun);
  const discover = useRunStatus(discoverRun);
  const steps = wizardSteps(setup.kind);
  const index = steps.indexOf(step);
  const registered = !!connectionId;
  const problems = setupProblems(setup);
  const { publicConfig, agentFile } = buildConfig(setup);
  const secrets = secretFields(setup);
  const update = (patch: Partial<SetupState>) => setSetup((s) => ({ ...s, ...patch }));
  const next = () => setStep(steps[index + 1]!);
  const connection = data?.connections.find((c) => c.id === connectionId);

  async function register() {
    const out = await actions.run(async () => {
      // Non-secret settings only (buildConfig's allowlist); secrets stay in the agent file.
      let sid = sourceId;
      if (!sid) {
        const src = await actions.createSource({
          data: {
            name: setup.name.trim(),
            kind: setup.kind,
            target: targetTable(setup.name),
            config: publicConfig,
          },
        });
        sid = src.id as string;
        setSourceId(sid);
        setAgentToken(src.token);
      }
      const c = await actions.create({
        data: {
          sourceId: sid,
          name: setup.name.trim(),
          credentialRef: setup.credentialRef,
          ...(setup.cdc && setup.kind !== "kafka" ? { cdc: true } : {}),
        },
      });
      return c.id;
    }, "Connection registered");
    if (out) {
      setConnectionId(out);
      next();
    }
  }
  async function enqueue(op: "check" | "discover") {
    if (!connectionId) return;
    const r = await actions.run(() => actions.queue(connectionId, op));
    if (r) (op === "check" ? setCheckRun : setDiscoverRun)(r.id as string);
  }
  const minutes = Number(interval);
  const intervalProblem = scheduleProblem(minutes);

  return (
    <div className="space-y-4" aria-label="New connection">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-lg font-semibold">New connection</h3>
        <Button
          variant="ghost"
          size="sm"
          onClick={registered ? () => onDone(connectionId) : onClose}
        >
          <X />
          {registered ? "Finish later" : "Cancel"}
        </Button>
      </div>
      <ol className="flex flex-wrap gap-1 text-xs" aria-label="Wizard steps">
        {steps.map((s, i) => (
          <li
            key={s}
            aria-current={s === step ? "step" : undefined}
            className={`rounded-full border px-2 py-0.5 ${s === step ? "border-primary bg-primary text-primary-foreground" : i < index ? "bg-muted" : "text-muted-foreground"}`}
          >
            {i + 1}. {LABEL[s]}
          </li>
        ))}
      </ol>

      {step === "type" && <SourceTypeStep setup={setup} onChange={setSetup} />}
      {step === "security" && <KafkaSecurityStep setup={setup} update={update} />}
      {step === "credentials" && (
        <div className="space-y-4">
          <CredentialsStep setup={setup} update={update} />
          {problems.length > 0 && (
            <ul
              className="list-disc space-y-1 pl-5 text-sm text-destructive"
              aria-label="Setup problems"
            >
              {problems.map((p) => (
                <li key={p}>{p}</li>
              ))}
            </ul>
          )}
        </div>
      )}
      {step === "test" && (
        <div className="space-y-3">
          {agentToken && <AgentToken token={agentToken} />}
          <AgentFileTemplate
            credentialRef={setup.credentialRef}
            agentFile={agentFile}
            secrets={secrets}
          />
          <p className="text-sm">
            Place the file on the agent host, then test. The agent signs in to the source with it.
          </p>
          <RunState run={check.data} pending={!!checkRun && !check.data} />
          {check.data?.status === "failed" && (
            <ErrorGuidance
              code={check.data.error_code}
              onAction={() => void enqueue("check")}
              disabled={actions.busy}
            />
          )}
        </div>
      )}
      {step === "discover" && (
        <div className="space-y-3">
          <p className="text-sm">
            Discover lists the {setup.kind === "kafka" ? "topics" : "tables"} the agent can read.
          </p>
          <RunState run={discover.data} pending={!!discoverRun && !discover.data} />
          {discover.data?.status === "failed" && (
            <ErrorGuidance
              code={discover.data.error_code}
              onAction={() => void enqueue("discover")}
              disabled={actions.busy}
            />
          )}
          {discover.data?.status === "succeeded" && (
            <p className="text-sm">
              {readCatalog(connection?.discovered_catalog).length} stream(s) found.
            </p>
          )}
        </div>
      )}
      {step === "streams" && connectionId && (
        <StreamPicker
          catalog={readCatalog(connection?.discovered_catalog)}
          kafka={setup.kind === "kafka"}
          cdc={setup.cdc && setup.kind !== "kafka"}
          configured={[]}
          busy={actions.busy}
          saveLabel="Save and continue"
          onSave={async (chosen) => {
            const ok = await actions.run(
              () => saveStreams(actions, connectionId, chosen),
              "Streams saved",
            );
            if (ok !== undefined) next();
          }}
        />
      )}
      {step === "schedule" && (
        <div className="space-y-2">
          <label className="text-sm">
            Sync every (minutes, 0 = manual)
            <Input
              aria-label="Connect schedule minutes"
              type="number"
              min={0}
              value={interval}
              onChange={(e) => setInterval(e.target.value)}
            />
          </label>
          {intervalProblem && <p className="text-sm text-destructive">{intervalProblem}</p>}
        </div>
      )}

      <div className="flex flex-wrap gap-2 border-t pt-3">
        {index > 0 && !registered && (
          <Button variant="outline" onClick={() => setStep(steps[index - 1]!)}>
            <ArrowLeft />
            Back
          </Button>
        )}
        {(step === "type" || step === "security") && <Button onClick={next}>Continue</Button>}
        {step === "credentials" && (
          <Button disabled={actions.busy || problems.length > 0} onClick={() => void register()}>
            Register connection
          </Button>
        )}
        {step === "test" && (
          <>
            <Button
              variant="outline"
              disabled={actions.busy || isActive(check.data?.status)}
              onClick={() => void enqueue("check")}
            >
              Test connection
            </Button>
            <Button disabled={check.data?.status !== "succeeded"} onClick={next}>
              Continue
            </Button>
          </>
        )}
        {step === "discover" && (
          <>
            <Button
              variant="outline"
              disabled={actions.busy || isActive(discover.data?.status)}
              onClick={() => void enqueue("discover")}
            >
              Discover streams
            </Button>
            <Button
              disabled={
                discover.data?.status !== "succeeded" ||
                !readCatalog(connection?.discovered_catalog).length
              }
              onClick={next}
            >
              Continue
            </Button>
          </>
        )}
        {step === "schedule" && connectionId && (
          <Button
            disabled={actions.busy || !!intervalProblem}
            onClick={async () => {
              const ok = await actions.run(
                () => actions.configure({ data: { id: connectionId, interval: minutes } }),
                "Connection ready",
              );
              if (ok) onDone(connectionId);
            }}
          >
            Save schedule and finish
          </Button>
        )}
      </div>
    </div>
  );
}

const isActive = (s: string | undefined) => !!s && ["queued", "running", "retry_wait"].includes(s);

function RunState({
  run,
  pending,
}: {
  run: { status: string; attempts: number; max_attempts: number } | undefined;
  pending: boolean;
}) {
  if (pending) return <p role="status">Queuing…</p>;
  if (!run) return null;
  if (isActive(run.status))
    return (
      <p role="status" className="flex items-center gap-2 text-sm">
        <Loader2 className="h-4 w-4 animate-spin" />
        {run.status === "queued"
          ? "Waiting for the agent to pick this up…"
          : run.status === "retry_wait"
            ? `Retrying (attempt ${run.attempts} of ${run.max_attempts})…`
            : "Running…"}
      </p>
    );
  if (run.status === "succeeded")
    return (
      <p role="status" className="text-sm font-medium">
        Succeeded.
      </p>
    );
  if (run.status === "cancelled")
    return (
      <p role="status" className="text-sm">
        Cancelled.
      </p>
    );
  return null;
}
