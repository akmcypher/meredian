import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  buildConfig,
  KAFKA_PROFILES,
  publicConfigEntries,
  scheduleProblem,
  secretFields,
  setupFromStored,
  type ConnectSourceKind,
} from "@/lib/connect-setup";
import { AgentFileTemplate } from "./AgentFileTemplate";
import type { ConnectConnection, ConnectSourceRow, useConnectActions } from "./use-connect";

export function SettingsTab({
  connection: c,
  source,
  actions,
}: {
  connection: ConnectConnection;
  source: ConnectSourceRow | undefined;
  actions: ReturnType<typeof useConnectActions>;
}) {
  const [interval, setInterval] = useState(String(c.interval_minutes));
  const kind = c.source_version.split("-")[0] as ConnectSourceKind;
  const entries = source ? publicConfigEntries(source) : [];
  const setup = setupFromStored(kind, c.name, c.credential_ref, source ?? {});
  const { agentFile } = buildConfig(setup);
  const minutes = Number(interval);
  const problem = scheduleProblem(minutes);
  const profile = KAFKA_PROFILES.find((p) => p.id === source?.["profile"]);
  return (
    <div className="space-y-5">
      <section className="space-y-2">
        <h4 className="font-semibold">Schedule</h4>
        <div className="flex flex-wrap items-end gap-2">
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
          <Button
            variant="outline"
            disabled={actions.busy || !!problem}
            onClick={() =>
              void actions.run(
                () => actions.configure({ data: { id: c.id, interval: minutes } }),
                "Schedule saved",
              )
            }
          >
            Save schedule
          </Button>
        </div>
        {problem && <p className="text-sm text-destructive">{problem}</p>}
      </section>
      <section className="space-y-2">
        <h4 className="font-semibold">Source settings</h4>
        <dl className="grid gap-x-4 gap-y-1 text-sm sm:grid-cols-[max-content_1fr]">
          <dt className="text-muted-foreground">Credential reference</dt>
          <dd>
            <code>{c.credential_ref}</code>
          </dd>
          {profile && (
            <>
              <dt className="text-muted-foreground">Managed profile</dt>
              <dd>{profile.label}</dd>
            </>
          )}
          {entries
            .filter(([k]) => k !== "profile")
            .map(([k, v]) => (
              <div key={k} className="contents">
                <dt className="text-muted-foreground">
                  <code>{k}</code>
                </dt>
                <dd className="break-all">{v}</dd>
              </div>
            ))}
        </dl>
        {!entries.length && (
          <p className="text-sm text-muted-foreground">
            This source was registered before Connect recorded its settings; the agent file is the
            only copy.
          </p>
        )}
        <p className="text-xs text-muted-foreground">
          The agent reads its settings from its own file at run time. To change them, edit the file
          on the agent host, then test the connection.
        </p>
      </section>
      <AgentFileTemplate
        credentialRef={c.credential_ref}
        agentFile={agentFile}
        secrets={secretFields(setup)}
      />
    </div>
  );
}
