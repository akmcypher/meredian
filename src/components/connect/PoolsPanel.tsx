import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { poolSummary } from "./connect-view";
import { useConnectActions, usePools, type ConnectList } from "./use-connect";

/** Worker pools: agents that share the work of several connections in one workspace. */
export function PoolsPanel({ data }: { data: ConnectList }) {
  const query = usePools();
  const actions = useConnectActions();
  const [name, setName] = useState("");
  // Workspaces the user has connections in; a pool serves connections of its own workspace only.
  const workspaces = [...new Set(data.connections.map((c) => c.organization_id ?? ""))];
  const [workspace, setWorkspace] = useState(workspaces[0] ?? "");
  const [token, setToken] = useState<{ pool: string; value: string } | null>(null);
  const pools = query.data?.pools ?? [];
  const summary = poolSummary(pools, query.data?.workers ?? [], data.connections);
  const label = (org: string | null) => (org ? "Organization" : "Personal");
  return (
    <section className="space-y-3" aria-labelledby="pools-heading">
      <h3 id="pools-heading" className="text-lg font-semibold">
        Worker pools
      </h3>
      <p className="text-sm text-muted-foreground">
        A pool lets several agents share the work of many connections. Agents started with a
        pool&apos;s token claim tasks of every connection assigned to it, least recently served
        first. Every host in a pool needs the credential files of the pool&apos;s connections.
      </p>
      {query.error && <p role="alert">{query.error.message}</p>}
      {pools.length > 0 && (
        <ul className="divide-y rounded border">
          {pools.map((p) => {
            const s = summary.get(p.id)!;
            return (
              <li key={p.id} className="flex flex-wrap items-center gap-3 p-3 text-sm">
                <span className="font-medium">{p.name}</span>
                <Badge variant="outline">{label(p.organization_id)}</Badge>
                {!p.enabled && <Badge variant="destructive">Disabled</Badge>}
                <span>
                  {s.agentsOnline} agent{s.agentsOnline === 1 ? "" : "s"} online · {s.slotsOnline}{" "}
                  slot{s.slotsOnline === 1 ? "" : "s"} · {s.connections} connection
                  {s.connections === 1 ? "" : "s"}
                </span>
                <span className="ml-auto flex gap-2">
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={actions.busy}
                    onClick={() =>
                      void actions.run(
                        () => actions.updatePool({ data: { id: p.id, enabled: !p.enabled } }),
                        p.enabled ? "Pool disabled" : "Pool enabled",
                      )
                    }
                  >
                    {p.enabled ? "Disable" : "Enable"}
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={actions.busy}
                    onClick={async () => {
                      const r = await actions.run(() =>
                        actions.updatePool({ data: { id: p.id, rotate: true } }),
                      );
                      if (r?.token) setToken({ pool: p.name, value: r.token });
                    }}
                  >
                    New token
                  </Button>
                </span>
              </li>
            );
          })}
        </ul>
      )}
      {token && (
        <div role="status" className="space-y-1 rounded border p-3 text-sm">
          <p>
            Token for <strong>{token.pool}</strong>. It is shown only once; earlier tokens of this
            pool stop working.
          </p>
          <code className="block break-all rounded bg-muted p-2">{token.value}</code>
          <p>
            Save it in a file only the agent can read and start each agent with{" "}
            <code>INGEST_TOKEN_FILE</code> pointing at it and <code>CONNECT_SLOTS</code> set to the
            number of tasks it may run at once.
          </p>
          <Button size="sm" variant="outline" onClick={() => setToken(null)}>
            Done
          </Button>
        </div>
      )}
      <form
        className="flex flex-wrap items-end gap-2"
        onSubmit={async (e) => {
          e.preventDefault();
          const r = await actions.run(
            () =>
              actions.createPool({
                data: { name: name.trim(), organizationId: workspace || null },
              }),
            "Pool created",
          );
          if (r) {
            setToken({ pool: name.trim(), value: r.token });
            setName("");
          }
        }}
      >
        <label className="text-sm">
          New pool name
          <Input value={name} maxLength={80} onChange={(e) => setName(e.target.value)} />
        </label>
        {workspaces.length > 1 && (
          <label className="text-sm">
            Workspace
            <select
              className="block h-9 rounded border bg-background px-2"
              value={workspace}
              onChange={(e) => setWorkspace(e.target.value)}
            >
              {workspaces.map((w) => (
                <option key={w} value={w}>
                  {label(w || null)}
                  {w ? ` ${w.slice(0, 8)}` : ""}
                </option>
              ))}
            </select>
          </label>
        )}
        <Button type="submit" disabled={actions.busy || !name.trim()}>
          Create pool
        </Button>
      </form>
    </section>
  );
}

/** Settings for one connection: which pool runs it, and how many of its tasks may run at once. */
export function ConnectionPoolSettings({
  connection: c,
}: {
  connection: ConnectList["connections"][number];
}) {
  const query = usePools();
  const actions = useConnectActions();
  const [limit, setLimit] = useState(c.max_parallel_tasks ? String(c.max_parallel_tasks) : "");
  const pools = (query.data?.pools ?? []).filter((p) => p.organization_id === c.organization_id);
  const n = limit === "" ? null : Number(limit);
  const invalid = n !== null && (!Number.isInteger(n) || n < 1 || n > 64);
  return (
    <section className="space-y-2">
      <h4 className="font-semibold">Workers</h4>
      <label className="block text-sm">
        Worker pool
        <select
          className="block h-9 rounded border bg-background px-2"
          value={c.pool_id ?? ""}
          disabled={actions.busy}
          onChange={(e) =>
            void actions.run(
              () =>
                actions.assignPool({
                  data: { connectionId: c.id, poolId: e.target.value || null },
                }),
              "Worker pool saved",
            )
          }
        >
          <option value="">This connection&apos;s own agent only</option>
          {pools.map((p) => (
            <option key={p.id} value={p.id}>
              {p.name}
            </option>
          ))}
        </select>
      </label>
      <div className="flex flex-wrap items-end gap-2">
        <label className="text-sm">
          Parallel tasks limit (empty = no limit)
          <Input
            inputMode="numeric"
            value={limit}
            aria-invalid={invalid}
            onChange={(e) => setLimit(e.target.value.trim())}
          />
        </label>
        <Button
          variant="outline"
          disabled={actions.busy || invalid}
          onClick={() =>
            void actions.run(
              () => actions.assignPool({ data: { connectionId: c.id, maxParallelTasks: n } }),
              "Limit saved",
            )
          }
        >
          Save limit
        </Button>
      </div>
      {invalid && <p className="text-sm text-destructive">Enter a whole number from 1 to 64.</p>}
      <p className="text-xs text-muted-foreground">
        The limit counts this connection&apos;s running tasks on every agent, to protect the source.
      </p>
    </section>
  );
}
