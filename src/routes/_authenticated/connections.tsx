import { createFileRoute } from "@tanstack/react-router";
import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { Copy, Plug, Trash2 } from "lucide-react";
import { supabase } from "@/integrations/supabase/client";
import { PageHeader } from "@/components/PageHeader";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useAuth } from "@/hooks/use-auth";
import { CONNECTORS, connectorOf } from "@/lib/provisioning";
import { saveConnection, testConnection } from "@/lib/connections.functions";
import { audit } from "@/lib/audit";
import { MODULES } from "@/lib/modules";

const GROUPS = [
  ["warehouse", "warehouses"],
  ["storage", "lake & storage"],
  ["compute", "spark"],
  ["database", "databases"],
  ["semantic", "semantic"],
  ["bi", "dashboards"],
] as const;

export const Route = createFileRoute("/_authenticated/connections")({
  head: () => ({
    meta: [
      { title: "Connections & provisioning — Meridian" },
      {
        name: "description",
        content:
          "Connect S3, Redshift, Athena, Snowflake, BigQuery, Databricks, EMR/Spark, Postgres, MySQL, Superset, Cube and Iceberg.",
      },
      { property: "og:title", content: "Connections & provisioning — Meridian" },
      {
        property: "og:description",
        content: "Provision and connect external compute, BI and storage.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary" },
    ],
  }),
  component: ConnectionsPage,
});

function ConnectionsPage() {
  const { hasRole } = useAuth();
  const qc = useQueryClient();
  const canEdit = hasRole("admin", "engineer");
  const { data: conns = [] } = useQuery({
    queryKey: ["connections"],
    queryFn: async () =>
      (await supabase.from("connections").select("*").order("created_at")).data ?? [],
  });
  const [kind, setKind] = useState(CONNECTORS[0]!.kind);
  const [name, setName] = useState("");
  const [values, setValues] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState<string | null>(null);
  const def = connectorOf(kind)!;

  const save = async () => {
    try {
      const r = await saveConnection({ data: { kind, name: name || def.label, values } });
      await audit("connection.save", name || def.label, { kind, secrets_set: r.secretKeys });
      toast.success("Connection saved — secrets are stored server-side only");
      setValues({});
      setName("");
      qc.invalidateQueries({ queryKey: ["connections"] });
      void test(r.id);
    } catch (e) {
      toast.error((e as Error).message);
    }
  };
  const test = async (id: string) => {
    setBusy(id);
    try {
      const r = await testConnection({ data: { id } });
      (r.ok ? toast.success : toast.error)(r.message);
    } catch (e) {
      toast.error((e as Error).message);
    }
    setBusy(null);
    qc.invalidateQueries({ queryKey: ["connections"] });
  };
  const del = async (id: string, n: string) => {
    const { error } = await supabase.from("connections").delete().eq("id", id);
    if (error) {
      toast.error(error.message);
      return;
    }
    await audit("connection.delete", n);
    qc.invalidateQueries({ queryKey: ["connections"] });
  };

  return (
    <div>
      <PageHeader
        kicker="admin"
        title="Connections & provisioning"
        desc="Stand up the external systems with the templates below on your own servers, then register them here. Notebooks, jobs and dashboards can then run on them instead of the built-in engine."
      />

      {MODULES.flatMap((m) => m.connectionPanels ?? []).map((Panel, i) => (
        <Panel key={i} />
      ))}
      <div className="label-mono mb-2">registered · {conns.length}</div>
      <div className="mb-8 divide-y border bg-card">
        {!conns.length && (
          <div className="p-4 text-sm text-muted-foreground">
            Nothing connected yet — everything runs on the built-in engine.
          </div>
        )}
        {conns.map((c) => (
          <div key={c.id} className="flex flex-wrap items-center gap-3 p-3">
            <span className="font-semibold">{c.name}</span>
            <Badge variant="outline">{connectorOf(c.kind)?.label ?? c.kind}</Badge>
            <Badge
              variant={
                c.status === "ok" ? "default" : c.status === "error" ? "destructive" : "secondary"
              }
            >
              {c.status}
            </Badge>
            <span className="font-mono text-xs text-muted-foreground">
              {(c.config as Record<string, string>)["url"]}
            </span>
            <span className="text-xs text-muted-foreground">{c.status_message}</span>
            {canEdit && (
              <div className="ml-auto flex gap-1">
                <Button
                  size="sm"
                  variant="outline"
                  disabled={busy === c.id}
                  onClick={() => test(c.id)}
                >
                  <Plug className="mr-1 h-3 w-3" />
                  {busy === c.id ? "Testing…" : "Test"}
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  aria-label="Delete connection"
                  onClick={() => del(c.id, c.name)}
                >
                  <Trash2 className="h-3 w-3" />
                </Button>
              </div>
            )}
          </div>
        ))}
      </div>

      <div className="mb-4 space-y-2">
        {GROUPS.map(([g, label]) => (
          <div key={g} className="flex flex-wrap items-center gap-2">
            <span className="label-mono w-24 shrink-0">{label}</span>
            {CONNECTORS.filter((c) => c.group === g).map((c) => (
              <button
                key={c.kind}
                onClick={() => {
                  setKind(c.kind);
                  setValues({});
                }}
                className={`border px-3 py-1.5 text-sm ${kind === c.kind ? "border-primary bg-primary text-primary-foreground" : "bg-card"}`}
              >
                {c.label}
              </button>
            ))}
          </div>
        ))}
      </div>
      <div className="grid gap-6 lg:grid-cols-2 [&>*]:min-w-0">
        <section className="border bg-card p-4">
          <div className="label-mono mb-1">1 · provision</div>
          <p className="mb-3 text-sm text-muted-foreground">{def.desc}</p>
          <div className="flex items-center justify-between border-b pb-1 font-mono text-xs">
            <span>{def.template.file}</span>
            <button
              className="flex items-center gap-1 hover:text-primary"
              onClick={() => {
                void navigator.clipboard.writeText(def.template.body);
                toast.success("Copied");
              }}
            >
              <Copy className="h-3 w-3" />
              copy
            </button>
          </div>
          <pre className="max-h-96 overflow-auto bg-muted p-3 font-mono text-[11px]">
            {def.template.body}
          </pre>
        </section>
        <section className="border bg-card p-4">
          <div className="label-mono mb-3">2 · register</div>
          {!canEdit ? (
            <p className="text-sm text-muted-foreground">
              Only admins and engineers can add connections.
            </p>
          ) : (
            <div className="space-y-2">
              <Input
                placeholder="Connection name"
                value={name}
                onChange={(e) => setName(e.target.value)}
              />
              {def.managed && (
                <p className="border-l-2 border-primary bg-muted p-2 text-xs">
                  The login for this service (AWS keys, Snowflake or Google account) is added once
                  for the whole company by the app builder and never stored in this form. Until
                  then, "Test" shows "isn't connected yet".
                </p>
              )}
              {def.group === "database" && (
                <p className="border-l-2 border-primary bg-muted p-2 text-xs">
                  Private databases can't be reached from the cloud directly. Register it here for
                  the catalog, then set up the worker on the Ingestion page.
                </p>
              )}
              {def.fields.map((f) => (
                <div key={f.key}>
                  <label className="label-mono">
                    {f.label}
                    {f.secret && " · stored server-side"}
                  </label>
                  <Input
                    type={f.secret ? "password" : "text"}
                    placeholder={f.placeholder}
                    value={values[f.key] ?? ""}
                    onChange={(e) => setValues({ ...values, [f.key]: e.target.value })}
                  />
                </div>
              ))}
              <Button onClick={save}>Save & test</Button>
              <p className="text-xs text-muted-foreground">
                The Meridian server must be able to reach this address over the internet. Passwords
                and tokens are never sent back to the browser.
              </p>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
