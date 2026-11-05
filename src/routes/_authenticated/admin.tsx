import { createFileRoute, Link } from "@tanstack/react-router";
import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useServerFn } from "@tanstack/react-start";
import { toast } from "sonner";
import { supabase } from "@/integrations/supabase/client";
import { PageHeader, Stat } from "@/components/PageHeader";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useAuth } from "@/hooks/use-auth";
import {
  adminInviteUser,
  adminListUsers,
  adminOverview,
  adminSetOrgMember,
  adminSetRole,
  adminSetUserActive,
  adminVerifyAudit,
} from "@/lib/admin.functions";
import { MODULES } from "@/lib/modules";

export const Route = createFileRoute("/_authenticated/admin")({
  head: () => ({
    meta: [
      { title: "Admin console — Meridian" },
      {
        name: "description",
        content:
          "Manage users, roles, organizations, quotas, platform settings and the tamper-evident admin audit trail.",
      },
      { property: "og:title", content: "Admin console — Meridian" },
      { property: "og:description", content: "Meridian platform control plane." },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary" },
    ],
  }),
  component: AdminPage,
});

const MODULE_TABS = MODULES.flatMap((m) => m.adminTabs ?? []);

const ROLES = ["admin", "steward", "engineer", "analyst"] as const;

function AdminPage() {
  const { hasRole } = useAuth();
  if (!hasRole("admin"))
    return (
      <div className="p-6">
        <PageHeader
          kicker="admin"
          title="Admin console"
          desc="Only admins can open the admin console."
        />
      </div>
    );
  return (
    <div>
      <PageHeader
        kicker="control plane"
        title="Admin console"
        desc="Every action here is checked again on the server and written to an append-only, hash-chained audit trail."
      />
      <Tabs defaultValue="overview">
        <TabsList className="flex h-auto flex-wrap">
          {["overview", "users", "organizations", "settings", "audit", "engines"].map((t) => (
            <TabsTrigger key={t} value={t} className="capitalize">
              {t}
            </TabsTrigger>
          ))}
          {MODULE_TABS.map((t) => (
            <TabsTrigger key={t.id} value={t.id} className="capitalize">
              {t.label}
            </TabsTrigger>
          ))}
        </TabsList>
        <TabsContent value="overview">
          <Overview />
        </TabsContent>
        <TabsContent value="users">
          <Users />
        </TabsContent>
        <TabsContent value="organizations">
          <Orgs />
        </TabsContent>
        <TabsContent value="settings">
          <Settings />
        </TabsContent>
        <TabsContent value="audit">
          <AdminAudit />
        </TabsContent>
        <TabsContent value="engines">
          <Engines />
        </TabsContent>
        {MODULE_TABS.map((t) => (
          <TabsContent key={t.id} value={t.id}>
            <t.Component />
          </TabsContent>
        ))}
      </Tabs>
    </div>
  );
}

function Overview() {
  const fn = useServerFn(adminOverview);
  const { data, error } = useQuery({
    queryKey: ["admin_overview"],
    queryFn: () => fn(),
    refetchInterval: 30_000,
  });
  if (error) return <p className="text-sm text-destructive">{(error as Error).message}</p>;
  if (!data) return <p className="text-sm text-muted-foreground">Loading…</p>;
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <Stat label="organizations" value={data.organizations} />
        <Stat label="users" value={data.users} />
        <Stat label="tables / rows" value={`${data.tables} / ${data.rows.toLocaleString()}`} />
        <Stat label="storage (MB)" value={data.storageMb} />
        <Stat label="connections" value={data.connections} />
        <Stat label="ingestion sources" value={data.ingestSources} />
      </div>
      {MODULES.map((m) => m.AdminOverview && <m.AdminOverview key={m.id} />)}
      <p className="text-xs text-muted-foreground">
        Counts come from the live database. Cluster and executor metrics appear once you run the
        setup kit on your servers and connect them (Engines tab).
      </p>
    </div>
  );
}

function Users() {
  const qc = useQueryClient();
  const list = useServerFn(adminListUsers);
  const setRole = useServerFn(adminSetRole);
  const setActive = useServerFn(adminSetUserActive);
  const invite = useServerFn(adminInviteUser);
  const { data: users = [], error } = useQuery({
    queryKey: ["admin_users"],
    queryFn: () => list(),
  });
  const [email, setEmail] = useState("");
  const [role, setNewRole] = useState<(typeof ROLES)[number]>("analyst");
  const [q, setQ] = useState("");
  const refresh = () => qc.invalidateQueries({ queryKey: ["admin_users"] });
  const act = async (p: Promise<unknown>, msg: string) => {
    try {
      await p;
      toast.success(msg);
      refresh();
    } catch (e) {
      toast.error((e as Error).message);
    }
  };
  if (error) return <p className="text-sm text-destructive">{(error as Error).message}</p>;
  return (
    <div className="space-y-3">
      <div className="flex flex-wrap gap-2 border p-3">
        <Input
          className="min-w-0 flex-1"
          placeholder="new.user@example.com"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
        />
        <select
          aria-label="Role"
          className="border bg-background px-2 text-sm"
          value={role}
          onChange={(e) => setNewRole(e.target.value as typeof role)}
        >
          {ROLES.map((r) => (
            <option key={r}>{r}</option>
          ))}
        </select>
        <Button
          size="sm"
          onClick={() =>
            act(
              invite({ data: { email, role } }).then(() => setEmail("")),
              "User created",
            )
          }
        >
          Add user
        </Button>
      </div>
      <Input placeholder="Search users" value={q} onChange={(e) => setQ(e.target.value)} />
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="label-mono text-left">
              <th className="p-2">email</th>
              <th className="p-2">roles</th>
              <th className="p-2">last sign-in</th>
              <th className="p-2">status</th>
            </tr>
          </thead>
          <tbody>
            {users
              .filter((u) => u.email.includes(q.toLowerCase()))
              .map((u) => (
                <tr key={u.id} className="border-t">
                  <td className="p-2 font-mono text-xs">
                    {u.email}
                    <div className="text-muted-foreground">{u.provider}</div>
                  </td>
                  <td className="p-2">
                    <div className="flex flex-wrap gap-1">
                      {ROLES.map((r) => {
                        const has = u.roles.includes(r);
                        return (
                          <button
                            key={r}
                            onClick={() =>
                              act(
                                setRole({ data: { userId: u.id, role: r, grant: !has } }),
                                `${has ? "Removed" : "Granted"} ${r}`,
                              )
                            }
                          >
                            <Badge variant={has ? "default" : "outline"}>{r}</Badge>
                          </button>
                        );
                      })}
                    </div>
                  </td>
                  <td className="p-2 font-mono text-xs">
                    {u.lastSignIn ? new Date(u.lastSignIn).toLocaleString() : "never"}
                  </td>
                  <td className="p-2">
                    <Button
                      size="sm"
                      variant={u.banned ? "default" : "outline"}
                      onClick={() =>
                        act(
                          setActive({ data: { userId: u.id, active: u.banned } }),
                          u.banned ? "Reactivated" : "Deactivated",
                        )
                      }
                    >
                      {u.banned ? "Reactivate" : "Deactivate"}
                    </Button>
                  </td>
                </tr>
              ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function Orgs() {
  const qc = useQueryClient();
  const { user } = useAuth();
  const list = useServerFn(adminListUsers);
  const setMember = useServerFn(adminSetOrgMember);
  const { data: orgs = [] } = useQuery({
    queryKey: ["organizations"],
    queryFn: async () =>
      (await supabase.from("organizations").select("*").order("name")).data ?? [],
  });
  const { data: users = [] } = useQuery({ queryKey: ["admin_users"], queryFn: () => list() });
  const [name, setName] = useState("");
  const refresh = () => {
    qc.invalidateQueries({ queryKey: ["organizations"] });
    qc.invalidateQueries({ queryKey: ["admin_users"] });
  };
  const create = async () => {
    if (!name.trim() || !user) return;
    const { error } = await supabase
      .from("organizations")
      .insert({ name: name.trim(), created_by: user.id });
    if (error) toast.error(error.message);
    else {
      setName("");
      refresh();
    }
  };
  const update = async (
    id: string,
    patch: {
      status?: string;
      compute_quota_hours?: number;
      storage_quota_mb?: number;
      infra?: string;
    },
  ) => {
    const { error } = await supabase.from("organizations").update(patch).eq("id", id);
    if (error) toast.error(error.message);
    else refresh();
  };
  const remove = async (id: string) => {
    const { error } = await supabase.from("organizations").delete().eq("id", id);
    if (error) toast.error(error.message);
    else refresh();
  };
  return (
    <div className="space-y-3">
      <div className="flex gap-2 border p-3">
        <Input
          placeholder="Organization name"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
        <Button size="sm" onClick={create}>
          Create
        </Button>
      </div>
      {orgs.map((o) => (
        <div key={o.id} className="space-y-2 border p-3">
          <div className="flex flex-wrap items-center gap-2">
            <span className="font-semibold">{o.name}</span>
            <Badge variant={o.status === "active" ? "default" : "destructive"}>{o.status}</Badge>
            <div className="ml-auto flex gap-2">
              <Button
                size="sm"
                variant="outline"
                onClick={() =>
                  update(o.id, { status: o.status === "active" ? "suspended" : "active" })
                }
              >
                {o.status === "active" ? "Suspend" : "Activate"}
              </Button>
              <Button size="sm" variant="outline" onClick={() => remove(o.id)}>
                Delete
              </Button>
            </div>
          </div>
          <div className="grid grid-cols-1 gap-2 text-xs sm:grid-cols-3">
            <label>
              Compute quota (hours/month)
              <Input
                type="number"
                defaultValue={o.compute_quota_hours}
                onBlur={(e) => update(o.id, { compute_quota_hours: +e.target.value })}
              />
            </label>
            <label>
              Storage quota (MB)
              <Input
                type="number"
                defaultValue={o.storage_quota_mb}
                onBlur={(e) => update(o.id, { storage_quota_mb: +e.target.value })}
              />
            </label>
            <label>
              Infrastructure
              <select
                className="block w-full border bg-background p-2"
                defaultValue={o.infra}
                onChange={(e) => update(o.id, { infra: e.target.value })}
              >
                <option>shared</option>
                <option>dedicated</option>
              </select>
            </label>
          </div>
          <div className="flex flex-wrap gap-1">
            {users.map((u) => {
              const m = u.orgs.includes(o.id);
              return (
                <button
                  key={u.id}
                  onClick={async () => {
                    try {
                      await setMember({ data: { orgId: o.id, userId: u.id, member: !m } });
                      refresh();
                    } catch (e) {
                      toast.error((e as Error).message);
                    }
                  }}
                >
                  <Badge variant={m ? "default" : "outline"} className="font-mono text-[10px]">
                    {u.email}
                  </Badge>
                </button>
              );
            })}
          </div>
        </div>
      ))}
      {orgs.length === 0 && <p className="text-sm text-muted-foreground">No organizations yet.</p>}
    </div>
  );
}

function Settings() {
  const qc = useQueryClient();
  const { user } = useAuth();
  const { data = [] } = useQuery({
    queryKey: ["platform_settings"],
    queryFn: async () =>
      (await supabase.from("platform_settings").select("*").order("key")).data ?? [],
  });
  const [drafts, setDrafts] = useState<Record<string, string>>({});
  const save = async (key: string) => {
    let value: unknown;
    try {
      value = JSON.parse(drafts[key]!);
    } catch {
      toast.error('Value must be valid JSON (e.g. true, 480, {"a":1})');
      return;
    }
    const { error } = await supabase.from("platform_settings").upsert({
      key,
      value: value as never,
      updated_by: user?.id ?? null,
      updated_at: new Date().toISOString(),
    });
    if (error) toast.error(error.message);
    else {
      toast.success("Saved");
      qc.invalidateQueries({ queryKey: ["platform_settings"] });
    }
  };
  return (
    <div className="space-y-2">
      {data.map((s) => (
        <div key={s.key} className="flex flex-wrap items-center gap-2 border p-3">
          <span className="w-56 font-mono text-xs">{s.key}</span>
          <Input
            className="min-w-0 flex-1 font-mono text-xs"
            defaultValue={JSON.stringify(s.value)}
            onChange={(e) => setDrafts((d) => ({ ...d, [s.key]: e.target.value }))}
          />
          <Button size="sm" disabled={drafts[s.key] === undefined} onClick={() => save(s.key)}>
            Save
          </Button>
        </div>
      ))}
    </div>
  );
}

function AdminAudit() {
  const verify = useServerFn(adminVerifyAudit);
  const [result, setResult] = useState<string | null>(null);
  const { data = [] } = useQuery({
    queryKey: ["admin_audit"],
    refetchInterval: 15_000,
    queryFn: async () =>
      (await supabase.from("admin_audit").select("*").order("id", { ascending: false }).limit(300))
        .data ?? [],
  });
  const exportCsv = () => {
    const rows = [
      ["id", "created_at", "actor", "action", "target", "hash"],
      ...data.map((r) => [
        r.id,
        r.created_at,
        r.actor ?? "",
        r.action,
        r.target ?? "",
        r.hash ?? "",
      ]),
    ];
    const blob = new Blob(
      [rows.map((r) => r.map((c) => `"${String(c).replace(/"/g, '""')}"`).join(",")).join("\n")],
      { type: "text/csv" },
    );
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "meridian-admin-audit.csv";
    a.click();
  };
  return (
    <div className="space-y-3">
      <div className="flex flex-wrap gap-2">
        <Button
          size="sm"
          onClick={async () => {
            try {
              const r = await verify();
              setResult(
                r.ok
                  ? `Chain intact — ${r.checked} entries verified`
                  : `Chain BROKEN at entry #${r.broken_at}`,
              );
            } catch (e) {
              toast.error((e as Error).message);
            }
          }}
        >
          Verify integrity
        </Button>
        <Button size="sm" variant="outline" onClick={exportCsv}>
          Export CSV
        </Button>
        {result && (
          <span
            className={`text-sm ${result.startsWith("Chain intact") ? "text-success" : "text-destructive"}`}
          >
            {result}
          </span>
        )}
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="label-mono text-left">
              <th className="p-2">#</th>
              <th className="p-2">when</th>
              <th className="p-2">action</th>
              <th className="p-2">target</th>
              <th className="p-2">hash</th>
            </tr>
          </thead>
          <tbody>
            {data.map((r) => (
              <tr key={r.id} className="border-t font-mono">
                <td className="p-2">{r.id}</td>
                <td className="p-2">{new Date(r.created_at).toLocaleString()}</td>
                <td className="p-2">{r.action}</td>
                <td className="p-2">{r.target}</td>
                <td className="p-2 text-muted-foreground">{r.hash?.slice(0, 12)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="text-xs text-muted-foreground">
        Entries cannot be edited or deleted; each one includes the previous entry's hash, so any
        tampering breaks the chain.
      </p>
    </div>
  );
}

function Engines() {
  const { data = [] } = useQuery({
    queryKey: ["connections"],
    queryFn: async () =>
      (await supabase.from("connections").select("id,name,kind,status,last_tested_at")).data ?? [],
  });
  return (
    <div className="space-y-3">
      <p className="text-sm text-muted-foreground">
        Spark, Trino, Flink, MLflow, MinIO and Iceberg run on your own servers using the setup kit
        (in the project's <span className="font-mono">deploy/</span> folder). Once running, register
        each endpoint on the Connections page and it shows here.
      </p>
      <Link to="/connections" className="text-sm text-primary underline">
        Open Connections
      </Link>
      {data.map((c) => (
        <div key={c.id} className="flex items-center gap-2 border p-2 text-sm">
          <span className="font-semibold">{c.name}</span>
          <Badge variant="outline">{c.kind}</Badge>
          <Badge variant={c.status === "ok" ? "default" : "outline"}>
            {c.status ?? "untested"}
          </Badge>
          <span className="ml-auto font-mono text-xs text-muted-foreground">
            {c.last_tested_at ? new Date(c.last_tested_at).toLocaleString() : ""}
          </span>
        </div>
      ))}
    </div>
  );
}
