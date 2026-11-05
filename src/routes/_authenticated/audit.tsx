import { createFileRoute } from "@tanstack/react-router";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import { PageHeader } from "@/components/PageHeader";
import { Input } from "@/components/ui/input";
import { useAuth } from "@/hooks/use-auth";

export const Route = createFileRoute("/_authenticated/audit")({
  head: () => ({
    meta: [
      { title: "Audit log — Meridian" },
      {
        name: "description",
        content: "Who ran, uploaded, approved and configured what, and when.",
      },
      { property: "og:title", content: "Audit log — Meridian" },
      { property: "og:description", content: "Platform-wide audit trail." },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary" },
    ],
  }),
  component: AuditPage,
});

function AuditPage() {
  const { hasRole } = useAuth();
  const [q, setQ] = useState("");
  const { data = [] } = useQuery({
    queryKey: ["audit_log"],
    refetchInterval: 15_000,
    queryFn: async () =>
      (
        await supabase
          .from("audit_log")
          .select("*")
          .order("created_at", { ascending: false })
          .limit(500)
      ).data ?? [],
  });
  const rows = data.filter(
    (r) =>
      !q ||
      `${r.action} ${r.target} ${r.actor_email} ${JSON.stringify(r.details)}`
        .toLowerCase()
        .includes(q.toLowerCase()),
  );
  return (
    <div>
      <PageHeader
        kicker="governance"
        title="Audit log"
        desc="Every query run, upload, approval, job run and connection change is recorded. Only admins and stewards can read it."
      />
      {!hasRole("admin", "steward") ? (
        <div className="border bg-card p-4 text-sm text-muted-foreground">
          You need the admin or steward role to view the audit log.
        </div>
      ) : (
        <>
          <Input
            className="mb-3 max-w-sm"
            placeholder="Filter…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
          <div className="overflow-x-auto border bg-card">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left">
                  <th className="label-mono p-2">when</th>
                  <th className="label-mono p-2">who</th>
                  <th className="label-mono p-2">action</th>
                  <th className="label-mono p-2">target</th>
                  <th className="label-mono p-2">details</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.id} className="border-b align-top">
                    <td className="whitespace-nowrap p-2 font-mono text-xs">
                      {new Date(r.created_at).toLocaleString()}
                    </td>
                    <td className="p-2 text-xs">{r.actor_email}</td>
                    <td className="p-2 font-mono text-xs font-semibold">{r.action}</td>
                    <td className="p-2 font-mono text-xs">{r.target}</td>
                    <td
                      className="max-w-md truncate p-2 font-mono text-[11px] text-muted-foreground"
                      title={JSON.stringify(r.details)}
                    >
                      {JSON.stringify(r.details)}
                    </td>
                  </tr>
                ))}
                {!rows.length && (
                  <tr>
                    <td colSpan={5} className="p-4 text-muted-foreground">
                      Nothing recorded yet.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
