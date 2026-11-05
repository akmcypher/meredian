import { audit } from "@/lib/audit";
import { createFileRoute } from "@tanstack/react-router";
import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { CheckCircle2, XCircle } from "lucide-react";
import { supabase } from "@/integrations/supabase/client";
import { PageHeader, Stat } from "@/components/PageHeader";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Textarea } from "@/components/ui/textarea";
import { useAuth } from "@/hooks/use-auth";
import { notifyEvent } from "@/lib/alerts.functions";

export const Route = createFileRoute("/_authenticated/approvals")({
  head: () => ({
    meta: [
      { title: "Approvals — Meridian" },
      {
        name: "description",
        content:
          "Data stewards review and approve SQL and PySpark transformations before they reach production.",
      },
      { property: "og:title", content: "Approvals — Meridian" },
      {
        property: "og:description",
        content: "Steward approval workflow for data transformations.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary" },
    ],
  }),
  component: Approvals,
});

type V = {
  ok?: boolean;
  rows?: number;
  columns?: string[];
  pii_columns?: string[];
  stages?: number;
  error?: string;
};
const tone = { pending: "secondary", approved: "default", rejected: "destructive" } as const;

function Approvals() {
  const { user, hasRole } = useAuth();
  const qc = useQueryClient();
  const [notes, setNotes] = useState<Record<string, string>>({});
  const [filter, setFilter] = useState("pending");
  const { data = [] } = useQuery({
    queryKey: ["change_requests"],
    queryFn: async () =>
      (await supabase.from("change_requests").select("*").order("created_at", { ascending: false }))
        .data ?? [],
  });
  const steward = hasRole("steward", "admin");
  const decide = async (id: string, status: "approved" | "rejected") => {
    if (!user) return;
    const req = data.find((r) => r.id === id);
    const { error } = await supabase
      .from("change_requests")
      .update({
        status,
        reviewed_by: user.id,
        reviewer_email: user.email ?? null,
        review_note: notes[id] ?? "",
        reviewed_at: new Date().toISOString(),
      })
      .eq("id", id);
    if (error) {
      toast.error(error.message);
      return;
    }
    void audit(`change_request.${status}`, id);
    if (req)
      void notifyEvent({
        data: {
          kind: "approval_decided",
          requestId: req.id,
          targetUserId: req.submitted_by,
          title: `"${req.title}" was ${status}`,
          detail: "",
        },
      }).catch(() => {});
    toast.success(status === "approved" ? "Approved — scheduled for deployment" : "Rejected");
    qc.invalidateQueries({ queryKey: ["change_requests"] });
  };
  const shown = data.filter((r) => filter === "all" || r.status === filter);

  return (
    <div>
      <PageHeader
        kicker="governance"
        title="Steward approvals"
        desc="Every transformation is dry-run, then reviewed by a data steward other than its author before it can be deployed."
      />
      <div className="mb-6 grid grid-cols-3 gap-4">
        <Stat
          label="pending"
          value={data.filter((r) => r.status === "pending").length}
          tone="primary"
        />
        <Stat
          label="approved"
          value={data.filter((r) => r.status === "approved").length}
          tone="success"
        />
        <Stat
          label="rejected"
          value={data.filter((r) => r.status === "rejected").length}
          tone="destructive"
        />
      </div>
      <div className="mb-4 flex gap-2">
        {["pending", "approved", "rejected", "all"].map((f) => (
          <Button
            key={f}
            size="sm"
            variant={filter === f ? "default" : "outline"}
            onClick={() => setFilter(f)}
          >
            {f}
          </Button>
        ))}
      </div>
      {!steward && (
        <p className="mb-4 text-sm text-muted-foreground">
          You can submit changes from Notebooks. Only data stewards can approve.
        </p>
      )}
      <div className="space-y-4">
        {shown.length === 0 && (
          <p className="text-sm text-muted-foreground">
            Nothing here. Submit a cell from a notebook.
          </p>
        )}
        {shown.map((r) => {
          const v = r.validation as V;
          const own = r.submitted_by === user?.id;
          return (
            <div key={r.id} className="border bg-card p-4">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant={tone[r.status as keyof typeof tone] ?? "outline"}>{r.status}</Badge>
                <span className="font-semibold">{r.title}</span>
                <span className="font-mono text-xs text-muted-foreground">
                  {r.language} → {r.target}
                </span>
                <span className="ml-auto text-xs text-muted-foreground">
                  by {r.submitter_email} · {new Date(r.created_at).toLocaleString()}
                </span>
              </div>
              {r.description && <p className="mt-2 text-sm">{r.description}</p>}
              <pre className="mt-3 max-h-48 overflow-auto bg-muted p-3 font-mono text-xs">
                {r.code}
              </pre>
              <div className="mt-3 flex flex-wrap items-center gap-3 text-sm">
                {v.ok ? (
                  <span className="flex items-center gap-1 text-success">
                    <CheckCircle2 className="h-4 w-4" />
                    Dry run passed · {v.rows} rows · {v.stages} stages
                  </span>
                ) : (
                  <span className="flex items-center gap-1 text-destructive">
                    <XCircle className="h-4 w-4" />
                    Dry run failed: {v.error}
                  </span>
                )}
                {!!v.pii_columns?.length && (
                  <Badge variant="destructive">PII: {v.pii_columns.join(", ")}</Badge>
                )}
              </div>
              {r.status !== "pending" && (
                <p className="mt-3 text-sm text-muted-foreground">
                  {r.status} by {r.reviewer_email}
                  {r.review_note ? `: “${r.review_note}”` : ""}
                </p>
              )}
              {r.status === "pending" && steward && !own && (
                <div className="mt-3 flex flex-wrap items-end gap-2">
                  <Textarea
                    className="min-h-10 flex-1"
                    placeholder="Review note"
                    value={notes[r.id] ?? ""}
                    onChange={(e) => setNotes((n) => ({ ...n, [r.id]: e.target.value }))}
                  />
                  <Button onClick={() => decide(r.id, "approved")}>Approve</Button>
                  <Button variant="destructive" onClick={() => decide(r.id, "rejected")}>
                    Reject
                  </Button>
                </div>
              )}
              {r.status === "pending" && own && (
                <p className="mt-3 text-xs text-muted-foreground">
                  Waiting for another steward — authors can't approve their own changes.
                </p>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
