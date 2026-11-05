import { createFileRoute, Link } from "@tanstack/react-router";
import { ArrowRight } from "lucide-react";
import { PageHeader } from "@/components/PageHeader";
import { MODULES } from "@/lib/modules";

export const Route = createFileRoute("/")({
  head: () => ({
    meta: [
      { title: "Overview — Meridian" },
      {
        name: "description",
        content:
          "Bring data in, see where it landed, and check every installed product at a glance.",
      },
      { property: "og:title", content: "Overview — Meridian" },
      {
        property: "og:description",
        content: "Ingestion and the installed Meridian products at a glance.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary" },
    ],
  }),
  component: Overview,
});

const INGEST_CARDS = [
  {
    to: "/ingest" as const,
    t: "Ingestion",
    d: "Legacy REST/push raw landing and source registration. Use Meridian Connect for database and topic replication.",
  },
  {
    to: "/ingest/connectors" as const,
    t: "Meridian Connect · preview",
    d: "Primary PostgreSQL, MySQL and Kafka workflow: independent workers, durable checkpoints and raw Bronze.",
  },
  {
    to: "/partitions" as const,
    t: "Large files and tables",
    d: "Split big sources into parts that load in parallel and publish in one step.",
  },
];

function Overview() {
  return (
    <div>
      <PageHeader
        kicker="control plane"
        title="Overview"
        desc="One place to bring data in and trust what landed."
      />
      <div className="grid gap-4 md:grid-cols-3">
        {INGEST_CARDS.map((c) => (
          <Link
            key={c.to}
            to={c.to}
            className="group border bg-card p-6 transition hover:border-primary"
          >
            <div className="flex items-center justify-between text-lg font-semibold">
              {c.t}
              <ArrowRight className="h-4 w-4 transition group-hover:translate-x-1 group-hover:text-primary" />
            </div>
            <p className="mt-2 text-sm text-muted-foreground">{c.d}</p>
          </Link>
        ))}
      </div>
      {MODULES.map((m) => m.Overview && <m.Overview key={m.id} />)}
    </div>
  );
}
