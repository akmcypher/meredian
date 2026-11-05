import {
  BarChart3,
  ClipboardCheck,
  Database,
  Layers,
  LayoutGrid,
  MailCheck,
  ScrollText,
  Server,
  ShieldCheck,
  Upload,
  Workflow,
} from "lucide-react";
import { MODULES, type MeridianModule, type NavItem } from "@/lib/modules";

type Section = { id: string; label: string; icon: NavItem["icon"]; items: NavItem[] };

/** The shell: section headings and the destinations every distribution has. */
const SHELL: Section[] = [
  { id: "transform", label: "Transform", icon: Workflow, items: [] },
  {
    id: "engineering",
    label: "Data Engineering",
    icon: Workflow,
    items: [
      { to: "/ingest", label: "Ingestion", icon: Upload, order: 10 },
      { to: "/ingest/connectors", label: "Meridian Connect · preview", icon: Database, order: 20 },
      { to: "/data", label: "Data Upload", icon: Upload, order: 30 },
      { to: "/partitions", label: "Partitioned Ingestion", icon: Layers, order: 40 },
    ],
  },
  { id: "management", label: "Data Management", icon: Database, items: [] },
  {
    id: "governance",
    label: "Governance & Data Quality",
    icon: ShieldCheck,
    items: [
      { to: "/approvals", label: "Approvals", icon: ClipboardCheck, order: 30 },
      { to: "/audit", label: "Audit Log", icon: ScrollText, order: 50 },
    ],
  },
  { id: "analytics", label: "Analytics & Workspace", icon: BarChart3, items: [] },
  {
    id: "compute",
    label: "Integrations & Compute",
    icon: Server,
    items: [{ to: "/connections", label: "Connections", icon: Server, order: 10 }],
  },
  {
    id: "administration",
    label: "Administration",
    icon: ShieldCheck,
    items: [
      { to: "/admin", label: "Admin Console", icon: ShieldCheck, order: 10 },
      { to: "/settings", label: "Alert Settings", icon: MailCheck, order: 20 },
    ],
  },
];

const byOrder = (a: NavItem, b: NavItem) => a.order - b.order;

/** Shell sections plus every installed module's entries; sections left empty are hidden. */
export function buildNavigation(modules: readonly MeridianModule[]) {
  const global: NavItem[] = [
    { to: "/", label: "Overview", icon: LayoutGrid, order: 10 },
    ...modules.flatMap((m) => m.globalNav ?? []),
  ].sort(byOrder);
  const sections = SHELL.map((section) => ({
    ...section,
    items: [
      ...section.items,
      ...modules.flatMap((m) =>
        (m.nav ?? []).filter((n) => n.section === section.id).flatMap((n) => n.items),
      ),
    ].sort(byOrder),
  })).filter((section) => section.items.length > 0);
  return { global, sections };
}

const NAVIGATION = buildNavigation(MODULES);
export const GLOBAL_NAV = NAVIGATION.global;
export const NAV_SECTIONS = NAVIGATION.sections;

export function isNavigationActive(path: string, destination: string) {
  if (destination === "/ingest" || destination === "/transform") return path === destination;
  return path === destination || (destination !== "/" && path.startsWith(destination + "/"));
}
export function activeNavigationSection(path: string) {
  return NAV_SECTIONS.find((section) =>
    section.items.some((item) => isNavigationActive(path, item.to)),
  )?.id;
}
