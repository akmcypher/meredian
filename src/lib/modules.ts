/**
 * Product modules plug into the app shell here. Every file in `src/modules/` default-exports a
 * MeridianModule and is discovered at build time, so a distribution contains exactly the
 * modules whose files it ships: the shell never imports a module by name.
 */
import type { ComponentType, ReactNode } from "react";
import type { LucideIcon } from "lucide-react";

export type NavItem = { to: string; label: string; icon: LucideIcon; order: number };

export type MeridianModule = {
  id: string;
  /** Where a signed-in user lands; the first installed module with one wins, else Overview. */
  home?: string;
  /** Sidebar entries, placed into the shell's sections by section id. */
  nav?: { section: string; items: NavItem[] }[];
  /** Entries shown above the sections. */
  globalNav?: NavItem[];
  /** Wraps the signed-in workspace; remounted on every identity change. */
  Provider?: ComponentType<{ children: ReactNode }>;
  /** Rendered once inside the workspace, after the shell. */
  Background?: ComponentType;
  /** Sections on the Overview page. */
  Overview?: ComponentType;
  /** Sections under the admin console's overview counts. */
  AdminOverview?: ComponentType;
  /** Extra tabs in the admin console. */
  adminTabs?: { id: string; label: string; Component: ComponentType }[];
  /** Extra panels on the Connections page. */
  connectionPanels?: ComponentType[];
};

const found = import.meta.glob<{ default: MeridianModule }>("../modules/*.tsx", { eager: true });

export const MODULES: readonly MeridianModule[] = Object.keys(found)
  .sort()
  .map((k) => found[k]!.default);
