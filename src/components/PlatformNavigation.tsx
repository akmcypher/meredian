import { useId } from "react";
import { Link } from "@tanstack/react-router";
import { ChevronDown } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { GLOBAL_NAV, NAV_SECTIONS, isNavigationActive } from "./navigation-items";

export function PlatformNavigation({
  path,
  expanded,
  onToggle,
  onNavigate,
}: {
  path: string;
  expanded: Record<string, boolean>;
  onToggle: (section: string) => void;
  onNavigate?: (() => void) | undefined;
}) {
  const prefix = useId();
  const linkStyle =
    "flex min-h-10 items-center gap-2.5 rounded-sm px-3 py-2 text-[13px] leading-5 transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring";
  return (
    <nav
      aria-label="Platform navigation"
      className="min-h-0 flex-1 overflow-y-auto overscroll-contain px-3 pb-4"
    >
      <ul className="mb-5 space-y-1">
        {GLOBAL_NAV.map(({ to, label, icon: Icon }) => {
          const active = isNavigationActive(path, to);
          return (
            <li key={to}>
              <Link
                to={to}
                onClick={onNavigate}
                aria-current={active ? "page" : undefined}
                className={cn(
                  linkStyle,
                  active
                    ? "bg-primary text-primary-foreground"
                    : "text-ink-foreground/75 hover:bg-ink-foreground/10 hover:text-ink-foreground",
                )}
              >
                <Icon className="size-4 shrink-0" />
                {label}
              </Link>
            </li>
          );
        })}
      </ul>
      <div className="space-y-2">
        {NAV_SECTIONS.map((section) => {
          const active = section.items.some((item) => isNavigationActive(path, item.to));
          const open = expanded[section.id] ?? false;
          const controlId = `${prefix}-${section.id}`;
          return (
            <div key={section.id}>
              <Button
                type="button"
                variant="ghost"
                aria-expanded={open}
                aria-controls={controlId}
                id={`${controlId}-heading`}
                onClick={() => onToggle(section.id)}
                className={cn(
                  "h-auto min-h-11 w-full justify-start gap-2.5 rounded-sm px-3 py-2 text-left text-[13px] leading-5 whitespace-normal hover:bg-ink-foreground/10 hover:text-ink-foreground",
                  active ? "bg-ink-foreground/10 text-ink-foreground" : "text-ink-foreground/75",
                )}
              >
                <section.icon className="size-4 shrink-0" />
                <span className="min-w-0 flex-1">{section.label}</span>
                <ChevronDown
                  className={cn(
                    "size-3.5 shrink-0 transition-transform motion-reduce:transition-none",
                    open ? "rotate-0" : "-rotate-90",
                  )}
                />
              </Button>
              <ul
                id={controlId}
                aria-labelledby={`${controlId}-heading`}
                hidden={!open}
                className="ml-5 mt-1 space-y-0.5 border-l border-ink-foreground/15 pl-2"
              >
                {section.items.map(({ to, label, icon: Icon }) => {
                  const current = isNavigationActive(path, to);
                  return (
                    <li key={to}>
                      <Link
                        to={to}
                        aria-current={current ? "page" : undefined}
                        onClick={onNavigate}
                        className={cn(
                          linkStyle,
                          "gap-2",
                          current
                            ? "bg-primary text-primary-foreground font-medium"
                            : "text-ink-foreground/65 hover:bg-ink-foreground/10 hover:text-ink-foreground",
                        )}
                      >
                        <Icon className="size-3.5 shrink-0" />
                        <span>{label}</span>
                      </Link>
                    </li>
                  );
                })}
              </ul>
            </div>
          );
        })}
      </div>
    </nav>
  );
}
