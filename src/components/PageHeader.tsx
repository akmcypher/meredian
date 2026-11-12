import type { ReactNode } from "react";

export function PageHeader({
  kicker,
  title,
  desc,
  actions,
}: {
  kicker: string;
  title: string;
  desc: string;
  actions?: ReactNode;
}) {
  return (
    <div className="mb-6 flex flex-wrap items-end justify-between gap-4 border-b pb-6 md:mb-8">
      <div className="min-w-0">
        <div className="label-mono text-primary">{kicker}</div>
        <h1 className="mt-1 text-2xl font-bold md:text-3xl tracking-tight">{title}</h1>
        <p className="mt-1 max-w-xl text-sm text-muted-foreground">{desc}</p>
      </div>
      <div className="flex w-full flex-wrap gap-2 sm:w-auto">{actions}</div>
    </div>
  );
}

export function Stat({
  label,
  value,
  tone,
}: {
  label: string;
  value: ReactNode;
  tone?: "primary" | "destructive" | "success" | undefined;
}) {
  const c =
    tone === "primary"
      ? "text-primary"
      : tone === "destructive"
        ? "text-destructive"
        : tone === "success"
          ? "text-success"
          : "";
  return (
    <div className="min-w-0 border bg-card p-4">
      <div className="label-mono leading-snug">{label}</div>
      <div className={`mt-2 truncate font-mono text-2xl font-semibold md:text-3xl ${c}`}>
        {value}
      </div>
    </div>
  );
}
