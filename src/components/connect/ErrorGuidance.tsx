import { AlertTriangle } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { guidance, type GuidanceAction } from "@/lib/connect-guidance";
import { HEALTH_LABEL, type HealthState } from "@/lib/connect-health";

const ACTION_LABEL: Record<Exclude<GuidanceAction, "none">, string> = {
  check: "Test connection",
  discover: "Discover streams",
  settings: "Review settings",
  runs: "Open run report",
  retry: "Sync now",
};

/** A run or event code as plain guidance with one next action. */
export function ErrorGuidance({
  code,
  onAction,
  disabled,
}: {
  code: string | null | undefined;
  onAction?: (action: GuidanceAction) => void;
  disabled?: boolean;
}) {
  const g = guidance(code);
  if (!g) return null;
  return (
    <Alert variant="destructive" data-code={code}>
      <AlertTriangle className="h-4 w-4" />
      <AlertTitle>{g.title}</AlertTitle>
      <AlertDescription className="space-y-2">
        <p>{g.explanation}</p>
        <p>
          <strong>Next:</strong> {g.next}
        </p>
        <div className="flex flex-wrap items-center gap-2">
          {onAction && g.action !== "none" && (
            <Button
              size="sm"
              variant="outline"
              disabled={disabled}
              onClick={() => onAction(g.action)}
            >
              {ACTION_LABEL[g.action]}
            </Button>
          )}
          <code className="text-xs text-muted-foreground">{code}</code>
          {g.terminal && (
            <span className="text-xs text-muted-foreground">Retrying alone will not fix this.</span>
          )}
        </div>
      </AlertDescription>
    </Alert>
  );
}

const HEALTH_VARIANT: Record<HealthState, "default" | "secondary" | "destructive" | "outline"> = {
  healthy: "default",
  attention: "destructive",
  untested: "outline",
  paused: "secondary",
  running: "secondary",
};

export function HealthBadge({ state }: { state: HealthState }) {
  return <Badge variant={HEALTH_VARIANT[state]}>{HEALTH_LABEL[state]}</Badge>;
}
