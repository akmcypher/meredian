import { Copy, Download, KeyRound } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import type { SecretField } from "@/lib/connect-setup";

/**
 * The agent's private `<reference>.json`, with secrets as placeholders. It is built in the browser
 * from non-secret settings only; secret values are typed on the agent host, never here.
 */
export function AgentFileTemplate({
  credentialRef,
  agentFile,
  secrets,
}: {
  credentialRef: string;
  agentFile: Record<string, unknown>;
  secrets: SecretField[];
}) {
  const text = JSON.stringify(agentFile, null, 2) + "\n";
  const file = `${credentialRef || "<reference>"}.json`;
  return (
    <div className="space-y-2 rounded-md border p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="text-sm font-medium">
          Agent file <code>{file}</code>
        </p>
        <div className="flex gap-2">
          <Button
            size="sm"
            variant="outline"
            onClick={() =>
              void navigator.clipboard
                .writeText(text)
                .then(() => toast.success("Template copied"))
                .catch(() => toast.error("Copy failed"))
            }
          >
            <Copy />
            Copy
          </Button>
          <Button
            size="sm"
            variant="outline"
            onClick={() => {
              const url = URL.createObjectURL(new Blob([text], { type: "application/json" }));
              const a = document.createElement("a");
              a.href = url;
              a.download = file;
              a.click();
              URL.revokeObjectURL(url);
            }}
          >
            <Download />
            Download template
          </Button>
        </div>
      </div>
      <pre
        className="max-h-64 overflow-auto rounded bg-muted p-2 text-xs"
        aria-label="Agent file template"
      >
        {text}
      </pre>
      {secrets.length > 0 && (
        <div className="text-sm">
          <p className="flex items-center gap-1 font-medium">
            <KeyRound className="h-4 w-4" />
            Fill in on the agent host (never entered in Meridian):
          </p>
          <ul className="ml-5 list-disc text-muted-foreground">
            {secrets.map((s) => (
              <li key={s.key}>
                <code>{s.key}</code> — {s.label}
                {s.hint ? `. ${s.hint}` : ""}
              </li>
            ))}
          </ul>
        </div>
      )}
      <p className="text-xs text-muted-foreground">
        Save it in the agent's <code>CONNECT_SECRET_DIR</code> (certificate files next to it). The
        agent reads it for every run; Meridian stores only the non-secret settings shown above.
      </p>
    </div>
  );
}

/**
 * The source's agent token (`INGEST_TOKEN_FILE`), which lets an agent claim this source's runs.
 * The server returns it once at registration and stores only its hash, so it is shown only here.
 */
export function AgentToken({ token }: { token: string }) {
  return (
    <div className="space-y-2 rounded-md border border-amber-500/50 p-3 text-sm">
      <p className="font-medium">Agent token — shown once</p>
      <div className="flex flex-wrap items-center gap-2">
        <code className="break-all rounded bg-muted px-2 py-1 text-xs" aria-label="Agent token">
          {token}
        </code>
        <Button
          size="sm"
          variant="outline"
          onClick={() =>
            void navigator.clipboard
              .writeText(token)
              .then(() => toast.success("Token copied"))
              .catch(() => toast.error("Copy failed"))
          }
        >
          <Copy />
          Copy
        </Button>
      </div>
      <p className="text-muted-foreground">
        Save it in a file on the agent host and start the agent with <code>MERIDIAN_URL</code>,{" "}
        <code>INGEST_TOKEN_FILE</code> (that file) and <code>CONNECT_SECRET_DIR</code> (the folder
        with the agent file). Meridian keeps only its hash; if it is lost, register the source
        again.
      </p>
    </div>
  );
}
