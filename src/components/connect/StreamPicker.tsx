import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { cursorOptions, streamKey, type CatalogStream, type StreamChoice } from "./connect-view";

/** Choose discovered streams with a cursor and mode each; only discovered streams can be saved. */
export function StreamPicker({
  catalog,
  kafka,
  cdc = false,
  configured,
  busy,
  onSave,
  saveLabel = "Save streams",
}: {
  catalog: CatalogStream[];
  kafka: boolean;
  /** A CDC connection: every stream reads the change log, so there is no cursor or mode to pick. */
  cdc?: boolean;
  configured: string[];
  busy?: boolean;
  onSave: (streams: StreamChoice[]) => void | Promise<void>;
  saveLabel?: string;
}) {
  const [picked, setPicked] = useState<
    Record<string, { cursor: string; mode: StreamChoice["mode"] }>
  >({});
  if (!catalog.length)
    return <p className="text-sm text-muted-foreground">No streams discovered yet.</p>;
  const choices = catalog
    .filter((s) => picked[streamKey(s)])
    .map((s) => ({
      namespace: s.namespace,
      name: s.name,
      primaryKey: s.primary_key,
      ...picked[streamKey(s)]!,
    }));
  return (
    <div className="space-y-2">
      <ul className="divide-y rounded-md border" aria-label="Discovered streams">
        {catalog.map((s) => {
          const key = streamKey(s);
          const p = picked[key];
          const cursors = cursorOptions(s, kafka);
          const id = `stream-${key}`;
          return (
            <li key={key} className="flex flex-wrap items-center gap-3 p-2 text-sm">
              <Checkbox
                id={id}
                checked={!!p}
                onCheckedChange={(v) =>
                  setPicked((all) => {
                    const next = { ...all };
                    if (v)
                      next[key] = cdc
                        ? // The cursor column is stored but not used by CDC; the first key column fits.
                          { cursor: [s.primary_key].flat()[0] ?? "", mode: "cdc" }
                        : { cursor: cursors[0] ?? "", mode: "incremental" };
                    else delete next[key];
                    return next;
                  })
                }
              />
              <label htmlFor={id} className="min-w-0 flex-1 font-medium">
                {key}
                <span className="ml-2 text-xs font-normal text-muted-foreground">
                  key {[s.primary_key].flat().join(", ")}
                  {configured.includes(key) ? " · configured" : ""}
                </span>
              </label>
              {p && cdc && (
                <span className="text-xs text-muted-foreground">
                  Copied once, then every insert, update and delete from the log
                </span>
              )}
              {p && !kafka && !cdc && (
                <>
                  <select
                    aria-label={`Cursor for ${key}`}
                    className="border bg-background p-1"
                    value={p.cursor}
                    onChange={(e) =>
                      setPicked((a) => ({ ...a, [key]: { ...p, cursor: e.target.value } }))
                    }
                  >
                    {cursors.map((c) => (
                      <option key={c}>{c}</option>
                    ))}
                  </select>
                  <select
                    aria-label={`Mode for ${key}`}
                    className="border bg-background p-1"
                    value={p.mode}
                    onChange={(e) =>
                      setPicked((a) => ({
                        ...a,
                        [key]: { ...p, mode: e.target.value as StreamChoice["mode"] },
                      }))
                    }
                  >
                    <option value="incremental">Incremental (cursor)</option>
                    <option value="full_refresh">Full refresh</option>
                  </select>
                </>
              )}
              {p && kafka && (
                <span className="text-xs text-muted-foreground">Read by partition offset</span>
              )}
            </li>
          );
        })}
      </ul>
      <Button disabled={busy || !choices.length} onClick={() => void onSave(choices)}>
        {saveLabel}
        {choices.length ? ` (${choices.length})` : ""}
      </Button>
    </div>
  );
}
