import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useAuth } from "@/hooks/use-auth";
import { supabase } from "@/integrations/supabase/client";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { cursorOptions, streamKey, type CatalogStream, type StreamChoice } from "./connect-view";

/** Choose discovered streams with a cursor and mode each; only discovered streams can be saved. */
type PickerProps = {
  catalog: CatalogStream[];
  kafka: boolean;
  /** CDC streams read the change log, without a cursor or mode choice. */
  cdc?: boolean;
  configured: string[];
  busy?: boolean;
  onSave: (streams: StreamChoice[]) => void | Promise<void>;
  saveLabel?: string;
};

export function StreamPicker(props: PickerProps) {
  return props.kafka ? <KafkaStreamPicker {...props} /> : <StreamChoices {...props} />;
}

function KafkaStreamPicker(props: PickerProps) {
  const { user, workspace, loading } = useAuth();
  if (loading) return <p role="status">Loading schema access…</p>;
  if (!user) return <p role="alert">Sign in to configure Kafka streams.</p>;
  const scope = `${user.id}:${workspace?.workspaceId ?? "personal"}`;
  return <KafkaStreamScope key={scope} {...props} scope={scope} />;
}

function KafkaStreamScope({ scope, ...props }: PickerProps & { scope: string }) {
  const subjects = useQuery({
    queryKey: ["connect-reader-subjects", scope],
    queryFn: async ({ signal }) => {
      // The signed-in client applies subject RLS; registry credentials never enter this query.
      const result = await supabase
        .from("schema_subjects")
        .select("id,name")
        .eq("format", "avro")
        .order("name")
        .abortSignal(signal);
      if (result.error) throw new Error("Reader schemas unavailable");
      return result.data;
    },
    retry: false,
    staleTime: Infinity,
    gcTime: 0,
  });
  return (
    <StreamChoices
      {...props}
      readers={{
        subjects: subjects.data ?? [],
        loading: subjects.isPending,
        error: subjects.isError,
      }}
    />
  );
}

function StreamChoices({
  catalog,
  kafka,
  cdc = false,
  configured,
  busy,
  onSave,
  saveLabel = "Save streams",
  readers,
}: PickerProps & {
  readers?: {
    subjects: { id: string; name: string }[];
    loading: boolean;
    error: boolean;
  };
}) {
  const [picked, setPicked] = useState<
    Record<string, Pick<StreamChoice, "cursor" | "mode" | "readerSchemaSubjectId">>
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
      {readers?.loading && (
        <p role="status" className="text-sm">
          Loading Avro reader schemas…
        </p>
      )}
      {readers?.error && (
        <p role="alert" className="text-sm text-destructive">
          Avro reader schemas could not be loaded. Automatic writer schemas are still available.
        </p>
      )}
      {readers && (
        <p className="text-xs text-muted-foreground">
          Automatic uses each record’s writer schema. Choosing a registered Avro subject pins its
          latest readable version when saved.
        </p>
      )}
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
                <>
                  <span className="text-xs text-muted-foreground">Read by partition offset</span>
                  <select
                    aria-label={`Avro reader schema for ${key}`}
                    className="border bg-background p-1"
                    value={p.readerSchemaSubjectId ?? ""}
                    disabled={busy || readers?.loading || readers?.error}
                    onChange={(e) =>
                      setPicked((all) => {
                        const automatic = { cursor: p.cursor, mode: p.mode };
                        return {
                          ...all,
                          [key]: e.target.value
                            ? { ...automatic, readerSchemaSubjectId: e.target.value }
                            : automatic,
                        };
                      })
                    }
                  >
                    <option value="">Automatic writer schemas</option>
                    {readers?.subjects.map((subject) => (
                      <option key={subject.id} value={subject.id}>
                        {subject.name}
                      </option>
                    ))}
                  </select>
                </>
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
