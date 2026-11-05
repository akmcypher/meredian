import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import { Button } from "@/components/ui/button";
import { ResultTable } from "@/components/ResultTable";

/** Published partitioned tables, read page-by-page with a keyset (never the whole table). */
export function PublishedTables() {
  const [open, setOpen] = useState<string | null>(null);
  const [cursor, setCursor] = useState<string[]>([]);
  const { data: tables = [] } = useQuery({
    queryKey: ["ingest_tables_all"],
    refetchInterval: 10_000,
    queryFn: async () =>
      (
        await supabase
          .from("ingest_tables")
          .select("name,row_count,published_at,current_run_id")
          .order("published_at", { ascending: false })
      ).data ?? [],
  });
  const after = cursor[cursor.length - 1] ?? null;
  const { data: page = [], isFetching } = useQuery({
    queryKey: ["ingest_page", open, after],
    enabled: !!open,
    queryFn: async () =>
      (
        await supabase.rpc(
          "read_ingest_table",
          after ? { _name: open!, _after: after, _limit: 50 } : { _name: open!, _limit: 50 },
        )
      ).data ?? [],
  });
  if (!tables.length) return null;
  return (
    <section className="rounded-lg border bg-card p-4 space-y-3" data-testid="published-tables">
      <h2 className="font-semibold">Published tables</h2>
      <div className="flex flex-wrap gap-2">
        {tables.map((t) => (
          <Button
            key={t.name}
            size="sm"
            variant={open === t.name ? "default" : "outline"}
            onClick={() => {
              setOpen(t.name);
              setCursor([]);
            }}
          >
            {t.name} · {Number(t.row_count).toLocaleString()}
          </Button>
        ))}
      </div>
      {open && (
        <>
          {isFetching && !page.length ? (
            <p className="text-sm text-muted-foreground">Loading…</p>
          ) : (
            <ResultTable
              columns={Object.keys((page[0]?.data as Record<string, unknown>) ?? {})}
              rows={page.map(
                (r) =>
                  Object.fromEntries(
                    Object.entries(r.data as Record<string, unknown>).map(([k, v]) => [
                      k,
                      v !== null && typeof v === "object" ? JSON.stringify(v) : v,
                    ]),
                  ) as never,
              )}
            />
          )}
          <div className="flex gap-2">
            <Button
              size="sm"
              variant="outline"
              disabled={!cursor.length}
              onClick={() => setCursor((c) => c.slice(0, -1))}
            >
              Previous
            </Button>
            <Button
              size="sm"
              variant="outline"
              disabled={page.length < 50}
              onClick={() => setCursor((c) => [...c, page[page.length - 1]!.pk])}
            >
              Next 50
            </Button>
          </div>
        </>
      )}
    </section>
  );
}
