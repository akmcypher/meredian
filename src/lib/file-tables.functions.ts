import { createServerFn } from "@tanstack/react-start";
import { z } from "zod";
import { requireSupabaseAuth } from "@/integrations/supabase/auth-middleware";
import type { Json } from "@/integrations/supabase/types";
import type { ObjectStore } from "./file-ingest";
import { readFileTablePage } from "./file-tables";
import { FILE_READ_LIMITS, validateObjectPrefix } from "./file-manifest";

/**
 * Teammates read file tables only here: the row is first found through the caller's own access rules,
 * parts are read on the server, and the database applies row rules + column masks before anything is returned.
 */
export const readSharedFileTable = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((d: unknown) =>
    z
      .object({
        id: z.string().uuid(),
        offset: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
        limit: z.number().int().min(1).max(FILE_READ_LIMITS.pageRows),
      })
      .parse(d),
  )
  .handler(async ({ data, context }) => {
    const { data: t, error } = await context.supabase
      .from("file_tables")
      .select(
        "id,owner_id,manifest_prefix,columns,part_count,row_count,source_sha256,manifest_sha256",
      )
      .eq("id", data.id)
      .maybeSingle();
    if (error || !t) throw new Error("Table not found");
    validateObjectPrefix(t.manifest_prefix);
    if (t.manifest_prefix.split("/")[0] !== t.owner_id)
      throw new Error("Invalid table storage scope");
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    const bucket = supabaseAdmin.storage.from("ingest-files");
    const store = {
      async get(key: string) {
        if (
          !key.startsWith(`${t.manifest_prefix}/`) ||
          key.includes("%") ||
          key.includes("..") ||
          key.includes("\\")
        )
          throw new Error("Invalid object scope");
        const info = await bucket.info(key);
        if (info.error || !info.data) throw new Error("Object not found");
        const maxBytes = key.endsWith("/_manifest.json")
          ? FILE_READ_LIMITS.manifestBytes
          : FILE_READ_LIMITS.partBytes;
        if (Number(info.data.size) > maxBytes) throw new Error("Object exceeds read limit");
        const r = await bucket.download(key);
        return r.data ? new Uint8Array(await r.data.arrayBuffer()) : null;
      },
    } as ObjectStore;
    const page = await readFileTablePage(store, t.manifest_prefix, data.offset, data.limit, t);
    const { data: safe, error: pe } = await context.supabase.rpc("apply_file_table_policies", {
      _table: t.id,
      _rows: page.rows as Json,
    });
    if (pe) throw new Error(pe.message);
    return {
      columns: page.columns,
      rows: (safe ?? []) as Record<string, string>[],
      total: page.total,
    };
  });
