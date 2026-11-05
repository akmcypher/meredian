import { createServerFn } from "@tanstack/react-start";
import { z } from "zod";
import { requireSupabaseAuth } from "@/integrations/supabase/auth-middleware";
import { validateObjectPrefix, FILE_READ_LIMITS } from "./file-manifest";
import { copyVerifiedFileRun } from "./file-publication";
import type { ObjectStore } from "./file-ingest";

export const publishVerifiedFileRun = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((input: unknown) =>
    z
      .object({
        prefix: z.string().max(900),
        name: z.string().regex(/^[a-z_][a-z0-9_]{0,62}$/),
        organizationId: z.string().uuid().nullable(),
      })
      .parse(input),
  )
  .handler(async ({ data, context }) => {
    validateObjectPrefix(data.prefix);
    if (!data.prefix.startsWith(`${context.userId}/runs/`))
      throw new Error("Publication staging scope is invalid");
    if (data.organizationId) {
      const member = await context.supabase.rpc("is_org_member", { _org: data.organizationId });
      if (member.error || !member.data) throw new Error("Organization membership required");
    }
    const destination = `${context.userId}/published/${crypto.randomUUID()}`;
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    const bucket = supabaseAdmin.storage.from("ingest-files");
    const created: string[] = [];
    const store = {
      async get(key: string) {
        if (!key.startsWith(`${data.prefix}/`) && !key.startsWith(`${destination}/`))
          throw new Error("Object out of publication scope");
        const info = await bucket.info(key);
        if (info.error || !info.data) return null;
        const size = Number(info.data.size);
        const limit = key.endsWith("/_manifest.json")
          ? FILE_READ_LIMITS.manifestBytes
          : FILE_READ_LIMITS.partBytes;
        if (!Number.isFinite(size) || size > limit)
          throw new Error("Publication object exceeds byte limit");
        const read = await bucket.download(key);
        return read.data ? new Uint8Array(await read.data.arrayBuffer()) : null;
      },
      async put(key: string, bytes: Uint8Array) {
        if (!key.startsWith(`${destination}/`)) throw new Error("Invalid publication destination");
        const upload = await bucket.upload(key, new Blob([bytes as BlobPart]), { upsert: false });
        if (upload.error) throw new Error("Publication copy failed");
        created.push(key);
      },
    } as ObjectStore;
    let registered = false;
    try {
      const { manifest, digest } = await copyVerifiedFileRun(store, data.prefix, destination);
      const args = {
        _owner: context.userId,
        _name: data.name,
        _prefix: destination,
        _columns: manifest.columns,
        _parts: manifest.parts.length,
        _rows: manifest.totalRows,
        _source_sha: manifest.sourceSha256,
        _manifest_sha: digest,
      };
      const result = data.organizationId
        ? await supabaseAdmin.rpc("register_verified_file_table", {
            ...args,
            _org: data.organizationId,
          })
        : await supabaseAdmin.rpc("register_personal_verified_file_table", args);
      if (result.error) throw new Error("Verified table registration failed");
      registered = true;
      return { id: result.data, prefix: destination };
    } finally {
      if (!registered && created.length) {
        // A lost RPC response may hide a successful commit. Never delete possibly registered bytes.
        const committed = await supabaseAdmin
          .from("file_tables")
          .select("id")
          .eq("manifest_prefix", destination)
          .maybeSingle();
        if (!committed.error && !committed.data) await bucket.remove(created);
      }
    }
  });
