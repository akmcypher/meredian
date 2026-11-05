import { createServerFn } from "@tanstack/react-start";
import { requireSupabaseAuth } from "@/integrations/supabase/auth-middleware";
import { z } from "zod";
import { COMPATIBILITY_MODES } from "./avro-schema";
export const registerSchema = createServerFn({ method: "POST" })
  .middleware([requireSupabaseAuth])
  .inputValidator((v: unknown) =>
    z
      .object({
        subject: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$/),
        organizationId: z.string().uuid().nullable().optional(),
        schema: z.unknown().refine((s) => JSON.stringify(s ?? null).length <= 262144),
        compatibility: z.enum(COMPATIBILITY_MODES as [string, ...string[]]).optional(),
      })
      .parse(v),
  )
  .handler(async ({ context, data }) => {
    const { supabaseAdmin } = await import("@/integrations/supabase/client.server");
    const { registerSchemaVersion } = await import("./schema-registry.server");
    return registerSchemaVersion(context.supabase, supabaseAdmin, context.userId, {
      ...data,
      compatibility: data.compatibility as never,
    });
  });
