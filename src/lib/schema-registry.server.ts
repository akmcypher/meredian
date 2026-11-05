// Server-side schema registration. The compatibility check
// runs here with avro-schema.ts; schema_register_version is service-only and refuses a write when
// another version landed after the one we checked against, so no incompatible version slips in.
import type { SupabaseClient } from "@supabase/supabase-js";
import {
  checkCompatibility,
  parseAvroSchema,
  type CompatibilityMode,
  type SchemaIssue,
} from "./avro-schema";
import { sha256 } from "./ingest";

/** Key-sorted JSON, so the same schema always has the same fingerprint. */
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object")
    return `{${Object.keys(value)
      .sort()
      .map((k) => `${JSON.stringify(k)}:${canonicalJson((value as Record<string, unknown>)[k])}`)
      .join(",")}}`;
  return JSON.stringify(value);
}

export type RegisterResult =
  | { compatible: true; subjectId: string; version: number; created: boolean }
  | { compatible: false; issues: (SchemaIssue & { version: number; direction: string })[] };

export async function registerSchemaVersion(
  // The caller's RLS client: history is read with the caller's rights.
  user: SupabaseClient,
  admin: SupabaseClient,
  actor: string,
  input: {
    subject: string;
    organizationId?: string | null | undefined;
    schema: unknown;
    compatibility?: CompatibilityMode | undefined;
  },
): Promise<RegisterResult> {
  const parsed = parseAvroSchema(input.schema);
  const fingerprint = await sha256(canonicalJson(input.schema));
  const org = input.organizationId ?? null;
  for (let attempt = 0; attempt < 3; attempt++) {
    let q = user.from("schema_subjects").select("id,compatibility").eq("name", input.subject);
    q = org ? q.eq("organization_id", org) : q.is("organization_id", null).eq("owner_id", actor);
    const subject = await q.maybeSingle();
    if (subject.error) throw new Error("Schema subject read failed");
    const versions = subject.data
      ? await user
          .from("schema_versions")
          .select("version,schema,fingerprint")
          .eq("subject_id", subject.data.id)
          .order("version")
      : { data: [], error: null };
    if (versions.error) throw new Error("Schema history read failed");
    const history = versions.data ?? [];
    const same = history.find((v) => v.fingerprint === fingerprint);
    if (!same) {
      const mode = (subject.data?.compatibility ??
        input.compatibility ??
        "BACKWARD") as CompatibilityMode;
      const check = checkCompatibility(
        parsed,
        history.map((v) => parseAvroSchema(v.schema)),
        mode,
      );
      if (!check.compatible) return { compatible: false, issues: check.issues };
    }
    const r = await admin.rpc("schema_register_version", {
      _actor: actor,
      _organization: org as string,
      _name: input.subject,
      _schema: input.schema as never,
      _fingerprint: fingerprint,
      _expected_latest: history.length ? history[history.length - 1]!.version : 0,
      ...(input.compatibility ? { _compatibility: input.compatibility } : {}),
    });
    if (r.error?.message.includes("schema_registry_conflict")) continue;
    if (r.error) throw new Error(r.error.message);
    const out = r.data as { subject_id: string; version: number; created: boolean };
    return {
      compatible: true,
      subjectId: out.subject_id,
      version: out.version,
      created: out.created,
    };
  }
  throw new Error("Schema subject is changing too fast; retry");
}

/** Latest version of a subject the caller can read, for pinning as a stream's reader schema. */
export async function latestSchema(user: SupabaseClient, subjectId: string) {
  const v = await user
    .from("schema_versions")
    .select("version,schema")
    .eq("subject_id", subjectId)
    .order("version", { ascending: false })
    .limit(1)
    .maybeSingle();
  if (v.error || !v.data) throw new Error("Schema subject not found");
  return v.data;
}
