// Dead-letter record a push worker sends with a batch (/api/public/ingest). Shared by the
// endpoint and its tests; raw key/value are base64.
import { z } from "zod";

export const ingestDeadLetter = z.object({
  topic: z.string().min(1).max(249),
  partition: z.number().int().nonnegative(),
  offset: z.string().regex(/^[0-9]{1,20}$/),
  error: z.enum(["invalid_json", "unrecognized_event", "invalid_event", "schema_incompatible"]),
  key: z.string().max(1_400_000).nullable(),
  value: z.string().max(1_400_000).nullable(),
});
export type IngestDeadLetter = z.infer<typeof ingestDeadLetter>;
