// @vitest-environment node
import { expect, it } from "vitest";
import type { SupabaseClient } from "@supabase/supabase-js";
import { reconcileIngestStorage } from "@/lib/storage-reconcile.server";

it("refuses destructive cleanup before accessing storage or the database", async () => {
  await expect(
    reconcileIngestStorage({} as SupabaseClient, { apply: true, minAgeHours: 0 }),
  ).rejects.toThrow("destructive_cleanup_disabled");
});
