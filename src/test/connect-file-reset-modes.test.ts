// @vitest-environment node
import { expect, it, vi } from "vitest";
import type { SupabaseClient } from "@supabase/supabase-js";
import { hydrateFileResetModes } from "@/lib/connect-agent.server";

it("keeps configured full and incremental file modes distinct when reset claims dispatch full refresh", async () => {
  const query = vi.fn().mockResolvedValue({
    data: [
      { id: "full", sync_mode: "full_refresh" },
      { id: "incremental", sync_mode: "incremental" },
    ],
    error: null,
  });
  const db = {
    from: vi.fn(() => ({ select: () => ({ in: query }) })),
  } as unknown as SupabaseClient;
  const claim = {
    run_id: "run",
    lease_token: "lease",
    source_version: "spark-files-v1",
    operation: "reset",
    streams: [{ id: "full" }, { id: "incremental" }],
  };
  const result = await hydrateFileResetModes(claim, db);
  expect(query).toHaveBeenCalledWith("id", ["full", "incremental"]);
  expect(result.streams.map((s) => s.configured_sync_mode)).toEqual([
    "full_refresh",
    "incremental",
  ]);
});
it("does no extra lookup for ordinary syncs or other source kinds", async () => {
  const from = vi.fn();
  const db = { from } as unknown as SupabaseClient;
  for (const [source_version, operation] of [
    ["spark-files-v1", "sync"],
    ["spark-postgres-v1", "reset"],
  ] as const) {
    await hydrateFileResetModes(
      {
        run_id: "r",
        lease_token: "l",
        source_version,
        operation,
        streams: [{ id: "s" }],
      },
      db,
    );
  }
  expect(from).not.toHaveBeenCalled();
});
it("refuses a reset whose configured mode could not be read", async () => {
  const db = {
    from: () => ({ select: () => ({ in: async () => ({ data: [], error: null }) }) }),
  } as unknown as SupabaseClient;
  await expect(
    hydrateFileResetModes(
      {
        run_id: "r",
        lease_token: "l",
        source_version: "spark-files-v1",
        operation: "backfill",
        streams: [{ id: "s" }],
      },
      db,
    ),
  ).rejects.toThrow();
});
