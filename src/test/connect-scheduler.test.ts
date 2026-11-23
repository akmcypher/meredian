// @vitest-environment node
import { describe, expect, it } from "vitest";
import { scheduleConnectRuns } from "@/lib/connect-scheduler.server";

describe("Connect scheduler tick", () => {
  it("reports persisted queued runs without any agent calls", async () => {
    expect(await scheduleConnectRuns(async () => ({ data: { queued: 2 }, error: null }))).toEqual({
      queued: 2,
    });
  });
  it("rejects database failures instead of reporting an idle scheduler", async () => {
    await expect(
      scheduleConnectRuns(async () => ({ data: null, error: { message: "private detail" } })),
    ).rejects.toThrow("Unable to schedule Connect runs");
  });
  it.each([{ queued: -1 }, { queued: 0.5 }, {}, null])(
    "rejects invalid database result %j",
    async (data) => {
      await expect(scheduleConnectRuns(async () => ({ data, error: null }))).rejects.toThrow();
    },
  );
});
