import { afterEach, describe, expect, it, vi } from "vitest";
import { authenticateCronRequest } from "@/integrations/supabase/cron-auth";

const call = (token?: string) =>
  authenticateCronRequest(
    new Request("http://localhost/api/public/jobs/tick", {
      method: "POST",
      headers: token ? { authorization: `Bearer ${token}` } : {},
    }),
  );

describe("cron authentication", () => {
  afterEach(() => vi.unstubAllEnvs());

  it("fails closed when no secret is configured", async () => {
    vi.stubEnv("CRON_SECRET", "");
    expect((await call("anything"))?.status).toBe(500);
  });

  it("accepts CRON_SECRET and the previous secret during rotation", async () => {
    vi.stubEnv("CRON_SECRET", "current-secret");
    vi.stubEnv("CRON_SECRET_PREVIOUS", "previous-secret");
    expect(await call("current-secret")).toBeNull();
    expect(await call("previous-secret")).toBeNull();
    expect((await call("wrong"))?.status).toBe(401);
    expect((await call())?.status).toBe(401);
  });
});
