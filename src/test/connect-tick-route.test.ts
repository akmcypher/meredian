// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";
const database = vi.hoisted(() => ({ rpc: vi.fn() }));
vi.mock("@tanstack/react-router", () => ({ createFileRoute: () => (options: unknown) => options }));
vi.mock("@/integrations/supabase/client.server", () => ({ supabaseAdmin: database }));
import { Route } from "@/routes/api/public/connect-tick";
const handler = (
  Route as unknown as {
    server: { handlers: { POST: (args: { request: Request }) => Promise<Response> } };
  }
).server.handlers.POST;
const request = (token?: string) => ({
  request: new Request("http://localhost/api/public/connect-tick", {
    method: "POST",
    headers: token ? { authorization: `Bearer ${token}` } : {},
  }),
});
describe("ingestion-only authenticated Connect tick", () => {
  afterEach(() => {
    vi.clearAllMocks();
    vi.unstubAllEnvs();
  });
  it("queues due Connect runs without querying warehouse jobs", async () => {
    vi.stubEnv("CRON_SECRET", "ingestion-secret");
    database.rpc.mockResolvedValue({ data: { queued: 2 }, error: null });
    const response = await handler(request("ingestion-secret"));
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ connectQueued: 2 });
    expect(database.rpc).toHaveBeenCalledExactlyOnceWith("connect_schedule_due");
  });
  it("rejects unauthorized requests before scheduling", async () => {
    vi.stubEnv("CRON_SECRET", "ingestion-secret");
    expect((await handler(request("wrong"))).status).toBe(401);
    expect(database.rpc).not.toHaveBeenCalled();
  });
  it("fails closed when cron authentication is not configured", async () => {
    vi.stubEnv("CRON_SECRET", "");
    expect((await handler(request("ingestion-secret"))).status).toBe(500);
    expect(database.rpc).not.toHaveBeenCalled();
  });
  it("sanitizes scheduling failures", async () => {
    vi.stubEnv("CRON_SECRET", "ingestion-secret");
    database.rpc.mockResolvedValue({ data: null, error: { message: "private database detail" } });
    const response = await handler(request("ingestion-secret"));
    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ error: "Unable to schedule Connect runs" });
  });
});
