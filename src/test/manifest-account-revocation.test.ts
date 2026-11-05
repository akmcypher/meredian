// @vitest-environment node
import { beforeEach, describe, expect, it, vi } from "vitest";
const mocks = vi.hoisted(() => ({ rpc: vi.fn(), sign: vi.fn() }));
vi.mock("@/lib/engine-token.server", () => ({
  verifyEngineToken: async () => ({
    userId: "11111111-1111-4111-8111-111111111111",
    engineId: "22222222-2222-4222-8222-222222222222",
  }),
}));
vi.mock("@/integrations/supabase/client.server", () => ({
  supabaseAdmin: {
    from: () => ({
      select: () => ({
        eq: () => ({
          maybeSingle: async () => ({
            data: {
              enabled: true,
              owner_id: "11111111-1111-4111-8111-111111111111",
              organization_id: null,
            },
          }),
        }),
      }),
    }),
    rpc: mocks.rpc,
    storage: { from: () => ({ createSignedUrl: mocks.sign }) },
  },
}));
import { handleServeManifest } from "@/lib/serve-manifest.server";
describe("engine manifest account revocation", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.rpc.mockResolvedValue({
      data: null,
      error: { code: "42501", message: "Active verified account required" },
    });
  });
  it.each([
    { names: [] },
    { names: ["not a dataset identifier"] },
    { names: ["dataset"] },
    {
      bronze: {
        streamId: "33333333-3333-4333-8333-333333333333",
        runId: "44444444-4444-4444-8444-444444444444",
      },
    },
  ])("denies a deactivated subject before signing object URLs", async (body) => {
    const response = await handleServeManifest(
      new Request("https://app.example.test/api/public/serve-manifest", {
        method: "POST",
        headers: {
          authorization: "Bearer synthetic-engine-token",
          "content-type": "application/json",
        },
        body: JSON.stringify(body),
      }),
    );
    expect(response.status).toBe(401);
    expect(await response.json()).toEqual({ error: "unauthorized" });
    expect(mocks.sign).not.toHaveBeenCalled();
  });
});
