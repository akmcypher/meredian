// @vitest-environment node
import { beforeEach, expect, it, vi } from "vitest";
const m = vi.hoisted(() => ({ insert: vi.fn(), rpc: vi.fn() }));
vi.mock("@tanstack/react-start", () => ({
  createServerFn: () => {
    let validate: (v: unknown) => unknown;
    const chain = {
      middleware: () => chain,
      inputValidator: (fn: typeof validate) => {
        validate = fn;
        return chain;
      },
      handler: (fn: (arg: unknown) => unknown) => (arg: { data: unknown; context: unknown }) =>
        fn({ ...arg, data: validate(arg.data) }),
    };
    return chain;
  },
}));
vi.mock("@/integrations/supabase/auth-middleware", () => ({ requireSupabaseAuth: {} }));
vi.mock("@/integrations/supabase/client.server", () => ({
  supabaseAdmin: { from: () => ({ insert: m.insert }), rpc: m.rpc },
}));
import { createConnect } from "@/lib/connect.functions";
import { createSource } from "@/lib/ingest.functions";
const id = "11111111-1111-4111-8111-111111111111";
const context = (source: unknown) => ({
  userId: id,
  supabase: {
    from: () => ({
      select: () => ({ eq: () => ({ maybeSingle: async () => ({ data: source }) }) }),
    }),
  },
});
const call = (source: unknown, cdc = false) =>
  (createConnect as unknown as (a: unknown) => Promise<unknown>)({
    context: context(source),
    data: { sourceId: id, name: "Mongo orders", credentialRef: "mongo-orders", cdc },
  });
beforeEach(() => {
  m.insert.mockReset();
  m.rpc.mockReset();
  m.insert.mockReturnValue({
    select: () => ({ single: async () => ({ data: { id }, error: null }) }),
  });
});
it("registers an owned RLS-visible MongoDB source as mongodb-v1", async () => {
  await expect(
    call({ id, owner_id: id, organization_id: "workspace", kind: "mongodb" }),
  ).resolves.toEqual({ id });
  expect(m.insert).toHaveBeenCalledWith(
    expect.objectContaining({
      source_version: "mongodb-v1",
      owner_id: id,
      organization_id: "workspace",
    }),
  );
});
it("registers MongoDB change streams as mongodb-cdc-v1", async () => {
  await expect(call({ id, owner_id: id, kind: "mongodb" }, true)).resolves.toEqual({ id });
  expect(m.insert).toHaveBeenCalledWith(
    expect.objectContaining({ source_version: "mongodb-cdc-v1", owner_id: id }),
  );
});
it("still refuses CDC for SQL Server before privileged registration", async () => {
  await expect(call({ id, owner_id: id, kind: "sqlserver" }, true)).rejects.toThrow(
    /Change data capture/,
  );
  expect(m.insert).not.toHaveBeenCalled();
});
it.each([null, { id, owner_id: "other", kind: "mongodb" }])(
  "requires RLS visibility and ownership",
  async (source) => {
    await expect(call(source)).rejects.toThrow();
    expect(m.insert).not.toHaveBeenCalled();
  },
);
it("routes MongoDB source creation through verified actor registration", async () => {
  m.rpc.mockResolvedValue({ data: id, error: null });
  const result = await (createSource as unknown as (a: unknown) => Promise<{ token: string }>)({
    context: { userId: id },
    data: {
      name: "Mongo orders",
      kind: "mongodb",
      target: "orders",
      config: { host: "mongo.example.test", port: 27017 },
    },
  });
  expect(result.token).toMatch(/^mi_/);
  expect(m.rpc).toHaveBeenCalledWith(
    "create_ingest_source_verified",
    expect.objectContaining({
      _actor: id,
      _kind: "mongodb",
      _config: { host: "mongo.example.test", port: 27017 },
    }),
  );
});
