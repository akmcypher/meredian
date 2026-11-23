// @vitest-environment node
import { beforeEach, expect, it, vi } from "vitest";
import { sha256 } from "@/lib/ingest";
const fixture = vi.hoisted(() => ({
  rows: {} as Record<string, Record<string, unknown>[]>,
  rpc: vi.fn(),
}));
vi.mock("@/integrations/supabase/client.server", () => ({
  supabaseAdmin: {
    from(table: string) {
      const filters: [string, unknown][] = [];
      const query = {
        select: () => query,
        eq: (key: string, value: unknown) => {
          filters.push([key, value]);
          return query;
        },
        maybeSingle: async () => ({
          data:
            fixture.rows[table]?.find((row) =>
              filters.every(([key, value]) => row[key] === value),
            ) ?? null,
          error: null,
        }),
      };
      return query;
    },
    rpc: fixture.rpc,
  },
}));
import { handleConnectAgent } from "@/lib/connect-agent.server";
const cid = "11111111-1111-4111-8111-111111111111",
  sid = "22222222-2222-4222-8222-222222222222";
const rid = "33333333-3333-4333-8333-333333333333",
  token = "44444444-4444-4444-8444-444444444444";
const sourceToken = `mi_${"a".repeat(64)}`,
  poolToken = `mp_${"b".repeat(64)}`;
const call = (data: unknown, auth = sourceToken) =>
  handleConnectAgent(
    new Request("http://test/api/public/connect-agent", {
      method: "POST",
      headers: { "x-ingest-token": auth },
      body: JSON.stringify(data),
    }),
  );
beforeEach(async () => {
  fixture.rows = {
    ingest_sources: [
      {
        id: "source",
        owner_id: "owner",
        organization_id: null,
        enabled: true,
        token_hash: await sha256(sourceToken),
      },
    ],
    connect_pools: [
      {
        id: "pool",
        owner_id: "owner",
        organization_id: null,
        enabled: true,
        token_hash: await sha256(poolToken),
      },
    ],
    connect_connections: [
      {
        id: cid,
        source_id: "source",
        owner_id: "owner",
        organization_id: null,
        enabled: true,
        pool_id: "pool",
        source_version: "kafka-v1",
      },
    ],
    connect_runs: [{ id: rid, connection_id: cid, status: "running" }],
  };
  fixture.rpc.mockReset().mockImplementation(async (name: string) => ({
    error: null,
    data: name === "connect_lease_check" ? true : null,
  }));
});
it("uses the source token's own connection and refuses a foreign selection", async () => {
  expect((await call({ action: "group_context" })).status).toBe(200);
  expect(fixture.rpc).toHaveBeenCalledWith("connect_group_context", { _connection: cid });
  fixture.rpc.mockClear();
  expect((await call({ action: "group_context", connectionId: sid })).status).toBe(403);
  expect(fixture.rpc).not.toHaveBeenCalled();
});
it("requires explicit pool connection and refuses unassigned connections", async () => {
  expect((await call({ action: "group_context" }, poolToken)).status).toBe(400);
  expect((await call({ action: "claim_group_task", assignments: {} }, poolToken)).status).toBe(400);
  expect((await call({ action: "group_context", connectionId: sid }, poolToken)).status).toBe(409);
  expect(fixture.rpc).not.toHaveBeenCalled();
});
it("preserves pool scope, agent slots and worker identity in shared claims", async () => {
  const assignments = { [sid]: [0, 2] };
  expect(
    (
      await call(
        {
          action: "claim_group_task",
          connectionId: cid,
          assignments,
          worker: "a:0",
          agent: "a",
          slots: 2,
          version: "v1",
        },
        poolToken,
      )
    ).status,
  ).toBe(200);
  expect(fixture.rpc).toHaveBeenCalledWith("connect_claim_pool_group_task", {
    _pool: "pool",
    _connection: cid,
    _assignments: assignments,
    _worker: "a:0",
    _agent: "a",
    _slots: 2,
    _version: "v1",
  });
  expect((await call({ action: "group_context", connectionId: cid }, poolToken)).status).toBe(200);
  expect(fixture.rpc).toHaveBeenCalledWith("connect_group_context", {
    _pool: "pool",
    _connection: cid,
  });
});
it("denies unsupported source versions and workspace mismatch", async () => {
  fixture.rows["connect_connections"]![0]!["source_version"] = "spark-kafka-v1";
  expect((await call({ action: "claim_group_task", assignments: {} })).status).toBe(403);
  fixture.rows["connect_connections"]![0]!["source_version"] = "kafka-v1";
  fixture.rows["connect_connections"]![0]!["owner_id"] = "other";
  expect((await call({ action: "group_context", connectionId: cid }, poolToken)).status).toBe(403);
  expect(fixture.rpc).not.toHaveBeenCalled();
});
it("checks run scope and lease before accepting a rebalance handoff", async () => {
  expect((await call({ action: "abandon_task", runId: rid, token })).status).toBe(200);
  expect(fixture.rpc).toHaveBeenCalledWith("connect_abandon_task", { _run: rid, _token: token });
  fixture.rpc.mockClear().mockResolvedValue({ error: null, data: false });
  expect((await call({ action: "abandon_task", runId: rid, token })).status).toBe(409);
  expect(fixture.rpc).not.toHaveBeenCalledWith("connect_abandon_task", expect.anything());
  fixture.rows["connect_runs"]![0]!["connection_id"] = sid;
  expect((await call({ action: "abandon_task", runId: rid, token }, poolToken)).status).toBe(409);
});
