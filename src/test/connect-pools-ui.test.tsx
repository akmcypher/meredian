// Worker pools in the Connect UI (deploy/connect/README.md, "Worker pools"): the pool list counts
// agents online and their slots, a new pool's token is shown once, and a connection can be moved
// to a pool of its own workspace and given a parallel task limit.
import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ConnectList } from "@/components/connect/use-connect";

const m = vi.hoisted(() => ({
  createPool: vi.fn(async () => ({ id: "p2", token: `mp_${"a".repeat(64)}` })),
  updatePool: vi.fn(async () => ({ token: null })),
  assignPool: vi.fn(async () => ({ saved: true })),
}));
const now = Date.now();
vi.mock("@/components/connect/use-connect", () => ({
  useConnectActions: () => ({
    busy: false,
    run: async (fn: () => Promise<unknown>) => fn(),
    ...m,
  }),
  usePools: () => ({
    data: {
      pools: [
        {
          id: "p1",
          owner_id: "u",
          organization_id: null,
          name: "etl",
          enabled: true,
          created_at: "",
        },
        {
          id: "o1",
          owner_id: "u",
          organization_id: "org1",
          name: "team",
          enabled: true,
          created_at: "",
        },
      ],
      workers: [
        {
          pool_id: "p1",
          agent: "a",
          version: "m2",
          slots: 4,
          last_seen_at: new Date(now).toISOString(),
        },
        {
          pool_id: "p1",
          agent: "b",
          version: "m2",
          slots: 2,
          last_seen_at: new Date(now - 30000).toISOString(),
        },
        // Not seen for ten minutes: offline.
        {
          pool_id: "p1",
          agent: "c",
          version: "m2",
          slots: 8,
          last_seen_at: new Date(now - 600000).toISOString(),
        },
      ],
    },
  }),
}));
import { ConnectionPoolSettings, PoolsPanel } from "@/components/connect/PoolsPanel";
import { poolSummary } from "@/components/connect/connect-view";

const connection = {
  id: "c1",
  name: "orders",
  source_id: "s1",
  source_version: "postgres-v1",
  credential_ref: "orders",
  enabled: true,
  health: "reachable",
  interval_minutes: 0,
  next_run_at: null,
  discovered_catalog: [],
  created_at: "",
  organization_id: null,
  pool_id: "p1",
  max_parallel_tasks: null,
};
const data = {
  connections: [connection],
  sources: [],
  runs: [],
  streams: [],
  lag: [],
  deadLetters: new Map(),
} as unknown as ConnectList;

describe("Worker pools", () => {
  it("counts agents online, their slots and assigned connections", () => {
    const s = poolSummary(
      [{ id: "p1" }],
      [
        { pool_id: "p1", slots: 4, last_seen_at: new Date(now).toISOString() },
        { pool_id: "p1", slots: null, last_seen_at: new Date(now).toISOString() },
        { pool_id: "p1", slots: 8, last_seen_at: new Date(now - 600000).toISOString() },
      ],
      [{ pool_id: "p1" }, { pool_id: null }],
      now,
    );
    expect(s.get("p1")).toEqual({ agentsOnline: 2, slotsOnline: 5, connections: 1 });
  });

  it("lists pools and shows a new pool's token once", async () => {
    render(<PoolsPanel data={data} />);
    expect(screen.getByText(/2 agents online · 6 slots · 1 connection/)).toBeTruthy();
    fireEvent.change(screen.getByLabelText("New pool name"), { target: { value: " nightly " } });
    fireEvent.click(screen.getByRole("button", { name: "Create pool" }));
    await waitFor(() => expect(screen.getByText(`mp_${"a".repeat(64)}`)).toBeTruthy());
    expect(m.createPool).toHaveBeenCalledWith({
      data: { name: "nightly", organizationId: null },
    });
    expect(screen.getByText(/shown only once/)).toBeTruthy();
  });

  it("offers only pools of the connection's workspace and saves a limit", async () => {
    render(<ConnectionPoolSettings connection={connection} />);
    const select = screen.getByLabelText("Worker pool") as HTMLSelectElement;
    expect([...select.options].map((o) => o.textContent)).toEqual([
      "This connection's own agent only",
      "etl",
    ]);
    fireEvent.change(select, { target: { value: "" } });
    expect(m.assignPool).toHaveBeenCalledWith({ data: { connectionId: "c1", poolId: null } });
    const limit = screen.getByLabelText(/Parallel tasks limit/);
    fireEvent.change(limit, { target: { value: "0" } });
    expect(screen.getByText(/whole number from 1 to 64/)).toBeTruthy();
    fireEvent.change(limit, { target: { value: "2" } });
    fireEvent.click(screen.getByRole("button", { name: "Save limit" }));
    await waitFor(() =>
      expect(m.assignPool).toHaveBeenCalledWith({
        data: { connectionId: "c1", maxParallelTasks: 2 },
      }),
    );
  });
});
