import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ConnectionList } from "@/components/connect/ConnectionList";
import { ErrorGuidance } from "@/components/connect/ErrorGuidance";
import { NewConnectionWizard } from "@/components/connect/NewConnectionWizard";
import { wizardSteps } from "@/components/connect/connect-view";
import type { ConnectList } from "@/components/connect/use-connect";

const m = vi.hoisted(() => ({
  createSource: vi.fn(),
  create: vi.fn(),
  configure: vi.fn(),
  queue: vi.fn(),
  run: { status: "succeeded", error_code: null as string | null },
}));
vi.mock("@/components/connect/use-connect", () => ({
  useConnectActions: () => ({
    busy: false,
    run: async (fn: () => Promise<unknown>) => {
      try {
        return await fn();
      } catch {
        return undefined;
      }
    },
    createSource: m.createSource,
    create: m.create,
    configure: m.configure,
    queue: m.queue,
    cancel: vi.fn(),
  }),
  useRunStatus: (id: string | null) => ({
    data: id ? { id, attempts: 1, max_attempts: 3, finished_at: null, ...m.run } : undefined,
  }),
}));

const list = (): ConnectList => ({
  connections: [
    {
      id: "c1",
      name: "orders",
      source_id: "s1",
      source_version: "kafka-v1",
      credential_ref: "orders",
      enabled: true,
      health: "reachable",
      interval_minutes: 15,
      next_run_at: null,
      discovered_catalog: [],
      created_at: "2026-11-01T00:00:00Z",
    },
    {
      id: "c2",
      name: "crm",
      source_id: "s2",
      source_version: "postgres-v1",
      credential_ref: "crm",
      enabled: true,
      health: "reachable",
      interval_minutes: 0,
      next_run_at: null,
      discovered_catalog: [],
      created_at: "2026-11-02T00:00:00Z",
    },
  ],
  sources: [],
  runs: [
    {
      id: "r1",
      connection_id: "c2",
      operation: "sync",
      status: "failed",
      attempts: 1,
      rows_committed: 0,
      bytes_committed: 0,
      error_code: "auth_failed",
      created_at: "2026-11-14T00:00:00Z",
      finished_at: "2026-11-14T00:00:00Z",
    },
  ],
  streams: [{ id: "st1", connection_id: "c1", last_success_at: "2026-11-14T00:00:00Z" }],
  lag: [
    { stream_id: "st1", partition: 0, lag: 1200, observed_at: "2026-11-14T00:00:00Z" },
    { stream_id: "st1", partition: 1, lag: 34, observed_at: "2026-11-14T00:00:00Z" },
  ],
  deadLetters: new Map(),
});

describe("ConnectionList", () => {
  it("shows health, last successful sync and total lag per connection", () => {
    const onOpen = vi.fn();
    render(<ConnectionList data={list()} onOpen={onOpen} onNew={vi.fn()} />);
    const orders = screen.getByText("orders").closest("tr")!;
    expect(within(orders).getByText("Healthy")).toBeDefined();
    expect(within(orders).getByText((1234).toLocaleString())).toBeDefined();
    expect(within(orders).getByText("Every 15 min")).toBeDefined();
    const crm = screen.getByText("crm").closest("tr")!;
    expect(within(crm).getByText("Needs attention")).toBeDefined();
    expect(within(crm).getByText("Sign-in to the source failed")).toBeDefined();
    expect(within(crm).getByText("Never")).toBeDefined();
    expect(screen.getByText(/1 needs attention/)).toBeDefined();
    fireEvent.click(screen.getByText("crm"));
    expect(onOpen).toHaveBeenCalledWith("c2");
  });
});

describe("ErrorGuidance", () => {
  it("turns a code into guidance with its next action", () => {
    const onAction = vi.fn();
    render(<ErrorGuidance code="schema_drift" onAction={onAction} />);
    expect(screen.getByText("The source schema changed")).toBeDefined();
    fireEvent.click(screen.getByRole("button", { name: "Discover streams" }));
    expect(onAction).toHaveBeenCalledWith("discover");
  });
});

describe("NewConnectionWizard", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    m.run = { status: "succeeded", error_code: null };
    m.createSource.mockResolvedValue({ id: "11111111-1111-4111-8111-111111111111", token: "t" });
    m.create.mockResolvedValue({ id: "22222222-2222-4222-8222-222222222222" });
    m.queue.mockResolvedValue({ id: "33333333-3333-4333-8333-333333333333" });
  });
  const type = (label: string, value: string) =>
    fireEvent.change(screen.getByLabelText(label), { target: { value } });

  it("has a security step for Kafka only", () => {
    expect(wizardSteps("kafka")).toContain("security");
    expect(wizardSteps("postgres")).not.toContain("security");
  });

  it("shows only the fields a profile needs and registers without any secret", async () => {
    render(<NewConnectionWizard data={undefined} onClose={vi.fn()} onDone={vi.fn()} />);
    fireEvent.click(screen.getByRole("button", { name: "Continue" }));
    fireEvent.click(screen.getByLabelText(/Amazon MSK \(IAM\)/));
    fireEvent.click(screen.getByRole("button", { name: "Continue" }));
    expect(screen.getByLabelText("AWS region")).toBeDefined();
    expect(screen.queryByLabelText("SASL mechanism")).toBeNull();
    expect(screen.queryByLabelText(/password/i)).toBeNull();
    type("Connection name", "events");
    type("Credential reference", "events-msk");
    type("Bootstrap servers", "b-1.msk.example.com:9098");
    const register = screen.getByRole("button", { name: "Register connection" });
    expect(register).toHaveProperty("disabled", true);
    type("AWS region", "eu-west-1");
    expect(register).toHaveProperty("disabled", false);
    fireEvent.click(register);
    await waitFor(() => expect(m.create).toHaveBeenCalled());
    expect(m.createSource.mock.calls[0]![0].data).toEqual({
      name: "events",
      kind: "kafka",
      target: "connect_events",
      config: {
        bootstrap_servers: "b-1.msk.example.com:9098",
        profile: "aws_msk_iam",
        aws_region: "eu-west-1",
      },
    });
    expect(m.create.mock.calls[0]![0].data).toMatchObject({ credentialRef: "events-msk" });
    // Test step: the agent file template is shown and the check runs through the queue.
    expect(await screen.findByLabelText("Agent file template")).toBeDefined();
    fireEvent.click(screen.getByRole("button", { name: "Test connection" }));
    await waitFor(() =>
      expect(m.queue).toHaveBeenCalledWith("22222222-2222-4222-8222-222222222222", "check"),
    );
  });

  it("blocks PLAIN without TLS with the backend's reason", () => {
    render(<NewConnectionWizard data={undefined} onClose={vi.fn()} onDone={vi.fn()} />);
    fireEvent.click(screen.getByRole("button", { name: "Continue" }));
    fireEvent.click(screen.getByLabelText(/SASL without TLS/));
    fireEvent.click(screen.getByRole("button", { name: "Continue" }));
    type("Connection name", "dev");
    type("Credential reference", "dev");
    type("Bootstrap servers", "localhost:9092");
    type("SASL mechanism", "PLAIN");
    expect(screen.getByText(/PLAIN without TLS sends the password in clear text/)).toBeDefined();
    expect(screen.getByRole("button", { name: "Register connection" })).toHaveProperty(
      "disabled",
      true,
    );
  });

  it("shows guidance with a retry when the connection test fails", async () => {
    m.run = { status: "failed", error_code: "source_unavailable" };
    render(<NewConnectionWizard data={undefined} onClose={vi.fn()} onDone={vi.fn()} />);
    fireEvent.click(screen.getByLabelText(/PostgreSQL/));
    fireEvent.click(screen.getByRole("button", { name: "Continue" }));
    type("Connection name", "crm");
    type("Credential reference", "crm");
    type("Host", "db.internal");
    type("Database", "crm");
    type("User", "reader");
    fireEvent.click(screen.getByRole("button", { name: "Register connection" }));
    fireEvent.click(await screen.findByRole("button", { name: "Test connection" }));
    expect(await screen.findByText("The source could not be reached")).toBeDefined();
    expect(screen.getByRole("button", { name: "Continue" })).toHaveProperty("disabled", true);
    expect(m.createSource.mock.calls[0]![0].data.config).not.toHaveProperty("password");
  });
});
