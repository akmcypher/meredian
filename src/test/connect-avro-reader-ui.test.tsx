import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { StreamPicker } from "@/components/connect/StreamPicker";
import { StreamsTab } from "@/components/connect/StreamsTab";
import { saveStreams, type StreamChoice } from "@/components/connect/connect-view";
import type { ConnectConnection, useConnectActions } from "@/components/connect/use-connect";

const m = vi.hoisted(() => ({
  user: { id: "alice" } as { id: string } | null,
  read: vi.fn(),
  select: vi.fn(),
  eq: vi.fn(),
}));
vi.mock("@/hooks/use-auth", () => ({
  useAuth: () => ({ user: m.user, workspace: { workspaceId: "workspace" }, loading: false }),
}));
vi.mock("@/integrations/supabase/client", () => ({
  supabase: {
    from: (table: string) => {
      expect(table).toBe("schema_subjects");
      return { select: m.select };
    },
  },
}));
const catalog = [
  { namespace: "kafka", name: "orders", primary_key: "_offset" },
  { namespace: "kafka", name: "customers", primary_key: "_offset" },
];
function Wrapper({ children }: { children: React.ReactNode }) {
  const [client] = useState(() => new QueryClient());
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}
function pick(onSave: (streams: StreamChoice[]) => void | Promise<void> = vi.fn()) {
  return render(<StreamPicker catalog={catalog} kafka configured={[]} onSave={onSave} />, {
    wrapper: Wrapper,
  });
}
beforeEach(() => {
  m.user = { id: "alice" };
  m.read
    .mockReset()
    .mockResolvedValue({ data: [{ id: "subject-a", name: "Order reader" }], error: null });
  m.select.mockImplementation(() => ({ eq: m.eq }));
  m.eq.mockImplementation(() => ({ order: () => ({ abortSignal: m.read }) }));
});

describe("Kafka Avro reader choice", () => {
  it("shows pending access, disables reader selection and cancels the read on unmount", async () => {
    let signal!: AbortSignal;
    m.read.mockImplementationOnce((next: AbortSignal) => {
      signal = next;
      return new Promise(() => {});
    });
    const view = pick();
    expect(await screen.findByRole("status")).toHaveTextContent(/Loading/);
    fireEvent.click(screen.getAllByRole("checkbox")[0]!);
    expect(screen.getByLabelText("Avro reader schema for kafka.orders")).toBeDisabled();
    view.unmount();
    expect(signal.aborted).toBe(true);
  });

  it("offers only automatic when RLS returns no readable subjects", async () => {
    m.read.mockResolvedValue({ data: [], error: null });
    pick();
    fireEvent.click((await screen.findAllByRole("checkbox"))[0]!);
    await waitFor(() =>
      expect(screen.getByLabelText("Avro reader schema for kafka.orders")).toBeEnabled(),
    );
    expect(screen.getAllByRole("option")).toHaveLength(1);
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("removes configured choices when signed out without fetching another subject list", async () => {
    const view = pick();
    fireEvent.click((await screen.findAllByRole("checkbox"))[0]!);
    await screen.findByText("Order reader");
    m.user = null;
    view.rerender(<StreamPicker catalog={catalog} kafka configured={[]} onSave={vi.fn()} />);
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
    expect(screen.getByRole("alert")).toHaveTextContent(/Sign in/);
    expect(m.read).toHaveBeenCalledTimes(1);
  });

  it("defaults to automatic writer schemas and reads subjects once for all topics", async () => {
    const onSave = vi.fn();
    pick(onSave);
    fireEvent.click((await screen.findAllByRole("checkbox"))[0]!);
    await screen.findByText("Order reader");
    expect(screen.getByLabelText("Avro reader schema for kafka.orders")).toHaveValue("");
    fireEvent.click(screen.getByRole("button", { name: /Save streams/ }));
    expect(onSave).toHaveBeenCalledWith([
      {
        namespace: "kafka",
        name: "orders",
        primaryKey: "_offset",
        cursor: "_offset",
        mode: "incremental",
      },
    ]);
    fireEvent.click(screen.getAllByRole("checkbox")[1]!);
    expect(m.read).toHaveBeenCalledTimes(1);
    expect(m.select).toHaveBeenCalledWith("id,name");
    expect(m.eq).toHaveBeenCalledWith("format", "avro");
  });

  it("submits the selected subject ID through stream configuration and clears it for automatic", async () => {
    const configure = vi.fn().mockResolvedValue({});
    const actions = { configure } as unknown as ReturnType<typeof useConnectActions>;
    pick((streams) => saveStreams(actions, "connection", streams).then(() => {}));
    fireEvent.click((await screen.findAllByRole("checkbox"))[0]!);
    const select = await screen.findByLabelText("Avro reader schema for kafka.orders");
    await screen.findByText("Order reader");
    fireEvent.change(select, { target: { value: "subject-a" } });
    fireEvent.click(screen.getByRole("button", { name: /Save streams/ }));
    await waitFor(() =>
      expect(configure).toHaveBeenCalledWith({
        data: {
          id: "connection",
          stream: expect.objectContaining({ readerSchemaSubjectId: "subject-a" }),
        },
      }),
    );
    fireEvent.change(select, { target: { value: "" } });
    fireEvent.click(screen.getByRole("button", { name: /Save streams/ }));
    await waitFor(() => expect(configure).toHaveBeenCalledTimes(2));
    expect(configure.mock.calls[1]![0].data.stream).not.toHaveProperty("readerSchemaSubjectId");
  });

  it("recognizes Spark Kafka and submits offset streams with the reader selection", async () => {
    const configure = vi.fn().mockResolvedValue({});
    const actions = {
      configure,
      run: (fn: () => Promise<unknown>) => fn(),
    } as unknown as ReturnType<typeof useConnectActions>;
    render(
      <StreamsTab
        connection={
          {
            id: "spark",
            source_version: "spark-kafka-v1",
            discovered_catalog: catalog,
          } as unknown as ConnectConnection
        }
        details={undefined}
        actions={actions}
      />,
      { wrapper: Wrapper },
    );
    fireEvent.click(screen.getByRole("button", { name: "Add or change streams" }));
    fireEvent.click((await screen.findAllByRole("checkbox"))[0]!);
    await screen.findByText("Order reader");
    expect(screen.queryByLabelText("Cursor for kafka.orders")).not.toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("Avro reader schema for kafka.orders"), {
      target: { value: "subject-a" },
    });
    fireEvent.click(screen.getByRole("button", { name: /Save streams/ }));
    await waitFor(() =>
      expect(configure).toHaveBeenCalledWith({
        data: {
          id: "spark",
          stream: expect.objectContaining({
            cursor: "_offset",
            readerSchemaSubjectId: "subject-a",
          }),
        },
      }),
    );
  });

  it.each(["denied", "network"])(
    "reports %s subject reads without exposing errors and keeps automatic available",
    async (failure) => {
      if (failure === "denied")
        m.read.mockResolvedValue({ data: null, error: { message: "secret registry credentials" } });
      else m.read.mockRejectedValue(new Error("secret registry credentials"));
      const onSave = vi.fn();
      pick(onSave);
      await screen.findByRole("alert");
      fireEvent.click(screen.getAllByRole("checkbox")[0]!);
      expect(screen.getByLabelText("Avro reader schema for kafka.orders")).toBeDisabled();
      expect(screen.queryByText(/secret registry/)).not.toBeInTheDocument();
      fireEvent.click(screen.getByRole("button", { name: /Save streams/ }));
      expect(onSave).toHaveBeenCalledWith([
        expect.not.objectContaining({ readerSchemaSubjectId: expect.anything() }),
      ]);
    },
  );

  it("clears choices on identity changes and ignores the previous pending subject read", async () => {
    let finish!: (value: unknown) => void;
    m.read.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    const onSave = vi.fn();
    const view = pick(onSave);
    fireEvent.click((await screen.findAllByRole("checkbox"))[0]!);
    m.user = { id: "bob" };
    m.read.mockResolvedValue({ data: [], error: null });
    view.rerender(<StreamPicker catalog={catalog} kafka configured={[]} onSave={onSave} />);
    await waitFor(() => expect(m.read).toHaveBeenCalledTimes(2));
    finish({ data: [{ id: "old", name: "Alice reader" }], error: null });
    await waitFor(() => expect(screen.getAllByRole("checkbox")[0]!).not.toBeChecked());
    expect(screen.queryByText("Alice reader")).not.toBeInTheDocument();
  });
});
