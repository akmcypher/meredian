import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { StreamsTab } from "@/components/connect/StreamsTab";
import type {
  ConnectConnection,
  ConnectDetails,
  useConnectActions,
} from "@/components/connect/use-connect";
describe("stream retirement controls", () => {
  function show(status?: string) {
    const retireStream = vi.fn().mockResolvedValue({ cleanupRun: "run" });
    const actions = {
      busy: false,
      retireStream,
      run: (fn: () => Promise<unknown>) => fn(),
    } as unknown as ReturnType<typeof useConnectActions>;
    const connection = {
      id: "connection",
      source_version: "postgres-cdc-v1",
      discovered_catalog: [],
    } as unknown as ConnectConnection;
    const details = {
      streams: [
        {
          id: "stream",
          namespace: "public",
          name: "orders",
          state: status ? { retirement: { status } } : {},
          sync_mode: "cdc",
        },
      ],
      lag: [],
    } as unknown as ConnectDetails;
    render(<StreamsTab connection={connection} details={details} actions={actions} />);
    return retireStream;
  }
  it("queues retirement for the selected stream", () => {
    const retire = show();
    fireEvent.click(screen.getByRole("button", { name: "Retire stream" }));
    expect(retire).toHaveBeenCalledWith({ data: { id: "stream" } });
  });
  it("exposes retry until source cleanup is complete", () => {
    const retire = show("pending");
    fireEvent.click(screen.getByRole("button", { name: "Retry source cleanup" }));
    expect(retire).toHaveBeenCalledWith({ data: { id: "stream" } });
  });
  it("does not offer to revive a retired stream", () => {
    show("done");
    expect(screen.getByText("Retired")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Retire stream" })).not.toBeInTheDocument();
  });
});
