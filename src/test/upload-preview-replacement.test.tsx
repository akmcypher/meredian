import { render, screen, fireEvent, waitFor, cleanup, act } from "@testing-library/react";
import { describe, it, expect, vi, afterEach } from "vitest";
import type { ComponentType } from "react";
const mocks = vi.hoisted(() => ({ parse: vi.fn(), error: vi.fn(), csv: vi.fn() }));
vi.mock("@tanstack/react-router", () => ({
  createFileRoute: () => (options: unknown) => ({ options }),
}));
vi.mock("@tanstack/react-query", () => ({
  useQueryClient: () => ({ invalidateQueries: vi.fn() }),
  useQuery: () => ({ data: [] }),
}));
vi.mock("@/hooks/use-auth", () => ({ useAuth: () => ({ user: { id: "owner" } }) }));
vi.mock("@/hooks/use-user-tables", () => ({ useUserTables: () => ({ data: [] }) }));
vi.mock("@/integrations/supabase/client", () => ({ supabase: {} }));
vi.mock("papaparse", () => ({ default: { parse: mocks.csv } }));
vi.mock("@/lib/file-formats", () => ({ parseFile: mocks.parse }));
vi.mock("@/lib/audit", () => ({ audit: vi.fn() }));
vi.mock("@/lib/tables", () => ({
  fieldsOf: () => [{ name: "id", type: "number" }],
  writeTable: vi.fn(),
}));
vi.mock("sonner", () => ({ toast: { error: mocks.error, warning: vi.fn(), success: vi.fn() } }));
import { Route } from "@/routes/_authenticated/data";
const Page = Route.options.component as ComponentType;
function file() {
  const f = new File(["[]"], "sample.json", { type: "application/json" });
  Object.defineProperty(f, "arrayBuffer", { value: async () => new ArrayBuffer(0) });
  return f;
}
// Regression: a failed selection must not leave the previous file available to save.
// Value: protects=upload preview identity; fails_when=invalid input leaves old Create table enabled;
describe("upload preview replacement", () => {
  afterEach(cleanup);
  it.each(["invalid", "empty"])("clears old rows after an %s selection", async (kind) => {
    mocks.parse.mockReset();
    mocks.parse.mockResolvedValueOnce({ rows: [{ id: 1 }], total: 1, format: "json" });
    const ui = render(<Page />);
    const input = ui.container.querySelector("input[type=file]")!;
    fireEvent.change(input, { target: { files: [file()] } });
    await screen.findByRole("button", { name: "Create table" });
    if (kind === "invalid") mocks.parse.mockRejectedValueOnce(new Error("Invalid JSON"));
    else mocks.parse.mockResolvedValueOnce({ rows: [], total: 0, format: "json" });
    fireEvent.change(input, { target: { files: [file()] } });
    await waitFor(() => expect(screen.queryByRole("button", { name: "Create table" })).toBeNull());
  });
  it("ignores an older JSON parse finishing after a failed replacement", async () => {
    mocks.parse.mockReset();
    mocks.error.mockClear();
    let resolve!: (value: unknown) => void;
    mocks.parse.mockReturnValueOnce(
      new Promise((r) => {
        resolve = r;
      }),
    );
    const ui = render(<Page />);
    const input = ui.container.querySelector("input[type=file]")!;
    fireEvent.change(input, { target: { files: [file()] } });
    mocks.parse.mockRejectedValueOnce(new Error("Invalid JSON"));
    fireEvent.change(input, { target: { files: [file()] } });
    await waitFor(() => expect(mocks.error).toHaveBeenCalled());
    await act(async () => resolve({ rows: [{ id: 1 }], total: 1, format: "json" }));
    expect(screen.queryByRole("button", { name: "Create table" })).toBeNull();
  });
  it("ignores an older CSV callback after an empty replacement", async () => {
    mocks.parse.mockReset();
    mocks.error.mockClear();
    let complete!: (result: { data: { id: string }[] }) => void;
    mocks.csv.mockImplementation((_file, options) => {
      complete = options.complete;
    });
    const ui = render(<Page />);
    const input = ui.container.querySelector("input[type=file]")!;
    fireEvent.change(input, { target: { files: [new File(["id\n1"], "old.csv")] } });
    mocks.parse.mockResolvedValueOnce({ rows: [], total: 0, format: "json" });
    fireEvent.change(input, { target: { files: [file()] } });
    await waitFor(() => expect(mocks.error).toHaveBeenCalled());
    act(() => complete({ data: [{ id: "1" }] }));
    expect(screen.queryByRole("button", { name: "Create table" })).toBeNull();
  });
});
