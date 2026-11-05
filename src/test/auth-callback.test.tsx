import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ComponentType } from "react";
const mocks = vi.hoisted(() => ({
  exchange: vi.fn(),
  user: vi.fn(),
  workspace: vi.fn(),
  navigate: vi.fn(),
}));
vi.mock("@tanstack/react-router", () => ({
  createFileRoute: () => (options: unknown) => ({ options }),
  useNavigate: () => mocks.navigate,
  Link: ({ children }: { children: React.ReactNode }) => children,
}));
vi.mock("@/integrations/supabase/client", () => ({
  supabase: { auth: { exchangeCodeForSession: mocks.exchange, getUser: mocks.user } },
}));
vi.mock("@/lib/workspace", () => ({ ensureWorkspace: mocks.workspace }));
import { Route } from "@/routes/auth_.callback";
const Callback = (Route as unknown as { options: { component: ComponentType } }).options.component;
describe("verification callback boundaries", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.exchange.mockResolvedValue({ error: null });
    mocks.user.mockResolvedValue({
      error: null,
      data: { user: { email_confirmed_at: "2026-11-04" } },
    });
    mocks.workspace.mockRejectedValue(new Error("Workspace access unavailable"));
  });
  afterEach(cleanup);
  it("allows password recovery without workspace access", async () => {
    window.history.replaceState(null, "", "/auth/callback?code=synthetic-code&flow=recovery");
    render(<Callback />);
    await waitFor(() =>
      expect(mocks.navigate).toHaveBeenCalledWith({ to: "/account/recovery", replace: true }),
    );
    expect(mocks.workspace).not.toHaveBeenCalled();
    expect(window.location.search).toBe("");
  });
  it("still requires authorized workspace access for normal sign-in", async () => {
    window.history.replaceState(null, "", "/auth/callback?code=synthetic-code");
    render(<Callback />);
    expect(await screen.findByRole("alert")).toBeTruthy();
    expect(mocks.workspace).toHaveBeenCalledOnce();
    expect(mocks.navigate).not.toHaveBeenCalled();
  });
});
