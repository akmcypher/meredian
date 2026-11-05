import { render, screen, fireEvent, waitFor, cleanup } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ComponentType } from "react";
const mocks = vi.hoisted(() => ({ policy: vi.fn(), signUp: vi.fn(), navigate: vi.fn() }));
vi.mock("@tanstack/react-router", () => ({
  createFileRoute: () => (options: unknown) => ({ options }),
  useNavigate: () => mocks.navigate,
}));
vi.mock("@/hooks/use-auth", () => ({ useAuth: () => ({ user: null }) }));
vi.mock("@/lib/modules", () => ({ MODULES: [] }));
vi.mock("@/integrations/supabase/client", () => ({
  loadSignupPolicy: mocks.policy,
  supabase: {
    auth: { signUp: mocks.signUp, signInWithPassword: vi.fn(), signInWithOAuth: vi.fn() },
  },
}));
import { Route } from "@/routes/auth";
const Page = Route.options.component as ComponentType;
// Regression: disabled backend registration must not offer self-service signup.
// Value: protects=backend signup policy; fails_when=signup is offered unconditionally;
describe("authentication registration policy", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });
  afterEach(cleanup);
  it("hides signup while policy is loading and when backend disables it", async () => {
    let resolve!: (enabled: boolean) => void;
    mocks.policy.mockReturnValue(
      new Promise<boolean>((r) => {
        resolve = r;
      }),
    );
    render(<Page />);
    expect(screen.queryByRole("button", { name: "No account? Sign up" })).toBeNull();
    resolve(false);
    await waitFor(() => expect(screen.getByText(/Contact an administrator/)).toBeInTheDocument());
    expect(screen.queryByRole("button", { name: "No account? Sign up" })).toBeNull();
    expect(mocks.signUp).not.toHaveBeenCalled();
  });
  it("offers registration only when backend enables it", async () => {
    mocks.policy.mockResolvedValue(true);
    render(<Page />);
    fireEvent.click(await screen.findByRole("button", { name: "No account? Sign up" }));
    expect(screen.getByRole("heading", { name: "Create account" })).toBeInTheDocument();
  });
  it("keeps sign-in available and hides signup if settings cannot load", async () => {
    mocks.policy.mockRejectedValue(new Error("settings unavailable"));
    render(<Page />);
    await waitFor(() => expect(screen.getByText(/Contact an administrator/)).toBeInTheDocument());
    expect(screen.getByRole("button", { name: "Sign in" })).toBeEnabled();
    expect(screen.queryByRole("button", { name: "No account? Sign up" })).toBeNull();
  });
});
