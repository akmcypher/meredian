import { render, screen, fireEvent, waitFor, cleanup } from "@testing-library/react";
import { beforeEach, afterEach, describe, it, expect, vi } from "vitest";
import type { ComponentType } from "react";
const mocks = vi.hoisted(() => ({
  policy: vi.fn(),
  signup: vi.fn(),
  reset: vi.fn(),
  resend: vi.fn(),
  navigate: vi.fn(),
}));
vi.mock("@tanstack/react-router", () => ({
  createFileRoute: () => (options: unknown) => ({ options }),
  useNavigate: () => mocks.navigate,
}));
vi.mock("@/hooks/use-auth", () => ({
  useAuth: () => ({
    user: null,
    loading: false,
    workspace: null,
    workspaceError: null,
    refreshWorkspace: vi.fn(),
  }),
}));
vi.mock("@/lib/modules", () => ({ MODULES: [] }));
vi.mock("@/integrations/supabase/client", () => ({
  loadSignupPolicy: mocks.policy,
  supabase: {
    auth: {
      signUp: mocks.signup,
      signInWithPassword: vi.fn(),
      signInWithOAuth: vi.fn(),
      resetPasswordForEmail: mocks.reset,
      resend: mocks.resend,
    },
  },
}));
import { Route } from "@/routes/auth";
const Page = Route.options.component as ComponentType;
const fill = (label: string, value: string) =>
  fireEvent.change(screen.getByLabelText(label), { target: { value } });
describe("self-service auth forms", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    sessionStorage.clear();
    mocks.policy.mockResolvedValue(true);
    mocks.signup.mockResolvedValue({ data: { session: null }, error: null });
    mocks.reset.mockResolvedValue({ data: {}, error: null });
  });
  afterEach(cleanup);
  async function signup() {
    render(<Page />);
    fireEvent.click(await screen.findByRole("button", { name: "No account? Sign up" }));
  }
  it("collects name and confirmation and refuses a mismatched password", async () => {
    await signup();
    fill("Full name", "New developer");
    fill("Email", "new@example.test");
    fill("Password", "Local-Strong-123");
    fill("Confirm password", "Mismatch-111111");
    fireEvent.click(screen.getByRole("button", { name: "Create account" }));
    await screen.findByRole("alert");
    expect(mocks.signup).not.toHaveBeenCalled();
  });
  it("uses the Auth signup API and explains verification without claiming a session", async () => {
    await signup();
    fill("Full name", "New developer");
    fill("Email", "new@example.test");
    fill("Password", "Local-Strong-123");
    fill("Confirm password", "Local-Strong-123");
    fireEvent.click(screen.getByRole("button", { name: "Create account" }));
    await waitFor(() => expect(mocks.signup).toHaveBeenCalledOnce());
    await screen.findByRole("heading", { name: "Check your email" });
    expect(screen.getByRole("button", { name: "Resend verification email" })).toBeDisabled();
    expect(mocks.navigate).not.toHaveBeenCalled();
  });
  it("provides a recovery request with an enumeration-safe success message", async () => {
    render(<Page />);
    fireEvent.click(screen.getByRole("button", { name: "Forgot password?" }));
    fill("Email", "new@example.test");
    fireEvent.click(screen.getByRole("button", { name: "Send reset email" }));
    await waitFor(() => expect(mocks.reset).toHaveBeenCalledOnce());
    expect(await screen.findByText(/If an account exists/)).toBeInTheDocument();
  });
});
