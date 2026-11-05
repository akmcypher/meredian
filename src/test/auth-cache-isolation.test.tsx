import { act, render, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { AuthProvider, useAuth } from "@/hooks/use-auth";

const mocks = vi.hoisted(() => ({
  listener: undefined as
    undefined | ((event: string, session: { user: { id: string } } | null) => void),
}));
vi.mock("@/integrations/supabase/client", () => ({
  supabase: {
    auth: {
      onAuthStateChange: (fn: typeof mocks.listener) => {
        mocks.listener = fn;
        return { data: { subscription: { unsubscribe: vi.fn() } } };
      },
      getSession: async () => ({ data: { session: { user: { id: "alice" } } } }),
    },
    from: () => ({ select: () => ({ eq: async () => ({ data: [{ role: "engineer" }] }) }) }),
  },
}));
function Identity() {
  const { user } = useAuth();
  return <span>{user?.id ?? "none"}</span>;
}

describe("identity cache isolation", () => {
  beforeEach(() => {
    mocks.listener = undefined;
  });
  it("clears private cached rows on account switch and sign-out, not token refresh", async () => {
    const client = new QueryClient();
    const ui = render(
      <QueryClientProvider client={client}>
        <AuthProvider>
          <Identity />
        </AuthProvider>
      </QueryClientProvider>,
    );
    await waitFor(() => expect(ui.getByText("alice")).toBeDefined());
    client.setQueryData(["notebooks"], [{ owner: "alice" }]);
    act(() => mocks.listener?.("TOKEN_REFRESHED", { user: { id: "alice" } }));
    expect(client.getQueryData(["notebooks"])).toEqual([{ owner: "alice" }]);
    act(() => mocks.listener?.("SIGNED_IN", { user: { id: "bob" } }));
    expect(client.getQueryData(["notebooks"])).toBeUndefined();
    client.setQueryData(["notebooks"], [{ owner: "bob" }]);
    act(() => mocks.listener?.("SIGNED_OUT", null));
    expect(client.getQueryData(["notebooks"])).toBeUndefined();
    ui.unmount();
  });
});
