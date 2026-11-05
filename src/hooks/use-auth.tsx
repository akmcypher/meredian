import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { useQueryClient } from "@tanstack/react-query";
import type { User } from "@supabase/supabase-js";
import { supabase } from "@/integrations/supabase/client";
import { ensureWorkspace, type Workspace } from "@/lib/workspace";
type Auth = {
  user: User | null;
  roles: string[];
  workspace: Workspace | null;
  workspaceError: string | null;
  loading: boolean;
  hasRole: (...r: string[]) => boolean;
  refreshRoles: () => void;
  refreshWorkspace: () => Promise<Workspace | null>;
};
const Ctx = createContext<Auth>({
  user: null,
  roles: [],
  workspace: null,
  workspaceError: null,
  loading: true,
  hasRole: () => false,
  refreshRoles: () => {},
  refreshWorkspace: async () => null,
});
export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null),
    [roles, setRoles] = useState<string[]>([]),
    [workspace, setWorkspace] = useState<Workspace | null>(null),
    [workspaceError, setWorkspaceError] = useState<string | null>(null),
    [loading, setLoading] = useState(true);
  const queryClient = useQueryClient(),
    identity = useRef<string | null>(null),
    scope = useRef<string | null>(null);
  const loadRoles = useCallback(async (u: User | null) => {
    if (!u) return setRoles([]);
    const { data } = await supabase.from("user_roles").select("role").eq("user_id", u.id);
    if (identity.current === u.id) setRoles((data ?? []).map((r) => r.role));
  }, []);
  const loadWorkspace = useCallback(
    async (u: User | null): Promise<Workspace | null> => {
      if (!u?.email_confirmed_at) {
        if (identity.current === (u?.id ?? null)) {
          setWorkspace(null);
          setWorkspaceError(null);
        }
        return null;
      }
      try {
        const next = await ensureWorkspace();
        if (identity.current !== u.id) return null;
        if (scope.current && scope.current !== next.workspaceId) {
          await queryClient.cancelQueries();
          queryClient.clear();
        }
        scope.current = next.workspaceId;
        setWorkspace(next);
        setWorkspaceError(null);
        return next;
      } catch {
        if (identity.current === u.id) {
          setWorkspace(null);
          setWorkspaceError(
            "Workspace access is unavailable. Verify your email or contact an administrator.",
          );
        }
        return null;
      }
    },
    [queryClient],
  );
  useEffect(() => {
    let version = 0;
    const hydrate = async (u: User | null) => {
      await Promise.all([loadRoles(u), loadWorkspace(u)]);
      if (identity.current === (u?.id ?? null)) setLoading(false);
    };
    const accept = (u: User | null) => {
      const next = u?.id ?? null;
      if (identity.current !== next) {
        void queryClient.cancelQueries();
        queryClient.clear();
        setRoles([]);
        setWorkspace(null);
        setWorkspaceError(null);
        scope.current = null;
        identity.current = next;
      }
      setUser(u);
      setLoading(true);
      setTimeout(() => void hydrate(u), 0);
    };
    const { data: sub } = supabase.auth.onAuthStateChange((event, session) => {
      if (event === "TOKEN_REFRESHED") return;
      version++;
      accept(session?.user ?? null);
    });
    const initial = version;
    void supabase.auth.getSession().then(({ data }) => {
      if (version === initial) accept(data.session?.user ?? null);
    });
    return () => sub.subscription.unsubscribe();
  }, [queryClient, loadRoles, loadWorkspace]);
  return (
    <Ctx.Provider
      value={{
        user,
        roles,
        workspace,
        workspaceError,
        loading,
        hasRole: (...r) => r.some((x) => roles.includes(x)),
        refreshRoles: () => void loadRoles(user),
        refreshWorkspace: () => loadWorkspace(user),
      }}
    >
      {children}
    </Ctx.Provider>
  );
}
export const useAuth = () => useContext(Ctx);
