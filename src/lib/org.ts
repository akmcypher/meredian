import { ensureWorkspace } from "./workspace";
/** The authenticated, membership-checked active organization. */
export async function myOrgId(): Promise<string | null> {
  return (await ensureWorkspace()).workspaceId;
}
