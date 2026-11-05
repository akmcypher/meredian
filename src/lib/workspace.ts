import { z } from "zod";
import { supabase } from "@/integrations/supabase/client";
const workspaceSchema = z.object({
  workspaceId: z.string().uuid(),
  name: z.string(),
  description: z.string(),
  status: z.string(),
  workspaceRole: z.enum(["owner", "admin", "engineer", "member"]),
  onboardingStep: z.number().int().min(0).max(3),
  onboardingCompleted: z.boolean(),
  displayName: z.string().nullable(),
  firstSource: z.string().nullable(),
});
export type Workspace = z.infer<typeof workspaceSchema>;
export function parseWorkspace(data: unknown): Workspace {
  const result = workspaceSchema.safeParse(data);
  if (!result.success) throw new Error("Workspace information is unavailable. Please retry.");
  return result.data;
}
export async function ensureWorkspace(): Promise<Workspace> {
  const { data, error } = await supabase.rpc("ensure_workspace");
  if (error)
    throw new Error(
      "Workspace access is unavailable. Verify your email or contact an administrator.",
    );
  return parseWorkspace(data);
}
export async function saveOnboarding(
  step: number,
  details: { name?: string; description?: string; firstSource?: string } = {},
): Promise<Workspace> {
  const { data, error } = await supabase.rpc("update_onboarding", {
    _step: step,
    ...(details.name !== undefined ? { _name: details.name } : {}),
    ...(details.description !== undefined ? { _description: details.description } : {}),
    ...(details.firstSource !== undefined ? { _first_source: details.firstSource } : {}),
  });
  if (error)
    throw new Error(
      "Your workspace settings could not be saved. Check the current step and try again.",
    );
  return parseWorkspace(data);
}
