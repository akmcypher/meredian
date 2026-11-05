export function authorizeAlertTarget(
  kind: string,
  caller: string,
  requested: string | undefined,
  request?: { submitted_by: string; reviewed_by: string | null; status: string },
) {
  if (kind === "job_failed" || kind === "check_failed") {
    if (requested && requested !== caller) throw new Error("Not authorized to notify this user");
    return caller;
  }
  if (!request) throw new Error("Approval request not found");
  if (kind === "approval_pending") {
    if (request.submitted_by !== caller || request.status !== "pending")
      throw new Error("Not authorized to announce this request");
    return request.submitted_by;
  }
  if (
    request.reviewed_by !== caller ||
    !["approved", "rejected"].includes(request.status) ||
    (requested && requested !== request.submitted_by)
  )
    throw new Error("Not authorized to announce this decision");
  return request.submitted_by;
}
