import { describe, expect, it } from "vitest";
import { authorizeAlertTarget } from "@/lib/alert-authorization";

describe("alert authorization", () => {
  it("keeps failure alerts caller-only rather than global or forged targets", () => {
    expect(authorizeAlertTarget("job_failed", "alice", undefined)).toBe("alice");
    expect(() => authorizeAlertTarget("check_failed", "alice", "bob")).toThrow();
  });
  it("only lets the submitter announce a pending request", () => {
    const request = { submitted_by: "alice", reviewed_by: null, status: "pending" };
    expect(authorizeAlertTarget("approval_pending", "alice", undefined, request)).toBe("alice");
    expect(() => authorizeAlertTarget("approval_pending", "bob", undefined, request)).toThrow();
    expect(() => authorizeAlertTarget("approval_pending", "alice", undefined)).toThrow();
  });
  it("requires the actual reviewer and persisted final status for decision alerts", () => {
    const request = { submitted_by: "alice", reviewed_by: "erin", status: "approved" };
    expect(authorizeAlertTarget("approval_decided", "erin", "alice", request)).toBe("alice");
    expect(() => authorizeAlertTarget("approval_decided", "bob", "alice", request)).toThrow();
    expect(() => authorizeAlertTarget("approval_decided", "erin", "bob", request)).toThrow();
    expect(() =>
      authorizeAlertTarget("approval_decided", "erin", "alice", { ...request, status: "pending" }),
    ).toThrow();
  });
});
