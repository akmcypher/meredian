import { describe, it, expect } from "vitest";
import { registrationInput, authCallbackUrl, authErrorMessage } from "@/lib/auth-input";
const input = {
  fullName: "Developer",
  email: "owner@example.test",
  password: "Long-Password-123",
  confirmation: "Long-Password-123",
};
describe("authentication input boundary", () => {
  it("accepts a valid registration and normalizes display/email whitespace", () => {
    expect(
      registrationInput.parse({ ...input, fullName: " Developer ", email: " owner@example.test " })
        .fullName,
    ).toBe("Developer");
  });
  it.each([
    { password: "weak", confirmation: "weak" },
    { email: "invalid" },
    { confirmation: "Different-Password-123" },
    { fullName: " " },
    { password: "A".repeat(150), confirmation: "A".repeat(150) },
  ])("rejects invalid input %#", (data) => {
    expect(registrationInput.safeParse({ ...input, ...data }).success).toBe(false);
  });
  it("builds same-origin callbacks and refuses insecure remote origins", () => {
    expect(authCallbackUrl("https://app.example/untrusted?next=https://other.example")).toBe(
      "https://app.example/auth/callback",
    );
    expect(authCallbackUrl("http://localhost:8090", true)).toBe(
      "http://localhost:8090/auth/callback?flow=recovery",
    );
    expect(() => authCallbackUrl("http://remote.example")).toThrow();
  });
  it("never exposes unknown backend details in an auth error", () => {
    expect(authErrorMessage({ code: "database-internal" })).toBe(
      "We could not complete that request. Please try again.",
    );
  });
});
