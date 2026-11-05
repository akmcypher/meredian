// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
  vi.resetModules();
});

describe("server error logging boundary", () => {
  it("omits untrusted error details and structured payloads in production", async () => {
    vi.stubEnv("DEV", false);
    const sink = vi.spyOn(console, "error").mockImplementation(() => {});
    const { consumeLastCapturedError } = await import("@/lib/error-capture");
    const error = Object.assign(
      new TypeError("postgres://user:synthetic-password@db.invalid/private"),
      {
        status: 503,
        cause: new Error("Authorization: Bearer synthetic-token"),
      },
    );
    console.error(
      error,
      { password: "synthetic-password", email: "private@example.test" },
      "token=synthetic-token",
    );
    const output = JSON.stringify(sink.mock.calls);
    expect(output).toContain("TypeError");
    expect(output).toContain("503");
    for (const secret of [
      "synthetic-password",
      "synthetic-token",
      "private@example.test",
      "postgres://",
    ])
      expect(output).not.toContain(secret);
    expect(consumeLastCapturedError()).toBe(error);
  });
  it("does not trust a custom Error name as safe production metadata", async () => {
    vi.stubEnv("DEV", false);
    vi.spyOn(console, "error").mockImplementation(() => {});
    const { describeError } = await import("@/lib/error-capture");
    const error = new Error("private detail");
    error.name = "token=synthetic-token";
    expect(describeError(error)).toBe("Error");
  });
  it("retains diagnostic messages and causes in development", async () => {
    vi.stubEnv("DEV", true);
    vi.spyOn(console, "error").mockImplementation(() => {});
    const { describeError } = await import("@/lib/error-capture");
    const output = describeError(new Error("outer", { cause: new Error("inner") }));
    expect(output).toContain("outer");
    expect(output).toContain("caused by:");
    expect(output).toContain("inner");
  });
});
