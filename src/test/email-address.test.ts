import { describe, expect, it } from "vitest";
import { isSingleEmailAddress, requireSingleEmailAddress } from "@/lib/email-address";
import { buildEmail } from "@/lib/alerts.server";

describe("single alert email address", () => {
  it.each(["me@example.com", "first.last+alerts@sub.mail.example", "o'neil@example.org"])(
    "accepts %s",
    (value) => expect(isSingleEmailAddress(value)).toBe(true),
  );

  it.each([
    "",
    "me@x, a@y",
    "me@x; a@y",
    "me@example.com\r\nBcc: list@evil.test",
    "me@example.com\nBcc: list@evil.test",
    "Me <me@example.com>",
    "me @example.com",
    "me@localhost",
    "me@-bad.example.com",
    `${"a".repeat(250)}@example.com`,
  ])("rejects %j", (value) => expect(isSingleEmailAddress(value)).toBe(false));

  it("trims a valid address and throws for anything else", () => {
    expect(requireSingleEmailAddress("  me@example.com ")).toBe("me@example.com");
    expect(() => requireSingleEmailAddress("me@x, a@y")).toThrow("single email address");
  });

  it("builds a message with exactly one recipient and refuses injected headers", () => {
    expect(buildEmail(" me@example.com ", "Subject", "Body")).toEqual({
      to: "me@example.com",
      subject: "Subject",
      text: "Body",
    });
    expect(() => buildEmail("me@x.example\r\nBcc: list@evil.test", "Subject", "Body")).toThrow();
    expect(() => buildEmail("me@example.com", "Subject\r\nBcc: list@evil.test", "Body")).toThrow();
  });
});
