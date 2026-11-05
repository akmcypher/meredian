import { describe, expect, it } from "vitest";
import { getEmailProvider, readEmailConfig } from "@/lib/email-provider.server";

describe("email provider configuration", () => {
  it("is not configured without EMAIL_PROVIDER", () => {
    expect(readEmailConfig({})).toBeNull();
    expect(getEmailProvider({})).toBeNull();
  });

  it("reads an SMTP relay with STARTTLS by default", () => {
    expect(
      readEmailConfig({
        EMAIL_PROVIDER: "smtp",
        SMTP_HOST: "smtp.example.com",
        SMTP_USER: "relay",
        SMTP_PASSWORD: "pw",
        EMAIL_FROM: "alerts@example.com",
      }),
    ).toEqual({
      provider: "smtp",
      host: "smtp.example.com",
      port: 587,
      security: "starttls",
      user: "relay",
      password: "pw",
      from: "alerts@example.com",
    });
    expect(
      readEmailConfig({
        EMAIL_PROVIDER: "smtp",
        SMTP_HOST: "smtp.example.com",
        SMTP_SECURITY: "tls",
        EMAIL_FROM: "alerts@example.com",
      })?.port,
    ).toBe(465);
  });

  it("refuses incomplete or unsafe settings", () => {
    expect(() => readEmailConfig({ EMAIL_PROVIDER: "gmail-gateway" })).toThrow(/Unsupported/);
    expect(() => readEmailConfig({ EMAIL_PROVIDER: "smtp", EMAIL_FROM: "a@example.com" })).toThrow(
      /SMTP_HOST/,
    );
    expect(() =>
      readEmailConfig({
        EMAIL_PROVIDER: "smtp",
        SMTP_HOST: "h",
        EMAIL_FROM: "a@x.example, b@y.example",
      }),
    ).toThrow(/single email address/);
    expect(() =>
      readEmailConfig({
        EMAIL_PROVIDER: "smtp",
        SMTP_HOST: "h",
        SMTP_SECURITY: "maybe",
        EMAIL_FROM: "a@example.com",
      }),
    ).toThrow(/SMTP_SECURITY/);
  });
});
