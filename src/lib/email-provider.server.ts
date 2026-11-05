/**
 * Provider-neutral outbound email. Alerts call `getEmailProvider()`; which provider runs is
 * configuration, not code. Implemented: SMTP (any relay: Amazon SES, SendGrid, Postmark,
 * Mailgun, Microsoft 365, a self-hosted MTA, or Mailpit locally). Unset = not configured, and
 * sending fails loudly instead of dropping mail. Credentials are server env only.
 *
 * Runtime: SMTP needs TCP sockets, so it works on the Node server build (the default), not on
 * edge runtimes such as Cloudflare Workers; an HTTP-API provider would be added as another
 * implementation of EmailProvider for those.
 */
import { requireSingleEmailAddress } from "./email-address";

export type EmailMessage = { to: string; subject: string; text: string };
export interface EmailProvider {
  readonly name: string;
  send(message: EmailMessage): Promise<{ messageId: string }>;
}

export type SmtpConfig = {
  provider: "smtp";
  host: string;
  port: number;
  /** "tls" = implicit TLS (465); "starttls" = upgrade required; "none" = local relays only. */
  security: "tls" | "starttls" | "none";
  user?: string;
  password?: string;
  from: string;
};

type Env = Record<string, string | undefined>;

/** Reads EMAIL_PROVIDER / SMTP_* / EMAIL_FROM; null when email is not configured. */
export function readEmailConfig(env: Env = process.env): SmtpConfig | null {
  const provider = env["EMAIL_PROVIDER"]?.trim().toLowerCase();
  if (!provider) return null;
  if (provider !== "smtp") throw new Error(`Unsupported EMAIL_PROVIDER "${provider}" (use smtp)`);
  const host = env["SMTP_HOST"]?.trim();
  const from = env["EMAIL_FROM"]?.trim();
  if (!host || !from) throw new Error("EMAIL_PROVIDER=smtp needs SMTP_HOST and EMAIL_FROM");
  const security = (env["SMTP_SECURITY"]?.trim().toLowerCase() || "starttls") as
    "tls" | "starttls" | "none";
  if (!["tls", "starttls", "none"].includes(security))
    throw new Error("SMTP_SECURITY must be tls, starttls or none");
  const port = Number(env["SMTP_PORT"] || (security === "tls" ? 465 : 587));
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error("Invalid SMTP_PORT");
  const user = env["SMTP_USER"]?.trim();
  const password = env["SMTP_PASSWORD"];
  return {
    provider: "smtp",
    host,
    port,
    security,
    from: requireSingleEmailAddress(from),
    ...(user ? { user } : {}),
    ...(password ? { password } : {}),
  };
}

export function smtpProvider(config: SmtpConfig): EmailProvider {
  return {
    name: "smtp",
    async send({ to, subject, text }) {
      const { createTransport } = await import("nodemailer");
      const transport = createTransport({
        host: config.host,
        port: config.port,
        secure: config.security === "tls",
        requireTLS: config.security === "starttls",
        ignoreTLS: config.security === "none",
        ...(config.user ? { auth: { user: config.user, pass: config.password ?? "" } } : {}),
        connectionTimeout: 15000,
        greetingTimeout: 15000,
        socketTimeout: 30000,
      });
      try {
        const info = await transport.sendMail({ from: config.from, to, subject, text });
        if (info.rejected.length) throw new Error("The mail server rejected the recipient");
        return { messageId: info.messageId };
      } finally {
        transport.close();
      }
    },
  };
}

export function getEmailProvider(env: Env = process.env): EmailProvider | null {
  const config = readEmailConfig(env);
  return config ? smtpProvider(config) : null;
}
