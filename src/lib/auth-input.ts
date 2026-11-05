import { z } from "zod";
export const emailInput = z.string().trim().email("Enter a valid email address").max(254);
export const passwordInput = z
  .string()
  .min(12, "Use at least 12 characters")
  .max(128, "Use at most 128 characters")
  .regex(/[a-z]/, "Include a lowercase letter")
  .regex(/[A-Z]/, "Include an uppercase letter")
  .regex(/[0-9]/, "Include a number");
export const registrationInput = z
  .object({
    fullName: z.string().trim().min(2, "Enter your full name").max(80),
    email: emailInput,
    password: passwordInput,
    confirmation: z.string(),
  })
  .refine((v) => v.password === v.confirmation, {
    message: "Passwords do not match",
    path: ["confirmation"],
  });
export function authCallbackUrl(origin: string, recovery = false) {
  const url = new URL(origin);
  if (
    url.protocol !== "https:" &&
    !(url.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname))
  )
    throw new Error("Use a secure application origin");
  return new URL("/auth/callback" + (recovery ? "?flow=recovery" : ""), url.origin).href;
}
export function authErrorMessage(error: { code?: string | undefined } | null) {
  switch (error?.code) {
    case "invalid_credentials":
      return "Email or password is incorrect.";
    case "email_not_confirmed":
      return "Verify your email before signing in.";
    case "weak_password":
      return "Use at least 12 characters with uppercase, lowercase and a number.";
    case "over_email_send_rate_limit":
    case "over_request_rate_limit":
      return "Too many attempts. Wait a minute and try again.";
    case "signup_disabled":
      return "Registration is unavailable. Contact an administrator.";
    case "otp_expired":
    case "flow_state_expired":
    case "flow_state_not_found":
      return "This link is invalid or expired. Request a new email.";
    case "reauthentication_needed":
      return "For your security, sign in again or request a password reset.";
    default:
      return "We could not complete that request. Please try again.";
  }
}
