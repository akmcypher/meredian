/**
 * One plain addr-spec (`local@domain`), nothing else: no display name, no list separators,
 * no whitespace or line breaks. Alert addresses are written straight into a `To:` header, so
 * anything looser allows extra recipients or header injection. The same rule is
 * enforced by the alert_settings_alert_email_single_address CHECK in the database.
 */
const SINGLE_ADDRESS =
  /^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$/;

export const MAX_EMAIL_LENGTH = 254;

export function isSingleEmailAddress(value: string): boolean {
  return value.length <= MAX_EMAIL_LENGTH && SINGLE_ADDRESS.test(value);
}

/** Returns the trimmed address, or throws when it is not exactly one plain address. */
export function requireSingleEmailAddress(value: string): string {
  const address = value.trim();
  if (!isSingleEmailAddress(address)) throw new Error("Enter a single email address");
  return address;
}
