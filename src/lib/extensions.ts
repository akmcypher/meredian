/**
 * Versioned extension contracts.
 *
 * An extension implements a named contract (`apiVersion`) and is selected on the server only for
 * organizations that hold its entitlement (has_entitlement); without one, the Community default
 * of that contract runs. Implementations may run as separately signed services. Changing a
 * contract's shape requires a new apiVersion.
 */
export interface Extension<V extends string = string> {
  apiVersion: V;
  id: string;
  /** Entitlement an organization needs; undefined for a Community default. */
  entitlement?: string;
}

/** Picks the first extension the organization is entitled to, else the Community default. */
export function selectExtension<E extends Extension>(
  candidates: readonly E[],
  fallback: E,
  entitled: (feature: string) => boolean,
): E {
  return candidates.find((c) => c.entitlement && entitled(c.entitlement)) ?? fallback;
}
