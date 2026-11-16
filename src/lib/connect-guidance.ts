// Plain-language guidance for Meridian Connect run codes (connect-protocol.ts `safeError`) and
// the operational event codes the agent records. Each entry says what happened and the next action
// the owner can take; `action` names the control the UI offers for it.

export type GuidanceAction =
  | "check" // re-test the connection
  | "discover" // re-discover streams
  | "settings" // review the agent configuration
  | "runs" // inspect the run's report / accept a gap
  | "retry" // run again
  | "none";

export interface Guidance {
  title: string;
  explanation: string;
  next: string;
  action: GuidanceAction;
  /** Retrying alone does not help. */
  terminal: boolean;
}

const G: Record<string, Guidance> = {
  auth_failed: {
    title: "Sign-in to the source failed",
    explanation:
      "The source rejected the credentials, or TLS failed: wrong user or password, unknown user, an untrusted broker certificate or a hostname mismatch.",
    next: "Correct the credentials or certificate files in the agent's <reference>.json, then test the connection again.",
    action: "check",
    terminal: true,
  },
  source_unavailable: {
    title: "The source could not be reached",
    explanation:
      "The agent could not connect: the host or port is wrong, the network blocks it, the source is down, or a broker dropped a client without a trusted certificate.",
    next: "Check the address and that the agent host can reach it (firewall, VPC, allow-list). Meridian retries automatically; test again once it is reachable.",
    action: "check",
    terminal: false,
  },
  schema_drift: {
    title: "The source schema changed",
    explanation:
      "A configured stream no longer matches the source: a table or column was renamed or removed, or a topic was deleted or recreated.",
    next: "Discover streams again and re-save the affected stream with a valid cursor and key.",
    action: "discover",
    terminal: true,
  },
  connector_failed: {
    title: "The connector refused its configuration",
    explanation:
      "The agent configuration is missing a required setting or contains one the connector refuses (for example PLAIN without TLS, or a managed profile with conflicting security settings).",
    next: "Review the agent file against the settings shown here, fix it on the agent host, and test again.",
    action: "settings",
    terminal: true,
  },
  destination_failed: {
    title: "Writing to bronze failed",
    explanation:
      "Records were read but could not be stored. Nothing was committed and no checkpoint moved.",
    next: "Run the sync again. If it keeps failing, check the agent's storage access and logs.",
    action: "retry",
    terminal: false,
  },
  timeout: {
    title: "The run took too long",
    explanation:
      "The operation exceeded its time limit before finishing; committed batches are kept.",
    next: "Run again; the next run resumes from the last checkpoint.",
    action: "retry",
    terminal: false,
  },
  dead_letter_budget: {
    title: "Too many unreadable records",
    explanation:
      "More records failed to decode than the dead-letter budget allows in one run, so the run stopped without moving past them.",
    next: "Inspect the dead letters (often a producer or schema change), fix the cause, then replay or discard them.",
    action: "runs",
    terminal: true,
  },
  offset_out_of_range: {
    title: "Data expired before it was read",
    explanation:
      "Kafka retention deleted records past the saved position. Nothing was read and no checkpoint moved.",
    next: "Review the gap in the run report and accept it to resume at the oldest available offset; the accepted range is recorded.",
    action: "runs",
    terminal: true,
  },
  source_changed: {
    title: "The topic is not the one that was read before",
    explanation:
      "The cluster or topic identity changed, a partition disappeared, or a saved offset is past the end. Reading on could mix data from different topics.",
    next: "Confirm the source in the run report. If the change is intended, reset the stream to read it from the beginning.",
    action: "runs",
    terminal: true,
  },
  record_too_large: {
    title: "A record is too large",
    explanation: "A single record exceeds the size Connect can store or dead-letter.",
    next: "Reduce the record size at the producer, or skip it by rewinding past the offset shown in the run report.",
    action: "runs",
    terminal: true,
  },
  replay_incomplete: {
    title: "Some dead letters still fail",
    explanation: "A replay finished, but some selected records still could not be decoded.",
    next: "Fix the cause (usually the schema), then replay again or discard them.",
    action: "runs",
    terminal: true,
  },
  // Operational event codes (connect_events), not run failures.
  tls_hostname_verification_disabled: {
    title: "Broker hostname is not verified",
    explanation:
      "The agent file turns off TLS hostname verification, so a broker presenting another host's certificate would be trusted.",
    next: "Remove ssl_endpoint_identification_algorithm from the agent file unless you are testing locally.",
    action: "settings",
    terminal: false,
  },
  run_limit_reached: {
    title: "Run stopped at its limit",
    explanation:
      "The run reached max_run_records or max_run_seconds and stopped at a checkpoint; this is expected when a lot of data is waiting.",
    next: "Nothing to fix: the next run continues from there. Lag shows what is left.",
    action: "none",
    terminal: false,
  },
};

export function guidance(code: string | null | undefined): Guidance | null {
  if (!code || code === "ok") return null;
  return (
    G[code] ?? {
      title: `Run failed (${code})`,
      explanation: "The agent reported a failure Meridian has no specific guidance for.",
      next: "Check the run's events, then run again.",
      action: "retry",
      terminal: false,
    }
  );
}

export const GUIDED_CODES = Object.keys(G);

/** Guidance only for codes Meridian explains (operational events use this: most are routine). */
export function knownGuidance(code: string | null | undefined): Guidance | null {
  return code && Object.hasOwn(G, code) ? G[code]! : null;
}
