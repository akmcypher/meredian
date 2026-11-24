// New-connection setup model for Meridian Connect. It mirrors what the actors accept
// (deploy/connect/source_kafka.py PROFILES / SECURITY / client_config, source_postgres.py and
// source_mysql.py spec) so the wizard asks only for what a choice needs and refuses, with the same
// reasons, settings the agent would refuse at run time.
//
// Secrets never enter the control plane: database/broker credentials stay worker-local in the
// agent's private `<credential_ref>.json` (CONNECT_SECRET_DIR). The browser records only non-secret
// settings and produces a template of that file in which every secret is a placeholder.

export type ConnectSourceKind = "postgres" | "mysql" | "sqlserver" | "oracle" | "mongodb" | "kafka";

export type KafkaProfileId =
  | "confluent_cloud"
  | "azure_event_hubs"
  | "aws_msk_scram"
  | "aws_msk_iam"
  | "redpanda_cloud"
  | "aiven";

/** Self-managed Kafka security: SASL over TLS, mTLS, or (local development) no security. */
export type KafkaSelfManaged = "sasl_ssl" | "sasl_plaintext" | "mtls" | "plaintext";

export type SaslMechanism = "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512";
export type MskIamCredentials = "default_chain" | "role" | "keys";

export interface SetupState {
  kind: ConnectSourceKind;
  name: string;
  credentialRef: string;
  // PostgreSQL / MySQL / SQL Server / Oracle / MongoDB (host may list several MongoDB hosts;
  // database is the Oracle service name)
  host: string;
  port: string;
  database: string;
  user: string;
  sslmode: string;
  sslCa: string;
  // Change data capture (deploy/connect/README.md): read the database's change log. PostgreSQL
  // reads the tables of a publication the user creates.
  cdc: boolean;
  publication: string;
  // Oracle: the schema (owner) whose tables are offered; empty means the user's own schema.
  schema: string;
  // MongoDB
  replicaSet: string;
  authSource: string;
  // Kafka
  bootstrapServers: string;
  kafkaMode:
    { type: "profile"; profile: KafkaProfileId } | { type: "self"; security: KafkaSelfManaged };
  saslMechanism: SaslMechanism;
  aivenAuth: "SSL" | "SASL_SSL";
  awsRegion: string;
  mskIamCredentials: MskIamCredentials;
  awsRoleArn: string;
  caFile: string;
  certificateFile: string;
  keyFile: string;
  keyHasPassword: boolean;
  schemaRegistryUrl: string;
  schemaRegistryAuth: boolean;
}

export const initialSetup = (kind: ConnectSourceKind = "kafka"): SetupState => ({
  kind,
  name: "",
  credentialRef: "",
  host: "",
  port:
    kind === "mysql"
      ? "3306"
      : kind === "sqlserver"
        ? "1433"
        : kind === "oracle"
          ? "2484"
          : kind === "mongodb"
            ? "27017"
            : "5432",
  database: "",
  user: "",
  sslmode: "require",
  sslCa: "",
  cdc: false,
  publication: "meridian",
  schema: "",
  replicaSet: "",
  authSource: "admin",
  bootstrapServers: "",
  kafkaMode: { type: "profile", profile: "confluent_cloud" },
  saslMechanism: "SCRAM-SHA-512",
  aivenAuth: "SSL",
  awsRegion: "",
  mskIamCredentials: "default_chain",
  awsRoleArn: "",
  caFile: "",
  certificateFile: "",
  keyFile: "",
  keyHasPassword: false,
  schemaRegistryUrl: "",
  schemaRegistryAuth: false,
});

export const SOURCE_KINDS: { id: ConnectSourceKind; label: string; summary: string }[] = [
  {
    id: "kafka",
    label: "Apache Kafka",
    summary: "Topics to bronze, per-partition offsets; managed services or self-managed clusters.",
  },
  {
    id: "postgres",
    label: "PostgreSQL",
    summary: "Tables by cursor or full refresh (no CDC yet).",
  },
  { id: "mysql", label: "MySQL", summary: "Tables by cursor or full refresh (no CDC yet)." },
  {
    id: "sqlserver",
    label: "SQL Server",
    summary: "Tables by cursor or full refresh over verified TLS (no CDC yet).",
  },
  {
    id: "oracle",
    label: "Oracle Database",
    summary: "Tables by cursor or full refresh over verified TLS (TCPS; no CDC yet).",
  },
  {
    id: "mongodb",
    label: "MongoDB",
    summary: "Collections by cursor, full refresh or change streams over verified TLS.",
  },
];

export interface KafkaProfile {
  id: KafkaProfileId;
  label: string;
  summary: string;
  /** Security settings the profile fixes; the agent refuses anything that contradicts them. */
  fixed: Record<string, string>;
  bootstrapHint: string;
}

export const KAFKA_PROFILES: KafkaProfile[] = [
  {
    id: "confluent_cloud",
    label: "Confluent Cloud",
    summary: "Cluster API key and secret (SASL_SSL PLAIN).",
    fixed: { security_protocol: "SASL_SSL", sasl_mechanism: "PLAIN" },
    bootstrapHint: "pkc-xxxxx.region.provider.confluent.cloud:9092",
  },
  {
    id: "azure_event_hubs",
    label: "Azure Event Hubs",
    summary: "Namespace or hub connection string (SASL_SSL PLAIN, port 9093).",
    fixed: {
      security_protocol: "SASL_SSL",
      sasl_mechanism: "PLAIN",
      sasl_username: "$ConnectionString",
    },
    bootstrapHint: "my-namespace.servicebus.windows.net:9093",
  },
  {
    id: "aws_msk_scram",
    label: "Amazon MSK (SCRAM)",
    summary: "SCRAM-SHA-512 user and password from Secrets Manager.",
    fixed: { security_protocol: "SASL_SSL", sasl_mechanism: "SCRAM-SHA-512" },
    bootstrapHint: "b-1.cluster.xxxx.kafka.region.amazonaws.com:9096",
  },
  {
    id: "aws_msk_iam",
    label: "Amazon MSK (IAM)",
    summary: "Short-lived SigV4 tokens over SASL OAUTHBEARER.",
    fixed: { security_protocol: "SASL_SSL", sasl_mechanism: "OAUTHBEARER" },
    bootstrapHint: "b-1.cluster.xxxx.kafka.region.amazonaws.com:9098",
  },
  {
    id: "redpanda_cloud",
    label: "Redpanda Cloud",
    summary: "SCRAM-SHA-256 (default) or SCRAM-SHA-512 over SASL_SSL.",
    fixed: { security_protocol: "SASL_SSL" },
    bootstrapHint: "seed-xxxx.cluster.redpanda.com:9092",
  },
  {
    id: "aiven",
    label: "Aiven for Apache Kafka",
    summary: "Client certificates (SSL) or SCRAM over SASL_SSL.",
    fixed: {},
    bootstrapHint: "kafka-xxxx.aivencloud.com:12345",
  },
];

export const SELF_MANAGED: { id: KafkaSelfManaged; label: string; summary: string }[] = [
  {
    id: "sasl_ssl",
    label: "SASL over TLS",
    summary: "SCRAM-SHA-256/512 or PLAIN user and password, encrypted with TLS.",
  },
  {
    id: "mtls",
    label: "Mutual TLS",
    summary: "Client certificate and key; broker certificate verified.",
  },
  {
    id: "sasl_plaintext",
    label: "SASL without TLS",
    summary: "SCRAM only; the connection is not encrypted.",
  },
  {
    id: "plaintext",
    label: "No security (local development)",
    summary: "PLAINTEXT listener with no authentication.",
  },
];

/** Which mechanisms a choice may use; empty when the mechanism is fixed or not SASL. */
export function mechanismChoices(s: SetupState): SaslMechanism[] {
  const m = s.kafkaMode;
  if (m.type === "profile") {
    if (m.profile === "redpanda_cloud") return ["SCRAM-SHA-256", "SCRAM-SHA-512"];
    if (m.profile === "aiven" && s.aivenAuth === "SASL_SSL")
      return ["SCRAM-SHA-256", "SCRAM-SHA-512"];
    return [];
  }
  if (m.security === "sasl_ssl" || m.security === "sasl_plaintext")
    return ["SCRAM-SHA-512", "SCRAM-SHA-256", "PLAIN"];
  return [];
}

/** Effective security_protocol / sasl_mechanism after the profile is applied. */
export function effectiveSecurity(s: SetupState): { protocol: string; mechanism: string | null } {
  const m = s.kafkaMode;
  if (m.type === "profile") {
    const fixed = KAFKA_PROFILES.find((p) => p.id === m.profile)!.fixed;
    if (m.profile === "aiven")
      return {
        protocol: s.aivenAuth,
        mechanism: s.aivenAuth === "SASL_SSL" ? s.saslMechanism : null,
      };
    if (m.profile === "redpanda_cloud") return { protocol: "SASL_SSL", mechanism: s.saslMechanism };
    return { protocol: fixed["security_protocol"]!, mechanism: fixed["sasl_mechanism"] ?? null };
  }
  switch (m.security) {
    case "sasl_ssl":
      return { protocol: "SASL_SSL", mechanism: s.saslMechanism };
    case "sasl_plaintext":
      return { protocol: "SASL_PLAINTEXT", mechanism: s.saslMechanism };
    case "mtls":
      return { protocol: "SSL", mechanism: null };
    case "plaintext":
      return { protocol: "PLAINTEXT", mechanism: null };
  }
}

export interface SecretField {
  /** Key in the agent's private `<credential_ref>.json`. */
  key: string;
  label: string;
  hint?: string;
}

/** Secret values the agent file must hold for this choice. They are never entered in the browser. */
export function secretFields(s: SetupState): SecretField[] {
  if (s.kind !== "kafka") return [{ key: "password", label: "Database password" }];
  const out: SecretField[] = [];
  const m = s.kafkaMode;
  const { protocol, mechanism } = effectiveSecurity(s);
  if (m.type === "profile" && m.profile === "confluent_cloud")
    out.push(
      { key: "api_key", label: "Cluster API key" },
      { key: "api_secret", label: "Cluster API secret" },
    );
  else if (m.type === "profile" && m.profile === "azure_event_hubs")
    out.push({
      key: "connection_string",
      label: "Event Hubs connection string",
      hint: "Must start with Endpoint=sb:// — the agent refuses anything else.",
    });
  else if (m.type === "profile" && m.profile === "aws_msk_iam") {
    if (s.mskIamCredentials === "keys")
      out.push(
        { key: "aws_access_key_id", label: "AWS access key ID" },
        { key: "aws_secret_access_key", label: "AWS secret access key" },
        {
          key: "aws_session_token",
          label: "AWS session token (optional)",
          hint: "Remove the key if you use long-lived keys.",
        },
      );
  } else if (protocol.startsWith("SASL") && mechanism)
    out.push(
      { key: "sasl_username", label: "SASL user name" },
      { key: "sasl_password", label: "SASL password" },
    );
  if (protocol === "SSL" && s.keyHasPassword)
    out.push({ key: "ssl_key_password", label: "Client key password" });
  if (s.schemaRegistryUrl.trim() && s.schemaRegistryAuth)
    out.push(
      { key: "schema_registry_username", label: "Schema registry user" },
      { key: "schema_registry_password", label: "Schema registry password" },
    );
  return out;
}

/** Visible non-secret fields for the chosen source and Kafka security. */
export function visibleFields(s: SetupState) {
  const { protocol } = effectiveSecurity(s);
  const m = s.kafkaMode;
  const kafka = s.kind === "kafka";
  return {
    database: !kafka,
    awsRegion: kafka && m.type === "profile" && m.profile === "aws_msk_iam",
    mskIamCredentials: kafka && m.type === "profile" && m.profile === "aws_msk_iam",
    awsRoleArn:
      kafka &&
      m.type === "profile" &&
      m.profile === "aws_msk_iam" &&
      s.mskIamCredentials === "role",
    aivenAuth: kafka && m.type === "profile" && m.profile === "aiven",
    mechanism: kafka && mechanismChoices(s).length > 0,
    // Managed services use public CAs from the system trust store, except Aiven, which signs
    // brokers with a per-project CA; self-managed clusters may use a private CA.
    caFile:
      kafka &&
      (protocol === "SSL" || protocol === "SASL_SSL") &&
      (m.type === "self" || m.profile === "aiven"),
    clientCertificate: kafka && protocol === "SSL",
  };
}

const HOST_PORT = /^[A-Za-z0-9.-]+:\d{1,5}$/;
const CREDENTIAL_REF = /^[a-zA-Z0-9_-]{1,80}$/;
// Same pattern as AWS_REGION in source_kafka.py.
const AWS_REGION = /^[a-z]{2}(-gov)?-[a-z]+-\d$/;
const ROLE_ARN = /^arn:aws[a-z-]*:iam::\d{12}:role\/[\w+=,.@/-]{1,512}$/;
// File names are resolved next to the agent config; no parent-directory escapes.
const FILE_NAME = /^[\w.-]{1,120}$/;

/**
 * Problems that block the credentials step. Each mirrors a refusal in the actor (same reason),
 * or a value the server-side validators reject.
 */
export function setupProblems(s: SetupState): string[] {
  const p: string[] = [];
  if (!s.name.trim()) p.push("Name the connection.");
  else if (s.name.trim().length > 80) p.push("Connection names are at most 80 characters.");
  if (!CREDENTIAL_REF.test(s.credentialRef))
    p.push(
      "Credential reference: 1–80 letters, digits, '-' or '_'. It names the agent's private <reference>.json file.",
    );
  if (s.kind !== "kafka") {
    if (s.kind === "sqlserver" && s.cdc) p.push("SQL Server CDC is not available yet.");
    if (s.kind === "oracle") {
      if (s.cdc) p.push("Oracle CDC is not available yet.");
      if (/[@/()=\s]/.test(s.host.trim()))
        p.push("Enter the Oracle host name only, not a connect descriptor or EZConnect string.");
      if (s.database.trim() && !/^[A-Za-z0-9_.-]{1,128}$/.test(s.database.trim()))
        p.push("Service name: 1–128 letters, digits, '.', '-' or '_'.");
      if (s.schema.trim() && !/^[A-Za-z_][A-Za-z0-9_]{0,62}$/.test(s.schema.trim()))
        p.push("Schema: letters, digits and '_', starting with a letter.");
    }
    if (s.kind === "mongodb") {
      if (s.cdc && !s.replicaSet.trim())
        p.push("Change streams need a replica set or sharded cluster: enter the replica set name.");
      if (/[@/?#\s]/.test(s.host.trim()))
        p.push("Enter MongoDB hosts as host or host1,host2 without credentials or options.");
      if (s.replicaSet.trim() && !/^[A-Za-z0-9_.-]{1,64}$/.test(s.replicaSet.trim()))
        p.push("Replica set: 1–64 letters, digits, '.', '-' or '_'.");
      if (!/^[A-Za-z0-9_.-]{1,64}$/.test(s.authSource.trim()))
        p.push("Authentication database: 1–64 letters, digits, '.', '-' or '_'.");
    }
    if (!s.host.trim()) p.push("Enter the database host.");
    const port = Number(s.port);
    if (!Number.isInteger(port) || port < 1 || port > 65535) p.push("Port must be 1–65535.");
    if (!s.database.trim())
      p.push(s.kind === "oracle" ? "Enter the service name." : "Enter the database name.");
    if (!s.user.trim()) p.push("Enter the database user.");
    if (
      s.cdc &&
      s.kind === "postgres" &&
      !/^[A-Za-z_][A-Za-z0-9_]{0,62}$/.test(s.publication.trim())
    )
      p.push("Publication: the name of a PostgreSQL publication (letters, digits and '_').");
    return p;
  }
  const servers = s.bootstrapServers
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
  if (!servers.length || !servers.every((x) => HOST_PORT.test(x)))
    p.push("Bootstrap servers: comma-separated host:port pairs.");
  const m = s.kafkaMode;
  const { protocol, mechanism } = effectiveSecurity(s);
  if (protocol === "SASL_PLAINTEXT" && mechanism === "PLAIN")
    p.push(
      "PLAIN without TLS sends the password in clear text; the agent refuses it. Use SCRAM, or SASL over TLS.",
    );
  if (m.type === "profile" && m.profile === "aws_msk_iam") {
    if (!AWS_REGION.test(s.awsRegion.trim()))
      p.push("MSK IAM needs the cluster's AWS region, for example eu-west-1.");
    if (s.mskIamCredentials === "role" && !ROLE_ARN.test(s.awsRoleArn.trim()))
      p.push("Enter the IAM role ARN to assume (arn:aws:iam::<account>:role/<name>).");
  }
  if (m.type === "profile" && m.profile === "azure_event_hubs" && servers.length)
    if (!servers.every((x) => x.endsWith(":9093")))
      p.push("Event Hubs exposes its Kafka endpoint on port 9093.");
  if (protocol === "SSL" && (!s.certificateFile.trim() || !s.keyFile.trim()))
    p.push("Mutual TLS needs the client certificate and key file names.");
  for (const [label, v] of [
    ["CA file", s.caFile],
    ["Certificate file", s.certificateFile],
    ["Key file", s.keyFile],
  ] as const)
    if (v.trim() && !FILE_NAME.test(v.trim()))
      p.push(`${label}: a file name in the agent's secrets directory (no folders).`);
  const registry = s.schemaRegistryUrl.trim();
  if (registry) {
    let url: URL | null = null;
    try {
      url = new URL(registry);
    } catch {
      url = null;
    }
    if (!url || !["http:", "https:"].includes(url.protocol) || url.username || url.password)
      p.push("Schema registry URL: http(s) address with no credentials in it.");
    else if (s.schemaRegistryAuth && url.protocol === "http:")
      p.push("Schema registry credentials are refused over plain HTTP; use https.");
  }
  return p;
}

/** Warnings that do not block (the agent accepts the setting). */
export function setupWarnings(s: SetupState): string[] {
  if (s.kind !== "kafka") return [];
  const { protocol } = effectiveSecurity(s);
  if (protocol === "PLAINTEXT")
    return ["No encryption and no authentication: use only for a local development broker."];
  if (protocol === "SASL_PLAINTEXT")
    return ["The connection is not encrypted; SCRAM protects the password but not the data."];
  return [];
}

export const SECRET_PLACEHOLDER = "<set on the agent host>";

/**
 * The non-secret settings recorded with the source (`ingest_sources.config`) and the agent file
 * template. Both are built from an allowlist; secret values are only ever placeholders.
 */
export function buildConfig(s: SetupState): {
  publicConfig: Record<string, string | number>;
  agentFile: Record<string, string | number | boolean>;
} {
  const pub: Record<string, string | number> = {};
  if (s.kind === "postgres") {
    Object.assign(pub, {
      host: s.host.trim(),
      port: Number(s.port),
      dbname: s.database.trim(),
      user: s.user.trim(),
      sslmode: s.sslmode,
    });
    if (s.cdc) pub["publication"] = s.publication.trim();
  } else if (s.kind === "mysql" || s.kind === "sqlserver") {
    Object.assign(pub, {
      host: s.host.trim(),
      port: Number(s.port),
      database: s.database.trim(),
      user: s.user.trim(),
    });
    if (s.sslCa.trim()) pub["ssl_ca"] = s.sslCa.trim();
  } else if (s.kind === "oracle") {
    Object.assign(pub, {
      host: s.host.trim(),
      port: Number(s.port),
      service_name: s.database.trim(),
      user: s.user.trim(),
    });
    if (s.schema.trim()) pub["schema"] = s.schema.trim();
    if (s.sslCa.trim()) pub["ssl_ca"] = s.sslCa.trim();
  } else if (s.kind === "mongodb") {
    Object.assign(pub, {
      hosts: s.host.trim(),
      port: Number(s.port),
      database: s.database.trim(),
      user: s.user.trim(),
      auth_source: s.authSource.trim() || "admin",
    });
    if (s.replicaSet.trim()) pub["replica_set"] = s.replicaSet.trim();
    if (s.sslCa.trim()) pub["ssl_ca"] = s.sslCa.trim();
  } else {
    const m = s.kafkaMode;
    const show = visibleFields(s);
    pub["bootstrap_servers"] = s.bootstrapServers
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean)
      .join(",");
    const { protocol, mechanism } = effectiveSecurity(s);
    if (m.type === "profile") {
      pub["profile"] = m.profile;
      // Only the choices a profile leaves open; the profile fills in the rest.
      if (m.profile === "aiven") pub["security_protocol"] = protocol;
      if (show.mechanism && mechanism) pub["sasl_mechanism"] = mechanism;
      if (m.profile === "aws_msk_iam") {
        pub["aws_region"] = s.awsRegion.trim();
        if (s.mskIamCredentials === "role") pub["aws_role_arn"] = s.awsRoleArn.trim();
      }
    } else {
      pub["security_protocol"] = protocol;
      if (mechanism) pub["sasl_mechanism"] = mechanism;
    }
    if (show.caFile && s.caFile.trim()) pub["ssl_ca_location"] = s.caFile.trim();
    if (show.clientCertificate) {
      pub["ssl_certificate_location"] = s.certificateFile.trim();
      pub["ssl_key_location"] = s.keyFile.trim();
    }
    if (s.schemaRegistryUrl.trim()) pub["schema_registry_url"] = s.schemaRegistryUrl.trim();
  }
  const agentFile: Record<string, string | number | boolean> = { ...pub };
  for (const f of secretFields(s)) agentFile[f.key] = SECRET_PLACEHOLDER;
  return { publicConfig: pub, agentFile };
}

/** Keys a stored source config may show on read-back; anything else is hidden. */
export const PUBLIC_CONFIG_KEYS = new Set([
  "host",
  "port",
  "dbname",
  "database",
  "service_name",
  "schema",
  "user",
  "sslmode",
  "ssl_ca",
  "publication",
  "hosts",
  "replica_set",
  "auth_source",
  "bootstrap_servers",
  "profile",
  "security_protocol",
  "sasl_mechanism",
  "aws_region",
  "aws_role_arn",
  "ssl_ca_location",
  "ssl_certificate_location",
  "ssl_key_location",
  "schema_registry_url",
]);

/** Allowlisted settings of a source row read back with `config->>key` selects. */
export function publicConfigEntries(row: Partial<Record<string, unknown>>): [string, string][] {
  return [...PUBLIC_CONFIG_KEYS]
    .filter((k) => row[k] !== null && row[k] !== undefined && row[k] !== "")
    .map((k) => [k, String(row[k])]);
}

/** Bronze target table name for a new source (server re-validates with safeTableName). */
export function targetTable(name: string) {
  const t = name
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 50);
  return `connect_${t || "source"}`.slice(0, 60);
}

/** 0 = manual; otherwise 5..10080 minutes (connect_connections CHECK and configureConnect). */
export function scheduleProblem(minutes: number): string | null {
  if (!Number.isInteger(minutes)) return "Use whole minutes.";
  if (minutes === 0) return null;
  if (minutes < 5) return "Scheduled syncs run at most every 5 minutes (0 = manual only).";
  if (minutes > 10080) return "The longest schedule is weekly (10080 minutes).";
  return null;
}

/**
 * Rebuilds a setup from a source's stored non-secret settings (the inverse of buildConfig's public
 * part), so the Settings tab can show the same agent file template. Secrets are not stored, so
 * which optional secrets apply (key password, registry login, MSK keys) cannot be recovered.
 */
export function setupFromStored(
  kind: ConnectSourceKind,
  name: string,
  credentialRef: string,
  row: Partial<Record<string, unknown>>,
): SetupState {
  const v = (k: string) => (row[k] === null || row[k] === undefined ? "" : String(row[k]));
  const s = { ...initialSetup(kind), name, credentialRef };
  if (kind !== "kafka")
    return {
      ...s,
      host: v(kind === "mongodb" ? "hosts" : "host"),
      port: v("port") || s.port,
      database: v(kind === "postgres" ? "dbname" : kind === "oracle" ? "service_name" : "database"),
      user: v("user"),
      sslmode: v("sslmode") || s.sslmode,
      sslCa: v("ssl_ca"),
      cdc: kind === "postgres" && !!v("publication"),
      publication: v("publication") || s.publication,
      schema: v("schema"),
      replicaSet: v("replica_set"),
      authSource: v("auth_source") || s.authSource,
    };
  const protocol = v("security_protocol");
  const mechanism = (v("sasl_mechanism") || s.saslMechanism) as SaslMechanism;
  const profile = KAFKA_PROFILES.find((p) => p.id === v("profile"));
  const security: KafkaSelfManaged =
    protocol === "SASL_SSL"
      ? "sasl_ssl"
      : protocol === "SASL_PLAINTEXT"
        ? "sasl_plaintext"
        : protocol === "SSL"
          ? "mtls"
          : "plaintext";
  return {
    ...s,
    bootstrapServers: v("bootstrap_servers"),
    kafkaMode: profile ? { type: "profile", profile: profile.id } : { type: "self", security },
    saslMechanism: mechanism,
    aivenAuth: protocol === "SASL_SSL" ? "SASL_SSL" : "SSL",
    awsRegion: v("aws_region"),
    mskIamCredentials: v("aws_role_arn") ? "role" : "default_chain",
    awsRoleArn: v("aws_role_arn"),
    caFile: v("ssl_ca_location"),
    certificateFile: v("ssl_certificate_location"),
    keyFile: v("ssl_key_location"),
    schemaRegistryUrl: v("schema_registry_url"),
  };
}
