import { describe, expect, it } from "vitest";
import {
  buildConfig,
  initialSetup,
  KAFKA_PROFILES,
  PUBLIC_CONFIG_KEYS,
  publicConfigEntries,
  scheduleProblem,
  SECRET_PLACEHOLDER,
  secretFields,
  setupFromStored,
  setupProblems,
  targetTable,
  visibleFields,
  type SetupState,
} from "@/lib/connect-setup";
import { guidance, GUIDED_CODES, knownGuidance } from "@/lib/connect-guidance";
import { safeError } from "@/lib/connect-protocol";
import { connectionHealth, relativeTime, totalLag } from "@/lib/connect-health";

const kafka = (patch: Partial<SetupState>): SetupState => ({
  ...initialSetup("kafka"),
  name: "orders",
  credentialRef: "orders-kafka",
  bootstrapServers: "b1.example.com:9092",
  ...patch,
});
const SECRET_KEYS = [
  "password",
  "sasl_password",
  "sasl_username",
  "api_key",
  "api_secret",
  "connection_string",
  "aws_access_key_id",
  "aws_secret_access_key",
  "aws_session_token",
  "ssl_key_password",
  "schema_registry_password",
];

describe("connect setup: managed Kafka profiles", () => {
  it("covers exactly the actor's PROFILES", () => {
    expect(KAFKA_PROFILES.map((p) => p.id).sort()).toEqual([
      "aiven",
      "aws_msk_iam",
      "aws_msk_scram",
      "azure_event_hubs",
      "confluent_cloud",
      "redpanda_cloud",
    ]);
  });

  it("Confluent Cloud: only the profile is stored; key and secret are agent-file placeholders", () => {
    const s = kafka({ kafkaMode: { type: "profile", profile: "confluent_cloud" } });
    const { publicConfig, agentFile } = buildConfig(s);
    expect(publicConfig).toEqual({
      bootstrap_servers: "b1.example.com:9092",
      profile: "confluent_cloud",
    });
    expect(agentFile["api_key"]).toBe(SECRET_PLACEHOLDER);
    expect(agentFile["api_secret"]).toBe(SECRET_PLACEHOLDER);
    // A profile's fixed settings are never written: the agent refuses contradicting ones.
    expect(agentFile).not.toHaveProperty("security_protocol");
    expect(visibleFields(s)).toMatchObject({ mechanism: false, caFile: false, awsRegion: false });
  });

  it("Event Hubs requires port 9093 and asks for the connection string", () => {
    const s = kafka({ kafkaMode: { type: "profile", profile: "azure_event_hubs" } });
    expect(setupProblems(s)).toContain("Event Hubs exposes its Kafka endpoint on port 9093.");
    expect(setupProblems({ ...s, bootstrapServers: "ns.servicebus.windows.net:9093" })).toEqual([]);
    expect(secretFields(s).map((f) => f.key)).toEqual(["connection_string"]);
  });

  it("MSK IAM validates region and role ARN like the actor and stores no keys", () => {
    const s = kafka({
      kafkaMode: { type: "profile", profile: "aws_msk_iam" },
      awsRegion: "EU-west",
    });
    expect(setupProblems(s).join()).toMatch(/AWS region/);
    const role = {
      ...s,
      awsRegion: "eu-west-1",
      mskIamCredentials: "role" as const,
      awsRoleArn: "nope",
    };
    expect(setupProblems(role).join()).toMatch(/role ARN/);
    const ok = { ...role, awsRoleArn: "arn:aws:iam::123456789012:role/meridian" };
    expect(setupProblems(ok)).toEqual([]);
    expect(buildConfig(ok).publicConfig).toMatchObject({
      profile: "aws_msk_iam",
      aws_region: "eu-west-1",
      aws_role_arn: "arn:aws:iam::123456789012:role/meridian",
    });
    expect(secretFields(ok)).toEqual([]);
    const keys = { ...ok, mskIamCredentials: "keys" as const };
    expect(secretFields(keys).map((f) => f.key)).toEqual([
      "aws_access_key_id",
      "aws_secret_access_key",
      "aws_session_token",
    ]);
    expect(buildConfig(keys).publicConfig).not.toHaveProperty("aws_access_key_id");
  });

  it("Redpanda and Aiven expose only the choices the profile leaves open", () => {
    const rp = kafka({
      kafkaMode: { type: "profile", profile: "redpanda_cloud" },
      saslMechanism: "SCRAM-SHA-256",
    });
    expect(buildConfig(rp).publicConfig).toMatchObject({
      profile: "redpanda_cloud",
      sasl_mechanism: "SCRAM-SHA-256",
    });
    const aiven = kafka({ kafkaMode: { type: "profile", profile: "aiven" }, aivenAuth: "SSL" });
    expect(visibleFields(aiven)).toMatchObject({
      clientCertificate: true,
      caFile: true,
      mechanism: false,
    });
    expect(setupProblems(aiven).join()).toMatch(/client certificate and key/);
  });
});

describe("connect setup: self-managed Kafka", () => {
  it("blocks PLAIN without TLS with the actor's reason", () => {
    const s = kafka({
      kafkaMode: { type: "self", security: "sasl_plaintext" },
      saslMechanism: "PLAIN",
    });
    expect(setupProblems(s).join()).toMatch(/PLAIN without TLS sends the password in clear text/);
    expect(setupProblems({ ...s, saslMechanism: "SCRAM-SHA-512" })).toEqual([]);
    expect(setupProblems({ ...s, kafkaMode: { type: "self", security: "sasl_ssl" } })).toEqual([]);
  });

  it("mTLS stores file names only and needs certificate + key", () => {
    const s = kafka({ kafkaMode: { type: "self", security: "mtls" } });
    expect(setupProblems(s).join()).toMatch(/client certificate and key/);
    const ok = {
      ...s,
      certificateFile: "client.pem",
      keyFile: "client.key",
      caFile: "ca.pem",
      keyHasPassword: true,
    };
    expect(setupProblems(ok)).toEqual([]);
    expect(buildConfig(ok).publicConfig).toEqual({
      bootstrap_servers: "b1.example.com:9092",
      security_protocol: "SSL",
      ssl_ca_location: "ca.pem",
      ssl_certificate_location: "client.pem",
      ssl_key_location: "client.key",
    });
    expect(secretFields(ok).map((f) => f.key)).toEqual(["ssl_key_password"]);
    expect(setupProblems({ ...ok, keyFile: "../etc/key" }).join()).toMatch(/no folders/);
  });

  it("refuses registry credentials over plain HTTP and credentials in the URL", () => {
    const s = kafka({
      kafkaMode: { type: "self", security: "plaintext" },
      schemaRegistryUrl: "http://reg:8081",
      schemaRegistryAuth: true,
    });
    expect(setupProblems(s).join()).toMatch(/refused over plain HTTP/);
    expect(setupProblems({ ...s, schemaRegistryUrl: "https://u:p@reg" }).join()).toMatch(
      /no credentials/,
    );
  });

  it("never puts a secret key in the stored config, for every security choice", () => {
    const variants: SetupState[] = [
      ...KAFKA_PROFILES.map((p) =>
        kafka({
          kafkaMode: { type: "profile", profile: p.id },
          awsRegion: "us-east-1",
          mskIamCredentials: "keys",
          schemaRegistryUrl: "https://r",
          schemaRegistryAuth: true,
          keyHasPassword: true,
        }),
      ),
      ...(["sasl_ssl", "sasl_plaintext", "mtls", "plaintext"] as const).map((security) =>
        kafka({ kafkaMode: { type: "self", security }, keyHasPassword: true }),
      ),
      { ...initialSetup("postgres"), host: "db", database: "app", user: "u" },
      { ...initialSetup("mysql"), host: "db", database: "app", user: "u" },
    ];
    for (const v of variants) {
      const { publicConfig, agentFile } = buildConfig(v);
      for (const k of Object.keys(publicConfig)) expect(PUBLIC_CONFIG_KEYS.has(k)).toBe(true);
      for (const k of SECRET_KEYS) expect(publicConfig).not.toHaveProperty(k);
      for (const f of secretFields(v)) expect(agentFile[f.key]).toBe(SECRET_PLACEHOLDER);
    }
  });

  it("round-trips stored settings into the same agent file template", () => {
    const s = kafka({
      kafkaMode: { type: "profile", profile: "aiven" },
      aivenAuth: "SASL_SSL",
      saslMechanism: "SCRAM-SHA-512",
      caFile: "ca.pem",
    });
    const stored = buildConfig(s).publicConfig;
    const back = setupFromStored("kafka", s.name, s.credentialRef, stored);
    expect(buildConfig(back)).toEqual(buildConfig(s));
    const pg = {
      ...initialSetup("postgres"),
      name: "p",
      credentialRef: "pg",
      host: "db",
      database: "app",
      user: "u",
    };
    expect(
      buildConfig(setupFromStored("postgres", "p", "pg", buildConfig(pg).publicConfig)),
    ).toEqual(buildConfig(pg));
  });

  it("reads back only allowlisted settings", () => {
    expect(publicConfigEntries({ host: "db", password: "x", port: 5432, user: null })).toEqual([
      ["host", "db"],
      ["port", "5432"],
    ]);
  });
});

describe("connect setup: helpers", () => {
  it("validates database settings, references, schedule and target names", () => {
    const pg = { ...initialSetup("postgres"), name: "p", credentialRef: "bad ref" };
    expect(setupProblems(pg).length).toBe(4);
    expect(scheduleProblem(0)).toBeNull();
    expect(scheduleProblem(3)).toMatch(/every 5 minutes/);
    expect(scheduleProblem(10081)).toMatch(/weekly/);
    expect(targetTable("Orders Topic!")).toBe("connect_orders_topic");
    expect(targetTable("***")).toBe("connect_source");
  });
});

describe("connect guidance", () => {
  it("explains every run failure code with a next action", () => {
    for (const code of safeError.options.filter((c) => c !== "ok")) {
      expect(GUIDED_CODES).toContain(code);
      const g = guidance(code)!;
      expect(g.next.length).toBeGreaterThan(10);
    }
    expect(guidance("ok")).toBeNull();
    expect(guidance("auth_failed")).toMatchObject({ action: "check", terminal: true });
    expect(guidance("source_unavailable")).toMatchObject({ action: "check", terminal: false });
    expect(guidance("schema_drift")).toMatchObject({ action: "discover" });
    expect(guidance("something_new")?.title).toMatch(/something_new/);
    // Routine events are not failures.
    expect(knownGuidance("run_succeeded")).toBeNull();
    expect(knownGuidance("run_limit_reached")?.action).toBe("none");
  });
});

describe("relative time", () => {
  it("describes past and future instants", () => {
    const now = Date.parse("2026-11-15T12:00:00Z");
    expect(relativeTime(null, now)).toBe("Never");
    expect(relativeTime("2026-11-15T11:59:30Z", now)).toBe("just now");
    expect(relativeTime("2026-11-15T11:45:00Z", now)).toBe("15 min ago");
    expect(relativeTime("2026-11-15T12:15:00Z", now)).toBe("in 15 min");
    expect(relativeTime("2026-11-12T12:00:00Z", now)).toBe("3 d ago");
  });
});

describe("connect health", () => {
  const c = { id: "c1", enabled: true, health: "reachable" };
  const run = (status: string, at: string, error_code: string | null = null) => ({
    connection_id: "c1",
    operation: "sync",
    status,
    error_code,
    created_at: at,
    finished_at: at,
  });
  const streams = [
    { id: "s1", connection_id: "c1", last_success_at: "2026-11-10T00:00:00Z" },
    { id: "s2", connection_id: "c1", last_success_at: "2026-11-12T00:00:00Z" },
    { id: "x", connection_id: "other", last_success_at: "2026-11-14T00:00:00Z" },
  ];
  const lag = [
    { stream_id: "s1", partition: 0, lag: 5, observed_at: "2026-11-12T00:00:00Z" },
    { stream_id: "s1", partition: 1, lag: 7, observed_at: "2026-11-13T00:00:00Z" },
    { stream_id: "x", partition: 0, lag: 100, observed_at: "2026-11-13T00:00:00Z" },
  ];

  it("is healthy after a success and sums this connection's lag only", () => {
    const h = connectionHealth(c, [run("succeeded", "2026-11-12T00:00:00Z")], streams, lag, 0);
    expect(h).toMatchObject({
      state: "healthy",
      lastSuccessAt: "2026-11-12T00:00:00Z",
      totalLag: 12,
    });
    expect(h.lagObservedAt).toBe("2026-11-13T00:00:00Z");
  });

  it("needs attention when the latest finished run failed or dead letters are open", () => {
    const runs = [
      run("succeeded", "2026-11-11T00:00:00Z"),
      run("failed", "2026-11-12T00:00:00Z", "auth_failed"),
    ];
    expect(connectionHealth(c, runs, streams, lag, 0)).toMatchObject({
      state: "attention",
      errorCode: "auth_failed",
    });
    const recovered = [...runs, run("succeeded", "2026-11-13T00:00:00Z")];
    expect(connectionHealth(c, recovered, streams, lag, 0).state).toBe("healthy");
    expect(connectionHealth(c, recovered, streams, lag, 2).state).toBe("attention");
  });

  it("reports paused, running and untested states", () => {
    expect(connectionHealth({ ...c, enabled: false }, [], [], [], 0).state).toBe("paused");
    expect(connectionHealth(c, [run("running", "2026-11-12T00:00:00Z")], [], [], 0).state).toBe(
      "running",
    );
    expect(connectionHealth({ ...c, health: "unchecked" }, [], [], [], 0)).toMatchObject({
      state: "untested",
      totalLag: null,
      lastSuccessAt: null,
    });
    expect(totalLag([])).toEqual({ total: null, observedAt: null });
  });
});

describe("CDC setup", () => {
  const base: SetupState = {
    ...initialSetup("postgres"),
    name: "orders",
    credentialRef: "orders-db",
    host: "db.example.test",
    database: "app",
    user: "replicator",
  };
  it("records the publication for a PostgreSQL CDC connection only", () => {
    expect(buildConfig(base).publicConfig).not.toHaveProperty("publication");
    const cdc = { ...base, cdc: true, publication: "meridian_orders" };
    expect(setupProblems(cdc)).toEqual([]);
    expect(buildConfig(cdc).publicConfig).toMatchObject({ publication: "meridian_orders" });
    const stored = setupFromStored(
      "postgres",
      "orders",
      "orders-db",
      buildConfig(cdc).publicConfig,
    );
    expect(stored).toMatchObject({ cdc: true, publication: "meridian_orders" });
    expect(publicConfigEntries(buildConfig(cdc).publicConfig)).toContainEqual([
      "publication",
      "meridian_orders",
    ]);
  });
  it("refuses a publication name that isn't an identifier", () => {
    expect(setupProblems({ ...base, cdc: true, publication: "x; DROP" })).toContain(
      "Publication: the name of a PostgreSQL publication (letters, digits and '_').",
    );
  });
});
