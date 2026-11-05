// Shared by the local test runners: loads .env and refuses to continue unless every
// Supabase URL points at this machine, so the service-role key can never be used against
// a hosted project.
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { connect } from "node:net";
import { join } from "node:path";

const LOCAL_HOSTS = new Set(["127.0.0.1", "localhost", "[::1]"]);
const URL_KEYS = ["SUPABASE_URL", "VITE_SUPABASE_URL"];
const REQUIRED = [...URL_KEYS, "SUPABASE_PUBLISHABLE_KEY", "SUPABASE_SERVICE_ROLE_KEY"];

function readEnvFile(path) {
  if (!existsSync(path)) throw new Error(`${path} not found; create it from .env.example`);
  const env = {};
  for (const line of readFileSync(path, "utf8").split(/\r?\n/)) {
    const match = /^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/.exec(line);
    if (match) env[match[1]] = match[2].replace(/^(["'])(.*)\1$/, "$2");
  }
  return env;
}

export function isLocalHost(host) {
  return LOCAL_HOSTS.has(host);
}

export function isLocalUrl(value) {
  try {
    return isLocalHost(new URL(value).host.replace(/:\d+$/, ""));
  } catch {
    return false;
  }
}

/** Returns the .env values, or exits the process if any are missing or not local. */
export function loadLocalEnv() {
  const env = readEnvFile(".env");
  const missing = REQUIRED.filter((key) => !env[key]);
  if (missing.length) {
    console.error(`Missing in .env: ${missing.join(", ")}`);
    process.exit(1);
  }
  const remote = URL_KEYS.filter((key) => !isLocalUrl(env[key]));
  if (remote.length) {
    console.error(`Refusing to run: ${remote.join(", ")} must point at the local stack.`);
    process.exit(1);
  }
  return env;
}

/**
 * Connection settings for the Postgres fixtures (deploy/connect/test_postgres.py and the
 * connect-live source). Defaults are the local Supabase CLI database (config.toml [db] port);
 * exits if CONNECT_TEST_HOST is not this machine.
 */
export function localPostgresEnv() {
  const pg = {
    CONNECT_TEST_HOST: process.env["CONNECT_TEST_HOST"] ?? "127.0.0.1",
    CONNECT_TEST_PORT: process.env["CONNECT_TEST_PORT"] ?? "54322",
    CONNECT_TEST_DB: process.env["CONNECT_TEST_DB"] ?? "connect_source",
    PGPASSWORD: process.env["PGPASSWORD"] ?? "postgres",
  };
  if (!isLocalHost(pg.CONNECT_TEST_HOST)) {
    console.error("Refusing to run: CONNECT_TEST_HOST must be this machine.");
    process.exit(1);
  }
  return pg;
}

function portOpen(port) {
  return new Promise((resolve) => {
    const socket = connect({ host: "127.0.0.1", port });
    socket.setTimeout(1000);
    socket.once("connect", () => (socket.destroy(), resolve(true)));
    socket.once("timeout", () => (socket.destroy(), resolve(false)));
    socket.once("error", () => resolve(false));
  });
}

/**
 * MySQL / Kafka settings for the ingest worker suite, set only for services that answer
 * (deploy/ingest/test-sources.compose.yml), so the suite skips instead of failing without them.
 */
export async function localIngestSourcesEnv() {
  const env = {};
  if (process.env["MYSQL_TEST_URL"]) env.MYSQL_TEST_URL = process.env["MYSQL_TEST_URL"];
  else if (await portOpen(33306)) env.MYSQL_TEST_URL = "mysql://root:meridian-test@127.0.0.1:33306";
  if (process.env["KAFKA_TEST_BROKERS"]) env.KAFKA_TEST_BROKERS = process.env["KAFKA_TEST_BROKERS"];
  else if (await portOpen(39092)) env.KAFKA_TEST_BROKERS = "127.0.0.1:39092";
  // The local Supabase storage S3 endpoint's keys (partitioned-worker S3 suite); local values only.
  if (!process.env["S3_PROTOCOL_ACCESS_KEY_ID"]) {
    const status = spawnSync("supabase", ["status", "-o", "env"], {
      encoding: "utf8",
      shell: process.platform === "win32",
    });
    for (const line of (status.stdout ?? "").split("\n")) {
      const m = /^(S3_PROTOCOL_[A-Z_]+)="?([^"\r]*)"?/.exec(line.trim());
      if (m) env[m[1]] = m[2];
    }
  }
  // 3-broker Kafka + Karapace from deploy/connect/certify.compose.yml (connect-kafka-live suite).
  if (process.env["KAFKA_CERTIFY_BROKERS"])
    env.KAFKA_CERTIFY_BROKERS = process.env["KAFKA_CERTIFY_BROKERS"];
  else if ((await portOpen(39191)) && (await portOpen(39192)) && (await portOpen(39193)))
    env.KAFKA_CERTIFY_BROKERS = "127.0.0.1:39191,127.0.0.1:39192,127.0.0.1:39193";
  if (process.env["KAFKA_CERTIFY_REGISTRY"])
    env.KAFKA_CERTIFY_REGISTRY = process.env["KAFKA_CERTIFY_REGISTRY"];
  else if (await portOpen(38081)) env.KAFKA_CERTIFY_REGISTRY = "http://127.0.0.1:38081";
  // Mailpit from the Supabase CLI stack (config.toml [inbucket]) for the email delivery suite.
  if ((await portOpen(54324)) && (await portOpen(54325))) {
    env.MAILPIT_URL = "http://127.0.0.1:54324";
    env.MAILPIT_SMTP_PORT = "54325";
  }
  for (const [key, value] of Object.entries(env)) {
    if (key.startsWith("MAILPIT") || key.startsWith("S3_PROTOCOL_")) continue;
    const hosts =
      key === "MYSQL_TEST_URL" || key === "KAFKA_CERTIFY_REGISTRY"
        ? [new URL(value).hostname]
        : value.split(",").map((b) => b.split(":")[0]);
    if (!hosts.every(isLocalHost)) {
      console.error(`Refusing to run: ${key} must point at this machine.`);
      process.exit(1);
    }
  }
  return env;
}

/** The interpreter for deploy/ workers: $PYTHON, then the project .venv, then python3. */
export function pythonExecutable() {
  if (process.env["PYTHON"]) return process.env["PYTHON"];
  const venv =
    process.platform === "win32"
      ? join(".venv", "Scripts", "python.exe")
      : join(".venv", "bin", "python");
  return existsSync(venv) ? venv : "python3";
}
