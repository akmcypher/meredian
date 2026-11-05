// Runs the deploy/ Python test suites against the local stack only.
import { spawnSync } from "node:child_process";
import { delimiter } from "node:path";
import { loadLocalEnv, localPostgresEnv, pythonExecutable } from "./local-env.mjs";

// Refuses to continue unless .env points at the local stack.
loadLocalEnv();
const python = pythonExecutable();
// test_postgres creates and drops the connect_test schema; only ever on a local database.
const pg = localPostgresEnv();

const suites = [
  { file: "deploy/connect/test_destination.py", path: ["deploy/connect"] },
  { file: "deploy/connect/test_bronze_reader.py", path: ["deploy/connect"] },
  // Real child processes: cancellation and deadlines kill the actor's whole process group.
  { file: "deploy/connect/test_agent.py", path: ["deploy/connect"] },
  { file: "deploy/connect/test_kafka_readiness.py", path: ["deploy/connect"] },
  { file: "deploy/connect/test_postgres.py", path: ["deploy/connect"], env: pg },
  // Conformance kit certification (deploy/connect/conformance.py) against live sources.
  { file: "deploy/connect/test_certify_postgres.py", path: ["deploy/connect"], env: pg },
  // Skips unless deploy/ingest/test-sources.compose.yml is running (MySQL on 127.0.0.1:33306).
  { file: "deploy/connect/test_certify_mysql.py", path: ["deploy/connect"] },
  // Skips unless deploy/connect/certify.compose.yml is running (3 Kafka brokers + Karapace).
  { file: "deploy/connect/test_kafka.py", path: ["deploy/connect"] },
];
const only = process.argv.slice(2);
const selected = only.length ? suites.filter((s) => only.some((f) => s.file.includes(f))) : suites;

let failed = 0;
for (const suite of selected) {
  console.log(`\n=== ${suite.file}`);
  const result = spawnSync(python, [suite.file], {
    stdio: "inherit",
    env: {
      ...process.env,
      PYTHONPATH: suite.path.join(delimiter),
      PYTHONUTF8: "1",
      ...suite.env,
    },
  });
  console.log(
    JSON.stringify({
      suite: suite.file,
      exitCode: result.status,
      signal: result.signal,
      spawnError: result.error?.code ?? null,
    }),
  );
  if (result.status !== 0) failed++;
}
console.log(`\n${selected.length - failed}/${selected.length} Python suites passed`);
process.exit(failed ? 1 : 0);
