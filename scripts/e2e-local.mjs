// Runs the live e2e suites against the local Supabase stack only.
import { spawnSync } from "node:child_process";
import { resolve } from "node:path";
import {
  loadLocalEnv,
  localIngestSourcesEnv,
  localPostgresEnv,
  pythonExecutable,
} from "./local-env.mjs";

const env = loadLocalEnv();
const pg = localPostgresEnv();
const sources = await localIngestSourcesEnv();

// Optional args narrow the run to matching file names, e.g. `tenant-isolation`.
const filters = process.argv.length > 2 ? process.argv.slice(2) : [".e2e.test."];
const python = pythonExecutable();
const pythonEnv = { ...process.env, ...pg, PYTHONPATH: "deploy/connect" };

// connect-live and pipeline-chain read source rows from an isolated fixture database; reset it.
const seeded = ["connect-live.e2e.test.ts", "pipeline-chain.e2e.test.ts"];
if (
  filters.some((f) => seeded.some((file) => file.includes(f) || f.includes(file.split(".")[0])))
) {
  const seed = spawnSync(python, ["deploy/connect/seed_acceptance.py"], {
    stdio: "inherit",
    env: pythonEnv,
  });
  if (seed.status !== 0) {
    console.error("Could not seed the connect-live source fixture.");
    process.exit(1);
  }
}

if (!sources.MYSQL_TEST_URL || !sources.KAFKA_TEST_BROKERS)
  console.log(
    "MySQL/Kafka ingest worker tests skip without deploy/ingest/test-sources.compose.yml running.",
  );

// Live suites share one dev server, database and worker pool: run files one at a time.
const result = spawnSync("bun", ["x", "vitest", "run", "--no-file-parallelism", ...filters], {
  stdio: "inherit",
  shell: process.platform === "win32",
  env: {
    ...process.env,
    ...env,
    ...pg,
    ...sources,
    RUN_TENANT_E2E: "1",
    // connect-live / transform-live spawn deploy/ workers with this interpreter.
    PYTHON: python === "python3" ? python : resolve(python),
  },
});
process.exit(result.status ?? 1);
