// Local demo: a sample PostgreSQL shop database and a Connect agent for a running installation.
// It never touches the installation's own database. Demo state stays in .meridian/demo/.
//
//   node scripts/demo.mjs source   start the sample database and write the agent's credential file
//   node scripts/demo.mjs agent    run a Connect agent (asks once for the token the wizard shows)
//   node scripts/demo.mjs more     add 25 orders and update 10, to show an incremental sync
//   node scripts/demo.mjs down     remove the sample database and the demo state
//
// Needs Docker and Python 3.10 or later. See QUICKSTART.md.
import { execFileSync, spawn, spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createConnection } from "node:net";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const manifestFile = join(root, ".meridian", "installation.json");
const demo = join(root, ".meridian", "demo");
const credentialRef = "shop";
const credentialFile = join(demo, `${credentialRef}.json`);
const tokenFile = join(demo, "agent-token");
const stateFile = join(demo, "demo.json");
// Same pinned image as deploy/connect/compose.yml.
const image = "postgres:17@sha256:3b0cda93be7c8dba2ed484d19393d93845dc3dafb88973e248ba8e6d99d784e1";

const SEED = `
create schema shop;
create table shop.customers (
  id integer primary key,
  name text not null,
  email text not null,
  city text not null,
  updated_at timestamptz not null
);
create table shop.orders (
  id integer primary key,
  customer_id integer not null references shop.customers (id),
  amount numeric(10, 2) not null,
  status text not null,
  updated_at timestamptz not null
);
insert into shop.customers
select g, 'Customer ' || g, 'customer' || g || '@example.test',
       (array['Lisbon', 'Pune', 'Austin', 'Leeds', 'Osaka', 'Lagos'])[1 + g % 6],
       timestamptz '2026-01-01 00:00:00+00' + g * interval '1 hour'
from generate_series(1, 200) g;
insert into shop.orders
select g, 1 + (g * 7) % 200, round((5 + (g * 37) % 500 + (g % 100) / 100.0)::numeric, 2),
       (array['placed', 'paid', 'shipped', 'refunded'])[1 + g % 4],
       timestamptz '2026-02-01 00:00:00+00' + g * interval '10 minutes'
from generate_series(1, 1000) g;
`;

const MORE = `
insert into shop.orders
select g, 1 + (g * 7) % 200, round((5 + (g * 37) % 500)::numeric, 2), 'placed', now()
from generate_series((select max(id) + 1 from shop.orders), (select max(id) + 25 from shop.orders)) g;
update shop.orders set status = 'shipped', updated_at = now()
where id in (select id from shop.orders where status = 'paid' order by id limit 10);
`;

const fail = (message) => {
  console.error(`error: ${message}`);
  process.exit(1);
};
function docker(args, input) {
  const r = spawnSync("docker", args, { input, encoding: "utf8", timeout: 600000 });
  if (r.status !== 0) {
    // Docker errors can echo environment values; report the command only.
    fail(`docker ${args[0]} failed (exit ${r.status}). Is Docker running?`);
  }
  return r.stdout.trim();
}
function installation() {
  if (!existsSync(manifestFile))
    fail("no installation here. Install first (QUICKSTART.md), then run the demo from its folder.");
  return JSON.parse(readFileSync(manifestFile, "utf8"));
}
const readState = () =>
  existsSync(stateFile) ? JSON.parse(readFileSync(stateFile, "utf8")) : null;
const container = () => `${installation().id}_demo_shop`;
const portFree = (port) =>
  new Promise((resolve) => {
    const socket = createConnection({ host: "127.0.0.1", port });
    socket.once("connect", () => {
      socket.destroy();
      resolve(false);
    });
    socket.once("error", () => resolve(true));
  });
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const psql = (sql, user = "postgres") =>
  docker(
    [
      "exec",
      "-i",
      container(),
      "psql",
      "-v",
      "ON_ERROR_STOP=1",
      "-q",
      "-U",
      user,
      "-d",
      "shop",
      "-Atf",
      "-",
    ],
    sql,
  );

async function source() {
  installation();
  mkdirSync(demo, { recursive: true, mode: 0o700 });
  let state = readState();
  if (state && docker(["ps", "-aq", "--filter", `name=^${container()}$`])) {
    docker(["start", container()]);
  } else {
    const port = Number(process.env.MERIDIAN_DEMO_DB_PORT ?? 55440);
    if (!(await portFree(port)))
      fail(`port ${port} is in use; set MERIDIAN_DEMO_DB_PORT to a free port.`);
    state = { port, password: randomBytes(18).toString("base64url") };
    console.log("Starting the sample PostgreSQL database (first run downloads the image)...");
    docker([
      "run",
      "-d",
      "--name",
      container(),
      "--label",
      "meridian.demo=shop",
      "-e",
      `POSTGRES_PASSWORD=${randomBytes(18).toString("base64url")}`,
      "-e",
      "POSTGRES_DB=shop",
      // Loopback only: the demo database is reachable from this computer and nowhere else.
      "-p",
      `127.0.0.1:${port}:5432`,
      image,
    ]);
    for (let i = 0; ; i++) {
      const ready = spawnSync("docker", [
        "exec",
        container(),
        "pg_isready",
        "-U",
        "postgres",
        "-d",
        "shop",
        "-h",
        "127.0.0.1",
      ]);
      if (ready.status === 0) break;
      if (i > 60) fail("the sample database did not become ready.");
      await sleep(1000);
    }
    psql(SEED);
    // A login that cannot change data, as a real source would have.
    psql(
      `create role meridian_reader login password '${state.password}';
       grant usage on schema shop to meridian_reader;
       grant select on all tables in schema shop to meridian_reader;`,
    );
    writeFileSync(stateFile, JSON.stringify(state, null, 2), { mode: 0o600 });
  }
  writeFileSync(
    credentialFile,
    JSON.stringify(
      {
        host: "127.0.0.1",
        port: state.port,
        dbname: "shop",
        user: "meridian_reader",
        password: state.password,
        // Local loopback demo only. Use TLS (sslmode require or verify-full) for real databases.
        sslmode: "disable",
      },
      null,
      2,
    ) + "\n",
    { mode: 0o600 },
  );
  const counts = psql(
    "select (select count(*) from shop.customers) || ' customers, ' || (select count(*) from shop.orders) || ' orders'",
  );
  console.log(`Sample database ready: ${counts}.`);
  console.log(`The agent's credential file is ${credentialFile}.`);
  console.log(`
In Meridian, open Connectors > Meridian Connect > New connection, choose PostgreSQL and enter:
  Connection name       shop
  Credential reference  ${credentialRef}
  Host                  127.0.0.1
  Port                  ${state.port}
  Database              shop
  User                  meridian_reader
  TLS (sslmode)         disable   (this loopback demo only)
Choose Register connection, copy the agent token it shows, then run:
  node scripts/demo.mjs agent`);
}

async function readToken() {
  if (!process.stdin.isTTY) {
    let text = "";
    for await (const chunk of process.stdin) text += chunk;
    return text.trim();
  }
  process.stdout.write("Paste the agent token (input is hidden), then press Enter: ");
  process.stdin.setRawMode(true);
  process.stdin.resume();
  process.stdin.setEncoding("utf8");
  return new Promise((resolve) => {
    let value = "";
    const onData = (chunk) => {
      for (const ch of chunk) {
        if (ch === "\r" || ch === "\n") {
          process.stdin.setRawMode(false);
          process.stdin.pause();
          process.stdin.off("data", onData);
          process.stdout.write("\n");
          resolve(value.trim());
          return;
        }
        if (ch === "\u0003") process.exit(130);
        if (ch === "\u007f" || ch === "\b") value = value.slice(0, -1);
        else value += ch;
      }
    };
    process.stdin.on("data", onData);
  });
}

function findPython() {
  for (const [cmd, ...pre] of [["python3"], ["python"], ["py", "-3"]]) {
    const r = spawnSync(cmd, [...pre, "-c", "import sys; print(sys.version_info >= (3, 10))"], {
      encoding: "utf8",
    });
    if (r.status === 0 && r.stdout.trim() === "True") return [cmd, pre];
  }
  fail("Python 3.10 or later is required for the Connect agent.");
}

async function agent() {
  const { port } = installation();
  if (!existsSync(credentialFile)) fail("run `node scripts/demo.mjs source` first.");
  if (process.argv.includes("--new-token") || !existsSync(tokenFile)) {
    const token = await readToken();
    // Source tokens start with mi_, worker pool tokens with mp_.
    if (!/^m[ip]_[A-Za-z0-9_-]{16,}$/.test(token))
      fail("that does not look like an agent token (mi_... or mp_...).");
    writeFileSync(tokenFile, token, { mode: 0o600 });
  }
  const venv = join(demo, "venv");
  const python = join(venv, process.platform === "win32" ? "Scripts/python.exe" : "bin/python");
  if (!existsSync(python)) {
    const [cmd, pre] = findPython();
    console.log("Creating the agent's Python environment (first run only)...");
    execFileSync(cmd, [...pre, "-m", "venv", venv], { stdio: "inherit" });
    // The PostgreSQL actor needs only psycopg; pinned as in deploy/connect/requirements.txt.
    execFileSync(
      python,
      ["-m", "pip", "install", "--disable-pip-version-check", "-q", "psycopg[binary]==3.2.10"],
      {
        stdio: "inherit",
      },
    );
  }
  console.log(`Agent polling http://localhost:${port} every 10 seconds. Ctrl+C stops it.`);
  const child = spawn(python, ["agent.py"], {
    cwd: join(root, "deploy", "connect"),
    stdio: "inherit",
    env: {
      ...process.env,
      MERIDIAN_URL: `http://localhost:${port}`,
      INGEST_TOKEN_FILE: tokenFile,
      CONNECT_SECRET_DIR: demo,
      CONNECT_WORKER_ID: "demo-agent",
    },
  });
  const stop = () => child.kill("SIGTERM");
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  child.once("exit", (code) => process.exit(code ?? 0));
}

function more() {
  if (!readState()) fail("run `node scripts/demo.mjs source` first.");
  psql(MORE);
  const counts = psql("select count(*) || ' orders' from shop.orders");
  console.log(
    `Added 25 orders and shipped 10 paid ones (${counts}). Choose Sync now to land the changes.`,
  );
}

function down() {
  installation();
  if (docker(["ps", "-aq", "--filter", `name=^${container()}$`]))
    docker(["rm", "-f", "-v", container()]);
  rmSync(demo, { recursive: true, force: true });
  console.log("Sample database and demo state removed. Your installation is unchanged.");
}

const commands = { source, agent, more, down };
const command = commands[process.argv[2]];
if (!command) {
  console.log("Usage: node scripts/demo.mjs source|agent [--new-token]|more|down");
  process.exit(1);
}
await command();
