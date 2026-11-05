// Single-machine preview installer. State and credentials remain inside .meridian/.
import { execFileSync, spawnSync, spawn } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  cpSync,
  openSync,
  closeSync,
} from "node:fs";
import { generateKeyPairSync, randomBytes, createHash } from "node:crypto";
import { createConnection } from "node:net";
import { resolve, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const state = join(root, ".meridian");
const manifest = join(state, "installation.json");
const command = process.argv[2];
const port = Number(process.env.MERIDIAN_PORT ?? 8080);
const apiPort = Number(process.env.MERIDIAN_BACKEND_PORT ?? 58321);
function run(executable, args, env = process.env) {
  try {
    return execFileSync(executable, args, {
      cwd: root,
      env,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      maxBuffer: 16 * 1024 * 1024,
      timeout: executable === "docker" && args[0] === "network" ? 30000 : 600000,
    }).trim();
  } catch {
    // CLI error objects may include environment credentials in stdout/stderr.
    throw new Error(
      `${executable} ${args[0]} failed; check Docker availability and required versions.`,
    );
  }
}
const backend = (...args) => run("supabase", [...args, "--workdir", state]);
function startBackend() {
  const network = `${installation().id}_local`;
  const networks = run("docker", ["network", "ls", "--format", "{{.Name}}"]);
  if (!networks.split(/\r?\n/).includes(network))
    run("docker", [
      "network",
      "create",
      "-o",
      "com.docker.network.bridge.host_binding_ipv4=127.0.0.1",
      network,
    ]);
  backend(
    "start",
    "--network-id",
    network,
    "--exclude",
    "studio,imgproxy,edge-runtime,logflare,vector,supavisor",
  );
  const names = run("docker", [
    "ps",
    "--filter",
    `label=com.supabase.cli.project=${installation().id}`,
    "--format",
    "{{.Names}}",
  ]);
  for (const name of names.split(/\r?\n/).filter(Boolean)) {
    const ports = JSON.parse(
      run("docker", ["inspect", "--format", "{{json .NetworkSettings.Ports}}", name]),
    );
    if (
      Object.values(ports ?? {})
        .flat()
        .some((binding) => binding && binding.HostIp !== "127.0.0.1")
    )
      throw new Error(
        "Backend ports are not loopback-only; stop this installation before using accounts.",
      );
  }
}
function installation() {
  if (!existsSync(manifest)) throw new Error("Run install first.");
  return JSON.parse(readFileSync(manifest, "utf8"));
}
function environment() {
  return Object.fromEntries(
    readFileSync(join(state, "runtime.env"), "utf8")
      .trim()
      .split(/\r?\n/)
      .map((line) => {
        const split = line.indexOf("=");
        return [line.slice(0, split), line.slice(split + 1)];
      }),
  );
}
async function requireStoppedApp() {
  const installed = installation();
  await new Promise((resolve, reject) => {
    const socket = createConnection({ host: "127.0.0.1", port: installed.port });
    socket.once("connect", () => {
      socket.destroy();
      reject(new Error("Stop the web app and connector agents before backup, restore or upgrade."));
    });
    socket.once("error", (error) =>
      error.code === "ECONNREFUSED"
        ? resolve()
        : reject(new Error("Could not verify the application is stopped")),
    );
    socket.setTimeout(5000, () => {
      socket.destroy();
      reject(new Error("App stop check timed out"));
    });
  });
}
function transfer(file, args, direction) {
  const fd = openSync(file, direction === "out" ? "wx" : "r", 0o600);
  try {
    const r = spawnSync("docker", args, {
      stdio: direction === "out" ? ["ignore", fd, "pipe"] : [fd, "ignore", "pipe"],
      timeout: 600000,
    });
    if (r.status !== 0) {
      // Never expose raw stderr: database errors can include account/source data.
      const stderr = r.stderr?.toString() ?? "";
      const reason = /permission denied/i.test(stderr)
        ? "permission denied"
        : /must be owner/i.test(stderr)
          ? "ownership required"
          : /not found|no such file/i.test(stderr)
            ? "missing executable or path"
            : /authentication failed/i.test(stderr)
              ? "authentication failed"
              : /already exists/i.test(stderr)
                ? "object already exists"
                : "unclassified error";
      throw new Error(
        `${args.includes("pg_dump") ? "Database backup" : args.includes("pg_restore") ? "Database restore" : "Storage transfer"} failed (${reason}, exit ${r.status}); incomplete files must not be used.`,
      );
    }
  } finally {
    closeSync(fd);
  }
}
const digest = (file) => createHash("sha256").update(readFileSync(file)).digest("hex");
function migrationVersions() {
  return run("docker", [
    "exec",
    `supabase_db_${installation().id}`,
    "psql",
    "-U",
    "postgres",
    "-Atc",
    "select coalesce(string_agg(version,',' order by version),'') from supabase_migrations.schema_migrations",
  ]);
}
try {
  if (command === "install") {
    const previous = existsSync(manifest) ? installation() : null;
    if (previous && (previous.port !== port || previous.apiPort !== apiPort))
      throw new Error(
        "Resume with the original MERIDIAN_PORT and MERIDIAN_BACKEND_PORT; state preserved.",
      );
    for (const p of [port, apiPort])
      if (!Number.isInteger(p) || p < 1024 || p > 65529)
        throw new Error("Ports must be integers between1024 and65529.");
    const version = run("supabase", ["--version"]);
    if (version !== "2.122.0") throw new Error("This installer requires Supabase CLI2.122.0.");
    run("docker", ["version", "--format", "{{.Server.Version}}"]);
    run("bun", ["--version"]);
    const id = previous?.id ?? `meridian_${randomBytes(6).toString("hex")}`;
    mkdirSync(join(state, "supabase"), { recursive: true, mode: 0o700 });
    cpSync(join(root, "supabase", "migrations"), join(state, "supabase", "migrations"), {
      recursive: true,
    });
    let config = readFileSync(join(root, "supabase", "config.toml"), "utf8")
      .replace('project_id = "meridian"', `project_id = "${id}"`)
      .replaceAll("http://localhost:8080", `http://localhost:${port}`);
    config = config.replace(/\b5432[1-5]\b/g, (original) =>
      String(apiPort + Number(original) - 54321),
    );
    writeFileSync(join(state, "supabase", "config.toml"), config, { mode: 0o600 });
    // Persist identity before startup so partial failures can be inspected/recovered safely.
    writeFileSync(manifest, JSON.stringify({ id, port, apiPort, cliVersion: version }, null, 2), {
      mode: 0o600,
    });
    console.log("Starting isolated backend; initial image downloads may take several minutes.");
    startBackend();
    const values = JSON.parse(backend("status", "--output", "json"));
    const { privateKey } = generateKeyPairSync("ed25519");
    const env = {
      VITE_SUPABASE_URL: values.API_URL,
      VITE_SUPABASE_PUBLISHABLE_KEY: values.ANON_KEY,
      SUPABASE_URL: values.API_URL,
      SUPABASE_PUBLISHABLE_KEY: values.ANON_KEY,
      SUPABASE_SERVICE_ROLE_KEY: values.SERVICE_ROLE_KEY,
      ENGINE_TOKEN_SIGNING_KEY: existsSync(join(state, "runtime.env"))
        ? environment().ENGINE_TOKEN_SIGNING_KEY
        : privateKey.export({ type: "pkcs8", format: "der" }).toString("base64"),
      VITE_AUTH_GOOGLE_ENABLED: "false",
      HOST: "127.0.0.1",
      PORT: String(port),
    };
    if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_ROLE_KEY || !env.SUPABASE_PUBLISHABLE_KEY)
      throw new Error("Backend did not return required local configuration.");
    writeFileSync(
      join(state, "runtime.env"),
      Object.entries(env)
        .map(([k, v]) => `${k}=${v}`)
        .join("\n") + "\n",
      { mode: 0o600 },
    );
    run("bun", ["install", "--frozen-lockfile"]);
    run("bun", ["run", "build"], { ...process.env, ...env });
    console.log(`Installed. Run node scripts/self-host.mjs start; open http://localhost:${port}.`);
    console.log(
      `Local verification mail: http://localhost:${apiPort + 3}. Connector agent setup: deploy/connect/README.md.`,
    );
  } else if (command === "backup") {
    await requireStoppedApp();
    const installed = installation();
    const directory = join(state, "backups", new Date().toISOString().replaceAll(":", "-"));
    mkdirSync(directory, { recursive: true, mode: 0o700 });
    const database = join(directory, "database.dump"),
      storage = join(directory, "storage.tar");
    transfer(
      database,
      // Realtime owns rotating partitions whose inherited constraints cannot be
      // cleaned by pg_restore. Keep the fresh backend's tenant configuration too.
      [
        "exec",
        `supabase_db_${installed.id}`,
        "pg_dump",
        "-U",
        "postgres",
        "-Fc",
        "--exclude-schema=realtime",
        "--exclude-schema=_realtime",
        "postgres",
      ],
      "out",
    );
    transfer(storage, ["cp", `supabase_storage_${installed.id}:/mnt/.`, "-"], "out");
    writeFileSync(join(directory, "engine.key"), environment().ENGINE_TOKEN_SIGNING_KEY, {
      mode: 0o600,
    });
    writeFileSync(
      join(directory, "backup.json"),
      JSON.stringify(
        {
          format: 2,
          migrations: migrationVersions(),
          databaseSha256: digest(database),
          storageSha256: digest(storage),
          engineSha256: digest(join(directory, "engine.key")),
        },
        null,
        2,
      ),
      { mode: 0o600 },
    );
    console.log(
      `Backup complete: ${directory}. Keep it private; it contains account and source data.`,
    );
  } else if (command === "restore") {
    await requireStoppedApp();
    if (!process.argv[3]) throw new Error("Supply the private backup directory.");
    const directory = resolve(process.argv[3]);
    const metadata = JSON.parse(readFileSync(join(directory, "backup.json"), "utf8"));
    if (metadata.format !== 2 || metadata.migrations !== migrationVersions())
      throw new Error(
        "Restore requires a matching-release fresh installation; upgrade only after recovery.",
      );
    for (const [file, expected] of [
      ["database.dump", metadata.databaseSha256],
      ["storage.tar", metadata.storageSha256],
      ["engine.key", metadata.engineSha256],
    ])
      if (digest(join(directory, file)) !== expected)
        throw new Error("Backup integrity verification failed");
    const id = installation().id;
    const empty = run("docker", [
      "exec",
      `supabase_db_${id}`,
      "psql",
      "-U",
      "postgres",
      "-Atc",
      "select (select count(*) from auth.users)+(select count(*) from storage.objects)+(select count(*) from public.organizations)+(select count(*) from public.user_tables)+(select count(*) from public.ingest_sources)+(select count(*) from public.connect_commits)",
    ]);
    if (empty !== "0")
      throw new Error(
        "Restore refuses existing accounts or application data; use a fresh installation.",
      );
    run("docker", [
      "exec",
      `supabase_db_${id}`,
      "psql",
      "-U",
      "postgres",
      "-v",
      "ON_ERROR_STOP=1",
      "-c",
      `DO $$ DECLARE t record; n bigint; BEGIN FOR t IN SELECT schemaname,tablename FROM pg_tables WHERE schemaname IN ('public','core') LOOP EXECUTE format('select count(*) from %I.%I',t.schemaname,t.tablename) INTO n; IF n<>0 THEN RAISE EXCEPTION 'Restore requires empty application tables'; END IF; END LOOP; END $$;`,
    ]);
    transfer(
      join(directory, "database.dump"),
      [
        "exec",
        "-i",
        `supabase_db_${id}`,
        "pg_restore",
        "-U",
        // A full local backup includes provider-owned objects. The local
        // postgres role cannot replace these; preserve their original owners.
        "supabase_admin",
        "--clean",
        "--if-exists",
        "--single-transaction",
        "--exit-on-error",
        "-d",
        "postgres",
      ],
      "in",
    );
    transfer(join(directory, "storage.tar"), ["cp", "-", `supabase_storage_${id}:/mnt`], "in");
    const env = {
      ...environment(),
      ENGINE_TOKEN_SIGNING_KEY: readFileSync(join(directory, "engine.key"), "utf8"),
    };
    writeFileSync(
      join(state, "runtime.env"),
      Object.entries(env)
        .map(([k, v]) => `${k}=${v}`)
        .join("\n") + "\n",
      { mode: 0o600 },
    );
    run("docker", [
      "exec",
      `supabase_db_${id}`,
      "psql",
      "-U",
      "postgres",
      "-c",
      "NOTIFY pgrst, 'reload schema'",
    ]);
    console.log(
      "Restore completed. Start the app and verify sign-in, records and stored objects before accepting recovery.",
    );
  } else if (command === "upgrade") {
    await requireStoppedApp();
    cpSync(join(root, "supabase", "migrations"), join(state, "supabase", "migrations"), {
      recursive: true,
    });
    backend("migration", "up", "--local");
    run("bun", ["install", "--frozen-lockfile"]);
    run("bun", ["run", "build"], { ...process.env, ...environment() });
    console.log("Forward upgrade complete. Verify application health before resuming agents.");
  } else if (command === "bootstrap-admin") {
    const email = process.argv[3];
    if (!email || !email.includes("@"))
      throw new Error("Supply the verified administrator email address.");
    const env = environment();
    const result = await fetch(`${env.SUPABASE_URL}/rest/v1/rpc/bootstrap_platform_admin`, {
      method: "POST",
      headers: {
        apikey: env.SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ _email: email }),
      signal: AbortSignal.timeout(10000),
    });
    if (!result.ok)
      throw new Error("Administrator bootstrap rejected; register and verify this account first.");
    console.log("Platform administrator bootstrap complete.");
  } else if (command === "start") {
    installation();
    startBackend();
    const child = spawn(process.execPath, [resolve(root, ".output/server/index.mjs")], {
      cwd: root,
      env: { ...process.env, ...environment() },
      stdio: "inherit",
    });
    const stop = () => child.kill("SIGTERM");
    process.once("SIGINT", stop);
    process.once("SIGTERM", stop);
    process.exitCode = await new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code) => resolve(code ?? 1));
    });
    process.removeListener("SIGINT", stop);
    process.removeListener("SIGTERM", stop);
  } else if (command === "stop") {
    installation();
    // Supabase stop retains the installation's Docker data volumes by default.
    backend("stop");
    console.log("Backend stopped; data retained. Stop the web process with Ctrl+C.");
  } else if (command === "check") {
    const installed = installation();
    const env = environment();
    const response = await fetch(`${env.SUPABASE_URL}/auth/v1/health`, {
      headers: { apikey: env.SUPABASE_PUBLISHABLE_KEY },
      signal: AbortSignal.timeout(10000),
    });
    if (!response.ok) throw new Error(`Auth health failed (${response.status}).`);
    const app = await fetch(`http://localhost:${installed.port}/auth`, {
      signal: AbortSignal.timeout(10000),
    });
    if (!app.ok || !(app.headers.get("content-type") ?? "").includes("text/html"))
      throw new Error(`App health failed (${app.status}).`);
    console.log(
      JSON.stringify({
        installation: installed.id,
        auth: "healthy",
        app: `http://localhost:${installed.port}`,
      }),
    );
  } else {
    console.log(
      "Usage: node scripts/self-host.mjs install|start|check|stop|bootstrap-admin <verified-email>|backup|restore <directory>|upgrade",
    );
    process.exitCode = 1;
  }
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
