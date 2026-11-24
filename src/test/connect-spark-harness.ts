// Shared live-stack harness for the Spark ingestion suites (deploy/connect/README.md, "Spark ingestion").
// A real Spark standalone cluster (deploy/connect/spark/compose.yml), or Spark on Kubernetes when
// CONNECT_SPARK_K8S names a kubectl context (deploy/connect/spark/k8s), runs the agent's Spark
// applications; connections, streams and runs are created through the same tables and RPCs the
// app uses, and landed Bronze is read back from storage.
import { createClient } from "@supabase/supabase-js";
import { execFileSync, spawn } from "node:child_process";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { sha256 } from "@/lib/ingest";
import { bronzeParquetEnvelopes } from "@/lib/bronze-parquet";

export const url = process.env["SUPABASE_URL"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"],
  key = process.env["SUPABASE_PUBLISHABLE_KEY"] ?? process.env["VITE_SUPABASE_PUBLISHABLE_KEY"];
export const APP = process.env["APP_URL"] ?? "http://localhost:8080";
export const MASTER = process.env["SPARK_TEST_CONTAINER"] ?? "meridian-spark-spark-master-1";
// Set to a kubectl context (e.g. kind-meridian-spark) to run the suites on Spark on Kubernetes
// (deploy/connect/spark/k8s) instead of the standalone cluster.
export const K8S = process.env["CONNECT_SPARK_K8S"] ?? "";
export const NAMESPACE = "meridian-spark";
export function kube(args: string[], timeout = 60000) {
  return execFileSync("kubectl", ["--context", K8S, "-n", NAMESPACE, ...args], {
    encoding: "utf8",
    timeout,
  });
}

export function sparkUp() {
  if (K8S) {
    try {
      return kube(["get", "deploy/spark-driver", "-o", "jsonpath={.status.readyReplicas}"]) === "1";
    } catch {
      return false;
    }
  }
  try {
    const out = execFileSync(
      "docker",
      [
        "exec",
        MASTER,
        "python3",
        "-c",
        "import urllib.request,json;print(len([w for w in json.load(urllib.request.urlopen('http://127.0.0.1:8080/json/'))['workers'] if w['state']=='ALIVE']))",
      ],
      { encoding: "utf8", timeout: 20000 },
    );
    return Number(out.trim()) >= 2;
  } catch {
    return false;
  }
}
export const live = !!url && !!service && !!key && sparkUp();

export type Conn = { id: string; stream: string; token: string; credential: string };
export type Line = Record<string, unknown> & { data: Record<string, unknown> };

export class SparkHarness {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  db: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  owner: any;
  uid = "";
  folder = "";
  // How Spark containers reach this machine (Docker Desktop's host gateway, IPv4).
  gateway = "";
  // The source config as Spark containers see it (defaults to the database host on the gateway).
  overrides: ((gateway: string) => Record<string, unknown>) | null = null;
  constructor(readonly spark = true) {}
  // Extra spark-submit arguments for the next agent run (S5a turns a security setting off).
  submitConf: string[] = [];

  async setup() {
    this.db = createClient(url!, service!, { auth: { persistSession: false } });
    const email = `spark-${crypto.randomUUID()}@example.test`,
      password = `P-${crypto.randomUUID()}`;
    const u = await this.db.auth.admin.createUser({ email, password, email_confirm: true });
    if (u.error) throw u.error;
    this.uid = u.data.user.id;
    this.owner = createClient(url!, key!, { auth: { persistSession: false } });
    await this.owner.auth.signInWithPassword({ email, password });
    // Only Spark runs need the gateway; suites without a Spark cluster (CDC) skip the lookup.
    if (this.spark)
      this.gateway = (
        K8S
          ? kube([
              "exec",
              "deploy/spark-driver",
              "--",
              "getent",
              "ahostsv4",
              "host.docker.internal",
            ])
          : execFileSync("docker", ["exec", MASTER, "getent", "ahostsv4", "host.docker.internal"], {
              encoding: "utf8",
            })
      )
        .trim()
        .split(/\s+/)[0]!;
    this.folder = await mkdtemp(join(tmpdir(), "connect-spark-"));
  }
  async teardown() {
    if (this.db && this.uid) await this.db.auth.admin.deleteUser(this.uid);
  }
  // The agent reads source credentials from CONNECT_SECRET_DIR/<credential_ref>.json.
  async secret(ref: string, config: Record<string, unknown>) {
    await writeFile(join(this.folder, `${ref}.json`), JSON.stringify(config), { mode: 0o600 });
  }
  async connection(name: string, version: string, credential: string): Promise<Conn> {
    const token = `mi_${crypto.randomUUID().replaceAll("-", "")}${crypto.randomUUID().replaceAll("-", "")}`;
    const s = await this.db
      .from("ingest_sources")
      .insert({
        name,
        kind: version.includes("mongodb")
          ? "mongodb"
          : version.includes("sqlserver")
            ? "sqlserver"
            : version.includes("oracle")
              ? "oracle"
              : version.includes("mysql")
                ? "mysql"
                : version.includes("files")
                  ? "files"
                  : "postgres",
        owner_id: this.uid,
        target_table: `spark_${name}`,
        config: {},
        token_hash: await sha256(token),
      })
      .select("id")
      .single();
    if (s.error) throw s.error;
    const c = await this.db
      .from("connect_connections")
      .insert({
        source_id: s.data.id,
        owner_id: this.uid,
        name,
        credential_ref: credential,
        source_version: version,
      })
      .select("id")
      .single();
    if (c.error) throw c.error;
    return { id: c.data.id as string, stream: "", token, credential };
  }
  // Discovers through the connection, then configures one stream of the discovered table.
  async stream(
    c: Conn,
    table: { namespace: string; name: string; primary_key: string; cursor_field: string },
    sync_mode: "full_refresh" | "incremental" | "cdc",
  ) {
    await this.enqueue(c, "discover");
    await this.agent(c);
    const cat = (
      await this.db.from("connect_connections").select("discovered_catalog").eq("id", c.id).single()
    ).data.discovered_catalog;
    const found = cat.find(
      (x: { namespace: string; name: string }) =>
        x.namespace === table.namespace && x.name === table.name,
    );
    if (!found) throw new Error(`${table.namespace}.${table.name} not discovered`);
    const st = await this.db
      .from("connect_streams")
      .insert({ connection_id: c.id, ...table, sync_mode, schema_json: found.json_schema })
      .select("id")
      .single();
    if (st.error) throw st.error;
    c.stream = st.data.id;
  }
  // Runs one agent pass and resolves with its stdout (the agent's JSON log lines).
  async agent(c: Conn, options: Record<string, unknown> = {}): Promise<string> {
    const tokenFile = join(this.folder, `token_${c.id}`);
    await writeFile(tokenFile, c.token, { mode: 0o600 });
    const gateway = this.gateway;
    const extra = this.submitConf.map((a) => ` ${a}`).join("");
    return await new Promise<string>((resolve, reject) => {
      const p = spawn(process.env["PYTHON"] ?? "python3", ["deploy/connect/agent.py"], {
        env: {
          ...process.env,
          PYTHONPATH: "deploy/connect",
          MERIDIAN_URL: APP,
          INGEST_TOKEN_FILE: tokenFile,
          CONNECT_SECRET_DIR: this.folder,
          CONNECT_ONCE: "1",
          // On Kubernetes the driver pod's spark-defaults.conf holds the master, image and limits.
          CONNECT_SPARK_SUBMIT: K8S
            ? `kubectl --context ${K8S} -n ${NAMESPACE} exec -i deploy/spark-driver -- sh /opt/meridian/connect/spark-submit.sh${extra}`
            : `docker exec -i ${MASTER} sh /opt/meridian/connect/spark-submit.sh --master spark://spark-master:7077 --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 --conf spark.speculation=false --conf spark.executorEnv.PYTHONPATH=/opt/meridian/connect${extra}`,
          CONNECT_SPARK_MERIDIAN_URL: APP.replace(/localhost|127\.0\.0\.1/, gateway),
          CONNECT_SPARK_CONFIG_OVERRIDES: JSON.stringify(
            this.overrides ? this.overrides(gateway) : { host: gateway },
          ),
          CONNECT_SPARK_OPTIONS: JSON.stringify({
            partitions: 6,
            rewrite: [
              [
                url!.replace("localhost", "127.0.0.1"),
                url!.replace(/localhost|127\.0\.0\.1/, gateway),
              ],
            ],
            ...options,
          }),
        },
        stdio: ["ignore", "pipe", "pipe"],
      });
      let out = "";
      p.stdout!.setEncoding("utf8").on("data", (d: string) => (out += d));
      p.stderr!.resume();
      p.on("close", (code) =>
        code === 0 ? resolve(out) : reject(new Error("Agent process failed")),
      );
    });
  }
  async enqueue(c: Conn, operation: string) {
    const r = await this.owner.rpc("connect_enqueue", {
      _connection: c.id,
      _request: crypto.randomUUID(),
      _operation: operation,
    });
    if (r.error) throw r.error;
    return r.data as string;
  }
  async run(id: string) {
    return (
      await this.db
        .from("connect_runs")
        .select("status,attempts,rows_committed,error_code")
        .eq("id", id)
        .single()
    ).data;
  }
  async state(c: Conn) {
    return (await this.db.from("connect_streams").select("state").eq("id", c.stream).single()).data
      .state;
  }
  // Every landed Bronze envelope of a run (NDJSON or Parquet parts), with the manifest of the
  // commit that landed it.
  async landed(run: string) {
    const commits = (
      await this.db
        .from("connect_commits")
        .select("sequence,batch_id,manifest_key,row_count")
        .eq("run_id", run)
        .order("sequence")
    ).data as { sequence: number; batch_id: string; manifest_key: string; row_count: number }[];
    const lines: Line[] = [];
    const manifests: Record<string, unknown>[] = [];
    for (const c of commits) {
      const m = JSON.parse(
        await (await this.db.storage.from("ingest-files").download(c.manifest_key)).data.text(),
      );
      manifests.push(m);
      const body = (await this.db.storage.from("ingest-files").download(m.object_key)).data;
      if (String(m.object_key).endsWith("/landing.parquet"))
        lines.push(...((await bronzeParquetEnvelopes(await body.arrayBuffer())) as Line[]));
      else
        for (const l of (await body.text()).split("\n").filter(Boolean)) lines.push(JSON.parse(l));
    }
    return { commits, lines, manifests };
  }
}
// Executor hosts that wrote a run's parts (the driver's empty completion part excluded).
export function executors(manifests: Record<string, unknown>[]) {
  return new Set(
    manifests
      .map((m) => (m["produced_by"] as { executor?: string } | undefined)?.executor)
      .filter((e) => e && e !== "driver")
      .map((e) => e!.split(":")[0]),
  );
}
// Landed rows keyed by primary key, for comparing a Spark run with a single-worker run.
export function byKey(lines: Line[]) {
  return lines
    .map((l) => [JSON.stringify(l["primary_key"]), JSON.stringify(l.data)] as const)
    .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
}
