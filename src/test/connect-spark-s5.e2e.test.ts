// @vitest-environment node
// Spark ingestion S5a (deploy/connect/README.md, "Spark ingestion"): Spark RPC is authenticated and
// encrypted. The secured standalone cluster (deploy/connect/spark/compose.yml) refuses an
// application without its shared secret, and the driver refuses to send any task (with the
// credentials and lease token in it) unless authentication, network encryption and spill-file
// encryption are all on. Needs the dev app, the seeded connect_acceptance fixture and the Spark
// cluster; skips otherwise.
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawnSync } from "node:child_process";
import { K8S, MASTER, SparkHarness, live, type Conn } from "./connect-spark-harness";

const h = new SparkHarness();
let conn: Conn;
const table = {
  namespace: "public",
  name: "connect_acceptance",
  primary_key: "id",
  cursor_field: "updated_at",
};

describe.skipIf(!live)("Spark ingestion (S5a): secured Spark RPC", { timeout: 600000 }, () => {
  beforeAll(async () => {
    await h.setup();
    await h.secret("postgres", {
      host: process.env["CONNECT_TEST_HOST"] ?? "127.0.0.1",
      port: Number(process.env["CONNECT_TEST_PORT"] ?? 55439),
      dbname: process.env["CONNECT_TEST_DB"] ?? "postgres",
      user: "postgres",
      password: process.env["PGPASSWORD"] ?? "postgres",
      sslmode: "disable",
    });
    conn = await h.connection("spark", "spark-postgres-v1", "postgres");
    await h.stream(conn, table, "full_refresh");
  }, 600000);
  afterAll(() => h.teardown(), 60000);

  // Standalone only: on Kubernetes there is no shared master; each application starts its own
  // executors (connect-spark-k8s checks how they get the secret).
  it.skipIf(!!K8S)(
    "an application without the cluster secret cannot register with the master",
    () => {
      // Spark's own example job, with an empty configuration directory: no secret, no encryption.
      const r = spawnSync(
        "docker",
        [
          "exec",
          MASTER,
          "sh",
          "-c",
          "mkdir -p /tmp/s5-noconf && SPARK_CONF_DIR=/tmp/s5-noconf /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-master /opt/spark/examples/src/main/python/pi.py 2 2>&1",
        ],
        { encoding: "utf8", timeout: 240000 },
      );
      expect(r.status).not.toBe(0);
      expect(r.stdout).not.toContain("Pi is roughly");
      expect(r.stdout).toMatch(/SASL/);
    },
  );

  it("the driver refuses an application with spill encryption off, before reading or landing anything", async () => {
    const run = await h.enqueue(conn, "sync");
    h.submitConf = ["--conf", "spark.io.encryption.enabled=false"];
    let out: string;
    try {
      out = await h.agent(conn);
    } finally {
      h.submitConf = [];
    }
    const logs = out
      .split("\n")
      .filter((l) => l.startsWith("{"))
      .map((l) => JSON.parse(l) as { type?: string; log?: Record<string, unknown> });
    expect(logs).toContainEqual({
      type: "LOG",
      log: { level: "ERROR", code: "spark_insecure", settings: ["spark.io.encryption.enabled"] },
    });
    const r = await h.run(run);
    expect(r.status).not.toBe("succeeded");
    expect(r.rows_committed ?? 0).toBe(0);
    expect((await h.landed(run)).manifests).toHaveLength(0);
    await h.db
      .from("connect_runs")
      .update({ status: "cancelled", lease_token: null, lease_until: null })
      .eq("id", run);
  });

  it("the same stream loads in full on the secured cluster", async () => {
    const run = await h.enqueue(conn, "sync");
    const out = await h.agent(conn);
    expect(out).not.toContain("spark_insecure");
    expect(await h.run(run)).toMatchObject({ status: "succeeded", rows_committed: 1200 });
  });
});
