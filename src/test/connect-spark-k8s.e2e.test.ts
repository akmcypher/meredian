// @vitest-environment node
// Spark ingestion S5b (deploy/connect/README.md, "Spark ingestion"): Spark on Kubernetes
// (deploy/connect/spark/k8s). A run's executors are pods with resource limits, the restricted Pod
// Security profile and no Kubernetes API token; the Spark secret reaches them by reference, never
// as a value in the pod spec; the namespace refuses privileged or oversized pods; and only Spark
// pods can reach the driver. Needs CONNECT_SPARK_K8S (a kubectl context with up.sh applied), the
// dev app and the seeded connect_acceptance fixture; skips otherwise.
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import {
  K8S,
  NAMESPACE,
  SparkHarness,
  executors,
  kube,
  live,
  type Conn,
} from "./connect-spark-harness";

const run = promisify(execFile);
const h = new SparkHarness();
let conn: Conn;
const table = {
  namespace: "public",
  name: "connect_acceptance",
  primary_key: "id",
  cursor_field: "updated_at",
};

type Container = {
  env?: { name: string; value?: string; valueFrom?: { secretKeyRef?: Record<string, string> } }[];
  resources: { limits: Record<string, string>; requests: Record<string, string> };
  volumeMounts: { mountPath: string }[];
  securityContext: {
    allowPrivilegeEscalation: boolean;
    readOnlyRootFilesystem: boolean;
    capabilities: { drop: string[] };
  };
};
type Pod = {
  metadata: { name: string; ownerReferences?: { kind: string; name: string }[] };
  spec: {
    serviceAccountName: string;
    volumes: { name: string; projected?: unknown }[];
    securityContext: { runAsNonRoot: boolean; seccompProfile: { type: string } };
    containers: Container[];
  };
};

// Records every executor pod's spec while the agent runs (executors are deleted when it ends).
async function watchExecutors(done: () => boolean) {
  const seen = new Map<string, Pod>();
  while (!done()) {
    try {
      const { stdout } = await run(
        "kubectl",
        [
          "--context",
          K8S,
          "-n",
          NAMESPACE,
          "get",
          "pods",
          "-l",
          "spark-role=executor",
          "-o",
          "json",
        ],
        { timeout: 20000 },
      );
      for (const p of (JSON.parse(stdout) as { items: Pod[] }).items) seen.set(p.metadata.name, p);
    } catch {
      // The API server can be briefly slow; the next poll catches up.
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  return [...seen.values()];
}

// Server-side dry run of a pod in the namespace: admission runs, nothing is created. Returns the
// refusal, or "" when the pod would be admitted.
function admit(name: string, container: Record<string, unknown>) {
  const manifest = {
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name, namespace: NAMESPACE },
    spec: {
      securityContext: {
        runAsNonRoot: true,
        runAsUser: 185,
        seccompProfile: { type: "RuntimeDefault" },
      },
      containers: [
        {
          name,
          image: "meridian-spark-ingest-k8s:local",
          imagePullPolicy: "Never",
          command: ["true"],
          securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } },
          ...container,
        },
      ],
    },
  };
  try {
    kube(["run", name, "--image=x", "--dry-run=server", "--overrides", JSON.stringify(manifest)]);
    return "";
  } catch (e) {
    return String((e as { stderr?: string }).stderr ?? e);
  }
}

describe.skipIf(!live || !K8S)(
  "Spark ingestion (S5b): Spark on Kubernetes",
  { timeout: 600000 },
  () => {
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
      conn = await h.connection("k8s", "spark-postgres-v1", "postgres");
      await h.stream(conn, table, "full_refresh");
    }, 600000);
    afterAll(() => h.teardown(), 60000);

    it("loads the stream on executor pods with limits, restricted security and the secret by reference", async () => {
      const id = await h.enqueue(conn, "sync");
      let finished = false;
      const [out, pods] = await Promise.all([
        h.agent(conn).finally(() => (finished = true)),
        watchExecutors(() => finished),
      ]);
      expect(out).not.toContain("spark_insecure");
      expect(await h.run(id)).toMatchObject({ status: "succeeded", rows_committed: 1200 });
      expect(executors((await h.landed(id)).manifests).size).toBeGreaterThanOrEqual(2);

      expect(pods.length).toBeGreaterThanOrEqual(2);
      const driver = kube([
        "get",
        "pods",
        "-l",
        "app=spark-driver",
        "-o",
        "jsonpath={.items[0].metadata.name}",
      ]);
      const encoded = kube(["get", "secret", "spark-auth", "-o", "jsonpath={.data.key\\.b64}"]);
      const value = Buffer.from(encoded, "base64").toString("utf8");
      expect(value.length).toBeGreaterThan(40);
      for (const p of pods) {
        const c = p.spec.containers[0]!;
        expect(c.resources.limits["cpu"]).toBe("1");
        expect(["1Gi", "1024Mi"]).toContain(c.resources.limits["memory"]);
        expect(c.resources.requests["cpu"]).toBe("500m");
        expect(p.spec.serviceAccountName).toBe("spark-executor");
        // No Kubernetes API credentials are mounted.
        expect(
          p.spec.volumes.some((v) => v.projected || v.name.startsWith("kube-api-access")),
        ).toBe(false);
        expect(
          c.volumeMounts.some((m) => m.mountPath.startsWith("/var/run/secrets/kubernetes.io")),
        ).toBe(false);
        expect(p.spec.securityContext).toMatchObject({
          runAsNonRoot: true,
          seccompProfile: { type: "RuntimeDefault" },
        });
        expect(c.securityContext).toMatchObject({
          allowPrivilegeEscalation: false,
          readOnlyRootFilesystem: true,
        });
        expect(c.securityContext.capabilities.drop).toContain("ALL");
        expect(p.metadata.ownerReferences).toContainEqual(
          expect.objectContaining({ kind: "Pod", name: driver }),
        );
        // The secret reaches the executor by reference only; its value is nowhere in the spec.
        expect(c.env).toContainEqual({
          name: "_SPARK_AUTH_SECRET",
          valueFrom: { secretKeyRef: { name: "spark-auth", key: "key.b64" } },
        });
        expect(c.env?.some((e) => e.name.includes("AUTH") && e.value !== undefined)).toBe(false);
        expect(JSON.stringify(p)).not.toContain(value);
      }
    });

    it("the driver refuses an application with network encryption off", async () => {
      const id = await h.enqueue(conn, "sync");
      h.submitConf = ["--conf", "spark.network.crypto.enabled=false"];
      let out: string;
      try {
        out = await h.agent(conn);
      } finally {
        h.submitConf = [];
      }
      expect(out).toContain('"code": "spark_insecure"');
      expect(out).toContain("spark.network.crypto.enabled");
      expect((await h.run(id)).status).not.toBe("succeeded");
      expect((await h.landed(id)).manifests).toHaveLength(0);
      await h.db
        .from("connect_runs")
        .update({ status: "cancelled", lease_token: null, lease_until: null })
        .eq("id", id);
    });

    it("the namespace refuses privileged and oversized pods", () => {
      expect(
        admit("s5b-privileged", {
          securityContext: { privileged: true, allowPrivilegeEscalation: true },
        }),
      ).toMatch(/PodSecurity "restricted/);
      expect(
        admit("s5b-oversized", { resources: { limits: { cpu: "1", memory: "4Gi" } } }),
      ).toMatch(/maximum memory usage per Container is 2Gi/);
      expect(admit("s5b-fine", {})).toBe("");
    });

    it("only Spark pods can reach the driver", async () => {
      const ip = kube([
        "get",
        "pods",
        "-l",
        "app=spark-driver",
        "-o",
        "jsonpath={.items[0].status.podIP}",
      ]);
      const probe = (name: string, labels: string[]) =>
        run(
          "kubectl",
          [
            "--context",
            K8S,
            "-n",
            NAMESPACE,
            "run",
            name,
            ...labels,
            "--image=meridian-spark-ingest-k8s:local",
            "--restart=Never",
            "--rm",
            "-i",
            "--quiet",
            "--overrides",
            JSON.stringify({
              apiVersion: "v1",
              spec: {
                securityContext: {
                  runAsNonRoot: true,
                  runAsUser: 185,
                  seccompProfile: { type: "RuntimeDefault" },
                },
                containers: [
                  {
                    name: "probe",
                    image: "meridian-spark-ingest-k8s:local",
                    imagePullPolicy: "Never",
                    command: [
                      "python3",
                      "-c",
                      "import socket,sys\ntry:\n socket.create_connection((sys.argv[1],7078),5); print('OPEN')\nexcept Exception as e: print('BLOCKED', type(e).__name__)",
                      ip,
                    ],
                    securityContext: {
                      allowPrivilegeEscalation: false,
                      capabilities: { drop: ["ALL"] },
                    },
                  },
                ],
              },
            }),
          ],
          { timeout: 180000 },
        ).then((r) => r.stdout.trim());
      // No application is running, so a permitted connection is refused by the driver pod itself;
      // a connection the network policy drops times out.
      expect(await probe(`s5b-outsider-${Date.now()}`, [])).toContain("BLOCKED TimeoutError");
      expect(await probe(`s5b-executor-${Date.now()}`, ["--labels=spark-role=executor"])).toContain(
        "BLOCKED ConnectionRefusedError",
      );
    });
  },
);
