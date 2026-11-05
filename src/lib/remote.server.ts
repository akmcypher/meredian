/** Server-only: runs SQL on external warehouses and reads S3 objects. Shared by server functions and the job scheduler route. */
import { safeOutboundFetch, boundedResponseBytes } from "./outbound.server";
export type Cfg = Record<string, string>;
export type QueryOut = { columns: string[]; rows: Record<string, string | null>[] };

export const WAREHOUSE_KINDS = [
  "databricks",
  "redshift",
  "athena",
  "snowflake",
  "bigquery",
] as const;
/** Kinds whose requests need a signed transport (AWS SigV4, Snowflake, Google OAuth). */
export const SIGNED_REMOTE_KINDS = new Set(["s3", "redshift", "athena", "snowflake", "bigquery"]);
/**
 * No transport is configured for these kinds yet. A single company-wide credential per kind
 * would let any engineer read the company bucket and run SQL on the company warehouse, so they
 * stay unavailable until per-connection credential adapters with an allow-list exist; the
 * request builders below are ready for that transport.
 * Databricks is unaffected: it uses the connection's own token from connection_secrets.
 */
function remoteUnavailable(kind: string): never {
  throw new Error(
    `${kind} connections are not available yet: shared company credentials are not supported ` +
      "and per-connection credentials are pending.",
  );
}

/** Signed request for a SIGNED_REMOTE_KINDS kind; the per-connection adapter plugs in here. */
export async function remoteRequest(
  kind: string,
  _path: string,
  _init: RequestInit = {},
  _ms = 30000,
): Promise<Response> {
  return remoteUnavailable(kind);
}
async function okJson<T>(r: Response): Promise<T> {
  const t = new TextDecoder().decode(await boundedResponseBytes(r));
  if (!r.ok) throw new Error(`Provider request failed [${r.status}]`);
  return (t ? JSON.parse(t) : {}) as T;
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const s = (v: unknown) => (v == null ? null : String(v));

async function redshift(op: string, body: object) {
  return okJson<Record<string, unknown>>(
    await remoteRequest("redshift", "/", {
      method: "POST",
      headers: {
        "X-Amz-Target": `RedshiftData.${op}`,
        "Content-Type": "application/x-amz-json-1.1",
      },
      body: JSON.stringify(body),
    }),
  );
}
type AthenaResponse = {
  QueryExecutionId?: string;
  QueryExecution?: { Status?: { State?: string; StateChangeReason?: string } };
  ResultSet?: { Rows?: { Data?: { VarCharValue?: string }[] }[] };
};
async function athena(op: string, body: object) {
  return okJson<AthenaResponse>(
    await remoteRequest("athena", "/", {
      method: "POST",
      headers: {
        "X-Amz-Target": `AmazonAthena.${op}`,
        "Content-Type": "application/x-amz-json-1.1",
      },
      body: JSON.stringify(body),
    }),
  );
}

/** Run one SQL statement on a connected warehouse and return up to ~1000 rows. */
export async function runWarehouseSql(kind: string, all: Cfg, sql: string): Promise<QueryOut> {
  if (kind === "databricks") {
    const u = (all["url"] ?? "").replace(/\/+$/, "");
    const r = await safeOutboundFetch(
      `${u}/api/2.0/sql/statements`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${all["token"] ?? ""}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          warehouse_id: all["warehouse_id"],
          statement: sql,
          wait_timeout: "30s",
        }),
      },
      40000,
    );
    const j = await okJson<{
      status?: { state: string; error?: { message: string } };
      manifest?: { schema?: { columns?: { name: string }[] } };
      result?: { data_array?: unknown[][] };
    }>(r);
    if (j.status?.state === "FAILED")
      throw new Error(j.status.error?.message ?? "Statement failed");
    if (j.status?.state !== "SUCCEEDED")
      throw new Error(
        "Statement outcome is not confirmed; reconcile it in Databricks before retrying",
      );
    const cols = j.manifest?.schema?.columns?.map((x) => x.name) ?? [];
    return {
      columns: cols,
      rows: (j.result?.data_array ?? []).map((a) =>
        Object.fromEntries(cols.map((k, i) => [k, s(a[i])])),
      ),
    };
  }
  if (kind === "redshift") {
    const wg = process.env["REDSHIFT_WORKGROUP"],
      cl = process.env["REDSHIFT_CLUSTER_IDENTIFIER"],
      db = process.env["REDSHIFT_DATABASE"],
      user = process.env["REDSHIFT_DB_USER"];
    if (!db) throw new Error("Redshift database isn't set on the connection");
    if (!wg && !cl) throw new Error("Redshift workgroup or cluster isn't set on the connection");
    const { Id } = await redshift("ExecuteStatement", {
      ...(wg
        ? { WorkgroupName: wg }
        : { ClusterIdentifier: cl, ...(user ? { DbUser: user } : {}) }),
      Database: db,
      Sql: sql,
    });
    for (let i = 0; ; i++) {
      if (i > 60) throw new Error("Redshift query timed out");
      const d = await redshift("DescribeStatement", { Id });
      if (d["Status"] === "FINISHED") {
        if (!d["HasResultSet"])
          return {
            columns: ["result"],
            rows: [{ result: `ok · ${d["ResultRows"] ?? 0} rows affected` }],
          };
        break;
      }
      if (d["Status"] === "FAILED" || d["Status"] === "ABORTED")
        throw new Error(`Redshift: ${d["Error"] ?? d["Status"]}`);
      await sleep(500);
    }
    const res = await redshift("GetStatementResult", { Id });
    const cols = ((res["ColumnMetadata"] ?? []) as { name: string }[]).map((c) => c.name);
    const rows = ((res["Records"] ?? []) as Record<string, unknown>[][]).map((rec) =>
      Object.fromEntries(
        cols.map((c, i) => {
          const f = rec[i] ?? {};
          return [c, f["isNull"] ? null : s(Object.values(f)[0])];
        }),
      ),
    );
    return { columns: cols, rows };
  }
  if (kind === "athena") {
    const wg = all["workgroup"] || process.env["AWS_ATHENA_WORKGROUP"],
      out = all["output_location"] || process.env["AWS_ATHENA_OUTPUT_LOCATION"];
    const { QueryExecutionId } = await athena("StartQueryExecution", {
      QueryString: sql,
      ClientRequestToken: crypto.randomUUID(),
      ...(all["database"] ? { QueryExecutionContext: { Database: all["database"] } } : {}),
      ...(wg ? { WorkGroup: wg } : {}),
      ...(out ? { ResultConfiguration: { OutputLocation: out } } : {}),
    });
    let delay = 800;
    for (let i = 0; ; i++) {
      if (i > 40) {
        await athena("StopQueryExecution", { QueryExecutionId }).catch(() => {});
        throw new Error("Athena query timed out");
      }
      await sleep(delay);
      delay = Math.min(delay * 1.4, 4000);
      const { QueryExecution } = await athena("GetQueryExecution", { QueryExecutionId });
      const st = QueryExecution?.Status?.State;
      if (st === "SUCCEEDED") break;
      if (st === "FAILED" || st === "CANCELLED")
        throw new Error(`Athena: ${QueryExecution?.Status?.StateChangeReason ?? st}`);
    }
    const { ResultSet } = await athena("GetQueryResults", { QueryExecutionId, MaxResults: 1000 });
    const [head, ...rest] = ResultSet?.Rows ?? [];
    const cols = (head?.Data ?? []).map((d) => d.VarCharValue ?? "");
    return {
      columns: cols,
      rows: rest.map((r) =>
        Object.fromEntries(cols.map((c, i) => [c, r.Data?.[i]?.VarCharValue ?? null])),
      ),
    };
  }
  if (kind === "snowflake") {
    if (!all["warehouse"]) throw new Error("Set the Snowflake warehouse on this connection");
    const j = await okJson<{
      data?: unknown[][];
      resultSetMetaData?: { rowType?: { name: string }[] };
      message?: string;
    }>(
      await remoteRequest(
        "snowflake",
        "/v2/statements",
        {
          method: "POST",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({
            statement: sql,
            warehouse: all["warehouse"],
            ...(all["database"] ? { database: all["database"] } : {}),
            ...(all["schema"] ? { schema: all["schema"] } : {}),
            timeout: 60,
          }),
        },
        70000,
      ),
    );
    const cols = j.resultSetMetaData?.rowType?.map((c) => c.name) ?? [];
    if (!cols.length)
      throw new Error(j.message ?? "Snowflake statement is still running — try again shortly");
    return {
      columns: cols,
      rows: (j.data ?? []).map((a) => Object.fromEntries(cols.map((c, i) => [c, s(a[i])]))),
    };
  }
  if (kind === "bigquery") {
    const p = all["project_id"];
    if (!p) throw new Error("Set the BigQuery project ID on this connection");
    const j = await okJson<{
      schema?: { fields?: { name: string }[] };
      rows?: { f: { v: unknown }[] }[];
      jobComplete?: boolean;
    }>(
      await remoteRequest(
        "bigquery",
        `/bigquery/v2/projects/${encodeURIComponent(p)}/queries`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            query: sql,
            useLegacySql: false,
            maximumBytesBilled: all["max_bytes"] || "1073741824",
            timeoutMs: 30000,
            maxResults: 1000,
          }),
        },
        40000,
      ),
    );
    if (j.jobComplete === false)
      throw new Error("BigQuery query is still running — try again shortly");
    const cols = j.schema?.fields?.map((f) => f.name) ?? [];
    return {
      columns: cols,
      rows: (j.rows ?? []).map((r) => Object.fromEntries(cols.map((c, i) => [c, s(r.f[i]?.v)]))),
    };
  }
  throw new Error(`${kind} can't run SQL`);
}

/** Cheap credential check for signed remote connectors. */
export async function testRemoteConnection(kind: string, all: Cfg): Promise<string> {
  if (kind === "s3") {
    const r = await remoteRequest("s3", "/?list-type=2&max-keys=1");
    if (!r.ok) throw new Error(`HTTP ${r.status}: ${(await r.text()).slice(0, 200)}`);
    return "Bucket reachable";
  }
  if (kind === "redshift" || kind === "athena" || kind === "snowflake" || kind === "bigquery") {
    const out = await runWarehouseSql(kind, all, "SELECT 1 AS ok");
    return `Query ran · ${out.rows.length} row`;
  }
  throw new Error("No test for this type");
}

export type S3Item = { key: string; size: number; modified: string };
export async function listS3(prefix: string, token?: string) {
  const q = new URLSearchParams({ "list-type": "2", "max-keys": "200", delimiter: "/" });
  if (prefix) q.set("prefix", prefix);
  if (token) q.set("continuation-token", token);
  const r = await remoteRequest("s3", `/?${q}`);
  const xml = await r.text();
  if (!r.ok) throw new Error(`S3 list failed [${r.status}]: ${xml.slice(0, 300)}`);
  const tag = (blk: string, t: string) =>
    blk.match(new RegExp(`<${t}>([\\s\\S]*?)</${t}>`))?.[1] ?? "";
  const dec = (x: string) =>
    x
      .replace(/&amp;/g, "&")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"')
      .replace(/&apos;/g, "'");
  const items: S3Item[] = [...xml.matchAll(/<Contents>([\s\S]*?)<\/Contents>/g)].map((m) => ({
    key: dec(tag(m[1]!, "Key")),
    size: Number(tag(m[1]!, "Size")),
    modified: tag(m[1]!, "LastModified"),
  }));
  const folders = [...xml.matchAll(/<CommonPrefixes>([\s\S]*?)<\/CommonPrefixes>/g)].map((m) =>
    dec(tag(m[1]!, "Prefix")),
  );
  return {
    items,
    folders,
    next: tag(xml, "IsTruncated") === "true" ? tag(xml, "NextContinuationToken") : null,
  };
}

export const MAX_S3_BYTES = 15 * 1024 * 1024;
export async function readS3(key: string): Promise<string> {
  return downloadBase64(await signedS3ReadUrl(key));
}

async function signedS3ReadUrl(_key: string): Promise<string> {
  return remoteUnavailable("s3");
}

async function downloadBase64(url: string): Promise<string> {
  const r = await safeOutboundFetch(url, {}, 45000, MAX_S3_BYTES);
  if (!r.ok) throw new Error(`Download failed [${r.status}]`);
  const len = Number(r.headers.get("content-length") ?? 0);
  if (len > MAX_S3_BYTES)
    throw new Error(`File is ${(len / 1e6).toFixed(1)} MB — the limit is 15 MB`);
  const buf = new Uint8Array(await r.arrayBuffer());
  if (buf.byteLength > MAX_S3_BYTES) throw new Error("File is larger than 15 MB");
  let bin = "";
  for (let i = 0; i < buf.length; i += 0x8000)
    bin += String.fromCharCode(...buf.subarray(i, i + 0x8000));
  return btoa(bin);
}
