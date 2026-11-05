// Pure ingestion helpers shared by the REST sync, the push endpoint and tests.
import { inferSchema, type Field } from "@/lib/schema";

export type Row = Record<string, unknown>;
export const MAX_INGEST_ROWS = 5000;
export type IngestKind = "rest" | "postgres" | "mysql" | "kafka_cdc";
export type WriteMode = "replace" | "append" | "upsert";

/** Read a dotted path like `data.items` (a trailing `[*]` is allowed). Empty path = root. */
export function extractPath(json: unknown, path: string): Row[] {
  let cur: unknown = json;
  const parts = path
    .replace(/\[\*\]$/, "")
    .split(".")
    .map((p) => p.trim())
    .filter(Boolean);
  for (const p of parts) {
    if (cur && typeof cur === "object") cur = (cur as Record<string, unknown>)[p];
    else return [];
  }
  if (Array.isArray(cur)) return cur.filter((r) => r && typeof r === "object").map(flatten);
  if (cur && typeof cur === "object") return [flatten(cur as Row)];
  return [];
}

/** Flatten nested objects one level deep into `a_b` columns; arrays become JSON text. */
export function flatten(r: unknown): Row {
  const out: Row = {};
  for (const [k, v] of Object.entries(r as Row)) {
    if (v && typeof v === "object" && !Array.isArray(v)) {
      for (const [k2, v2] of Object.entries(v as Row))
        out[`${k}_${k2}`] = typeof v2 === "object" && v2 !== null ? JSON.stringify(v2) : v2;
    } else out[k] = Array.isArray(v) ? JSON.stringify(v) : v;
  }
  return out;
}

export function schemaFor(rows: Row[]): Field[] {
  return inferSchema(
    rows
      .slice(0, 200)
      .map((r) =>
        Object.fromEntries(Object.entries(r).map(([k, v]) => [k, v == null ? "" : String(v)])),
      ),
  );
}

/** Combine existing rows with an incoming batch. Upsert replaces rows that share `key`. */
export function mergeRows(existing: Row[], incoming: Row[], mode: WriteMode, key?: string): Row[] {
  let out: Row[];
  if (mode === "replace") out = incoming;
  else if (mode === "append" || !key) out = [...existing, ...incoming];
  else {
    const idx = new Map(existing.map((r, i) => [String(r[key]), i]));
    out = [...existing];
    for (const r of incoming) {
      const i = idx.get(String(r[key]));
      if (i === undefined) {
        idx.set(String(r[key]), out.length);
        out.push(r);
      } else out[i] = r;
    }
  }
  return out.slice(-MAX_INGEST_ROWS);
}

/** Debezium-style change events: op c/r/u insert or update `after`, d deletes by `before[key]`. */
export type CdcEvent = {
  op: "c" | "r" | "u" | "d";
  before?: Row | null | undefined;
  after?: Row | null | undefined;
};
export function applyCdc(existing: Row[], events: CdcEvent[], key: string): Row[] {
  const map = new Map(existing.map((r) => [String(r[key]), r]));
  for (const e of events) {
    if (e.op === "d") {
      const k = e.before?.[key];
      if (k !== undefined) map.delete(String(k));
    } else if (e.after) map.set(String(e.after[key]), flatten(e.after));
  }
  return [...map.values()].slice(-MAX_INGEST_ROWS);
}

export async function sha256(s: string): Promise<string> {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export const safeTableName = (s: string) =>
  s
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^(\d)/, "t_$1")
    .slice(0, 60);

/**
 * Node worker the user runs next to their database / Kafka; pushes batches to the ingest endpoint.
 * Delivery is at-least-once: database cursors and Kafka offsets advance only after the endpoint
 * accepts a batch, and each batch keeps one id across retries so a lost response cannot land it
 * twice. Set ONCE=1 (databases) or IDLE_EXIT_MS (Kafka) to stop instead of polling forever.
 */
export function workerScript(
  kind: IngestKind,
  endpoint: string,
  cfg: { query?: string; cursor?: string; key?: string; topic?: string },
): string {
  const head = `// Meridian ingest worker — run with: INGEST_TOKEN=... node worker.mjs
const ENDPOINT = ${JSON.stringify(endpoint)};
const TOKEN = process.env.INGEST_TOKEN;
if (!TOKEN) throw new Error("Set INGEST_TOKEN");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// Lands one batch or throws. The batch id stays the same across retries.
async function push(body) {
  const payload = JSON.stringify({ ...body, batchId: crypto.randomUUID() });
  for (let attempt = 0; ; attempt++) {
    let status = 0;
    try {
      const r = await fetch(ENDPOINT, { method: "POST", headers: { "content-type": "application/json", "x-ingest-token": TOKEN }, body: payload });
      status = r.status;
      console.log(new Date().toISOString(), r.status, await r.text());
      if (r.ok) return;
    } catch (e) {
      console.error(new Date().toISOString(), "push failed:", e.message);
    }
    const rejected = status >= 400 && status < 500 && status !== 429;
    if (rejected || attempt >= 5) throw new Error(\`batch not landed (status \${status || "network error"})\`);
    await sleep(1000 * 2 ** attempt);
  }
}
`;
  if (kind === "kafka_cdc")
    return (
      head +
      `// npm i kafkajs
import { Kafka } from "kafkajs";
const TOPIC = ${JSON.stringify(cfg.topic || "dbserver1.public.customers")};
const OPS = new Set(["c", "r", "u", "d"]);
// What happens to a record that is not a usable change event (DEAD_LETTER_POLICY):
//   store (default): send it to Meridian's dead-letter store, then move past it
//   fail: stop without committing the offset, so the record is read again after a fix
const POLICY = process.env.DEAD_LETTER_POLICY || "store";
if (!["store", "fail"].includes(POLICY)) throw new Error("DEAD_LETTER_POLICY must be store or fail");
const b64 = (buf) => (buf ? Buffer.from(buf).toString("base64") : null);
// Tombstones and Debezium truncate/message/schema-change records are expected and carry no row;
// anything else that is not a valid c/r/u/d event is a dead letter, never a silent skip.
function classify(message) {
  if (!message.value) return { skip: "tombstone" };
  let v;
  try { v = JSON.parse(message.value.toString()); } catch { return { dead: "invalid_json" }; }
  const p = v && typeof v === "object" ? (v.payload ?? v) : null;
  if (!p || typeof p !== "object") return { dead: "unrecognized_event" };
  if (p.op === "t" || p.op === "m" || (p.op === undefined && (p.ddl !== undefined || p.tableChanges !== undefined))) return { skip: "control" };
  if (!OPS.has(p.op)) return { dead: "unrecognized_event" };
  const row = p.op === "d" ? p.before : p.after;
  if (!row || typeof row !== "object") return { dead: "invalid_event" };
  return { event: { op: p.op, before: p.before ?? null, after: p.after ?? null } };
}
const kafka = new Kafka({ clientId: "meridian-ingest", brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(",") });
const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP || "meridian-ingest-" + TOPIC });
await consumer.connect();
await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
let lastActivity = Date.now();
// Offsets are committed only after the batch and its dead letters have landed, so a failed
// push is read again instead of being dropped.
await consumer.run({
  autoCommit: false,
  eachBatch: async ({ batch, heartbeat }) => {
    const events = [];
    const dead = [];
    for (const message of batch.messages) {
      const c = classify(message);
      if (c.event) events.push(c.event);
      else if (c.dead) dead.push({ topic: batch.topic, partition: batch.partition, offset: message.offset, error: c.dead, key: b64(message.key), value: b64(message.value) });
    }
    if (dead.length && POLICY === "fail") {
      console.error(new Date().toISOString(), "dead letters with DEAD_LETTER_POLICY=fail; offset not committed:", dead.map((d) => d.partition + ":" + d.offset + " " + d.error).join(", "));
      // Exit directly: disconnect() would wait for this batch and deadlock. Auto-commit is off,
      // so nothing in this batch is acknowledged.
      process.exit(2);
    }
    // Dead letters are stored before the offset moves past them, in chunks well under the
    // endpoint's 8 MB request cap.
    for (let i = 0; i < dead.length; ) {
      let size = 0, j = i;
      while (j < dead.length && j - i < 50 && (j === i || size + (dead[j].value?.length ?? 0) < 4e6)) size += dead[j++].value?.length ?? 0;
      await push({ deadLetters: dead.slice(i, j) });
      await heartbeat();
      i = j;
    }
    for (let i = 0; i < events.length; i += 5000) {
      await push({ events: events.slice(i, i + 5000) });
      await heartbeat();
    }
    const last = batch.messages[batch.messages.length - 1];
    if (last) await consumer.commitOffsets([{ topic: batch.topic, partition: batch.partition, offset: (BigInt(last.offset) + 1n).toString() }]);
    lastActivity = Date.now();
  },
});
const IDLE_EXIT_MS = Number(process.env.IDLE_EXIT_MS || 0);
if (IDLE_EXIT_MS) setInterval(async () => {
  if (Date.now() - lastActivity < IDLE_EXIT_MS) return;
  await consumer.disconnect();
  process.exit(0);
}, 1000);
`
    );
  const driver =
    kind === "mysql"
      ? `// npm i mysql2
import mysql from "mysql2/promise";
const db = await mysql.createConnection(process.env.DATABASE_URL);
const run = async (sql, params) => (await db.query(sql, params))[0];`
      : `// npm i pg
import pg from "pg";
const db = new pg.Client({ connectionString: process.env.DATABASE_URL }); await db.connect();
const run = async (sql, params) => (await db.query(sql, params)).rows;`;
  return (
    head +
    `import { readFileSync, renameSync, writeFileSync } from "node:fs";
` +
    driver +
    `
const BASE = ${JSON.stringify(cfg.query || "select * from customers")};
// The cursor column must be unique and only increase (an id or sequence): rows sharing a cursor
// value across a page boundary can be skipped.
const CURSOR = ${JSON.stringify(cfg.cursor || "")};
const PAGE = 5000;
const STATE_FILE = process.env.STATE_FILE || "meridian-cursor.json";
function loadCursor() {
  try { return JSON.parse(readFileSync(STATE_FILE, "utf8")).last ?? null; } catch { return null; }
}
function saveCursor(value) {
  writeFileSync(STATE_FILE + ".tmp", JSON.stringify({ last: value }));
  renameSync(STATE_FILE + ".tmp", STATE_FILE);
}
let last = CURSOR ? loadCursor() : null;
async function tick() {
  if (!CURSOR) {
    const rows = await run(\`\${BASE} limit \${PAGE}\`, []);
    if (rows.length) await push({ rows, mode: "replace" });
    return;
  }
  for (;;) {
    const where = last === null ? "" : \` where \${CURSOR} > ${kind === "mysql" ? "?" : "$1"}\`;
    const rows = await run(\`select * from (\${BASE}) s\${where} order by \${CURSOR} limit \${PAGE}\`, last === null ? [] : [last]);
    if (!rows.length) return;
    await push({ rows, mode: "upsert" });
    last = rows[rows.length - 1][CURSOR];
    saveCursor(last);
    if (rows.length < PAGE) return;
  }
}
const ONCE = process.env.ONCE === "1";
for (;;) {
  try {
    await tick();
  } catch (e) {
    console.error(new Date().toISOString(), e.message);
    if (ONCE) process.exit(1);
  }
  if (ONCE) break;
  await sleep(Number(process.env.INTERVAL_MS || 60000));
}
await db.end();
`
  );
}

export const CDC_COMPOSE = `# Kafka + Debezium CDC for Postgres. docker compose up -d, then register the connector below.
services:
  kafka:
    image: apache/kafka:3.9.1
    ports: ["127.0.0.1:9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: INTERNAL://:19092,CONTROLLER://:9093,HOST://:9092
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:19092,HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
  connect:
    image: debezium/connect:2.6
    ports: ["8083:8083"]
    depends_on: [kafka]
    environment:
      BOOTSTRAP_SERVERS: kafka:19092
      GROUP_ID: 1
      CONFIG_STORAGE_TOPIC: connect_configs
      OFFSET_STORAGE_TOPIC: connect_offsets
      STATUS_STORAGE_TOPIC: connect_status
# Register (your Postgres needs wal_level=logical):
# curl -X POST localhost:8083/connectors -H 'content-type: application/json' -d '{
#   "name":"pg","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector",
#   "database.hostname":"host.docker.internal","database.port":"5432","database.user":"postgres",
#   "database.password":"postgres","database.dbname":"app","topic.prefix":"dbserver1",
#   "plugin.name":"pgoutput","value.converter.schemas.enable":"false"}}'
`;
