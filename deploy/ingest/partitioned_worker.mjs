#!/usr/bin/env node
// Meridian partitioned worker. Pulls its plan + checkpoints from Meridian, claims partitions atomically
// (leases + heartbeats), streams keyset batches from the source and commits rows+checkpoint together.
//
//   npm i pg mysql2            # databases
//   npm i kafkajs              # kafka
//   npm i @aws-sdk/client-s3   # s3 (ADLS: use its S3-compatible endpoint or az copy to s3)
//
//   MERIDIAN_URL=https://<app> MERIDIAN_TOKEN=mi_... node partitioned_worker.mjs \
//     --kind postgres --url postgres://user:pw@host/db --table public.orders --threads 4 [--worker-id host-a]
//
// Options: --kind postgres|mysql|rest|kafka|s3  --table <qualified>  --threads N (<= source connection limit)
//          --rest-url https://api/items?page={page}  --kafka-brokers b:9092 --topic t  --bucket b --prefix p/
//          --poison quarantine|skip|fail   (Kafka records that are not JSON objects; default quarantine)
//          --s3-endpoint http://host/s3 --s3-force-path-style   (S3-compatible stores)
//
// Consistency contract for postgres (and mysql):
//   - Within one worker process every thread reads one exported snapshot, so a run that finishes
//     without a restart is a single point-in-time copy.
//   - After a restart the worker exports a new snapshot and each partition resumes by key after its
//     checkpoint. Rows at or before a checkpoint keep the values of the earlier snapshot (later
//     updates to them are not re-read); rows after it come from the new snapshot. Deletes are never
//     captured: a row already written stays in the run. The run's snapshot record says "mixed"
//     when this happened. For an exact copy after a restart, cancel the run and plan a new one;
//     change capture is not part of this worker.
import { createHash } from "node:crypto";
import os from "node:os";

const a = Object.fromEntries(
  process.argv
    .slice(2)
    .reduce(
      (acc, v, i, arr) =>
        v.startsWith("--")
          ? [...acc, [v.slice(2), arr[i + 1]?.startsWith("--") ? true : (arr[i + 1] ?? true)]]
          : acc,
      [],
    ),
);
if (a.help) {
  console.log("see header of this file");
  process.exit(0);
}
const BASE = (process.env.MERIDIAN_URL ?? a.endpoint ?? "").replace(/\/$/, "");
const TOKEN = process.env.MERIDIAN_TOKEN ?? a.token;
const THREADS = Number(a.threads ?? 4);
const LEASE = Number(a["lease-seconds"] ?? 120);
const WID = String(a["worker-id"] ?? `${os.hostname()}-${process.pid}`);
if (!BASE || !TOKEN) {
  console.error("MERIDIAN_URL and MERIDIAN_TOKEN are required");
  process.exit(2);
}
// What happens to a Kafka record that is not a JSON object (malformed JSON, null value, or a
// JSON scalar/array that cannot be a row):
//   quarantine: stored in Meridian's dead-letter store before the checkpoint moves past it
//   skip: logged and counted, then passed over
//   fail: the partition fails at that record and its checkpoint does not move
const POISON = String(a.poison ?? "quarantine");
if (!["quarantine", "skip", "fail"].includes(POISON)) {
  console.error("--poison must be quarantine, skip or fail");
  process.exit(2);
}

async function api(action, body = {}, worker = WID) {
  for (let t = 1; ; t++) {
    let r;
    try {
      r = await fetch(`${BASE}/api/public/ingest-partition`, {
        method: "POST",
        headers: { "content-type": "application/json", "x-ingest-token": TOKEN },
        body: JSON.stringify({ action, worker, ...body }),
      });
    } catch (e) {
      if (t >= 6) throw e;
      await sleep(500 * 2 ** t);
      continue;
    }
    const j = await r.json().catch(() => ({}));
    if (r.ok) return j;
    if (r.status === 409 || t >= 6 || (r.status < 500 && r.status !== 429)) {
      const e = new Error(`${action} ${r.status} ${j.error ?? ""}`);
      e.status = r.status;
      throw e;
    }
    await sleep(500 * 2 ** t);
  }
}
const sleep = (ms) => new Promise((s) => setTimeout(s, ms));
// Stable id for a retried request, so a lost response cannot store a dead-letter batch twice.
const stableId = (key) => {
  const h = createHash("sha256").update(key).digest("hex");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-a${h.slice(17, 20)}-${h.slice(20, 32)}`;
};
async function quarantine(letters, key) {
  for (let i = 0, n = 0; i < letters.length; n++) {
    let size = 0,
      j = i;
    while (
      j < letters.length &&
      j - i < 50 &&
      (j === i || size + (letters[j].value?.length ?? 0) < 4e6)
    )
      size += letters[j++].value?.length ?? 0;
    const body = JSON.stringify({
      batchId: stableId(`${key}:${n}`),
      deadLetters: letters.slice(i, j),
    });
    for (let t = 1; ; t++) {
      const r = await fetch(`${BASE}/api/public/ingest`, {
        method: "POST",
        headers: { "content-type": "application/json", "x-ingest-token": TOKEN },
        body,
      }).catch(() => null);
      if (r?.ok) break;
      if (t >= 6 || (r && r.status < 500 && r.status !== 429))
        throw new Error(`dead letters not stored (${r?.status ?? "network error"})`);
      await sleep(500 * 2 ** t);
    }
    i = j;
  }
}
function kafkaRecord(message) {
  if (message.value == null) return { poison: "invalid_event" };
  let v;
  try {
    v = JSON.parse(message.value.toString());
  } catch {
    return { poison: "invalid_json" };
  }
  if (!v || typeof v !== "object" || Array.isArray(v)) return { poison: "schema_incompatible" };
  return { row: v };
}

// ---------------- source adapters: open(), batch(where, checkpoint, size) -> { rows, checkpoint, done } ----------------
function sqlAdapter(kind, pk) {
  const q = (c) => (kind === "mysql" ? "`" + c + "`" : `"${c}"`);
  return {
    async open() {
      if (kind === "postgres") {
        const { default: pg } = await import("pg");
        const c = new pg.Client({ connectionString: a.url });
        await c.connect();
        return { q: async (s, p) => (await c.query(s, p)).rows, end: () => c.end() };
      }
      const mysql = await import("mysql2/promise");
      const c = await mysql.createConnection(a.url);
      return { q: async (s, p) => (await c.query(s, p))[0], end: () => c.end() };
    },
    async begin(c, snapshot) {
      if (kind === "postgres") {
        await c.q("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
        if (snapshot?.pg_snapshot) {
          try {
            await c.q(`SET TRANSACTION SNAPSHOT '${snapshot.pg_snapshot}'`);
            return "shared";
          } catch {
            await c.q("ROLLBACK");
            await c.q("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
          }
        }
        return "per-partition";
      }
      await c.q("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY");
      return "per-partition";
    },
    async batch(c, where, cp, size) {
      const last = cp?.last ?? null; // array of pk values (composite keys supported)
      const ph = (i) => (kind === "mysql" ? "?" : `$${i + 1}`);
      const keyset = last
        ? ` AND (${pk.map(q).join(", ")}) > (${pk.map((_, i) => ph(i)).join(", ")})`
        : "";
      const rows = await c.q(
        `SELECT * FROM ${a.table} WHERE (${where})${keyset} ORDER BY ${pk.map(q).join(", ")} LIMIT ${size}`,
        last ?? [],
      );
      const lastRow = rows[rows.length - 1];
      return {
        rows: JSON.parse(JSON.stringify(rows)),
        checkpoint: { last: lastRow ? pk.map((k) => lastRow[k]) : last },
        done: rows.length < size,
      };
    },
  };
}

function restAdapter() {
  // Partition predicates for REST are page ranges: label "page ∈ [lo, hi)" → predicate {kind:"range", column:"page"}.
  return {
    async open() {
      return { end: async () => {} };
    },
    async begin() {
      return "none";
    },
    async batch(_c, _where, cp, _size, part) {
      const page = cp?.page ?? part.lo;
      const r = await fetch(String(a["rest-url"]).replace("{page}", String(page)), {
        signal: AbortSignal.timeout(30000),
      });
      if (!r.ok) throw new Error(`REST ${r.status}`);
      const rows = await r.json();
      const next = page + 1;
      return { rows, checkpoint: { page: next }, done: !rows.length || next > part.hi };
    },
  };
}

function kafkaAdapter() {
  // One logical partition per topic partition; checkpoint = next offset. Bounded: stops at the high watermark seen at claim time.
  let kafka;
  return {
    async open() {
      const { Kafka } = await import("kafkajs");
      kafka = new Kafka({ brokers: String(a["kafka-brokers"]).split(",") });
      return { end: async () => {} };
    },
    async begin() {
      return "offsets";
    },
    async batch(_c, _where, cp, size, part) {
      const admin = kafka.admin();
      await admin.connect();
      const offs = await admin.fetchTopicOffsets(a.topic);
      await admin.disconnect();
      const high = Number(offs.find((o) => o.partition === part.lo)?.high ?? 0);
      const from = Number(cp?.offset ?? 0);
      if (from >= high) return { rows: [], checkpoint: { offset: from }, done: true };
      const consumer = kafka.consumer({ groupId: `meridian-${WID}-${part.lo}` });
      await consumer.connect();
      await consumer.subscribe({ topic: a.topic });
      const rows = [];
      const poison = [];
      let next = from;
      await new Promise((resolve) => {
        consumer.run({
          autoCommit: false,
          eachMessage: async ({ partition, message }) => {
            if (partition !== part.lo) return;
            const o = Number(message.offset);
            if (o < from || o >= high) return;
            const r = kafkaRecord(message);
            if (r.row) rows.push({ ...r.row, _offset: o, _partition: partition });
            else
              poison.push({
                topic: a.topic,
                partition,
                offset: String(o),
                error: r.poison,
                key: message.key ? Buffer.from(message.key).toString("base64") : null,
                value: message.value ? Buffer.from(message.value).toString("base64") : null,
              });
            next = o + 1;
            if (rows.length >= size || next >= high) resolve();
          },
        });
        consumer.seek({ topic: a.topic, partition: part.lo, offset: String(from) });
        setTimeout(resolve, 15000);
      });
      await consumer.disconnect();
      if (poison.length) {
        const at = poison.map((p) => `${p.partition}:${p.offset} ${p.error}`).join(", ");
        if (POISON === "fail") throw new Error(`poison record(s) at ${at}`);
        if (POISON === "skip")
          console.warn(`[kafka] skipped ${poison.length} poison record(s): ${at}`);
        // Stored before the batch (and its checkpoint) is written, so nothing is passed silently.
        else await quarantine(poison, `${run.id}:${part.lo}:${from}`);
      }
      return { rows, checkpoint: { offset: next }, done: next >= high };
    },
  };
}

function s3Adapter() {
  // File partitions: predicate {kind:"file", files:[keys]}. Objects are JSON lines read with byte
  // ranges, never whole: checkpoint = { file, byte, line } where byte is the start of the next
  // unread line. A range ends at its last newline; a record longer than the range doubles it.
  // Checkpoints from earlier workers ({ file, row }) resume by skipping that many lines.
  const CHUNK = Number(a["s3-chunk-bytes"] ?? 4 * 1024 * 1024);
  let s3;
  return {
    async open() {
      const { S3Client } = await import("@aws-sdk/client-s3");
      s3 = new S3Client({
        ...(a["s3-endpoint"] ? { endpoint: String(a["s3-endpoint"]) } : {}),
        ...(a["s3-force-path-style"] ? { forcePathStyle: true } : {}),
      });
      return { end: async () => {} };
    },
    async begin() {
      return "immutable-files";
    },
    async batch(_c, _where, cp, size, part) {
      const { GetObjectCommand } = await import("@aws-sdk/client-s3");
      const fi = cp?.file ?? 0;
      if (fi >= part.files.length) return { rows: [], checkpoint: cp, done: true };
      const key = part.files[fi];
      let byte = cp?.byte ?? 0,
        line = cp?.line ?? 0;
      const skipTo = cp?.byte === undefined ? (cp?.row ?? 0) : 0;
      const rows = [];
      let eof = false,
        span = CHUNK;
      while (rows.length < size && !eof) {
        let obj;
        if (a.verbose) console.log(`[s3] ${key} bytes=${byte}-${byte + span - 1}`);
        try {
          obj = await s3.send(
            new GetObjectCommand({
              Bucket: a.bucket,
              Key: key,
              Range: `bytes=${byte}-${byte + span - 1}`,
            }),
          );
        } catch (e) {
          // Reading exactly at the end of an object is answered with 416.
          if (e?.$metadata?.httpStatusCode === 416) {
            eof = true;
            break;
          }
          throw e;
        }
        const buf = Buffer.from(await obj.Body.transformToByteArray());
        const total = Number(String(obj.ContentRange ?? "").split("/")[1] ?? NaN);
        const atEnd = !Number.isFinite(total) || byte + buf.length >= total;
        let end = buf.lastIndexOf(10);
        if (end < 0 && !atEnd) {
          span *= 2; // one record is longer than the range
          continue;
        }
        if (atEnd) end = buf.length - 1;
        let start = 0;
        while (start <= end && rows.length < size) {
          let nl = buf.indexOf(10, start);
          if (nl < 0 || nl > end) nl = end + 1;
          const text = buf.subarray(start, nl).toString("utf8").trim();
          if (text && line >= skipTo)
            rows.push({ ...JSON.parse(text), _file: key, _line: line, _byte: byte + start });
          if (text) line++;
          start = nl + 1;
        }
        byte += start;
        span = CHUNK;
        if (atEnd && start > end) eof = true;
      }
      const cpNext = eof ? { file: fi + 1, byte: 0, line: 0 } : { file: fi, byte, line };
      return { rows, checkpoint: cpNext, done: cpNext.file >= part.files.length };
    },
  };
}

// ---------------- main loop ----------------
const reg = await api("register", { host: os.hostname() });
const kind = String(a.kind ?? reg.source.kind);
const plan = await api("plan");
if (!plan.run) {
  console.log("No open run for this source. Plan one in Meridian → Partitioned ingest.");
  process.exit(0);
}
const run = plan.run,
  pk = run.pk_columns?.length ? run.pk_columns : reg.source.pk;
const adapter =
  kind === "postgres" || kind === "mysql"
    ? sqlAdapter(kind, pk)
    : kind === "rest"
      ? restAdapter()
      : kind === "kafka"
        ? kafkaAdapter()
        : s3Adapter();
console.log(
  `run ${run.id} → ${run.target_table}: ${plan.partitions.length} partitions, ${plan.partitions.filter((p) => p.status === "done").length} already done, batch ${run.batch_size}, threads ${THREADS}`,
);

// Postgres: export one snapshot so every thread in this process reads the same point in time.
let coord = null;
const resumed = plan.partitions.some((p) => p.checkpoint && p.status !== "done");
if (kind === "postgres") {
  coord = await adapter.open();
  await coord.q("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  const [s] = await coord.q(
    "SELECT pg_export_snapshot() AS snap, pg_current_wal_lsn()::text AS lsn, now() AS at",
  );
  run.snapshotLocal = { pg_snapshot: s.snap };
  await api("snapshot", {
    run_id: run.id,
    snapshot: {
      lsn: s.lsn,
      at: s.at,
      // Resumed partitions keep rows read under an earlier snapshot (see the header contract).
      consistency: resumed ? "mixed" : "single",
      note: resumed
        ? "resumed after a restart: rows at or before each partition checkpoint are from an earlier snapshot, later rows from this one; deletes are not captured"
        : "one exported snapshot for every thread of this worker",
    },
  });
}

let total = 0,
  stopping = false;
process.on("SIGINT", () => {
  stopping = true;
  console.log("\nstopping after current batch… (checkpoints are saved)");
});

async function thread(n) {
  const worker = `${WID}-t${n}`;
  await api("register", { host: os.hostname() }, worker);
  const c = await adapter.open();
  try {
    while (!stopping) {
      const { partition: p } = await api("claim", { run_id: run.id, lease_seconds: LEASE }, worker);
      if (!p) return;
      const meta = plan.partitions.find((x) => x.idx === p.idx)?.predicate ?? {};
      const mode = await adapter.begin(c, run.snapshotLocal);
      let cp = p.checkpoint,
        done = false,
        leaseLost = false;
      // Heartbeat while holding the lease so slow batches are not mistaken for a dead worker.
      const hb = setInterval(
        () => {
          api(
            "heartbeat",
            { run_id: run.id, idx: p.idx, lease_token: p.lease_token },
            worker,
          ).catch(() => {
            leaseLost = true;
          });
        },
        Math.max(5000, (LEASE * 1000) / 4),
      );
      try {
        while (!done && !stopping && !leaseLost) {
          const t1 = Date.now();
          const b = await adapter.batch(c, p.where, cp, run.batch_size, meta);
          const t2 = Date.now();
          if (leaseLost) throw Object.assign(new Error("lease lost"), { status: 409 });
          const res = await api(
            "write",
            {
              run_id: run.id,
              idx: p.idx,
              lease_token: p.lease_token,
              rows: b.rows,
              checkpoint: { ...b.checkpoint, snapshot: mode },
              done: b.done,
            },
            worker,
          );
          total += b.rows.length;
          cp = b.checkpoint;
          done = b.done;
          if (a.verbose)
            console.log(
              `[${worker}] p${p.idx} read ${t2 - t1}ms write ${Date.now() - t2}ms rows ${b.rows.length}`,
            );
          if (done)
            console.log(
              `[${worker}] partition ${p.idx} done (${res.rows_written} rows, attempt ${p.attempts})`,
            );
        }
      } catch (e) {
        console.error(`[${worker}] partition ${p.idx} failed: ${e.message}`);
        if (e.status !== 409)
          await api(
            "fail",
            {
              run_id: run.id,
              idx: p.idx,
              lease_token: p.lease_token,
              error: "Source extraction failed",
            },
            worker,
          ).catch(() => {});
        await sleep(1000);
      } finally {
        clearInterval(hb);
        await c.q?.("COMMIT").catch(() => {});
      }
    }
  } finally {
    await c.end().catch(() => {});
  }
}

const t0 = Date.now();
const timer = setInterval(
  () =>
    console.log(
      `… ${total.toLocaleString()} rows, ${Math.round(total / ((Date.now() - t0) / 1000))} rows/s`,
    ),
  5000,
);
await Promise.all(Array.from({ length: THREADS }, (_, i) => thread(i + 1)));
clearInterval(timer);
if (coord) {
  await coord.q("COMMIT").catch(() => {});
  await coord.end();
}
console.log(
  `${stopping ? "stopped" : "finished"}: ${total.toLocaleString()} rows pushed in ${((Date.now() - t0) / 1000).toFixed(1)}s. Publish the run in Meridian.`,
);
process.exit(0);
