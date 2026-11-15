// @vitest-environment node
// Connect tasks (migration 0094) on the live stack: a run's streams are leased as separate tasks,
// so several agents work on one run at once. Every commit, heartbeat and finish is fenced by the
// task's own lease; pre-task agents keep run-level leases and never share a run with task agents.
import { createClient } from "@supabase/supabase-js";
import { beforeAll, describe, expect, it } from "vitest";
import { sha256 } from "@/lib/ingest";

const url = process.env["SUPABASE_URL"],
  key = process.env["SUPABASE_PUBLISHABLE_KEY"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"];

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Client = any;
type Claim = {
  run_id: string;
  lease_token: string;
  attempt: number;
  task?: boolean;
  streams: { id: string; sequence: number }[];
};
let db: Client, owner: Client;
let ownerId = "",
  org = "",
  source = "",
  connection = "";
const streams: string[] = [];

async function identity() {
  const email = `tasks-${crypto.randomUUID()}@example.test`,
    password = `P-${crypto.randomUUID()}`;
  const u = await db.auth.admin.createUser({ email, password, email_confirm: true });
  if (u.error) throw new Error("Test identity creation failed");
  const c = createClient(url!, key!, { auth: { persistSession: false } });
  const s = await c.auth.signInWithPassword({ email, password });
  if (s.error) throw new Error("Test sign-in failed");
  return { id: u.data.user.id as string, c };
}

async function enqueue(operation = "sync") {
  const q = await owner.rpc("connect_enqueue", {
    _connection: connection,
    _request: crypto.randomUUID(),
    _operation: operation,
  });
  if (q.error) throw new Error(q.error.message);
  return q.data as string;
}

async function claimTask(worker = "w:0") {
  const r = await db.rpc("connect_claim_task", { _connection: connection, _worker: worker });
  if (r.error) throw new Error(r.error.message);
  return r.data as Claim | null;
}

// Lands one real Bronze batch for the stream and commits it under the given lease token.
async function commit(run: string, token: string, stream: string, sequence: number, done: boolean) {
  const batch = crypto.randomUUID();
  const objectKey = `${ownerId}/published/${batch}/landing.ndjson`;
  const text = JSON.stringify({ protocol: "meridian-connect/1", data: { id: sequence } }) + "\n";
  const digest = await sha256(text);
  const up = await db.storage.from("ingest-files").upload(objectKey, new Blob([text]));
  if (up.error) throw new Error(up.error.message);
  const landed = await db.rpc("register_ingest_landing_batch", {
    _actor: ownerId,
    _source: source,
    _key: objectKey,
    _sha: digest,
    _rows: 1,
    _mode: "append",
    _batch: batch,
  });
  if (landed.error) throw new Error(landed.error.message);
  return db.rpc("connect_commit", {
    _run: run,
    _token: token,
    _stream: stream,
    _sequence: sequence,
    _batch: landed.data,
    _manifest: `${ownerId}/published/${batch}/manifest.json`,
    _sha: digest,
    _rows: 1,
    _bytes: text.length,
    _checkpoint: { cursor: sequence, completed: done },
    _completed: done,
  });
}

const finish = (run: string, token: string, ok: boolean, code = ok ? "ok" : "connector_failed") =>
  db.rpc("connect_finish", { _run: run, _token: token, _ok: ok, _code: code });

async function runRow(run: string) {
  return (await db.from("connect_runs").select("*").eq("id", run).single()).data;
}
async function tasks(run: string) {
  return (await db.from("connect_run_streams").select("*").eq("run_id", run)).data as {
    stream_id: string;
    task_status: string;
    attempts: number;
    lease_token: string | null;
  }[];
}
// Drains a task-mode run so the next test starts with no active run on the connection.
async function cancelActive() {
  await db
    .from("connect_runs")
    .update({ status: "cancelled", lease_token: null, lease_until: null })
    .eq("connection_id", connection)
    .in("status", ["queued", "running", "retry_wait"]);
}

describe.skipIf(!url || !key || !service)(
  "Connect tasks on the live stack",
  { timeout: 120000 },
  () => {
    beforeAll(async () => {
      db = createClient(url!, service!, { auth: { persistSession: false } });
      ({ id: ownerId, c: owner } = await identity());
      const o = await db
        .from("organizations")
        .insert({ name: `Tasks ${crypto.randomUUID()}`, created_by: ownerId })
        .select("id")
        .single();
      if (o.error) throw new Error(o.error.message);
      org = o.data.id;
      await db
        .from("org_members")
        .upsert(
          { org_id: org, user_id: ownerId, workspace_role: "engineer" },
          { onConflict: "org_id,user_id" },
        );
      await db.from("profiles").update({ active_organization_id: org }).eq("id", ownerId);
      const s = await db
        .from("ingest_sources")
        .insert({
          owner_id: ownerId,
          organization_id: org,
          name: "Tasks source",
          kind: "postgres",
          target_table: "raw_tasks",
          config: {},
        })
        .select("id")
        .single();
      if (s.error) throw new Error(s.error.message);
      source = s.data.id;
      const c = await db
        .from("connect_connections")
        .insert({
          source_id: source,
          owner_id: ownerId,
          organization_id: org,
          name: "Tasks DB",
          credential_ref: "test-only",
        })
        .select("id")
        .single();
      if (c.error) throw new Error(c.error.message);
      connection = c.data.id;
      for (const name of ["a", "b", "c"]) {
        const st = await db
          .from("connect_streams")
          .insert({
            connection_id: connection,
            namespace: "public",
            name: `t_${name}`,
            primary_key: "id",
            cursor_field: "id",
            schema_json: {},
          })
          .select("id")
          .single();
        if (st.error) throw new Error(st.error.message);
        streams.push(st.data.id);
      }
    });

    it("leases each stream of a run to a different task and fences commits per task", async () => {
      const run = await enqueue();
      const a = await claimTask("w:1");
      const b = await claimTask("w:2");
      const c = await claimTask("w:3");
      expect(await claimTask("w:4")).toBeNull();
      for (const x of [a, b, c]) {
        expect(x?.run_id).toBe(run);
        expect(x?.task).toBe(true);
        expect(x?.streams).toHaveLength(1);
      }
      const leased = [a, b, c].map((x) => x!.streams[0]!.id);
      expect(new Set(leased).size).toBe(3);
      expect(new Set([a, b, c].map((x) => x!.lease_token)).size).toBe(3);
      const r = await runRow(run);
      expect(r.lease_mode).toBe("task");
      expect(r.status).toBe("running");

      // A pre-task agent never takes a task-mode run.
      expect((await db.rpc("connect_claim", { _connection: connection })).data).toBeNull();

      // Another task's token, or a made-up one, cannot commit to this stream.
      expect((await commit(run, b!.lease_token, leased[0]!, 0, false)).error).toBeTruthy();
      expect((await commit(run, crypto.randomUUID(), leased[0]!, 0, false)).error).toBeTruthy();
      expect((await commit(run, a!.lease_token, leased[0]!, 0, false)).error).toBeNull();

      // Heartbeats and lease checks follow the task lease.
      expect((await db.rpc("connect_heartbeat", { _run: run, _token: a!.lease_token })).data).toBe(
        true,
      );
      const check = (token: string, stream?: string) =>
        db.rpc("connect_lease_check", {
          _run: run,
          _token: token,
          ...(stream ? { _stream: stream } : {}),
        });
      expect((await check(a!.lease_token, leased[0])).data).toBe(true);
      expect((await check(a!.lease_token, leased[1])).data).toBe(false);

      // A task cannot finish successfully before its stream is complete.
      expect((await finish(run, a!.lease_token, true)).error?.message).toContain(
        "Incomplete streams",
      );

      // Each task completes independently; the run succeeds only when the last one does.
      for (const [i, x] of [a, b, c].entries()) {
        const seq = i === 0 ? 1 : 0;
        expect((await commit(run, x!.lease_token, leased[i]!, seq, true)).error).toBeNull();
        expect((await finish(run, x!.lease_token, true)).error).toBeNull();
        expect((await runRow(run)).status).toBe(i < 2 ? "running" : "succeeded");
      }
      expect((await tasks(run)).every((t) => t.task_status === "done" && !t.lease_token)).toBe(
        true,
      );
      const attempts = await db.from("connect_attempts").select("status").eq("run_id", run);
      expect(attempts.data).toEqual([{ status: "succeeded" }]);
      // A finished task's token is dead.
      expect((await db.rpc("connect_heartbeat", { _run: run, _token: a!.lease_token })).data).toBe(
        false,
      );
    });

    it("retries only the failed task, with backoff, from its last checkpoint", async () => {
      const run = await enqueue();
      const a = (await claimTask())!;
      const stream = a.streams[0]!.id;
      expect((await finish(run, a.lease_token, false, "source_unavailable")).error).toBeNull();
      const t = (await tasks(run)).find((x) => x.stream_id === stream)!;
      expect(t.task_status).toBe("retry_wait");
      expect((await runRow(run)).status).toBe("running");
      // The other two tasks are still claimable; the failed one waits for its backoff.
      const others = [(await claimTask())!, (await claimTask())!];
      expect(others.map((x) => x.streams[0]!.id)).not.toContain(stream);
      expect(await claimTask()).toBeNull();
      await db
        .from("connect_run_streams")
        .update({ next_attempt_at: new Date(Date.now() - 1000).toISOString() })
        .eq("run_id", run)
        .eq("stream_id", stream);
      const again = (await claimTask())!;
      expect(again.streams[0]!.id).toBe(stream);
      expect(again.attempt).toBe(2);
      await cancelActive();
    });

    it("re-leases a task whose lease lapsed and rejects the old holder", async () => {
      const run = await enqueue();
      const first = (await claimTask("slow"))!;
      const stream = first.streams[0]!.id;
      await db
        .from("connect_run_streams")
        .update({ lease_until: new Date(Date.now() - 1000).toISOString() })
        .eq("run_id", run)
        .eq("stream_id", stream);
      const claims = [
        (await claimTask("fast"))!,
        (await claimTask("fast"))!,
        (await claimTask("fast"))!,
      ];
      const taken = claims.find((x) => x.streams[0]!.id === stream)!;
      expect(taken.lease_token).not.toBe(first.lease_token);
      expect((await commit(run, first.lease_token, stream, 0, false)).error).toBeTruthy();
      expect((await finish(run, first.lease_token, false)).error).toBeTruthy();
      expect((await commit(run, taken.lease_token, stream, 0, false)).error).toBeNull();
      await cancelActive();
    });

    it("fails the run and fences every other task on a terminal error", async () => {
      const run = await enqueue();
      const a = (await claimTask())!;
      const b = (await claimTask())!;
      expect((await finish(run, a.lease_token, false, "auth_failed")).error).toBeNull();
      const r = await runRow(run);
      expect(r.status).toBe("failed");
      expect(r.error_code).toBe("auth_failed");
      expect((await commit(run, b.lease_token, b.streams[0]!.id, 0, false)).error).toBeTruthy();
      expect((await tasks(run)).filter((t) => t.lease_token)).toEqual([]);
      expect(await claimTask()).toBeNull();
    });

    it("keeps whole-run leases for runs claimed by a pre-task agent", async () => {
      const run = await enqueue();
      const old = (await db.rpc("connect_claim", { _connection: connection })).data as Claim;
      expect(old.run_id).toBe(run);
      expect(old.streams).toHaveLength(3);
      expect((await runRow(run)).lease_mode).toBe("run");
      // A task agent defers to the run lease instead of splitting the run.
      expect(await claimTask()).toBeNull();
      expect((await commit(run, old.lease_token, streams[0]!, 0, false)).error).toBeNull();
      await cancelActive();
    });

    it("serves check and discover runs to task agents as whole runs", async () => {
      const run = await enqueue("check");
      const c = (await claimTask())!;
      expect(c.run_id).toBe(run);
      expect(c.task).toBeUndefined();
      expect((await runRow(run)).lease_mode).toBe("run");
      expect((await finish(run, c.lease_token, true)).error).toBeNull();
      expect((await runRow(run)).status).toBe("succeeded");
    });

    it("keeps task RPCs service-only", async () => {
      for (const [fn, args] of [
        ["connect_claim_task", { _connection: connection }],
        ["connect_lease_check", { _run: crypto.randomUUID(), _token: crypto.randomUUID() }],
        [
          "connect_finish_task",
          { _run: crypto.randomUUID(), _token: crypto.randomUUID(), _ok: true, _code: "ok" },
        ],
        ["connect_fail_task_run", { _run: crypto.randomUUID(), _code: "timeout" }],
      ] as const) {
        expect((await owner.rpc(fn, args)).error, fn).toBeTruthy();
      }
    });
  },
);
