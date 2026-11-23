// @vitest-environment node
// Two real sessions: a claim holding the connection lock must not deadlock with a finishing check.
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Client } from "pg";

// The isolated Community CI database, or any disposable local one named by CONNECT_DB_PORT.
const port =
  process.env["CONNECT_DB_PORT"] ??
  (process.env["SUPABASE_URL"] === "http://127.0.0.1:58721" ? "58722" : undefined);
const client = () =>
  new Client({
    host: "127.0.0.1",
    port: Number(port),
    user: "postgres",
    password: process.env["PGPASSWORD"] ?? "postgres",
    database: "postgres",
  });

describe.skipIf(!port)("connect_finish lock order", { timeout: 30000 }, () => {
  const claim = client();
  const finish = client();
  const uid = crypto.randomUUID();
  const token = crypto.randomUUID();
  let cid: string, rid: string, sourceId: string;

  beforeAll(async () => {
    await claim.connect();
    await finish.connect();
    // Committed, because the second session must see the run; removed in afterAll.
    await claim.query("insert into auth.users(id,email,email_confirmed_at) values($1,$2,now())", [
      uid,
      `${uid}@example.test`,
    ]);
    sourceId = (
      await claim.query(
        "insert into public.ingest_sources(owner_id,name,kind,target_table,config) values($1,'lock','postgres','lock','{}') returning id",
        [uid],
      )
    ).rows[0].id;
    cid = (
      await claim.query(
        "insert into public.connect_connections(source_id,owner_id,name,credential_ref,source_version) values($1,$2,'lock','local','postgres-v1') returning id",
        [sourceId, uid],
      )
    ).rows[0].id;
    rid = (
      await claim.query(
        "insert into public.connect_runs(connection_id,request_key,operation,status,lease_mode,attempts,lease_token,lease_until) values($1,gen_random_uuid(),'check','running','run',1,$2,now()+interval '5 minutes') returning id",
        [cid, token],
      )
    ).rows[0].id;
  });

  afterAll(async () => {
    await claim.query("rollback").catch(() => undefined);
    await finish.query("rollback").catch(() => undefined);
    await claim.query("delete from public.connect_runs where id=$1", [rid]);
    await claim.query("delete from public.connect_connections where id=$1", [cid]);
    await claim.query("delete from public.ingest_sources where id=$1", [sourceId]);
    await claim.query("delete from auth.users where id=$1", [uid]);
    await claim.end();
    await finish.end();
  });

  it("waits for the connection before locking the run", async () => {
    // A claim's order (0114): connection, then run.
    await claim.query("begin");
    await claim.query("select 1 from public.connect_connections where id=$1 for no key update", [
      cid,
    ]);
    await finish.query("begin");
    await finish.query("select set_config('request.jwt.claim.role','service_role',true)");
    const pid = (await finish.query("select pg_backend_pid() p")).rows[0].p;
    const done = finish.query("select public.connect_finish($1,$2,true,'ok') ok", [rid, token]);
    done.catch(() => undefined); // A deadlock is reported by the awaits below, not as unhandled.
    const deadline = Date.now() + 10000;
    for (;;) {
      const waiting = await claim.query(
        "select 1 from pg_stat_activity where pid=$1 and wait_event_type='Lock'",
        [pid],
      );
      if (waiting.rowCount) break;
      if (Date.now() > deadline) throw new Error("connect_finish never waited for a lock");
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    // With the old order connect_finish already held this row, and Postgres aborted one side.
    await claim.query("select 1 from public.connect_runs where id=$1 for update", [rid]);
    await claim.query("commit");
    expect((await done).rows[0].ok).toBe(true);
    const health = await finish.query("select health from public.connect_connections where id=$1", [
      cid,
    ]);
    expect(health.rows[0].health).toBe("reachable");
    await finish.query("rollback");
  });
});
