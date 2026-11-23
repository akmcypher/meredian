// @vitest-environment node
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Client } from "pg";
import { agentRequest } from "@/lib/connect-protocol";

const stream = "11111111-1111-4111-8111-111111111111";
it("accepts shared membership metadata, assignment claims and fenced handoffs", () => {
  for (const body of [
    { action: "group_context" },
    {
      action: "claim_group_task",
      assignments: { [stream]: [0, 2] },
      worker: "a:0",
      agent: "a",
      slots: 2,
    },
    { action: "abandon_task", runId: stream, token: stream },
  ])
    expect(agentRequest.safeParse(body).success).toBe(true);
});
it("normalizes assignment UUID case and rejects duplicate canonical stream ids", () => {
  const id = "abcdefab-abcd-4abc-8abc-abcdefabcdef";
  const result = agentRequest.parse({
    action: "claim_group_task",
    assignments: { [id.toUpperCase()]: [0] },
  });
  expect(result.action === "claim_group_task" && result.assignments).toEqual({ [id]: [0] });
  expect(
    agentRequest.safeParse({
      action: "claim_group_task",
      assignments: { [id]: [0], [id.toUpperCase()]: [1] },
    }).success,
  ).toBe(false);
});
it("rejects malformed shared selectors", () => {
  for (const assignments of [
    { nope: [0] },
    { [stream]: [-1] },
    { [stream]: [0.5] },
    { [stream]: [1000000] },
    { [stream]: [0, 0] },
  ])
    expect(agentRequest.safeParse({ action: "claim_group_task", assignments }).success).toBe(false);
});

// Every test is rolled back; use only the isolated Community CI database.
describe.skipIf(process.env["SUPABASE_URL"] !== "http://127.0.0.1:58721")(
  "shared Kafka control plane",
  () => {
    let db: Client, cid: string, sid: string, rid: string, uid: string, pool: string;
    const q = async (sql: string, args: unknown[] = []) => (await db.query(sql, args)).rows;
    const claim = async (assignments: Record<string, number[]>, worker = "a:0") =>
      (
        await q("select public.connect_claim_selected_task($1,$2,$3::jsonb) c", [
          cid,
          worker,
          JSON.stringify(assignments),
        ])
      )[0].c;
    const abandon = (token: string) => q("select public.connect_abandon_task($1,$2)", [rid, token]);
    beforeEach(async () => {
      db = new Client({
        host: "127.0.0.1",
        port: 58722,
        user: "postgres",
        password: "postgres",
        database: "postgres",
      });
      await db.connect();
      await q("begin");
      await q("select set_config('request.jwt.claim.role','service_role',true)");
      uid = crypto.randomUUID();
      await q("insert into auth.users(id,email,email_confirmed_at) values($1,$2,now())", [
        uid,
        `${uid}@example.test`,
      ]);
      const source = (
        await q(
          "insert into public.ingest_sources(owner_id,name,kind,target_table,config) values($1,'shared','kafka','shared','{}') returning id",
          [uid],
        )
      )[0].id;
      cid = (
        await q(
          "insert into public.connect_connections(source_id,owner_id,name,credential_ref,source_version) values($1,$2,'shared','local','kafka-v1') returning id",
          [source, uid],
        )
      )[0].id;
      sid = (
        await q(
          `insert into public.connect_streams(connection_id,namespace,name,primary_key,cursor_field,key_columns,schema_json) values($1,'kafka','events','_partition,_offset','_offset',ARRAY['_partition','_offset'],'{"x-kafka-topic":"events"}') returning id`,
          [cid],
        )
      )[0].id;
      rid = (
        await q(
          "insert into public.connect_runs(connection_id,request_key,operation,max_attempts) values($1,gen_random_uuid(),'sync',3) returning id",
          [cid],
        )
      )[0].id;
      await q(
        "insert into public.connect_run_streams(run_id,stream_id,schema_json,initial_state,checkpoint,generation) select $1,id,schema_json,'{}','{}',state_generation from public.connect_streams where id=$2",
        [rid, sid],
      );
      pool = (
        await q(
          "insert into public.connect_pools(owner_id,name,token_hash) values($1,'p',repeat('a',64)) returning id",
          [uid],
        )
      )[0].id;
    });
    afterEach(async () => {
      await db.query("rollback");
      await db.end();
    });
    it("limits planning and partition claims to broker assignments while generic claims still work", async () => {
      expect(await claim({})).toBeNull();
      expect(await claim({ [crypto.randomUUID()]: [0] })).toBeNull();
      expect(await claim({ [sid]: [] })).toBeNull();
      const plan = await claim({ [sid]: [0] });
      expect(plan.streams[0].shard).toBe("");
      await q("select public.connect_split_task($1,$2,ARRAY['0','1','2'])", [
        rid,
        plan.lease_token,
      ]);
      const a = await claim({ [sid]: [1] });
      expect(a.streams[0].shard).toBe("1");
      const b = await claim({ [sid]: [0, 2] }, "b:0");
      expect(b.streams[0].shard).toBe("0");
      expect(await claim({ [sid]: [1] })).toBeNull();
      expect(
        (await q("select public.connect_claim_task($1,'legacy') c", [cid]))[0].c.streams[0].shard,
      ).toBe("2");
    });
    it("matches uppercase assignment ids and rejects duplicate canonical selectors in SQL", async () => {
      const upper = sid.toUpperCase();
      expect((await claim({ [upper]: [0] })).streams[0].id).toBe(sid);
      // A digits-only UUID has no distinct uppercase spelling.
      if (upper !== sid) {
        await q("savepoint duplicate");
        await expect(claim({ [sid]: [0], [upper]: [1] })).rejects.toThrow(/Invalid/);
        await q("rollback to savepoint duplicate");
      }
    });
    it("keeps physical attempts monotonic through handoffs and still exhausts real failures", async () => {
      let last = "";
      for (let i = 0; i < 8; i++) {
        const c = await claim({ [sid]: [0] });
        expect(c.attempt).toBe(i + 1);
        last = c.lease_token;
        await abandon(last);
      }
      const current = await claim({ [sid]: [0] });
      await q("savepoint stale");
      await expect(abandon(last)).rejects.toThrow(/Stale/);
      await q("rollback to savepoint stale");
      expect(
        (
          await q("select handoffs,attempts from public.connect_run_streams where run_id=$1", [rid])
        )[0],
      ).toEqual({ handoffs: 8, attempts: 9 });
      for (let i = 0; i < 3; i++) {
        const c = i === 0 ? current : await claim({ [sid]: [0] });
        await q("select public.connect_finish_task($1,$2,false,'connector_failed')", [
          rid,
          c.lease_token,
        ]);
        if (i < 2) {
          expect(
            (await q("select status from public.connect_runs where id=$1", [rid]))[0].status,
          ).toBe("running");
          await q("update public.connect_run_streams set next_attempt_at=now() where run_id=$1", [
            rid,
          ]);
        }
      }
      expect((await q("select status from public.connect_runs where id=$1", [rid]))[0].status).toBe(
        "failed",
      );
    });
    it("finishes an already landed completed task and refuses expired handoffs", async () => {
      const a = await claim({ [sid]: [0] });
      await q(
        "update public.connect_run_streams set lease_until=now()-interval '1 second' where run_id=$1",
        [rid],
      );
      await q("savepoint expired");
      await expect(abandon(a.lease_token)).rejects.toThrow(/Stale/);
      await q("rollback to savepoint expired");
      const b = await claim({ [sid]: [0] });
      await q(
        "update public.connect_run_streams set completed=true,checkpoint='{\"completed\":true}' where run_id=$1",
        [rid],
      );
      await abandon(b.lease_token);
      expect((await q("select status from public.connect_runs where id=$1", [rid]))[0].status).toBe(
        "succeeded",
      );
      expect(
        (await q("select handoffs from public.connect_run_streams where run_id=$1", [rid]))[0]
          .handoffs,
      ).toBe(0);
    });
    it("returns snapshot topics for disabled streams and no credential values", async () => {
      await q("update public.connect_streams set enabled=false where id=$1", [sid]);
      const context = (await q("select public.connect_group_context($1) c", [cid]))[0].c;
      expect(context).toMatchObject({
        id: cid,
        source_version: "kafka-v1",
        credential_ref: "local",
        pending: true,
        streams: [{ id: sid, topic: "events" }],
      });
    });
    it("checks selected pool scope and connection quotas and records agent identity", async () => {
      await q("savepoint outside");
      await expect(
        q("select public.connect_claim_pool_group_task($1,$2,'{}')", [pool, cid]),
      ).rejects.toThrow(/scope|Scope/);
      await q("rollback to savepoint outside");
      await q("update public.connect_connections set pool_id=$1,max_parallel_tasks=1 where id=$2", [
        pool,
        cid,
      ]);
      const a = (
        await q("select public.connect_claim_pool_group_task($1,$2,$3::jsonb,'a:0','a',2,'v1') c", [
          pool,
          cid,
          JSON.stringify({ [sid]: [0] }),
        ])
      )[0].c;
      expect(a.connection_id).toBe(cid);
      expect(await claim({ [sid]: [0] })).toBeNull();
      expect(
        (
          await q("select agent,slots,version from public.connect_workers where pool_id=$1", [pool])
        )[0],
      ).toEqual({ agent: "a", slots: 2, version: "v1" });
      await abandon(a.lease_token);
      await q(
        "update public.ingest_sources set enabled=false where id=(select source_id from public.connect_connections where id=$1)",
        [cid],
      );
      expect((await q("select public.connect_claim_task($1) c", [cid]))[0].c).toBeNull();
      await q("savepoint source");
      await expect(q("select public.connect_group_context($1)", [cid])).rejects.toThrow();
      await q("rollback to savepoint source");
    });
    it("keeps completed snapshot topics until the entire active run finishes", async () => {
      await q(
        "update public.connect_run_streams set schema_json='{\"x-kafka-topic\":\"old-events\"}',completed=true,task_status='done' where run_id=$1",
        [rid],
      );
      expect((await q("select public.connect_group_context($1) c", [cid]))[0].c.streams).toEqual([
        { id: sid, topic: "old-events" },
      ]);
    });
    it("rejects invalid selectors in SQL and does not expose service RPCs to authenticated users", async () => {
      for (const selector of [
        [],
        { nope: [0] },
        { [sid]: [-1] },
        { [sid]: [1, 1] },
        { [sid]: [1.2] },
        { [sid]: null },
      ]) {
        await q("savepoint invalid");
        await expect(claim(selector as Record<string, number[]>)).rejects.toThrow(/Invalid/);
        await q("rollback to savepoint invalid");
      }
      await q("set local role authenticated");
      await expect(q("select public.connect_group_context($1)", [cid])).rejects.toThrow(
        /permission denied/,
      );
    });
    it("refuses disabled pools and inactive connection owners", async () => {
      await q("update public.connect_connections set pool_id=$1 where id=$2", [pool, cid]);
      await q("update public.connect_pools set enabled=false where id=$1", [pool]);
      await q("savepoint disabled");
      await expect(
        q("select public.connect_claim_pool_group_task($1,$2,'{}')", [pool, cid]),
      ).rejects.toThrow(/scope/);
      await q("rollback to savepoint disabled");
      await q("update auth.users set banned_until=now()+interval '1 hour' where id=$1", [uid]);
      await q("savepoint banned");
      await expect(claim({ [sid]: [0] })).rejects.toThrow(/scope/);
      await q("rollback to savepoint banned");
    });
    it("allows empty-assignment check bootstrap but rejects shared non Kafka sources", async () => {
      await q("delete from public.connect_run_streams where run_id=$1", [rid]);
      await q("update public.connect_runs set operation='check' where id=$1", [rid]);
      expect((await claim({})).operation).toBe("check");
      await q("update public.connect_connections set source_version='postgres-v1' where id=$1", [
        cid,
      ]);
      await q("savepoint kind");
      await expect(claim({})).rejects.toThrow(/Kafka/);
      await q("rollback to savepoint kind");
    });
  },
);
