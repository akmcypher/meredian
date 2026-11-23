// @vitest-environment node
import { beforeEach, afterEach, describe, expect, it } from "vitest";
import { Client } from "pg";
import { readFile } from "node:fs/promises";

const migration = new URL(
  "../../supabase/migrations/20261125000000_0112_connect_avro_reader_state.sql",
  import.meta.url,
);
const committed = {
  version: 1,
  positions: { "0": "36", "1": "18" },
  cluster_id: "cluster",
  topic_id: "topic",
};
const schema = {
  type: "object",
  "x-kafka-topic": "events",
  properties: { id: { type: "integer" } },
};
const reader = { type: "record", name: "Event", fields: [{ name: "id", type: "long" }] };

describe.skipIf(process.env["SUPABASE_URL"] !== "http://127.0.0.1:58721")(
  "Avro reader updates preserve Kafka checkpoints",
  () => {
    let db: Client, cid: string, sid: string;
    const q = async (sql: string, args: unknown[] = []) => (await db.query(sql, args)).rows;
    const row = async () =>
      (await q("select state,state_generation from public.connect_streams where id=$1", [sid]))[0];
    const updateSchema = (value: unknown, extra = "") =>
      q(`update public.connect_streams set schema_json=$2::jsonb${extra} where id=$1`, [
        sid,
        JSON.stringify(value),
      ]);
    const expectPreserved = async () =>
      expect(await row()).toEqual({ state: committed, state_generation: 7 });
    const expectReset = async () => expect(await row()).toEqual({ state: {}, state_generation: 8 });
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
      // Candidate DDL stays in this test's rollback; persistent application is controlled separately.
      if (process.env["CONNECT_AVRO_READER_CANDIDATE"] === "1")
        await q(await readFile(migration, "utf8"));
      await q("select set_config('request.jwt.claim.role','service_role',true)");
      const uid = crypto.randomUUID();
      await q("insert into auth.users(id,email,email_confirmed_at) values($1,$2,now())", [
        uid,
        `${uid}@example.test`,
      ]);
      const source = (
        await q(
          "insert into public.ingest_sources(owner_id,name,kind,target_table,config) values($1,'reader-state','kafka','reader_state','{}') returning id",
          [uid],
        )
      )[0].id;
      cid = (
        await q(
          "insert into public.connect_connections(source_id,owner_id,name,credential_ref,source_version) values($1,$2,'reader-state','local','kafka-v1') returning id",
          [source, uid],
        )
      )[0].id;
      sid = (
        await q(
          "insert into public.connect_streams(connection_id,namespace,name,primary_key,key_columns,cursor_field,schema_json,state,state_generation) values($1,'kafka','events','id',ARRAY['id'],'_offset',$2::jsonb,$3::jsonb,7) returning id",
          [cid, JSON.stringify(schema), JSON.stringify(committed)],
        )
      )[0].id;
    });
    afterEach(async () => {
      await db.query("rollback");
      await db.end();
    });

    it.each(["kafka-v1", "spark-kafka-v1"])(
      "preserves committed positions when %s pins, changes and unpins either reader field",
      async (version) => {
        await q("update public.connect_connections set source_version=$2 where id=$1", [
          cid,
          version,
        ]);
        for (const next of [
          { ...schema, "x-avro-reader-schema": reader },
          { ...schema, "x-avro-reader-schema": reader, "x-avro-reader-version": 1 },
          {
            ...schema,
            "x-avro-reader-schema": {
              ...reader,
              fields: [...reader.fields, { name: "label", type: "string", default: "unknown" }],
            },
            "x-avro-reader-version": 2,
          },
          { ...schema, "x-avro-reader-version": 2 },
          schema,
        ]) {
          await updateSchema(next, ",state='{}',state_generation=999");
          await expectPreserved();
        }
      },
    );
    it.each(["queued", "running", "retry_wait"])(
      "refuses reader changes during %s runs",
      async (status) => {
        await q(
          "insert into public.connect_runs(connection_id,request_key,operation,status) values($1,gen_random_uuid(),'sync',$2)",
          [cid, status],
        );
        await q("savepoint active");
        await expect(updateSchema({ ...schema, "x-avro-reader-version": 1 })).rejects.toThrow(
          /Active replication configuration is immutable/,
        );
        await q("rollback to savepoint active");
        await expectPreserved();
      },
    );
    it.each([
      ["topic", "schema_json=jsonb_set(schema_json,'{x-kafka-topic}','\"different\"')"],
      ["schema", "schema_json=jsonb_set(schema_json,'{properties,id,type}','\"string\"')"],
      ["namespace", "namespace='other'"],
      ["name", "name='other'"],
      ["key", "primary_key='other',key_columns=ARRAY['other']"],
      ["cursor", "cursor_field='other'"],
      ["mode", "sync_mode='full_refresh'"],
    ])(
      "resets checkpoint when actual %s configuration changes alongside a reader pin",
      async (_name, changes) => {
        const assignments = changes.startsWith("schema_json=")
          ? `${changes}||'{"x-avro-reader-version":1}'`
          : `${changes},schema_json=schema_json||'{"x-avro-reader-version":1}'`;
        await q(`update public.connect_streams set ${assignments} where id=$1`, [sid]);
        await expectReset();
      },
    );
    it("keeps resetting non-object schemas instead of treating array entries as reader fields", async () => {
      await updateSchema(["x-avro-reader-version", "other"]);
      await q("update public.connect_streams set state=$2::jsonb,state_generation=7 where id=$1", [
        sid,
        JSON.stringify(committed),
      ]);
      await updateSchema(["other"]);
      await expectReset();
    });
    it("resets rather than carrying a checkpoint to a different Kafka connection", async () => {
      const other = (
        await q(
          `with s as (
      insert into public.ingest_sources(owner_id,name,kind,target_table,config)
      select owner_id,'other-reader','kafka','other_reader','{}' from public.connect_connections where id=$1
      returning id,owner_id
    ) insert into public.connect_connections(source_id,owner_id,name,credential_ref,source_version)
      select id,owner_id,'other-reader','local','kafka-v1' from s returning id`,
          [cid],
        )
      )[0].id;
      await q(
        "update public.connect_streams set connection_id=$2,schema_json=schema_json||'{\"x-avro-reader-version\":1}' where id=$1",
        [sid, other],
      );
      await expectReset();
    });
    it.each(["postgres-v1", "spark-postgres-v1", "mysql-v1", "files-v1"])(
      "keeps resetting schema updates for %s",
      async (version) => {
        await q("update public.connect_connections set source_version=$2 where id=$1", [
          cid,
          version,
        ]);
        await updateSchema({ ...schema, "x-avro-reader-version": 1 });
        await expectReset();
      },
    );
  },
);
