// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { readFile } from "node:fs/promises";
import { Client } from "pg";

describe.skipIf(process.env["SUPABASE_URL"] !== "http://127.0.0.1:58721")(
  "SQL Server verified source authority",
  () => {
    let db: Client, uid: string;
    const q = (sql: string, args: unknown[] = []) => db.query(sql, args);
    const register = (config = {}) =>
      q(
        "select public.create_ingest_source_verified($1,'SQL orders','sqlserver','sql_orders',$2::jsonb,'{}',repeat('a',64)) id",
        [uid, JSON.stringify(config)],
      );
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
      if (process.env["CONNECT_SQLSERVER_CANDIDATE"] === "1")
        await q(
          await readFile(
            new URL(
              "../../supabase/migrations/20261126000000_0113_connect_sqlserver.sql",
              import.meta.url,
            ),
            "utf8",
          ),
        );
      await q("select set_config('request.jwt.claim.role','service_role',true)");
      uid = crypto.randomUUID();
      await q("insert into auth.users(id,email,email_confirmed_at) values($1,$2,now())", [
        uid,
        `${uid}@example.test`,
      ]);
    });
    afterEach(async () => {
      await q("rollback");
      await db.end();
    });
    it("creates SQL Server in the verified actor's selected workspace", async () => {
      const org = (
        await q("insert into public.organizations(name,created_by) values($1,$2) returning id", [
          `sql-${uid}`,
          uid,
        ])
      ).rows[0].id;
      await q(
        "insert into public.org_members(org_id,user_id,workspace_role) values($1,$2,'owner')",
        [org, uid],
      );
      await q("update public.profiles set active_organization_id=$1 where id=$2", [org, uid]);
      const id = (await register({ host: "sql.example.test", port: 1433 })).rows[0].id;
      expect(
        (
          await q("select owner_id,organization_id,kind from public.ingest_sources where id=$1", [
            id,
          ])
        ).rows[0],
      ).toEqual({ owner_id: uid, organization_id: org, kind: "sqlserver" });
    });
    it("rejects a stale active workspace without membership", async () => {
      const org = (
        await q("insert into public.organizations(name,created_by) values($1,$2) returning id", [
          `sql-${uid}`,
          uid,
        ])
      ).rows[0].id;
      await q("update public.profiles set active_organization_id=$1 where id=$2", [org, uid]);
      await expect(register()).rejects.toThrow(/Workspace access required/);
    });
    it("keeps source credentials out of public config", async () => {
      await expect(register({ password: "forbidden" })).rejects.toThrow(/Credentials/);
    });
    it("keeps direct verified registration service-only", async () => {
      const rows = (
        await q(
          "select has_function_privilege('anon','public.create_ingest_source_verified(uuid,text,text,text,jsonb,jsonb,text)','execute') anon, has_function_privilege('authenticated','public.create_ingest_source_verified(uuid,text,text,text,jsonb,jsonb,text)','execute') authenticated, has_function_privilege('service_role','public.create_ingest_source_verified(uuid,text,text,text,jsonb,jsonb,text)','execute') service",
        )
      ).rows;
      expect(rows[0]).toEqual({ anon: false, authenticated: false, service: true });
    });
    it("hides the created source from another authenticated personal workspace", async () => {
      const id = (await register()).rows[0].id;
      const other = crypto.randomUUID();
      await q("insert into auth.users(id,email,email_confirmed_at) values($1,$2,now())", [
        other,
        `${other}@example.test`,
      ]);
      await q(
        "select set_config('request.jwt.claim.sub',$1,true), set_config('request.jwt.claim.role','authenticated',true)",
        [other],
      );
      await q("set local role authenticated");
      expect((await q("select id from public.ingest_sources where id=$1", [id])).rows).toEqual([]);
    });
  },
);
