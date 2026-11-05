// @vitest-environment node
import { createClient } from "@supabase/supabase-js";
import { describe, it, expect } from "vitest";
import { sha256 } from "@/lib/ingest";
const url = process.env["SUPABASE_URL"],
  key = process.env["SUPABASE_PUBLISHABLE_KEY"],
  service = process.env["SUPABASE_SERVICE_ROLE_KEY"];
describe.skipIf(!url || !key || !service)(
  "durable replay deferral fencing",
  { timeout: 60000 },
  () => {
    it("survives lost claim responses, fences old attempts and prevents false success", async () => {
      const db = createClient(url!, service!, { auth: { persistSession: false } });
      const email = `deferral-${crypto.randomUUID()}@example.test`,
        password = `P-${crypto.randomUUID()}`;
      const user = await db.auth.admin.createUser({ email, password, email_confirm: true });
      if (user.error) throw user.error;
      const owner = createClient(url!, key!, { auth: { persistSession: false } });
      await owner.auth.signInWithPassword({ email, password });
      try {
        const source = await db
          .from("ingest_sources")
          .insert({
            owner_id: user.data.user.id,
            name: "Deferral evidence",
            kind: "kafka",
            target_table: "replay_evidence",
            config: {},
            token_hash: await sha256(crypto.randomUUID()),
          })
          .select("id")
          .single();
        if (source.error) throw source.error;
        const conn = await db
          .from("connect_connections")
          .insert({
            source_id: source.data.id,
            owner_id: user.data.user.id,
            name: "Deferral evidence",
            credential_ref: "kafka",
            source_version: "kafka-v1",
          })
          .select("id")
          .single();
        if (conn.error) throw conn.error;
        const stream = await db
          .from("connect_streams")
          .insert({
            connection_id: conn.data.id,
            name: "topic",
            namespace: "kafka",
            primary_key: "id",
            cursor_field: "id",
            schema_json: { type: "object" },
          })
          .select("id")
          .single();
        if (stream.error) throw stream.error;
        const original = await db
          .from("connect_runs")
          .insert({
            connection_id: conn.data.id,
            request_key: crypto.randomUUID(),
            operation: "sync",
            status: "failed",
          })
          .select("id")
          .single();
        if (original.error) throw original.error;
        const letter = await db
          .from("connect_dead_letters")
          .insert({
            connection_id: conn.data.id,
            stream_id: stream.data.id,
            run_id: original.data.id,
            sequence: 0,
            position: "0:1",
            error_code: "invalid_record",
            object_key: `${user.data.user.id}/dead-letters/metadata-only.ndjson`,
            line: 0,
          })
          .select("id")
          .single();
        if (letter.error) throw letter.error;
        const queued = await owner.rpc("connect_enqueue_replay", {
          _connection: conn.data.id,
          _request: crypto.randomUUID(),
          _dead_letters: [letter.data.id],
        });
        if (queued.error) throw queued.error;
        const first = await db.rpc("connect_claim", { _connection: conn.data.id });
        if (first.error) throw first.error;
        const args = {
          _run: queued.data,
          _token: first.data.lease_token,
          _stream: stream.data.id,
          _ids: [letter.data.id],
        };
        const denied = await owner.rpc("connect_defer_replay", args);
        expect(denied.error).toBeTruthy();
        const released = await db.rpc("connect_defer_replay", args);
        expect(released.error).toBeNull();
        expect(released.data).toBe(1);
        const again = await db.rpc("connect_defer_replay", args);
        expect(again.error).toBeNull();
        expect(again.data).toBe(1);
        // Simulate a lost claim response and a replacement attempt, not a normal actor report.
        await db
          .from("connect_runs")
          .update({ lease_until: new Date(Date.now() - 1000).toISOString() })
          .eq("id", queued.data);
        const second = await db.rpc("connect_claim", { _connection: conn.data.id });
        expect(second.error).toBeNull();
        expect(second.data.streams[0].deferred).toBe(1);
        expect(second.data.streams[0].dead_letters).toEqual([]);
        expect((await db.rpc("connect_defer_replay", args)).error).toBeTruthy();
        const finished = await db.rpc("connect_finish", {
          _run: queued.data,
          _token: second.data.lease_token,
          _ok: true,
          _code: "ok",
        });
        expect(finished.error).toBeNull();
        const run = await db
          .from("connect_runs")
          .select("status,error_code")
          .eq("id", queued.data)
          .single();
        expect(run.data).toEqual({ status: "failed", error_code: "replay_incomplete" });
        const open = await db
          .from("connect_dead_letters")
          .select("status,replay_run_id")
          .eq("id", letter.data.id)
          .single();
        expect(open.data).toEqual({ status: "open", replay_run_id: null });
        const retry = await owner.rpc("connect_enqueue_replay", {
          _connection: conn.data.id,
          _request: crypto.randomUUID(),
          _dead_letters: [letter.data.id],
        });
        expect(retry.error).toBeNull();
        const next = await db.rpc("connect_claim", { _connection: conn.data.id });
        expect(next.data.streams[0].deferred).toBe(0);
        expect(next.data.streams[0].dead_letters.map((d: { id: string }) => d.id)).toEqual([
          letter.data.id,
        ]);
        await owner.rpc("connect_cancel", { _run: retry.data });
      } finally {
        await db.auth.admin.deleteUser(user.data.user.id);
      }
    });
  },
);
