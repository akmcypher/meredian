-- Meridian Community database baseline.




SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


CREATE SCHEMA IF NOT EXISTS "core";


ALTER SCHEMA "core" OWNER TO "postgres";


COMMENT ON SCHEMA "public" IS 'standard public schema';



CREATE EXTENSION IF NOT EXISTS "pg_stat_statements" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA "extensions";






CREATE EXTENSION IF NOT EXISTS "supabase_vault" WITH SCHEMA "vault";






CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA "extensions";






CREATE TYPE "public"."app_role" AS ENUM (
    'admin',
    'steward',
    'engineer',
    'analyst'
);


ALTER TYPE "public"."app_role" OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "core"."immutable_version"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'core'
    AS $$
BEGIN RAISE EXCEPTION 'Dataset versions are immutable'; END $$;


ALTER FUNCTION "core"."immutable_version"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."admin_audit_block"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
BEGIN RAISE EXCEPTION 'admin_audit is append-only'; END $$;


ALTER FUNCTION "public"."admin_audit_block"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."admin_audit_chain"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('admin_audit_chain'));
  SELECT hash INTO NEW.prev_hash FROM public.admin_audit ORDER BY id DESC LIMIT 1;
  NEW.hash_alg := 'sha256';
  NEW.hash := encode(extensions.digest(coalesce(NEW.prev_hash,'') || '|' || coalesce(NEW.actor::text,'') || '|' || NEW.action || '|' || coalesce(NEW.target,'') || '|' || NEW.detail::text || '|' || NEW.created_at::text, 'sha256'), 'hex');
  RETURN NEW;
END $$;


ALTER FUNCTION "public"."admin_audit_chain"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."apply_file_table_policies"("_table" "uuid", "_rows" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE uid uuid := auth.uid(); t record; my_roles public.app_role[]; has_pol boolean;
  r jsonb; keep boolean; pol record; ap record; out_rows jsonb := '[]'::jsonb;
BEGIN
  IF uid IS NULL THEN RAISE EXCEPTION 'not signed in'; END IF;
 IF jsonb_typeof(_rows) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'rows must be an array'; END IF;
 IF jsonb_array_length(_rows)>1000 OR octet_length(_rows::text)>8388608 THEN RAISE EXCEPTION 'file policy input exceeds page budget'; END IF;
  SELECT * INTO t FROM public.file_tables ft WHERE ft.id = _table AND public.can_access(ft.owner_id, ft.organization_id);
  IF NOT FOUND THEN RAISE EXCEPTION 'table not found'; END IF;
  IF t.owner_id = uid OR public.has_role(uid, 'admin') THEN RETURN _rows; END IF;
  SELECT coalesce(array_agg(ur.role), '{}') INTO my_roles FROM public.user_roles ur WHERE ur.user_id = uid;
  SELECT EXISTS (SELECT 1 FROM public.row_policies p WHERE p.table_name = t.name AND (p.organization_id IS NULL OR p.organization_id = t.organization_id)) INTO has_pol;
  FOR r IN SELECT * FROM jsonb_array_elements(_rows) LOOP
    keep := NOT has_pol;
    IF has_pol THEN
      FOR pol IN SELECT * FROM public.row_policies p WHERE p.table_name = t.name AND p.role = ANY (my_roles) AND (p.organization_id IS NULL OR p.organization_id = t.organization_id) LOOP
        IF public.row_passes(r, pol.column_name, pol.op, pol.value) THEN keep := true; EXIT; END IF;
      END LOOP;
    END IF;
    IF keep THEN
      FOR ap IN SELECT * FROM public.access_policies a WHERE (a.dataset = t.name OR a.dataset = '*') AND a.masking <> 'none' AND NOT (a.allowed_roles && my_roles)
                AND (a.organization_id IS NULL OR a.organization_id = t.organization_id) LOOP
        IF r ? ap.column_name THEN r := jsonb_set(r, ARRAY[ap.column_name], to_jsonb(public.mask_text(r ->> ap.column_name, ap.masking))); END IF;
      END LOOP;
      out_rows := out_rows || jsonb_build_array(r);
    END IF;
  END LOOP;
  RETURN out_rows;
END $$;


ALTER FUNCTION "public"."apply_file_table_policies"("_table" "uuid", "_rows" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."apply_licence_entitlements"("_actor" "uuid", "_licence" "text", "_org" "uuid", "_features" "text"[], "_expires" timestamp with time zone) RETURNS integer
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE n integer;
BEGIN
 IF _actor IS NULL OR _licence IS NULL OR _org IS NULL OR _features IS NULL OR cardinality(_features) > 100 THEN
   RAISE EXCEPTION 'invalid licence';
 END IF;
 PERFORM pg_advisory_xact_lock(hashtext('licence:' || _org::text));
 DELETE FROM public.entitlements WHERE organization_id = _org AND (licence_id = _licence OR feature = ANY(_features));
 INSERT INTO public.entitlements (organization_id, feature, licence_id, expires_at, granted_by)
 SELECT _org, f, _licence, _expires, _actor FROM unnest(_features) AS f;
 GET DIAGNOSTICS n = ROW_COUNT;
 RETURN n;
END $$;


ALTER FUNCTION "public"."apply_licence_entitlements"("_actor" "uuid", "_licence" "text", "_org" "uuid", "_features" "text"[], "_expires" timestamp with time zone) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."bootstrap_platform_admin"("_email" "text") RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
declare
  target uuid;
begin
  if exists (select 1 from public.user_roles where role = 'admin') then
    raise exception 'A platform admin already exists; grant roles from the admin console';
  end if;
  select id into target from auth.users where lower(email) = lower(trim(_email));
  if target is null then
    raise exception 'No account exists for that email';
  end if;
  insert into public.user_roles (user_id, role)
  values (target, 'admin'), (target, 'steward'), (target, 'engineer')
  on conflict (user_id, role) do nothing;
  return target;
end $$;


ALTER FUNCTION "public"."bootstrap_platform_admin"("_email" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."can_access"("_owner" "uuid", "_org" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT auth.uid() IS NOT NULL AND (_owner = auth.uid() OR public.is_org_member(_org) OR public.has_role(auth.uid(), 'admin'))
$$;


ALTER FUNCTION "public"."can_access"("_owner" "uuid", "_org" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."can_govern"("_org" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT auth.uid() IS NOT NULL AND (public.has_role(auth.uid(),'admin')
    OR (_org IS NOT NULL AND public.is_org_member(_org) AND public.has_role(auth.uid(),'steward')))
$$;


ALTER FUNCTION "public"."can_govern"("_org" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."can_read_file_object"("_name" "text") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$ SELECT false $$;


ALTER FUNCTION "public"."can_read_file_object"("_name" "text") OWNER TO "postgres";


COMMENT ON FUNCTION "public"."can_read_file_object"("_name" "text") IS 'DEPRECATED: teammates read file tables through apply_file_table_policies on the server';



CREATE OR REPLACE FUNCTION "public"."can_read_scoped"("_owner" "uuid", "_org" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT auth.uid() IS NOT NULL AND (public.has_role(auth.uid(), 'admin') OR CASE
    WHEN _org IS NULL THEN _owner = auth.uid() ELSE public.is_org_member(_org) END)
$$;


ALTER FUNCTION "public"."can_read_scoped"("_owner" "uuid", "_org" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."can_see_user"("_user" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT auth.uid() IS NOT NULL AND (_user = auth.uid() OR public.shares_org_with(_user) OR public.has_role(auth.uid(), 'admin'))
$$;


ALTER FUNCTION "public"."can_see_user"("_user" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cancel_ingest_run"("_run" "uuid") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
 IF auth.uid() IS NULL THEN RAISE EXCEPTION 'not signed in'; END IF;
 UPDATE public.ingest_runs SET status='cancelled',finished_at=now() WHERE id=_run AND owner_id=auth.uid() AND status IN ('planned','running') AND (organization_id IS NULL OR public.is_org_member(organization_id));
 RETURN FOUND;
END $$;


ALTER FUNCTION "public"."cancel_ingest_run"("_run" "uuid") OWNER TO "postgres";

SET default_tablespace = '';

SET default_table_access_method = "heap";


CREATE TABLE IF NOT EXISTS "public"."ingest_partitions" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "run_id" "uuid" NOT NULL,
    "owner_id" "uuid" NOT NULL,
    "idx" integer NOT NULL,
    "label" "text" NOT NULL,
    "predicate" "jsonb" NOT NULL,
    "est_rows" bigint DEFAULT 0 NOT NULL,
    "status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "rows_written" bigint DEFAULT 0 NOT NULL,
    "batches" integer DEFAULT 0 NOT NULL,
    "attempts" integer DEFAULT 0 NOT NULL,
    "checkpoint" "jsonb",
    "worker" "text",
    "error" "text",
    "started_at" timestamp with time zone,
    "finished_at" timestamp with time zone,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "lease_until" timestamp with time zone,
    "heartbeat_at" timestamp with time zone,
    "lease_token" "uuid",
    "claim_version" bigint DEFAULT 0 NOT NULL,
    "claimed_at" timestamp with time zone
);


ALTER TABLE "public"."ingest_partitions" OWNER TO "postgres";


COMMENT ON COLUMN "public"."ingest_partitions"."worker" IS 'Descriptive worker identity only; lease_token and claim_version fence attempts';



CREATE OR REPLACE FUNCTION "public"."claim_ingest_partition"("_run" "uuid", "_worker" "text", "_lease_seconds" integer DEFAULT 60) RETURNS SETOF "public"."ingest_partitions"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE p public.ingest_partitions;
BEGIN
 IF _worker IS NULL OR length(_worker) NOT BETWEEN 1 AND 100 OR _lease_seconds IS NULL OR _lease_seconds NOT BETWEEN 10 AND 600 THEN RAISE EXCEPTION 'invalid lease parameters'; END IF;
 PERFORM 1 FROM public.ingest_runs r WHERE r.id = _run AND r.status IN ('planned','running') AND r.pk_encoding = 'json_v1'
   AND (r.source_id IS NULL OR EXISTS (SELECT 1 FROM public.ingest_sources s WHERE s.id = r.source_id AND s.enabled))
   AND (r.organization_id IS NULL OR EXISTS (SELECT 1 FROM public.org_members m WHERE m.org_id=r.organization_id AND m.user_id=r.owner_id)) FOR UPDATE;
 IF NOT FOUND THEN RETURN; END IF;
 SELECT * INTO p FROM public.ingest_partitions WHERE run_id=_run AND attempts<10 AND (status IN ('pending','failed') OR (status='running' AND (lease_until IS NULL OR lease_until<now()))) ORDER BY idx FOR UPDATE SKIP LOCKED LIMIT 1;
 IF p.id IS NULL THEN RETURN; END IF;
 UPDATE public.ingest_partitions SET status='running',worker=_worker,attempts=attempts+1,lease_token=gen_random_uuid(),claim_version=claim_version+1,claimed_at=now(),lease_until=now()+make_interval(secs=>_lease_seconds),heartbeat_at=now(),started_at=coalesce(started_at,now()),error=NULL,updated_at=now() WHERE id=p.id RETURNING * INTO p;
 UPDATE public.ingest_runs SET status='running',started_at=coalesce(started_at,now()) WHERE id=_run AND status='planned';
 RETURN NEXT p;
END $$;


ALTER FUNCTION "public"."claim_ingest_partition"("_run" "uuid", "_worker" "text", "_lease_seconds" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."claim_specific_partition"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_seconds" integer DEFAULT 60) RETURNS SETOF "public"."ingest_partitions"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE p public.ingest_partitions;
BEGIN
 IF _worker IS NULL OR length(_worker) NOT BETWEEN 1 AND 100 OR _lease_seconds IS NULL OR _lease_seconds NOT BETWEEN 10 AND 600 THEN RAISE EXCEPTION 'invalid lease parameters'; END IF;
 PERFORM 1 FROM public.ingest_runs r WHERE r.id=_run AND r.status IN ('planned','running') AND r.pk_encoding='json_v1' AND (r.organization_id IS NULL OR EXISTS (SELECT 1 FROM public.org_members m WHERE m.org_id=r.organization_id AND m.user_id=r.owner_id)) FOR UPDATE;
 IF NOT FOUND THEN RETURN; END IF;
 UPDATE public.ingest_partitions SET status='running',worker=_worker,attempts=attempts+1,lease_token=gen_random_uuid(),claim_version=claim_version+1,claimed_at=now(),lease_until=now()+make_interval(secs=>_lease_seconds),heartbeat_at=now(),started_at=coalesce(started_at,now()),error=NULL,updated_at=now() WHERE run_id=_run AND idx=_idx AND attempts<10 AND (status IN ('pending','failed') OR (status='running' AND (lease_until IS NULL OR lease_until<now()))) RETURNING * INTO p;
 IF p.id IS NULL THEN RETURN; END IF;
 UPDATE public.ingest_runs SET status='running',started_at=coalesce(started_at,now()) WHERE id=_run AND status='planned';
 RETURN NEXT p;
END $$;


ALTER FUNCTION "public"."claim_specific_partition"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_seconds" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."connect_accept_offset_gap"("_stream" "uuid") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE s public.connect_streams; c public.connect_connections; last_gap jsonb; approval jsonb;
BEGIN
 SELECT * INTO s FROM public.connect_streams WHERE id=_stream; SELECT * INTO c FROM public.connect_connections WHERE id=s.connection_id FOR UPDATE;
 IF auth.uid() IS NULL OR c.id IS NULL OR NOT public.can_read_scoped(c.owner_id,c.organization_id) OR c.owner_id<>auth.uid() THEN RAISE EXCEPTION 'Connection owner required'; END IF;
 IF EXISTS(SELECT 1 FROM public.connect_runs WHERE connection_id=c.id AND status IN ('queued','running','retry_wait')) THEN RAISE EXCEPTION 'Stop active run before accepting a gap'; END IF;
 SELECT (SELECT jsonb_agg(g) FROM jsonb_array_elements(e.detail) g WHERE g->>'stream_id'=s.id::text) INTO last_gap
  FROM public.connect_events e
  WHERE e.connection_id=c.id AND e.code='offset_out_of_range' AND e.detail IS NOT NULL
   AND EXISTS(SELECT 1 FROM jsonb_array_elements(e.detail) g WHERE g->>'stream_id'=s.id::text)
  ORDER BY e.created_at DESC LIMIT 1;
 IF last_gap IS NULL THEN RAISE EXCEPTION 'No reported offset gap for this stream'; END IF;
 approval:=jsonb_build_object(
  'topic',s.schema_json->>'x-kafka-topic',
  'topic_id',s.state->'topic_id',
  'generation',s.state_generation,
  'partitions',(SELECT jsonb_agg(jsonb_build_object('partition',(g->>'partition')::integer,'offset',g->>'offset','low',g->>'low','gap',g->>'gap')) FROM jsonb_array_elements(last_gap) g),
  'approved_by',auth.uid(),
  'approved_at',now());
 UPDATE public.connect_streams SET state=(state-'accept_offset_gap')||jsonb_build_object('gap_approval',approval) WHERE id=s.id;
 INSERT INTO public.connect_events(connection_id,code,actor_id,detail) VALUES(c.id,'offset_gap_accepted',auth.uid(),approval->'partitions'); RETURN true;
END $$;


ALTER FUNCTION "public"."connect_accept_offset_gap"("_stream" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."connect_cancel"("_run" "uuid") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$ BEGIN
 UPDATE public.connect_runs r SET status='cancelled',lease_token=NULL,lease_until=NULL,finished_at=now() FROM public.connect_connections c WHERE r.id=_run AND r.connection_id=c.id AND c.owner_id=auth.uid() AND public.can_read_scoped(c.owner_id,c.organization_id) AND r.status IN ('queued','running','retry_wait'); RETURN FOUND;
END $$;


ALTER FUNCTION "public"."connect_cancel"("_run" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."connect_claim"("_connection" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE c public.connect_connections; r public.connect_runs; token uuid; payload jsonb;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Agent service only'; END IF;
 SELECT * INTO c FROM public.connect_connections WHERE id=_connection FOR UPDATE;
 IF NOT FOUND OR NOT c.enabled OR NOT EXISTS(SELECT 1 FROM public.ingest_sources s WHERE s.id=c.source_id AND s.enabled) OR (c.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members WHERE org_id=c.organization_id AND user_id=c.owner_id)) THEN RETURN NULL; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE connection_id=c.id AND status IN ('queued','retry_wait','running') ORDER BY created_at LIMIT 1 FOR UPDATE;
 IF NOT FOUND AND c.interval_minutes>0 AND coalesce(c.next_run_at,now())<=now() AND EXISTS(SELECT 1 FROM public.connect_streams WHERE connection_id=c.id AND enabled) THEN
 INSERT INTO public.connect_runs(connection_id,request_key,operation) VALUES(c.id,gen_random_uuid(),'sync') RETURNING * INTO r;
 INSERT INTO public.connect_run_streams(run_id,stream_id,initial_state,checkpoint,schema_json,generation) SELECT r.id,s.id,CASE WHEN s.sync_mode='full_refresh' THEN '{}'::jsonb ELSE s.state END,CASE WHEN s.sync_mode='full_refresh' THEN '{}'::jsonb ELSE s.state END,s.schema_json,s.state_generation FROM public.connect_streams s WHERE s.connection_id=c.id AND s.enabled;
 UPDATE public.connect_connections SET next_run_at=now()+make_interval(mins=>interval_minutes) WHERE id=c.id; END IF;
 IF r.id IS NULL THEN RETURN NULL; END IF;
 IF r.status='running' AND r.lease_until>now() THEN RETURN NULL; END IF;
 IF r.status='running' THEN UPDATE public.connect_attempts SET status='interrupted',finished_at=now(),error_code='lease_expired' WHERE run_id=r.id AND attempt=r.attempts; END IF;
 IF r.attempts>=r.max_attempts THEN UPDATE public.connect_runs SET status='failed',error_code='attempts_exhausted',finished_at=now(),lease_token=NULL WHERE id=r.id; RETURN NULL; END IF;
 IF r.next_attempt_at>now() THEN RETURN NULL; END IF;
 token:=gen_random_uuid(); UPDATE public.connect_runs SET status='running',attempts=attempts+1,lease_token=token,lease_until=now()+interval '90 seconds' WHERE id=r.id RETURNING * INTO r;
 INSERT INTO public.connect_attempts(run_id,attempt,token) VALUES(r.id,r.attempts,token);
 SELECT coalesce(jsonb_agg(jsonb_build_object('id',s.id,'namespace',s.namespace,'name',s.name,'primary_key',s.primary_key,'primary_keys',to_jsonb(s.key_columns),'cursor_field',s.cursor_field,'sync_mode',CASE WHEN r.operation IN ('reset','backfill') THEN 'full_refresh' ELSE s.sync_mode END,'schema',rs.schema_json,'state',rs.checkpoint,'sequence',rs.sequence,'completed',rs.completed,'generation',rs.generation,'dead_letters',CASE WHEN r.operation='replay' THEN (SELECT coalesce(jsonb_agg(jsonb_build_object('id',x.id,'position',x.position,'error_code',x.error_code,'object_key',x.object_key,'line',x.line) ORDER BY x.created_at),'[]') FROM public.connect_dead_letters x WHERE x.replay_run_id=r.id AND x.stream_id=s.id AND x.status='open') END)), '[]') INTO payload FROM public.connect_run_streams rs JOIN public.connect_streams s ON s.id=rs.stream_id WHERE rs.run_id=r.id;
 INSERT INTO public.connect_events(connection_id,run_id,code) VALUES(c.id,r.id,'attempt_started');
 RETURN jsonb_build_object('run_id',r.id,'lease_token',token,'attempt',r.attempts,'operation',r.operation,'streams',payload,'credential_ref',c.credential_ref,'source_version',c.source_version,'destination_version',c.destination_version,'operation_timeout_seconds',c.operation_timeout_seconds);
END $$;


ALTER FUNCTION "public"."connect_claim"("_connection" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."connect_commit"("_run" "uuid", "_token" "uuid", "_stream" "uuid", "_sequence" integer, "_batch" "uuid", "_manifest" "text", "_sha" "text", "_rows" integer, "_bytes" integer, "_checkpoint" "jsonb", "_completed" boolean) RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE r public.connect_runs; c public.connect_connections; rs public.connect_run_streams; s public.connect_streams; prior public.connect_commits; b public.ingest_landing_batches; cid uuid; target text;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run FOR UPDATE; SELECT * INTO c FROM public.connect_connections WHERE id=r.connection_id;
 IF r.id IS NULL OR r.status<>'running' OR r.lease_token IS DISTINCT FROM _token OR r.lease_until<=now() OR NOT c.enabled OR (c.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members WHERE org_id=c.organization_id AND user_id=c.owner_id)) THEN RAISE EXCEPTION 'Stale or unauthorized commit'; END IF;
 SELECT * INTO prior FROM public.connect_commits WHERE run_id=r.id AND stream_id=_stream AND sequence=_sequence;
 IF FOUND THEN IF prior.sha256<>_sha OR prior.checkpoint IS DISTINCT FROM _checkpoint OR prior.row_count<>_rows OR prior.byte_count<>_bytes THEN RAISE EXCEPTION 'Commit identity conflict'; END IF; RETURN prior.id; END IF;
 SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id AND stream_id=_stream FOR UPDATE; SELECT * INTO s FROM public.connect_streams WHERE id=_stream;
 IF rs.run_id IS NULL OR rs.completed OR rs.sequence<>_sequence OR s.state_generation<>rs.generation THEN RAISE EXCEPTION 'Stream sequence or generation conflict'; END IF;
 SELECT * INTO b FROM public.ingest_landing_batches WHERE id=_batch AND source_id=c.source_id AND owner_id=c.owner_id;
 IF b.id IS NULL OR b.sha256<>_sha OR b.row_count<>_rows OR _bytes NOT BETWEEN 0 AND 8388608 OR _manifest<>c.owner_id::text||'/published/'||b.batch_key::text||'/manifest.json' THEN RAISE EXCEPTION 'Invalid destination attestation'; END IF;
 INSERT INTO public.connect_commits(run_id,stream_id,sequence,batch_id,manifest_key,sha256,row_count,byte_count,checkpoint) VALUES(r.id,s.id,_sequence,b.id,_manifest,_sha,_rows,_bytes,_checkpoint) RETURNING id INTO cid;
 UPDATE public.connect_run_streams SET checkpoint=_checkpoint,sequence=sequence+1,completed=_completed WHERE run_id=r.id AND stream_id=s.id;
 IF r.operation='replay' THEN UPDATE public.connect_dead_letters SET status='replayed',resolved_at=now() WHERE replay_run_id=r.id AND stream_id=s.id AND status='open' AND id::text IN (SELECT jsonb_array_elements_text(coalesce(_checkpoint->'replayed','[]'))); ELSE UPDATE public.connect_streams SET state=_checkpoint,last_success_at=now() WHERE id=s.id; END IF;
 UPDATE public.connect_runs SET rows_committed=rows_committed+_rows,bytes_committed=bytes_committed+_bytes WHERE id=r.id;
 target:='connect_'||replace(c.id::text,'-','')||'_'||replace(s.id::text,'-','');
 INSERT INTO public.catalog_entries(table_name,owner_id,organization_id,description,domain,tags) VALUES(target,c.owner_id,c.organization_id,'Committed raw bronze envelopes; not a queryable Iceberg/Delta table','Connect',ARRAY['bronze','raw','pending-native-publication']) ON CONFLICT(table_name) DO NOTHING;
 INSERT INTO public.lineage_edges_auto(source_table,target_table,query,created_by) VALUES('connect-source:'||c.id::text||':'||s.namespace||'.'||s.name,target,'Meridian Connect committed extraction',c.owner_id) ON CONFLICT(source_table,target_table) DO NOTHING;
 INSERT INTO public.connect_events(connection_id,run_id,code) VALUES(c.id,r.id,'batch_committed'); RETURN cid;
END $$;


ALTER FUNCTION "public"."connect_commit"("_run" "uuid", "_token" "uuid", "_stream" "uuid", "_sequence" integer, "_batch" "uuid", "_manifest" "text", "_sha" "text", "_rows" integer, "_bytes" integer, "_checkpoint" "jsonb", "_completed" boolean) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."connect_discard_dead_letters"("_connection" "uuid", "_dead_letters" "uuid"[]) RETURNS integer
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE c public.connect_connections; n integer;
BEGIN
 SELECT * INTO c FROM public.connect_connections WHERE id=_connection;
 IF auth.uid() IS NULL OR c.id IS NULL OR NOT public.can_read_scoped(c.owner_id,c.organization_id) OR c.owner_id<>auth.uid() THEN RAISE EXCEPTION 'Connection owner required'; END IF;
 UPDATE public.connect_dead_letters SET status='discarded',resolved_at=now() WHERE id=ANY(_dead_letters) AND connection_id=c.id AND status='open'; GET DIAGNOSTICS n=ROW_COUNT;
 INSERT INTO public.connect_events(connection_id,code,actor_id) VALUES(c.id,'dead_letters_discarded',auth.uid()); RETURN n;
END $$;


ALTER FUNCTION "public"."connect_discard_dead_letters"("_connection" "uuid", "_dead_letters" "uuid"[]) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."connect_enqueue"("_connection" "uuid", "_request" "uuid", "_operation" "text") RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE c public.connect_connections; rid uuid; s public.connect_streams;
BEGIN
 SELECT * INTO c FROM public.connect_connections WHERE id=_connection FOR UPDATE;
 IF auth.uid() IS NULL OR NOT public.can_read_scoped(c.owner_id,c.organization_id) OR c.owner_id<>auth.uid() THEN RAISE EXCEPTION 'Connection owner required'; END IF;
 IF NOT c.enabled OR _operation NOT IN ('check','discover','sync','reset','backfill') THEN RAISE EXCEPTION 'Invalid operation or disabled connection'; END IF;
 SELECT id INTO rid FROM public.connect_runs WHERE connection_id=c.id AND request_key=_request; IF FOUND THEN RETURN rid; END IF;
 IF _operation IN ('sync','reset','backfill') AND NOT EXISTS(SELECT 1 FROM public.connect_streams WHERE connection_id=c.id AND enabled) THEN RAISE EXCEPTION 'Select discovered streams first'; END IF;
 INSERT INTO public.connect_runs(connection_id,request_key,operation) VALUES(c.id,_request,_operation) RETURNING id INTO rid;
 IF _operation IN ('sync','reset','backfill') THEN FOR s IN SELECT * FROM public.connect_streams WHERE connection_id=c.id AND enabled FOR UPDATE LOOP
 INSERT INTO public.connect_run_streams(run_id,stream_id,initial_state,checkpoint,schema_json,generation) VALUES(rid,s.id,CASE WHEN (_operation IN ('reset','backfill') OR s.sync_mode='full_refresh') THEN '{}'::jsonb ELSE s.state END,CASE WHEN (_operation IN ('reset','backfill') OR s.sync_mode='full_refresh') THEN '{}'::jsonb ELSE s.state END,s.schema_json,s.state_generation); END LOOP; END IF;
 INSERT INTO public.connect_events(connection_id,run_id,code,actor_id) VALUES(c.id,rid,'run_queued',auth.uid()); RETURN rid;
END $$;


ALTER FUNCTION "public"."connect_enqueue"("_connection" "uuid", "_request" "uuid", "_operation" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."connect_enqueue_replay"("_connection" "uuid", "_request" "uuid", "_dead_letters" "uuid"[]) RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE c public.connect_connections; rid uuid; picked integer;
BEGIN
 SELECT * INTO c FROM public.connect_connections WHERE id=_connection FOR UPDATE;
 IF auth.uid() IS NULL OR c.id IS NULL OR NOT public.can_read_scoped(c.owner_id,c.organization_id) OR c.owner_id<>auth.uid() THEN RAISE EXCEPTION 'Connection owner required'; END IF;
 IF NOT c.enabled OR cardinality(_dead_letters) NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'Invalid replay request'; END IF;
 SELECT id INTO rid FROM public.connect_runs WHERE connection_id=c.id AND request_key=_request; IF FOUND THEN RETURN rid; END IF;
 INSERT INTO public.connect_runs(connection_id,request_key,operation) VALUES(c.id,_request,'replay') RETURNING id INTO rid;
 UPDATE public.connect_dead_letters d SET replay_run_id=rid WHERE d.id=ANY(_dead_letters) AND d.connection_id=c.id AND d.status='open'
  AND (d.replay_run_id IS NULL OR EXISTS(SELECT 1 FROM public.connect_runs x WHERE x.id=d.replay_run_id AND x.status IN ('succeeded','failed','cancelled')));
 GET DIAGNOSTICS picked=ROW_COUNT;
 IF picked<>cardinality(_dead_letters) THEN RAISE EXCEPTION 'Dead letters must be open and not in another replay'; END IF;
 INSERT INTO public.connect_run_streams(run_id,stream_id,initial_state,checkpoint,schema_json,generation)
  SELECT rid,s.id,'{}','{}',s.schema_json,s.state_generation FROM public.connect_streams s WHERE s.id IN (SELECT stream_id FROM public.connect_dead_letters WHERE replay_run_id=rid);
 INSERT INTO public.connect_events(connection_id,run_id,code,actor_id) VALUES(c.id,rid,'replay_queued',auth.uid()); RETURN rid;
END $$;


ALTER FUNCTION "public"."connect_enqueue_replay"("_connection" "uuid", "_request" "uuid", "_dead_letters" "uuid"[]) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."connect_finish"("_run" "uuid", "_token" "uuid", "_ok" boolean, "_code" "text", "_catalog" "jsonb" DEFAULT NULL::"jsonb") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE r public.connect_runs; wait_seconds integer;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run FOR UPDATE;
 IF r.status<>'running' OR r.lease_token IS DISTINCT FROM _token OR r.lease_until<=now() THEN RAISE EXCEPTION 'Stale completion'; END IF;
 IF _code NOT IN ('ok','auth_failed','source_unavailable','schema_drift','connector_failed','destination_failed','timeout','dead_letter_budget','offset_out_of_range','source_changed','record_too_large','replay_incomplete') THEN RAISE EXCEPTION 'Unknown safe error code'; END IF;
 IF _ok AND r.operation IN ('sync','reset','backfill','replay') AND EXISTS(SELECT 1 FROM public.connect_run_streams WHERE run_id=r.id AND NOT completed) THEN RAISE EXCEPTION 'Incomplete streams'; END IF;
 IF _ok AND r.operation='discover' THEN IF jsonb_typeof(_catalog)<>'array' OR jsonb_array_length(_catalog)>1000 THEN RAISE EXCEPTION 'Invalid discovery'; END IF; UPDATE public.connect_connections SET discovered_catalog=_catalog,health='reachable' WHERE id=r.connection_id; END IF;
 IF _ok AND r.operation='check' THEN UPDATE public.connect_connections SET health='reachable' WHERE id=r.connection_id; END IF;
 wait_seconds:=least(300,5*(2^r.attempts)::integer)+floor(random()*5)::integer;
 UPDATE public.connect_runs SET status=CASE WHEN _ok THEN 'succeeded' WHEN attempts>=max_attempts OR _code IN ('auth_failed','schema_drift','dead_letter_budget','offset_out_of_range','source_changed','record_too_large','replay_incomplete') THEN 'failed' ELSE 'retry_wait' END,error_code=CASE WHEN _ok THEN NULL ELSE _code END,lease_token=NULL,lease_until=NULL,next_attempt_at=now()+make_interval(secs=>wait_seconds),finished_at=CASE WHEN _ok OR attempts>=max_attempts OR _code IN ('auth_failed','schema_drift','dead_letter_budget','offset_out_of_range','source_changed','record_too_large','replay_incomplete') THEN now() ELSE NULL END WHERE id=r.id;
 UPDATE public.connect_attempts SET status=CASE WHEN _ok THEN 'succeeded' ELSE 'failed' END,finished_at=now(),error_code=CASE WHEN _ok THEN NULL ELSE _code END WHERE run_id=r.id AND attempt=r.attempts;
 INSERT INTO public.connect_events(connection_id,run_id,level,code) VALUES(r.connection_id,r.id,CASE WHEN _ok THEN 'INFO' ELSE 'ERROR' END,CASE WHEN _ok THEN 'run_succeeded' ELSE _code END); RETURN true;
END $$;


ALTER FUNCTION "public"."connect_finish"("_run" "uuid", "_token" "uuid", "_ok" boolean, "_code" "text", "_catalog" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."connect_guard_stream"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public'
    AS $$ BEGIN
 PERFORM 1 FROM public.connect_connections WHERE id=NEW.connection_id FOR UPDATE;
 IF TG_OP='INSERT' AND NEW.key_columns IS NULL THEN NEW.key_columns:=string_to_array(NEW.primary_key,','); END IF;
 IF TG_OP='UPDATE' AND (NEW.namespace<>OLD.namespace OR NEW.name<>OLD.name OR NEW.primary_key<>OLD.primary_key OR NEW.key_columns IS DISTINCT FROM OLD.key_columns OR NEW.cursor_field<>OLD.cursor_field OR NEW.sync_mode<>OLD.sync_mode OR NEW.schema_json IS DISTINCT FROM OLD.schema_json) THEN
  IF EXISTS(SELECT 1 FROM public.connect_runs WHERE connection_id=NEW.connection_id AND status IN ('queued','running','retry_wait')) THEN RAISE EXCEPTION 'Active replication configuration is immutable'; END IF;
  NEW.state:='{}'; NEW.state_generation:=OLD.state_generation+1;
 END IF; RETURN NEW;
END $$;


ALTER FUNCTION "public"."connect_guard_stream"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."connect_heartbeat"("_run" "uuid", "_token" "uuid") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$ BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 UPDATE public.connect_runs r SET lease_until=now()+interval '90 seconds' FROM public.connect_connections c WHERE r.id=_run AND r.connection_id=c.id AND c.enabled AND r.status='running' AND r.lease_token=_token AND r.lease_until>now(); RETURN FOUND;
END $$;


ALTER FUNCTION "public"."connect_heartbeat"("_run" "uuid", "_token" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."connect_record_dead_letters"("_run" "uuid", "_token" "uuid", "_stream" "uuid", "_sequence" integer, "_object" "text", "_entries" "jsonb") RETURNS integer
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE r public.connect_runs; c public.connect_connections; rs public.connect_run_streams; n integer;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run FOR UPDATE; SELECT * INTO c FROM public.connect_connections WHERE id=r.connection_id;
 IF r.id IS NULL OR r.status<>'running' OR r.operation NOT IN ('sync','reset','backfill') OR r.lease_token IS DISTINCT FROM _token OR r.lease_until<=now() OR NOT c.enabled THEN RAISE EXCEPTION 'Stale or unauthorized dead letter'; END IF;
 SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id AND stream_id=_stream;
 IF rs.run_id IS NULL OR rs.sequence<>_sequence OR _object NOT LIKE c.owner_id::text||'/dead-letters/%' OR jsonb_typeof(_entries)<>'array' OR jsonb_array_length(_entries) NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'Invalid dead letter batch'; END IF;
 -- Re-reading the same positions after a lost response or a new attempt records nothing twice.
 INSERT INTO public.connect_dead_letters(connection_id,stream_id,run_id,sequence,position,error_code,object_key,line)
  SELECT c.id,_stream,r.id,_sequence,e->>'position',e->>'error_code',_object,(e->>'line')::integer FROM jsonb_array_elements(_entries) e
  ON CONFLICT(stream_id,position) DO NOTHING;
 SELECT count(*) INTO n FROM public.connect_dead_letters WHERE run_id=r.id;
 IF n>c.dead_letter_budget THEN RAISE EXCEPTION 'dead_letter_budget'; END IF;
 INSERT INTO public.connect_events(connection_id,run_id,level,code) VALUES(c.id,r.id,'WARN','dead_letters_recorded'); RETURN n;
END $$;


ALTER FUNCTION "public"."connect_record_dead_letters"("_run" "uuid", "_token" "uuid", "_stream" "uuid", "_sequence" integer, "_object" "text", "_entries" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."connect_report"("_run" "uuid", "_token" "uuid", "_code" "text", "_detail" "jsonb", "_stream" "uuid" DEFAULT NULL::"uuid") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE r public.connect_runs; tagged jsonb;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run;
 IF r.id IS NULL OR r.status<>'running' OR r.lease_token IS DISTINCT FROM _token OR r.lease_until<=now() THEN RAISE EXCEPTION 'Stale report'; END IF;
 IF _code NOT IN ('offset_out_of_range','source_changed','record_too_large','replay_incomplete') OR jsonb_typeof(_detail)<>'array' OR jsonb_array_length(_detail) NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'Invalid report'; END IF;
 IF _stream IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.connect_run_streams WHERE run_id=r.id AND stream_id=_stream) THEN RAISE EXCEPTION 'Stream not in run'; END IF;
 SELECT jsonb_agg(CASE WHEN _stream IS NULL THEN g ELSE g||jsonb_build_object('stream_id',_stream) END) INTO tagged FROM jsonb_array_elements(_detail) g;
 INSERT INTO public.connect_events(connection_id,run_id,level,code,detail) VALUES(r.connection_id,r.id,'ERROR',_code,tagged); RETURN true;
END $$;


ALTER FUNCTION "public"."connect_report"("_run" "uuid", "_token" "uuid", "_code" "text", "_detail" "jsonb", "_stream" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."connect_rewind_stream"("_stream" "uuid", "_state" "jsonb") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $_$
DECLARE s public.connect_streams; c public.connect_connections;
BEGIN
 SELECT * INTO s FROM public.connect_streams WHERE id=_stream; SELECT * INTO c FROM public.connect_connections WHERE id=s.connection_id FOR UPDATE;
 IF auth.uid() IS NULL OR c.id IS NULL OR NOT public.can_read_scoped(c.owner_id,c.organization_id) OR c.owner_id<>auth.uid() THEN RAISE EXCEPTION 'Connection owner required'; END IF;
 IF EXISTS(SELECT 1 FROM public.connect_runs WHERE connection_id=c.id AND status IN ('queued','running','retry_wait')) THEN RAISE EXCEPTION 'Stop active run before rewinding'; END IF;
 IF jsonb_typeof(_state)<>'object' OR (_state<>'{}'::jsonb AND _state->'version' IS DISTINCT FROM '1'::jsonb) OR (_state ? 'positions' AND jsonb_typeof(_state->'positions') IS DISTINCT FROM 'object') OR length(_state::text)>65536 THEN RAISE EXCEPTION 'Invalid checkpoint'; END IF;
 IF _state ?| ARRAY['gap_approval','accept_offset_gap','replayed','gaps'] THEN RAISE EXCEPTION 'Approvals and run history cannot be set by rewind'; END IF;
 IF _state ? 'positions' AND EXISTS(SELECT 1 FROM jsonb_each(_state->'positions') p WHERE p.key !~ '^[0-9]{1,6}$' OR NOT ((jsonb_typeof(p.value)='string' AND p.value#>>'{}' ~ '^[0-9]{1,20}$') OR (jsonb_typeof(p.value)='number' AND p.value#>>'{}' ~ '^[0-9]{1,15}$'))) THEN RAISE EXCEPTION 'Positions must be partition: offset'; END IF;
 UPDATE public.connect_streams SET state=_state WHERE id=s.id;
 INSERT INTO public.connect_events(connection_id,code,actor_id,detail) VALUES(c.id,'stream_rewound',auth.uid(),jsonb_build_array(jsonb_build_object('stream_id',s.id,'positions',coalesce(_state->'positions','{}'::jsonb)))); RETURN true;
END $_$;


ALTER FUNCTION "public"."connect_rewind_stream"("_stream" "uuid", "_state" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."consume_rate_limit"("_key" "text", "_limit" integer, "_window_seconds" integer) RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE n integer;
BEGIN
 IF _key IS NULL OR length(_key) NOT BETWEEN 1 AND 200 OR _limit IS NULL OR _limit < 1
    OR _window_seconds IS NULL OR _window_seconds NOT BETWEEN 1 AND 86400 THEN
   RAISE EXCEPTION 'invalid rate limit';
 END IF;
 INSERT INTO public.rate_limit_windows AS w (key, window_start, hits)
 VALUES (_key, now(), 1)
 ON CONFLICT (key) DO UPDATE SET
   hits = CASE WHEN w.window_start <= now() - make_interval(secs => _window_seconds) THEN 1 ELSE w.hits + 1 END,
   window_start = CASE WHEN w.window_start <= now() - make_interval(secs => _window_seconds) THEN now() ELSE w.window_start END
 RETURNING hits INTO n;
 RETURN n <= _limit;
END $$;


ALTER FUNCTION "public"."consume_rate_limit"("_key" "text", "_limit" integer, "_window_seconds" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."create_demo_ingest_run_verified"("_actor" "uuid", "_target" "text", "_total" bigint, "_batch" integer, "_concurrency" integer, "_plan" "jsonb", "_partitions" "jsonb") RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
 IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'service only'; END IF;
 RETURN public.create_ingest_run_verified(_actor,NULL,_target,_total,_batch,_concurrency,_plan,_partitions,ARRAY['id']);
END $$;


ALTER FUNCTION "public"."create_demo_ingest_run_verified"("_actor" "uuid", "_target" "text", "_total" bigint, "_batch" integer, "_concurrency" integer, "_plan" "jsonb", "_partitions" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."create_ingest_run_atomic"("_source" "uuid", "_target" "text", "_total" bigint, "_batch" integer, "_concurrency" integer, "_plan" "jsonb", "_partitions" "jsonb", "_pk" "text"[]) RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $_$
DECLARE uid uuid:=auth.uid(); src public.ingest_sources; run_id_out uuid; kind_out text:='demo_orders'; org_out uuid; target_out text; p jsonb; idx_count integer; k text;
BEGIN
 IF uid IS NULL THEN RAISE EXCEPTION 'not signed in'; END IF;
 IF _total IS NULL OR _total<0 OR _batch IS NULL OR _batch NOT BETWEEN 100 AND 10000 OR _concurrency IS NULL OR _concurrency NOT BETWEEN 1 AND 64 OR jsonb_typeof(_plan) IS DISTINCT FROM 'object' OR jsonb_typeof(_partitions) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'invalid run parameters'; END IF;
 IF jsonb_array_length(_partitions) NOT BETWEEN 1 AND 5000 OR octet_length(_partitions::text)>2097152 THEN RAISE EXCEPTION 'invalid partition count or size'; END IF;
 IF cardinality(_pk) NOT BETWEEN 1 AND 8 OR _pk IS NULL THEN RAISE EXCEPTION 'invalid primary keys'; END IF;
 FOREACH k IN ARRAY _pk LOOP IF k IS NULL OR k !~ '^[A-Za-z_][A-Za-z0-9_]{0,62}$' THEN RAISE EXCEPTION 'invalid primary key'; END IF; END LOOP;
 IF (SELECT count(DISTINCT x) FROM unnest(_pk) x)<>cardinality(_pk) THEN RAISE EXCEPTION 'duplicate primary keys'; END IF;
 target_out:=_target;
 IF _source IS NOT NULL THEN
   SELECT * INTO src FROM public.ingest_sources WHERE id=_source AND owner_id=uid FOR UPDATE;
   IF NOT FOUND OR (src.organization_id IS NOT NULL AND NOT public.is_org_member(src.organization_id)) THEN RAISE EXCEPTION 'source not authorized'; END IF;
   IF EXISTS (SELECT 1 FROM public.ingest_runs WHERE source_id=_source AND status IN ('planned','running')) THEN RAISE EXCEPTION 'source already has an unfinished run'; END IF;
   kind_out:=src.kind;org_out:=src.organization_id;target_out:=src.target_table;
 END IF;
 IF target_out IS NULL OR target_out !~ '^[a-z_][a-z0-9_]{0,62}$' THEN RAISE EXCEPTION 'invalid target'; END IF;
 SELECT count(DISTINCT (e.value->>'idx')::integer) INTO idx_count FROM jsonb_array_elements(_partitions) e;
 IF idx_count<>jsonb_array_length(_partitions) THEN RAISE EXCEPTION 'duplicate partition indexes'; END IF;
 FOR p IN SELECT value FROM jsonb_array_elements(_partitions) LOOP
  IF (p->>'idx')::integer<0 OR (p->>'idx')::integer>=idx_count OR jsonb_typeof(p->'predicate') IS DISTINCT FROM 'object' OR (p->'predicate'->>'kind') NOT IN ('range','hash','modulo','date','key','key_other','null') OR (p->'predicate'->>'kind') IS NULL OR (p->>'estRows')::numeric<0 OR length(p->>'label')>300 THEN RAISE EXCEPTION 'invalid partition'; END IF;
  IF p->'predicate' ? 'where' THEN RAISE EXCEPTION 'raw predicates not allowed'; END IF;
 END LOOP;
 INSERT INTO public.ingest_runs(owner_id,source_id,source_kind,target_table,organization_id,plan,batch_size,concurrency,expected_rows,pk_columns) VALUES (uid,_source,kind_out,target_out,org_out,_plan,_batch,_concurrency,_total,_pk) RETURNING id INTO run_id_out;
 INSERT INTO public.ingest_partitions(run_id,owner_id,idx,label,predicate,est_rows) SELECT run_id_out,uid,(e.value->>'idx')::integer,e.value->>'label',e.value->'predicate',round((e.value->>'estRows')::numeric)::bigint FROM jsonb_array_elements(_partitions) e;
 IF _source IS NOT NULL THEN UPDATE public.ingest_sources SET pk_columns=_pk WHERE id=_source; END IF;
 RETURN run_id_out;
END $_$;


ALTER FUNCTION "public"."create_ingest_run_atomic"("_source" "uuid", "_target" "text", "_total" bigint, "_batch" integer, "_concurrency" integer, "_plan" "jsonb", "_partitions" "jsonb", "_pk" "text"[]) OWNER TO "postgres";


COMMENT ON FUNCTION "public"."create_ingest_run_atomic"("_source" "uuid", "_target" "text", "_total" bigint, "_batch" integer, "_concurrency" integer, "_plan" "jsonb", "_partitions" "jsonb", "_pk" "text"[]) IS 'Atomic owner-authorized run plus partitions and source key update; server callers rebuild strategy predicates before invoking';



CREATE OR REPLACE FUNCTION "public"."create_ingest_run_verified"("_actor" "uuid", "_source" "uuid", "_target" "text", "_total" bigint, "_batch" integer, "_concurrency" integer, "_plan" "jsonb", "_partitions" "jsonb", "_pk" "text"[]) RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
 IF auth.role() IS DISTINCT FROM 'service_role' OR _actor IS NULL THEN RAISE EXCEPTION 'service only'; END IF;
 PERFORM set_config('request.jwt.claim.sub',_actor::text,true);
 PERFORM set_config('request.jwt.claims',jsonb_build_object('sub',_actor,'role','service_role')::text,true);
 RETURN public.create_ingest_run_atomic(_source,_target,_total,_batch,_concurrency,_plan,_partitions,_pk);
END $$;


ALTER FUNCTION "public"."create_ingest_run_verified"("_actor" "uuid", "_source" "uuid", "_target" "text", "_total" bigint, "_batch" integer, "_concurrency" integer, "_plan" "jsonb", "_partitions" "jsonb", "_pk" "text"[]) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."create_ingest_source_verified"("_actor" "uuid", "_name" "text", "_kind" "text", "_target" "text", "_config" "jsonb", "_headers" "jsonb", "_hash" "text") RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$ DECLARE sid uuid; BEGIN
 IF _actor IS NULL OR _kind NOT IN ('rest','postgres','mysql','kafka_cdc','kafka','s3','adls') THEN RAISE EXCEPTION 'Invalid source'; END IF;
 INSERT INTO public.ingest_sources(owner_id,name,kind,target_table,config,token_hash) VALUES(_actor,_name,_kind,_target,_config,_hash) RETURNING id INTO sid;
 IF _kind='rest' THEN INSERT INTO public.ingest_source_secrets(source_id,headers) VALUES(sid,coalesce(_headers,'{}')); END IF;
 RETURN sid;
END $$;


ALTER FUNCTION "public"."create_ingest_source_verified"("_actor" "uuid", "_name" "text", "_kind" "text", "_target" "text", "_config" "jsonb", "_headers" "jsonb", "_hash" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."dataset_policy_decision"("_subject" "uuid", "_dataset" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'core'
    AS $$
DECLARE d core.datasets; roles public.app_role[]; has_pol boolean; filters jsonb; masks jsonb;
BEGIN
 SELECT * INTO d FROM core.datasets WHERE id = _dataset;
 IF d.id IS NULL OR NOT (d.owner_id = _subject OR (d.organization_id IS NOT NULL AND EXISTS (
   SELECT 1 FROM public.org_members m WHERE m.org_id = d.organization_id AND m.user_id = _subject))) THEN
  RETURN jsonb_build_object('allow', false);
 END IF;
 IF d.owner_id = _subject THEN
  RETURN jsonb_build_object('allow', true, 'rows', 'all', 'rowFilters', '[]'::jsonb, 'masks', '[]'::jsonb);
 END IF;
 SELECT coalesce(array_agg(role), '{}') INTO roles FROM public.user_roles WHERE user_id = _subject;
 SELECT EXISTS (SELECT 1 FROM public.row_policies p WHERE p.table_name = d.name
   AND (p.organization_id IS NULL OR p.organization_id = d.organization_id)) INTO has_pol;
 SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object('column', p.column_name, 'op', p.op, 'value', coalesce(p.value, ''))), '[]'::jsonb)
 INTO filters FROM public.row_policies p WHERE p.table_name = d.name AND p.role = ANY (roles)
   AND p.column_name IS NOT NULL AND p.op IN ('=','!=','in','>','<','>=','<=')
   AND (p.organization_id IS NULL OR p.organization_id = d.organization_id);
 SELECT coalesce(jsonb_agg(DISTINCT jsonb_build_object('column', a.column_name, 'mode', CASE WHEN a.masking = 'partial' THEN 'partial' ELSE 'full' END)), '[]'::jsonb)
 INTO masks FROM public.access_policies a WHERE (a.dataset = d.name OR a.dataset = '*') AND a.masking <> 'none'
   AND NOT (a.allowed_roles && roles) AND (a.organization_id IS NULL OR a.organization_id = d.organization_id);
 RETURN jsonb_build_object('allow', true,
  'rows', CASE WHEN NOT has_pol THEN 'all' WHEN jsonb_array_length(filters) = 0 THEN 'none' ELSE 'filtered' END,
  'rowFilters', CASE WHEN has_pol THEN filters ELSE '[]'::jsonb END, 'masks', masks);
END $$;


ALTER FUNCTION "public"."dataset_policy_decision"("_subject" "uuid", "_dataset" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."fail_ingest_partition_fenced"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_token" "uuid", "_error" "text") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
 PERFORM 1 FROM public.ingest_runs WHERE id=_run AND status IN ('planned','running') FOR UPDATE;
 IF NOT FOUND THEN RETURN false; END IF;
 UPDATE public.ingest_partitions SET status='failed',error=left(_error,500),lease_until=NULL,lease_token=NULL,updated_at=now() WHERE run_id=_run AND idx=_idx AND worker=_worker AND lease_token=_lease_token AND status='running' AND lease_until IS NOT NULL AND lease_until>=now();
 RETURN FOUND;
END $$;


ALTER FUNCTION "public"."fail_ingest_partition_fenced"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_token" "uuid", "_error" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."file_object_is_committed"("_name" "text") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$ SELECT EXISTS (SELECT 1 FROM public.file_tables ft WHERE left(_name,length(ft.manifest_prefix)+1)=ft.manifest_prefix||'/') $$;


ALTER FUNCTION "public"."file_object_is_committed"("_name" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."guard_ingest_credentials"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public'
    AS $$ BEGIN
 IF NEW.config ?| ARRAY['headers','password','token','secret','api_key','credentials'] THEN RAISE EXCEPTION 'Credentials must use the server-only secret store'; END IF;
 IF NEW.kind='rest' AND coalesce(NEW.config->>'url','') ~* '(^[^:]+://[^/]*@|[?&](token|key|api_key|password|secret)=)' THEN RAISE EXCEPTION 'Credentials cannot appear in URL'; END IF;
 RETURN NEW;
END $$;


ALTER FUNCTION "public"."guard_ingest_credentials"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."guard_resource_identity_update"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public'
    AS $$
BEGIN
 IF auth.role()='service_role' OR public.has_role(auth.uid(),'admin') THEN RETURN NEW; END IF;
 IF TG_TABLE_NAME='connections' THEN
  IF NEW.created_by IS DISTINCT FROM OLD.created_by THEN RAISE EXCEPTION 'creator ownership is immutable'; END IF;
 ELSE
  IF NEW.owner_id IS DISTINCT FROM OLD.owner_id THEN RAISE EXCEPTION 'catalog ownership is immutable'; END IF;
  IF NEW.organization_id IS DISTINCT FROM OLD.organization_id AND OLD.owner_id IS DISTINCT FROM auth.uid() THEN RAISE EXCEPTION 'only the owner may change sharing scope'; END IF;
 END IF;
 RETURN NEW;
END $$;


ALTER FUNCTION "public"."guard_resource_identity_update"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."handle_new_user"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
begin
  insert into public.profiles (id, email, display_name)
  values (new.id, new.email, coalesce(new.raw_user_meta_data->>'full_name', split_part(new.email, '@', 1)));
  return new;
end $$;


ALTER FUNCTION "public"."handle_new_user"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."has_entitlement"("_org" "uuid", "_feature" "text") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT (auth.uid() IS NULL OR public.is_org_member(_org) OR public.has_role(auth.uid(), 'admin'))
     AND EXISTS (SELECT 1 FROM public.entitlements e
                 WHERE e.organization_id = _org AND e.feature = _feature
                   AND (e.expires_at IS NULL OR e.expires_at > now()))
$$;


ALTER FUNCTION "public"."has_entitlement"("_org" "uuid", "_feature" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."has_role"("_user_id" "uuid", "_role" "public"."app_role") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  -- Signed-in callers may only probe their own roles; service role / internal callers (auth.uid() NULL) unrestricted.
  SELECT (auth.uid() IS NULL OR _user_id = auth.uid() OR EXISTS (SELECT 1 FROM public.user_roles a WHERE a.user_id = auth.uid() AND a.role = 'admin'))
     AND EXISTS (SELECT 1 FROM public.user_roles WHERE user_id = _user_id AND role = _role)
$$;


ALTER FUNCTION "public"."has_role"("_user_id" "uuid", "_role" "public"."app_role") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."heartbeat_ingest_partition"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_seconds" integer DEFAULT 60) RETURNS boolean
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  WITH u AS (UPDATE public.ingest_partitions SET lease_until = now() + make_interval(secs => _lease_seconds), heartbeat_at = now()
    WHERE run_id = _run AND idx = _idx AND worker = _worker AND status = 'running' AND lease_until >= now() RETURNING 1) SELECT EXISTS (SELECT 1 FROM u)
$$;


ALTER FUNCTION "public"."heartbeat_ingest_partition"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_seconds" integer) OWNER TO "postgres";


COMMENT ON FUNCTION "public"."heartbeat_ingest_partition"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_seconds" integer) IS 'DEPRECATED unsafe worker-name-only protocol; use fenced heartbeat';



CREATE OR REPLACE FUNCTION "public"."heartbeat_ingest_partition_fenced"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_token" "uuid", "_lease_seconds" integer DEFAULT 60) RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
 IF _lease_seconds IS NULL OR _lease_seconds NOT BETWEEN 10 AND 600 THEN RAISE EXCEPTION 'invalid lease duration'; END IF;
 PERFORM 1 FROM public.ingest_runs WHERE id=_run AND status IN ('planned','running') FOR UPDATE;
 IF NOT FOUND THEN RETURN false; END IF;
 UPDATE public.ingest_partitions SET lease_until=now()+make_interval(secs=>_lease_seconds),heartbeat_at=now(),updated_at=now() WHERE run_id=_run AND idx=_idx AND worker=_worker AND lease_token=_lease_token AND status='running' AND lease_until IS NOT NULL AND lease_until>=now();
 RETURN FOUND;
END $$;


ALTER FUNCTION "public"."heartbeat_ingest_partition_fenced"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_token" "uuid", "_lease_seconds" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."is_org_member"("_org" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT _org IS NOT NULL AND EXISTS (SELECT 1 FROM public.org_members m WHERE m.org_id = _org AND m.user_id = auth.uid())
$$;


ALTER FUNCTION "public"."is_org_member"("_org" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."is_steward_for"("_user" "uuid", "_org" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT auth.uid() IS NOT NULL AND (public.has_role(auth.uid(), 'admin') OR (public.has_role(auth.uid(), 'steward')
    AND (public.is_org_member(_org) OR public.shares_org_with(_user) OR _user = auth.uid())))
$$;


ALTER FUNCTION "public"."is_steward_for"("_user" "uuid", "_org" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."log_admin_change"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
  INSERT INTO public.admin_audit (actor, action, target, detail)
  VALUES (auth.uid(), TG_TABLE_NAME || '.' || lower(TG_OP), TG_TABLE_NAME,
    jsonb_build_object('new', CASE WHEN TG_OP <> 'DELETE' THEN to_jsonb(NEW) END, 'old', CASE WHEN TG_OP <> 'INSERT' THEN to_jsonb(OLD) END));
  RETURN coalesce(NEW, OLD);
END $$;


ALTER FUNCTION "public"."log_admin_change"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."mask_text"("_v" "text", "_mode" "text") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public'
    AS $$
  SELECT CASE WHEN _v IS NULL THEN NULL WHEN _mode = 'partial' THEN left(_v, 2) || '••••' ELSE '••••' END
$$;


ALTER FUNCTION "public"."mask_text"("_v" "text", "_mode" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."publish_ingest_run"("_run" "uuid") RETURNS bigint
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE r public.ingest_runs; n bigint; pending int; total int; em text;
BEGIN
  SELECT * INTO r FROM public.ingest_runs WHERE id = _run FOR UPDATE;
  IF r.id IS NULL OR r.owner_id IS DISTINCT FROM auth.uid() THEN RAISE EXCEPTION 'not found'; END IF;
  IF r.status IN ('cancelled','published') THEN RAISE EXCEPTION 'run is %', r.status; END IF;
  IF r.organization_id IS NOT NULL AND NOT public.is_org_member(r.organization_id) THEN RAISE EXCEPTION 'not a member of the run organization'; END IF;
  SELECT count(*), count(*) FILTER (WHERE status <> 'done') INTO total, pending FROM public.ingest_partitions WHERE run_id = _run;
  IF total = 0 THEN RAISE EXCEPTION 'run has no partitions'; END IF;
  IF pending > 0 THEN RAISE EXCEPTION '% partitions not finished', pending; END IF;
  SELECT count(*) INTO n FROM public.ingest_rows WHERE run_id = _run;
  IF r.expected_rows > 0 AND n <> r.expected_rows THEN RAISE EXCEPTION 'row count % does not match expected %', n, r.expected_rows; END IF;
  PERFORM pg_advisory_xact_lock(hashtext('publish_ingest_scope:'||r.target_table));
 IF EXISTS (SELECT 1 FROM public.ingest_tables t WHERE t.owner_id=r.owner_id AND t.name=r.target_table AND t.organization_id IS DISTINCT FROM r.organization_id) THEN RAISE EXCEPTION 'published table scope conflict'; END IF;
 IF EXISTS (SELECT 1 FROM public.catalog_entries c WHERE c.table_name=r.target_table AND (c.owner_id IS DISTINCT FROM r.owner_id OR c.organization_id IS DISTINCT FROM r.organization_id)) THEN RAISE EXCEPTION 'catalog identity scope conflict'; END IF;
 INSERT INTO public.ingest_tables(owner_id, name, current_run_id, row_count, published_at, organization_id)
  VALUES (r.owner_id, r.target_table, _run, n, now(), r.organization_id)
  ON CONFLICT (owner_id, name) DO UPDATE SET current_run_id = EXCLUDED.current_run_id, row_count = EXCLUDED.row_count, published_at = now();
  UPDATE public.ingest_runs SET status = 'published', published_at = now(), finished_at = coalesce(finished_at, now()), written_rows = n WHERE id = _run;
  SELECT email INTO em FROM public.profiles WHERE id = r.owner_id;
  INSERT INTO public.catalog_entries(table_name, owner_email, description, domain, tags, updated_by, updated_at, owner_id, organization_id)
  VALUES (r.target_table, em, format('Ingested by partitioned run %s (%s rows, source %s)', left(_run::text, 8), n, r.source_kind), 'ingestion', ARRAY['ingested', r.source_kind], r.owner_id, now(), r.owner_id, r.organization_id)
  ON CONFLICT (table_name) DO UPDATE SET description = EXCLUDED.description, updated_at = now(), tags = EXCLUDED.tags
    WHERE public.catalog_entries.owner_id = r.owner_id;
  RETURN n;
END $$;


ALTER FUNCTION "public"."publish_ingest_run"("_run" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."read_ingest_table"("_name" "text", "_after" "text" DEFAULT NULL::"text", "_limit" integer DEFAULT 200) RETURNS TABLE("pk" "text", "data" "jsonb")
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE uid uuid := auth.uid(); privileged boolean; my_roles public.app_role[]; t record; rr record; d jsonb; keep boolean; pol record; ap record; has_pol boolean; scanned int := 0; returned_bytes bigint := 0; n int := 0; lim int := least(greatest(_limit,1),1000);
BEGIN
  IF uid IS NULL THEN RETURN; END IF;
  privileged := public.has_role(uid,'admin');
  SELECT coalesce(array_agg(ur.role), '{}') INTO my_roles FROM public.user_roles ur WHERE ur.user_id = uid;
  SELECT * INTO t FROM public.ingest_tables it WHERE it.name = _name AND (it.owner_id = uid OR public.is_org_member(it.organization_id) OR privileged)
   ORDER BY (it.owner_id = uid) DESC, it.published_at DESC LIMIT 1;
  IF t.name IS NULL THEN RETURN; END IF;
  SELECT EXISTS (SELECT 1 FROM public.row_policies p WHERE p.table_name = _name AND (p.organization_id IS NULL OR p.organization_id = t.organization_id)) INTO has_pol;
  FOR rr IN SELECT r.pk, r.data FROM public.ingest_rows r WHERE r.run_id = t.current_run_id AND (_after IS NULL OR r.pk > _after) ORDER BY r.pk LOOP
    scanned := scanned + 1;
 IF scanned > 10000 THEN RAISE EXCEPTION 'filtered scan budget exceeded; use a narrower governed query'; END IF;
 d := rr.data; keep := true;
    IF NOT privileged AND t.owner_id <> uid THEN
      IF has_pol THEN
        keep := false;
        FOR pol IN SELECT * FROM public.row_policies p WHERE p.table_name = _name AND p.role = ANY (my_roles) AND (p.organization_id IS NULL OR p.organization_id = t.organization_id) LOOP
          IF public.row_passes(d, pol.column_name, pol.op, pol.value) THEN keep := true; EXIT; END IF;
        END LOOP;
      END IF;
      IF keep THEN
        FOR ap IN SELECT * FROM public.access_policies a WHERE (a.dataset = _name OR a.dataset = '*') AND a.masking <> 'none' AND NOT (a.allowed_roles && my_roles)
                  AND (a.organization_id IS NULL OR a.organization_id = t.organization_id) LOOP
          IF d ? ap.column_name THEN d := jsonb_set(d, ARRAY[ap.column_name], to_jsonb(public.mask_text(d ->> ap.column_name, ap.masking))); END IF;
        END LOOP;
      END IF;
    END IF;
    IF keep THEN returned_bytes := returned_bytes + octet_length(d::text);
 IF returned_bytes > 8388608 THEN RAISE EXCEPTION 'ingest page exceeds byte budget'; END IF;
 pk := rr.pk; data := d; RETURN NEXT; n := n + 1; EXIT WHEN n >= lim; END IF;
  END LOOP;
END $$;


ALTER FUNCTION "public"."read_ingest_table"("_name" "text", "_after" "text", "_limit" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."register_ingest_landing_batch"("_actor" "uuid", "_source" "uuid", "_key" "text", "_sha" "text", "_rows" integer, "_mode" "text", "_batch" "uuid") RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $_$
DECLARE src public.ingest_sources; out_id uuid;
BEGIN
 IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'service only'; END IF;
 SELECT * INTO src FROM public.ingest_sources WHERE id=_source AND owner_id=_actor FOR UPDATE;
 IF NOT FOUND OR (src.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members WHERE org_id=src.organization_id AND user_id=_actor)) THEN RAISE EXCEPTION 'source not authorized'; END IF;
 IF _key IS NULL OR _key <> _actor::text||'/published/'||_batch::text||'/landing.ndjson' OR _sha IS NULL OR _sha !~ '^[a-f0-9]{64}$' OR _rows IS NULL OR _rows NOT BETWEEN 0 AND 10000 OR _mode NOT IN ('replace','append','upsert','cdc') THEN RAISE EXCEPTION 'invalid landing batch'; END IF;
 INSERT INTO public.ingest_landing_batches(source_id,owner_id,organization_id,object_key,sha256,row_count,mode,batch_key) VALUES(_source,_actor,src.organization_id,_key,_sha,_rows,_mode,_batch) ON CONFLICT(source_id,batch_key) DO UPDATE SET batch_key=EXCLUDED.batch_key WHERE public.ingest_landing_batches.sha256=EXCLUDED.sha256 AND public.ingest_landing_batches.mode=EXCLUDED.mode AND public.ingest_landing_batches.row_count=EXCLUDED.row_count RETURNING id INTO out_id;
 IF out_id IS NULL THEN RAISE EXCEPTION 'batch key payload conflict'; END IF;
 UPDATE public.ingest_sources SET last_run_at=now(),last_status='landed_pending_format_commit',last_rows=_rows WHERE id=_source;
 RETURN out_id;
END $_$;


ALTER FUNCTION "public"."register_ingest_landing_batch"("_actor" "uuid", "_source" "uuid", "_key" "text", "_sha" "text", "_rows" integer, "_mode" "text", "_batch" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."register_personal_verified_file_table"("_owner" "uuid", "_name" "text", "_prefix" "text", "_columns" "text"[], "_parts" integer, "_rows" bigint, "_source_sha" "text", "_manifest_sha" "text") RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
 IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'service only'; END IF;
 RETURN public.register_verified_file_table(_owner,NULL,_name,_prefix,_columns,_parts,_rows,_source_sha,_manifest_sha);
END $$;


ALTER FUNCTION "public"."register_personal_verified_file_table"("_owner" "uuid", "_name" "text", "_prefix" "text", "_columns" "text"[], "_parts" integer, "_rows" bigint, "_source_sha" "text", "_manifest_sha" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."register_verified_file_table"("_owner" "uuid", "_org" "uuid", "_name" "text", "_prefix" "text", "_columns" "text"[], "_parts" integer, "_rows" bigint, "_source_sha" "text", "_manifest_sha" "text") RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $_$
DECLARE id_out uuid;
BEGIN
 IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'service only'; END IF;
 IF _org IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.org_members WHERE org_id=_org AND user_id=_owner) THEN RAISE EXCEPTION 'organization membership revoked'; END IF;
 IF _name IS NULL OR _name !~ '^[a-z_][a-z0-9_]{0,62}$' OR _prefix IS NULL OR _prefix !~ ('^'||_owner::text||'/published/[a-f0-9-]{36}$') OR _parts<1 OR _parts>10000 OR _rows<0 OR _source_sha !~ '^[a-f0-9]{64}$' OR _manifest_sha !~ '^[a-f0-9]{64}$' OR _manifest_sha IS NULL OR cardinality(_columns)<1 THEN RAISE EXCEPTION 'invalid verified publication'; END IF;
 INSERT INTO public.file_tables(owner_id,organization_id,name,manifest_prefix,columns,part_count,row_count,source_sha256,manifest_sha256) VALUES (_owner,_org,_name,_prefix,_columns,_parts,_rows,_source_sha,_manifest_sha) RETURNING id INTO id_out;
 INSERT INTO public.audit_log(actor_id,action,target,details) VALUES (_owner,'file.publish',id_out::text,jsonb_build_object('organization',_org,'name',_name,'parts',_parts,'rows',_rows));
 RETURN id_out;
END $_$;


ALTER FUNCTION "public"."register_verified_file_table"("_owner" "uuid", "_org" "uuid", "_name" "text", "_prefix" "text", "_columns" "text"[], "_parts" integer, "_rows" bigint, "_source_sha" "text", "_manifest_sha" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."reject_row_mutation"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public'
    AS $$
BEGIN
  RAISE EXCEPTION 'Rows in % are immutable', TG_TABLE_NAME;
END $$;


ALTER FUNCTION "public"."reject_row_mutation"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."row_passes"("_row" "jsonb", "_col" "text", "_op" "text", "_val" "text") RETURNS boolean
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public'
    AS $$
DECLARE v text := _row ->> _col;
BEGIN
  IF _col IS NULL THEN RETURN false; END IF;
  IF _op = '=' THEN RETURN v = _val;
  ELSIF _op = '!=' THEN RETURN v IS DISTINCT FROM _val;
  ELSIF _op = 'in' THEN RETURN v = ANY (string_to_array(_val, ','));
  ELSIF _op IN ('>','<','>=','<=') THEN
    BEGIN
      RETURN CASE _op WHEN '>' THEN v::numeric > _val::numeric WHEN '<' THEN v::numeric < _val::numeric
        WHEN '>=' THEN v::numeric >= _val::numeric ELSE v::numeric <= _val::numeric END;
    EXCEPTION WHEN others THEN RETURN false; END;
  END IF;
  RETURN false;
END $$;


ALTER FUNCTION "public"."row_passes"("_row" "jsonb", "_col" "text", "_op" "text", "_val" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."schema_register_version"("_actor" "uuid", "_organization" "uuid", "_name" "text", "_schema" "jsonb", "_fingerprint" "text", "_expected_latest" integer, "_compatibility" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE s public.schema_subjects; v public.schema_versions; latest integer;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 IF NOT (public.has_role(_actor,'admin') OR public.has_role(_actor,'engineer') OR public.has_role(_actor,'steward')) THEN RAISE EXCEPTION 'Schema registration requires engineer, steward or admin'; END IF;
 IF _organization IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members WHERE org_id=_organization AND user_id=_actor) THEN RAISE EXCEPTION 'Organization membership required'; END IF;
 INSERT INTO public.schema_subjects(owner_id,organization_id,name,compatibility) VALUES(_actor,_organization,_name,coalesce(_compatibility,'BACKWARD')) ON CONFLICT DO NOTHING;
 SELECT * INTO s FROM public.schema_subjects WHERE coalesce(organization_id,owner_id)=coalesce(_organization,_actor) AND name=_name FOR UPDATE;
 IF s.id IS NULL OR (s.organization_id IS NULL AND s.owner_id<>_actor) THEN RAISE EXCEPTION 'Subject not accessible'; END IF;
 SELECT * INTO v FROM public.schema_versions WHERE subject_id=s.id AND fingerprint=_fingerprint;
 IF FOUND THEN RETURN jsonb_build_object('subject_id',s.id,'version',v.version,'id',v.id,'created',false); END IF;
 SELECT coalesce(max(version),0) INTO latest FROM public.schema_versions WHERE subject_id=s.id;
 IF latest<>_expected_latest THEN RAISE EXCEPTION 'schema_registry_conflict'; END IF;
 INSERT INTO public.schema_versions(subject_id,version,schema,fingerprint,created_by) VALUES(s.id,latest+1,_schema,_fingerprint,_actor) RETURNING * INTO v;
 RETURN jsonb_build_object('subject_id',s.id,'version',v.version,'id',v.id,'created',true);
END $$;


ALTER FUNCTION "public"."schema_register_version"("_actor" "uuid", "_organization" "uuid", "_name" "text", "_schema" "jsonb", "_fingerprint" "text", "_expected_latest" integer, "_compatibility" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."secure_tables"() RETURNS TABLE("id" "uuid", "name" "text", "columns" "jsonb", "rows" "jsonb", "row_count" integer, "owner_id" "uuid", "created_at" timestamp with time zone, "filtered" boolean, "masked" "text"[])
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
  uid uuid := auth.uid(); privileged boolean; my_roles public.app_role[];
  total_bytes bigint := 0; total_rows bigint := 0; t record; r jsonb; out_rows jsonb; keep boolean; pol record; has_pol boolean; m text[]; ap record;
BEGIN
  IF uid IS NULL THEN RETURN; END IF;
  privileged := public.has_role(uid,'admin');
  SELECT coalesce(array_agg(ur.role), '{}') INTO my_roles FROM public.user_roles ur WHERE ur.user_id = uid;
  FOR t IN SELECT * FROM public.user_tables ut
           WHERE ut.owner_id = uid OR privileged OR public.is_org_member(ut.organization_id)
           ORDER BY ut.created_at DESC LOOP
    SELECT EXISTS (SELECT 1 FROM public.row_policies p WHERE p.table_name = t.name AND (p.organization_id IS NULL OR p.organization_id = t.organization_id)) INTO has_pol;
    IF jsonb_typeof(t.rows) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'invalid legacy row array'; END IF;
 total_bytes := total_bytes + octet_length(t.rows::text);
 total_rows := total_rows + jsonb_array_length(t.rows);
 IF jsonb_array_length(t.rows)>5000 OR total_rows>20000 OR total_bytes>16777216 THEN RAISE EXCEPTION 'legacy warehouse exceeds read budget; migrate tables to paged storage'; END IF;
 out_rows := '[]'::jsonb; m := '{}';
    FOR r IN SELECT * FROM jsonb_array_elements(t.rows) LOOP
      keep := true;
      IF has_pol AND NOT privileged AND t.owner_id <> uid THEN
        keep := false;
        FOR pol IN SELECT * FROM public.row_policies p WHERE p.table_name = t.name AND p.role = ANY (my_roles) AND (p.organization_id IS NULL OR p.organization_id = t.organization_id) LOOP
          IF public.row_passes(r, pol.column_name, pol.op, pol.value) THEN keep := true; EXIT; END IF;
        END LOOP;
      END IF;
      IF keep THEN
        IF NOT privileged AND t.owner_id <> uid THEN
          FOR ap IN SELECT * FROM public.access_policies a WHERE (a.dataset = t.name OR a.dataset = '*') AND a.masking <> 'none' AND NOT (a.allowed_roles && my_roles)
                    AND (a.organization_id IS NULL OR a.organization_id = t.organization_id) LOOP
            IF r ? ap.column_name THEN
              r := jsonb_set(r, ARRAY[ap.column_name], to_jsonb(public.mask_text(r ->> ap.column_name, ap.masking)));
              IF NOT ap.column_name = ANY (m) THEN m := m || ap.column_name; END IF;
            END IF;
          END LOOP;
        END IF;
        out_rows := out_rows || jsonb_build_array(r);
      END IF;
    END LOOP;
    id := t.id; name := t.name; columns := t.columns; rows := out_rows; row_count := jsonb_array_length(out_rows);
    owner_id := t.owner_id; created_at := t.created_at; filtered := has_pol AND NOT privileged AND t.owner_id <> uid; masked := m;
    RETURN NEXT;
  END LOOP;
END $$;


ALTER FUNCTION "public"."secure_tables"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."serve_manifest"("_subject" "uuid", "_names" "text"[]) RETURNS TABLE("dataset_id" "uuid", "name" "text", "layer" "text", "version_id" "uuid", "bucket" "text", "object_key" "text", "sha256" "text", "byte_count" bigint, "row_count" bigint, "schema" "jsonb", "produced_by" "jsonb", "evidence_class" "text", "created_at" timestamp with time zone, "policy" "jsonb")
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'core'
    AS $$
 WITH visible AS (
  SELECT d.*, d.owner_id = _subject AS own FROM core.datasets d
  WHERE d.layer = 'gold' AND d.current_version_id IS NOT NULL AND lower(d.name) = ANY(_names)
   AND (d.owner_id = _subject OR (d.organization_id IS NOT NULL AND EXISTS (
    SELECT 1 FROM public.org_members m WHERE m.org_id = d.organization_id AND m.user_id = _subject)))
 ), ranked AS (
  SELECT v.*, count(*) FILTER (WHERE NOT v.own) OVER (PARTITION BY lower(v.name)) AS others,
   bool_or(v.own) OVER (PARTITION BY lower(v.name)) AS has_own FROM visible v
 )
 SELECT r.id, r.name, r.layer, x.id, x.bucket, x.object_key, x.sha256, x.byte_count, x.row_count, x.schema, x.produced_by, x.evidence_class, x.created_at,
  public.dataset_policy_decision(_subject, r.id)
 FROM ranked r JOIN core.dataset_versions x ON x.id = r.current_version_id
 WHERE (r.own AND NOT EXISTS (SELECT 1 FROM ranked o WHERE o.own AND lower(o.name) = lower(r.name) AND o.id <> r.id))
    OR (NOT r.has_own AND r.others = 1)
 ORDER BY r.name
$$;


ALTER FUNCTION "public"."serve_manifest"("_subject" "uuid", "_names" "text"[]) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."shares_org_with"("_user" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT auth.uid() IS NOT NULL AND _user IS NOT NULL AND EXISTS (
    SELECT 1 FROM public.org_members a JOIN public.org_members b ON a.org_id = b.org_id
    WHERE a.user_id = auth.uid() AND b.user_id = _user)
$$;


ALTER FUNCTION "public"."shares_org_with"("_user" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."validate_file_table_scope"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public'
    AS $_$
BEGIN
 IF NEW.manifest_prefix IS NULL OR NEW.manifest_prefix !~ '^[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)+$' OR NEW.manifest_prefix ~ '(^|/)(\.|\.\.)(/|$)' OR split_part(NEW.manifest_prefix,'/',1)<>NEW.owner_id::text THEN RAISE EXCEPTION 'invalid manifest scope'; END IF;
 IF NEW.manifest_sha256 IS NOT NULL AND NEW.manifest_sha256 !~ '^[a-f0-9]{64}$' THEN RAISE EXCEPTION 'invalid manifest digest'; END IF;
 RETURN NEW;
END $_$;


ALTER FUNCTION "public"."validate_file_table_scope"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."validate_partition_creation"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public'
    AS $$
BEGIN
 IF auth.role()='authenticated' AND (NEW.status<>'pending' OR NEW.worker IS NOT NULL OR NEW.lease_token IS NOT NULL OR NEW.lease_until IS NOT NULL OR NEW.checkpoint IS NOT NULL OR NEW.attempts<>0 OR NEW.rows_written<>0 OR NEW.claim_version<>0) THEN RAISE EXCEPTION 'partition execution state is service managed'; END IF;
 IF NOT EXISTS (SELECT 1 FROM public.ingest_runs r WHERE r.id=NEW.run_id AND r.owner_id=NEW.owner_id) THEN RAISE EXCEPTION 'invalid partition owner'; END IF;
 RETURN NEW;
END $$;


ALTER FUNCTION "public"."validate_partition_creation"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."verify_admin_audit"() RETURNS TABLE("ok" boolean, "checked" integer, "broken_at" bigint)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE r record; prev text := NULL; n int := 0; expect text;
BEGIN
  IF NOT public.has_role(auth.uid(),'admin') THEN RAISE EXCEPTION 'forbidden'; END IF;
  FOR r IN SELECT * FROM public.admin_audit ORDER BY id LOOP
    n := n + 1;
    IF r.hash_alg = 'sha256' THEN
      expect := encode(extensions.digest(coalesce(r.prev_hash,'') || '|' || coalesce(r.actor::text,'') || '|' || r.action || '|' || coalesce(r.target,'') || '|' || r.detail::text || '|' || r.created_at::text, 'sha256'), 'hex');
    ELSE
      expect := md5(coalesce(r.prev_hash,'') || coalesce(r.actor::text,'') || r.action || coalesce(r.target,'') || r.detail::text || r.created_at::text);
    END IF;
    IF r.prev_hash IS DISTINCT FROM prev OR r.hash <> expect THEN
      ok := false; checked := n; broken_at := r.id; RETURN NEXT; RETURN;
    END IF;
    prev := r.hash;
  END LOOP;
  ok := true; checked := n; broken_at := NULL; RETURN NEXT;
END $$;


ALTER FUNCTION "public"."verify_admin_audit"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."write_ingest_batch"("_run" "uuid", "_idx" integer, "_worker" "text", "_rows" "jsonb", "_checkpoint" "jsonb", "_done" boolean, "_lease_seconds" integer DEFAULT 60) RETURNS bigint
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE p public.ingest_partitions; pkc text[]; enc text; st text; n bigint;
BEGIN
  SELECT * INTO p FROM public.ingest_partitions WHERE run_id = _run AND idx = _idx FOR UPDATE;
  IF p.id IS NULL THEN RAISE EXCEPTION 'partition not found'; END IF;
  IF p.status <> 'running' OR p.worker IS DISTINCT FROM _worker OR p.lease_until < now() THEN RAISE EXCEPTION 'lease lost'; END IF;
  SELECT pk_columns, pk_encoding, status INTO pkc, enc, st FROM public.ingest_runs WHERE id = _run;
  IF st IN ('cancelled','published') THEN RAISE EXCEPTION 'run is %', st; END IF;
  IF enc <> 'json_v1' THEN RAISE EXCEPTION 'run uses % key encoding; restart it as a new run', enc; END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(_rows) e, unnest(pkc) c WHERE NOT (e.value ? c) OR e.value -> c = 'null'::jsonb) THEN
    RAISE EXCEPTION 'primary key column missing or null';
  END IF;
  INSERT INTO public.ingest_rows(run_id, pk, partition_idx, data)
  SELECT _run, CASE WHEN array_length(pkc,1) = 1 THEN e.value ->> pkc[1]
                    ELSE (SELECT jsonb_agg(e.value -> c ORDER BY o) FROM unnest(pkc) WITH ORDINALITY AS u(c, o))::text END, _idx, e.value
    FROM jsonb_array_elements(_rows) e
  ON CONFLICT (run_id, pk) DO UPDATE SET data = EXCLUDED.data, partition_idx = EXCLUDED.partition_idx;
  SELECT count(*) INTO n FROM public.ingest_rows WHERE run_id = _run AND partition_idx = _idx;
  UPDATE public.ingest_partitions SET checkpoint = _checkpoint, rows_written = n, batches = batches + 1,
    status = CASE WHEN _done THEN 'done' ELSE 'running' END, finished_at = CASE WHEN _done THEN now() END,
    lease_until = now() + make_interval(secs => _lease_seconds), heartbeat_at = now(), updated_at = now()
   WHERE id = p.id;
  RETURN n;
END $$;


ALTER FUNCTION "public"."write_ingest_batch"("_run" "uuid", "_idx" integer, "_worker" "text", "_rows" "jsonb", "_checkpoint" "jsonb", "_done" boolean, "_lease_seconds" integer) OWNER TO "postgres";


COMMENT ON FUNCTION "public"."write_ingest_batch"("_run" "uuid", "_idx" integer, "_worker" "text", "_rows" "jsonb", "_checkpoint" "jsonb", "_done" boolean, "_lease_seconds" integer) IS 'DEPRECATED internal implementation; only called under token-checked wrapper locks';



CREATE OR REPLACE FUNCTION "public"."write_ingest_batch_fenced"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_token" "uuid", "_rows" "jsonb", "_checkpoint" "jsonb", "_done" boolean, "_lease_seconds" integer DEFAULT 60) RETURNS bigint
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
 IF _lease_seconds IS NULL OR _lease_seconds NOT BETWEEN 10 AND 600 OR _done IS NULL OR jsonb_typeof(_rows) IS DISTINCT FROM 'array' OR jsonb_typeof(_checkpoint) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'invalid batch'; END IF;
 IF jsonb_array_length(_rows)>10000 OR octet_length(_rows::text)>8388608 THEN RAISE EXCEPTION 'batch exceeds limits'; END IF;
 PERFORM 1 FROM public.ingest_runs r WHERE r.id=_run AND r.status IN ('planned','running')
   AND (r.source_id IS NULL OR EXISTS (SELECT 1 FROM public.ingest_sources s WHERE s.id = r.source_id AND s.enabled)) FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'run not active'; END IF;
 PERFORM 1 FROM public.ingest_partitions WHERE run_id=_run AND idx=_idx AND status='running' AND worker=_worker AND lease_token=_lease_token AND lease_until IS NOT NULL AND lease_until>=now() FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'lease lost'; END IF;
 RETURN public.write_ingest_batch(_run,_idx,_worker,_rows,_checkpoint,_done,_lease_seconds);
END $$;


ALTER FUNCTION "public"."write_ingest_batch_fenced"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_token" "uuid", "_rows" "jsonb", "_checkpoint" "jsonb", "_done" boolean, "_lease_seconds" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."yield_ingest_partition_fenced"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_token" "uuid") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
 PERFORM 1 FROM public.ingest_runs WHERE id=_run AND status IN ('planned','running') FOR UPDATE;
 IF NOT FOUND THEN RETURN false; END IF;
 UPDATE public.ingest_partitions SET status='pending',lease_token=NULL,lease_until=NULL,worker=NULL,updated_at=now(),attempts=greatest(0,attempts-1) WHERE run_id=_run AND idx=_idx AND worker=_worker AND lease_token=_lease_token AND status='running' AND lease_until>=now();
 RETURN FOUND;
END $$;


ALTER FUNCTION "public"."yield_ingest_partition_fenced"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_token" "uuid") OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "core"."dataset_versions" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "dataset_id" "uuid" NOT NULL,
    "source_version_id" "uuid" NOT NULL,
    "format" "text" NOT NULL,
    "bucket" "text" NOT NULL,
    "object_key" "text" NOT NULL,
    "sha256" "text" NOT NULL,
    "byte_count" bigint NOT NULL,
    "row_count" bigint NOT NULL,
    "schema" "jsonb" NOT NULL,
    "produced_by" "jsonb" NOT NULL,
    "evidence_class" "text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "dataset_versions_byte_count_check" CHECK (("byte_count" >= 0)),
    CONSTRAINT "dataset_versions_evidence_class_check" CHECK (("evidence_class" = ANY (ARRAY['worker_attested'::"text", 'server_verified'::"text"]))),
    CONSTRAINT "dataset_versions_format_check" CHECK (("format" = ANY (ARRAY['ndjson'::"text", 'parquet'::"text", 'iceberg'::"text"]))),
    CONSTRAINT "dataset_versions_row_count_check" CHECK (("row_count" >= 0)),
    CONSTRAINT "dataset_versions_sha256_check" CHECK (("sha256" ~ '^[a-f0-9]{64}$'::"text"))
);


ALTER TABLE "core"."dataset_versions" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "core"."datasets" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "owner_id" "uuid" NOT NULL,
    "organization_id" "uuid",
    "namespace" "text" NOT NULL,
    "name" "text" NOT NULL,
    "layer" "text" NOT NULL,
    "source_id" "uuid" NOT NULL,
    "current_version_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "datasets_layer_check" CHECK (("layer" = ANY (ARRAY['bronze'::"text", 'silver'::"text", 'gold'::"text", 'external'::"text"]))),
    CONSTRAINT "datasets_name_check" CHECK (("name" ~ '^[A-Za-z_][A-Za-z0-9_]{0,62}$'::"text")),
    CONSTRAINT "datasets_namespace_check" CHECK (("namespace" = 'transform'::"text"))
);


ALTER TABLE "core"."datasets" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."access_policies" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "dataset" "text" NOT NULL,
    "column_name" "text" NOT NULL,
    "classification" "text" DEFAULT 'internal'::"text" NOT NULL,
    "masking" "text" DEFAULT 'none'::"text" NOT NULL,
    "allowed_roles" "public"."app_role"[] DEFAULT '{admin}'::"public"."app_role"[] NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "organization_id" "uuid"
);


ALTER TABLE "public"."access_policies" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."admin_audit" (
    "id" bigint NOT NULL,
    "actor" "uuid",
    "action" "text" NOT NULL,
    "target" "text",
    "detail" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "prev_hash" "text",
    "hash" "text",
    "hash_alg" "text" DEFAULT 'sha256'::"text" NOT NULL
);


ALTER TABLE "public"."admin_audit" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."admin_audit_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE "public"."admin_audit_id_seq" OWNER TO "postgres";


ALTER SEQUENCE "public"."admin_audit_id_seq" OWNED BY "public"."admin_audit"."id";



CREATE TABLE IF NOT EXISTS "public"."alert_settings" (
    "user_id" "uuid" NOT NULL,
    "alert_email" "text",
    "notify_job_failures" boolean DEFAULT true NOT NULL,
    "notify_approvals" boolean DEFAULT true NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."alert_settings" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."audit_log" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "actor_id" "uuid" NOT NULL,
    "actor_email" "text",
    "action" "text" NOT NULL,
    "target" "text",
    "details" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."audit_log" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."change_requests" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "title" "text" NOT NULL,
    "description" "text" DEFAULT ''::"text" NOT NULL,
    "language" "text" DEFAULT 'sql'::"text" NOT NULL,
    "code" "text" NOT NULL,
    "target" "text" NOT NULL,
    "status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "validation" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "submitted_by" "uuid" NOT NULL,
    "submitter_email" "text",
    "reviewed_by" "uuid",
    "reviewer_email" "text",
    "review_note" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "reviewed_at" timestamp with time zone
);


ALTER TABLE "public"."change_requests" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."connect_attempts" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "run_id" "uuid" NOT NULL,
    "attempt" integer NOT NULL,
    "token" "uuid" NOT NULL,
    "status" "text" DEFAULT 'running'::"text" NOT NULL,
    "started_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "finished_at" timestamp with time zone,
    "error_code" "text"
);


ALTER TABLE "public"."connect_attempts" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."connect_commits" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "run_id" "uuid" NOT NULL,
    "stream_id" "uuid" NOT NULL,
    "sequence" integer NOT NULL,
    "batch_id" "uuid" NOT NULL,
    "manifest_key" "text" NOT NULL,
    "sha256" "text" NOT NULL,
    "row_count" integer NOT NULL,
    "byte_count" integer NOT NULL,
    "checkpoint" "jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."connect_commits" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."connect_connections" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "source_id" "uuid" NOT NULL,
    "owner_id" "uuid" NOT NULL,
    "organization_id" "uuid",
    "name" "text" NOT NULL,
    "source_version" "text" DEFAULT 'postgres-v1'::"text" NOT NULL,
    "destination_version" "text" DEFAULT 'bronze-v1'::"text" NOT NULL,
    "credential_ref" "text" NOT NULL,
    "enabled" boolean DEFAULT true NOT NULL,
    "interval_minutes" integer DEFAULT 0 NOT NULL,
    "next_run_at" timestamp with time zone,
    "health" "text" DEFAULT 'unchecked'::"text" NOT NULL,
    "discovered_catalog" "jsonb" DEFAULT '[]'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "dead_letter_budget" integer DEFAULT 100 NOT NULL,
    "operation_timeout_seconds" integer DEFAULT 3600 NOT NULL,
    CONSTRAINT "connect_connections_dead_letter_budget_check" CHECK ((("dead_letter_budget" >= 0) AND ("dead_letter_budget" <= 100000))),
    CONSTRAINT "connect_connections_interval_minutes_check" CHECK ((("interval_minutes" = 0) OR ("interval_minutes" >= 5))),
    CONSTRAINT "connect_connections_operation_timeout_seconds_check" CHECK ((("operation_timeout_seconds" >= 30) AND ("operation_timeout_seconds" <= 86400)))
);


ALTER TABLE "public"."connect_connections" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."connect_dead_letters" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "connection_id" "uuid" NOT NULL,
    "stream_id" "uuid" NOT NULL,
    "run_id" "uuid" NOT NULL,
    "sequence" integer NOT NULL,
    "position" "text" NOT NULL,
    "error_code" "text" NOT NULL,
    "object_key" "text" NOT NULL,
    "line" integer NOT NULL,
    "status" "text" DEFAULT 'open'::"text" NOT NULL,
    "replay_run_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "resolved_at" timestamp with time zone,
    CONSTRAINT "connect_dead_letters_error_code_check" CHECK (("error_code" = ANY (ARRAY['deserialization_failed'::"text", 'schema_not_found'::"text", 'schema_incompatible'::"text", 'record_too_large'::"text", 'invalid_record'::"text"]))),
    CONSTRAINT "connect_dead_letters_line_check" CHECK (("line" >= 0)),
    CONSTRAINT "connect_dead_letters_position_check" CHECK ((("length"("position") >= 1) AND ("length"("position") <= 200))),
    CONSTRAINT "connect_dead_letters_status_check" CHECK (("status" = ANY (ARRAY['open'::"text", 'replayed'::"text", 'discarded'::"text"])))
);


ALTER TABLE "public"."connect_dead_letters" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."connect_events" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "connection_id" "uuid" NOT NULL,
    "run_id" "uuid",
    "level" "text" DEFAULT 'INFO'::"text" NOT NULL,
    "code" "text" NOT NULL,
    "actor_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "detail" "jsonb",
    CONSTRAINT "connect_events_detail_check" CHECK ((("detail" IS NULL) OR (("jsonb_typeof"("detail") = 'array'::"text") AND ("jsonb_array_length"("detail") <= 1000))))
);


ALTER TABLE "public"."connect_events" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."connect_run_streams" (
    "run_id" "uuid" NOT NULL,
    "stream_id" "uuid" NOT NULL,
    "initial_state" "jsonb" NOT NULL,
    "checkpoint" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "schema_json" "jsonb" NOT NULL,
    "sequence" integer DEFAULT 0 NOT NULL,
    "completed" boolean DEFAULT false NOT NULL,
    "generation" integer NOT NULL
);


ALTER TABLE "public"."connect_run_streams" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."connect_runs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "connection_id" "uuid" NOT NULL,
    "request_key" "uuid" NOT NULL,
    "operation" "text" NOT NULL,
    "status" "text" DEFAULT 'queued'::"text" NOT NULL,
    "attempts" integer DEFAULT 0 NOT NULL,
    "max_attempts" integer DEFAULT 3 NOT NULL,
    "lease_token" "uuid",
    "lease_until" timestamp with time zone,
    "next_attempt_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "rows_committed" bigint DEFAULT 0 NOT NULL,
    "bytes_committed" bigint DEFAULT 0 NOT NULL,
    "error_code" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "finished_at" timestamp with time zone,
    CONSTRAINT "connect_runs_operation_check" CHECK (("operation" = ANY (ARRAY['check'::"text", 'discover'::"text", 'sync'::"text", 'reset'::"text", 'backfill'::"text", 'replay'::"text"]))),
    CONSTRAINT "connect_runs_status_check" CHECK (("status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'retry_wait'::"text", 'succeeded'::"text", 'failed'::"text", 'cancelled'::"text"])))
);


ALTER TABLE "public"."connect_runs" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."connect_streams" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "connection_id" "uuid" NOT NULL,
    "namespace" "text" NOT NULL,
    "name" "text" NOT NULL,
    "primary_key" "text" NOT NULL,
    "cursor_field" "text" NOT NULL,
    "sync_mode" "text" DEFAULT 'incremental'::"text" NOT NULL,
    "destination_mode" "text" DEFAULT 'append'::"text" NOT NULL,
    "schema_json" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "schema_policy" "text" DEFAULT 'fail'::"text" NOT NULL,
    "enabled" boolean DEFAULT true NOT NULL,
    "state" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "state_generation" integer DEFAULT 0 NOT NULL,
    "last_success_at" timestamp with time zone,
    "key_columns" "text"[] NOT NULL,
    CONSTRAINT "connect_stream_key_columns" CHECK (((("cardinality"("key_columns") >= 1) AND ("cardinality"("key_columns") <= 16)) AND ("array_to_string"("key_columns", ','::"text") = "primary_key"))),
    CONSTRAINT "connect_streams_destination_mode_check" CHECK (("destination_mode" = 'append'::"text")),
    CONSTRAINT "connect_streams_schema_policy_check" CHECK (("schema_policy" = 'fail'::"text")),
    CONSTRAINT "connect_streams_sync_mode_check" CHECK (("sync_mode" = ANY (ARRAY['full_refresh'::"text", 'incremental'::"text"])))
);


ALTER TABLE "public"."connect_streams" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."connection_secrets" (
    "connection_id" "uuid" NOT NULL,
    "secrets" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."connection_secrets" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."connections" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "kind" "text" NOT NULL,
    "name" "text" NOT NULL,
    "config" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "status" "text" DEFAULT 'untested'::"text" NOT NULL,
    "status_message" "text",
    "last_tested_at" timestamp with time zone,
    "created_by" "uuid" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "organization_id" "uuid"
);


ALTER TABLE "public"."connections" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."entitlements" (
    "organization_id" "uuid" NOT NULL,
    "feature" "text" NOT NULL,
    "licence_id" "text" NOT NULL,
    "expires_at" timestamp with time zone,
    "granted_by" "uuid",
    "granted_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "entitlements_feature_check" CHECK (("feature" ~ '^[a-z][a-z0-9_.-]{1,63}$'::"text")),
    CONSTRAINT "entitlements_licence_id_check" CHECK ((("length"("licence_id") >= 1) AND ("length"("licence_id") <= 100)))
);


ALTER TABLE "public"."entitlements" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."execution_engines" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "owner_id" "uuid" NOT NULL,
    "organization_id" "uuid",
    "name" "text" NOT NULL,
    "kind" "text" NOT NULL,
    "provider" "text" DEFAULT 'datafusion-http'::"text" NOT NULL,
    "ownership_mode" "text" DEFAULT 'customer-managed'::"text" NOT NULL,
    "endpoint" "text" NOT NULL,
    "enabled" boolean DEFAULT true NOT NULL,
    "capabilities" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "config" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "version" "text",
    "lifecycle_status" "text" DEFAULT 'registered'::"text" NOT NULL,
    "health" "text" DEFAULT 'untested'::"text" NOT NULL,
    "checked_at" timestamp with time zone,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "execution_engines_kind_check" CHECK (("kind" = ANY (ARRAY['datafusion'::"text", 'spark'::"text", 'trino'::"text"]))),
    CONSTRAINT "execution_engines_ownership_mode_check" CHECK (("ownership_mode" = ANY (ARRAY['embedded'::"text", 'meridian-managed'::"text", 'customer-managed'::"text", 'external-managed'::"text"])))
);


ALTER TABLE "public"."execution_engines" OWNER TO "postgres";


COMMENT ON TABLE "public"."execution_engines" IS 'Control-plane engine registration; endpoint is public HTTPS without credentials; runtime secrets are never client fields. No arbitrary lifecycle success is simulated.';



CREATE TABLE IF NOT EXISTS "public"."file_tables" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "owner_id" "uuid" DEFAULT "auth"."uid"() NOT NULL,
    "organization_id" "uuid",
    "name" "text" NOT NULL,
    "manifest_prefix" "text" NOT NULL,
    "columns" "text"[] DEFAULT '{}'::"text"[] NOT NULL,
    "part_count" integer NOT NULL,
    "row_count" bigint NOT NULL,
    "source_sha256" "text" NOT NULL,
    "published_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "manifest_sha256" "text",
    CONSTRAINT "file_tables_name_check" CHECK (("name" ~ '^[a-z_][a-z0-9_]{0,62}$'::"text")),
    CONSTRAINT "file_tables_part_count_check" CHECK (("part_count" >= 1)),
    CONSTRAINT "file_tables_row_count_check" CHECK (("row_count" >= 0)),
    CONSTRAINT "file_tables_source_sha256_check" CHECK (("source_sha256" ~ '^[0-9a-f]{64}$'::"text"))
);


ALTER TABLE "public"."file_tables" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."ingest_dead_letters" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "source_id" "uuid" NOT NULL,
    "owner_id" "uuid" NOT NULL,
    "organization_id" "uuid",
    "batch_id" "uuid" NOT NULL,
    "topic" "text" NOT NULL,
    "partition" integer NOT NULL,
    "offset" "text" NOT NULL,
    "error_code" "text" NOT NULL,
    "object_key" "text" NOT NULL,
    "line" integer NOT NULL,
    "status" "text" DEFAULT 'open'::"text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "payload_sha256" "text",
    CONSTRAINT "ingest_dead_letters_error_code_check" CHECK (("error_code" = ANY (ARRAY['invalid_json'::"text", 'unrecognized_event'::"text", 'invalid_event'::"text", 'schema_incompatible'::"text"]))),
    CONSTRAINT "ingest_dead_letters_line_check" CHECK (("line" >= 0)),
    CONSTRAINT "ingest_dead_letters_offset_check" CHECK (("offset" ~ '^[0-9]{1,20}$'::"text")),
    CONSTRAINT "ingest_dead_letters_partition_check" CHECK (("partition" >= 0)),
    CONSTRAINT "ingest_dead_letters_payload_sha256_check" CHECK ((("payload_sha256" IS NULL) OR ("payload_sha256" ~ '^[a-f0-9]{64}$'::"text"))),
    CONSTRAINT "ingest_dead_letters_status_check" CHECK (("status" = ANY (ARRAY['open'::"text", 'replayed'::"text", 'discarded'::"text"]))),
    CONSTRAINT "ingest_dead_letters_topic_check" CHECK ((("length"("topic") >= 1) AND ("length"("topic") <= 249)))
);


ALTER TABLE "public"."ingest_dead_letters" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."ingest_landing_batches" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "source_id" "uuid" NOT NULL,
    "owner_id" "uuid" NOT NULL,
    "organization_id" "uuid",
    "object_key" "text" NOT NULL,
    "sha256" "text" NOT NULL,
    "row_count" integer DEFAULT 0 NOT NULL,
    "mode" "text" NOT NULL,
    "batch_key" "uuid" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."ingest_landing_batches" OWNER TO "postgres";


COMMENT ON TABLE "public"."ingest_landing_batches" IS 'Immutable raw landing metadata. Not an Iceberg/Delta commit or a queryable analytical table. Legacy user_tables remain read-compatible.';



CREATE TABLE IF NOT EXISTS "public"."ingest_rows" (
    "run_id" "uuid" NOT NULL,
    "pk" "text" NOT NULL,
    "partition_idx" integer NOT NULL,
    "data" "jsonb" NOT NULL
);


ALTER TABLE "public"."ingest_rows" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."ingest_runs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "owner_id" "uuid" NOT NULL,
    "source_id" "uuid",
    "source_kind" "text" NOT NULL,
    "target_table" "text" NOT NULL,
    "plan" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "status" "text" DEFAULT 'planned'::"text" NOT NULL,
    "batch_size" integer DEFAULT 1000 NOT NULL,
    "concurrency" integer DEFAULT 4 NOT NULL,
    "expected_rows" bigint DEFAULT 0 NOT NULL,
    "written_rows" bigint DEFAULT 0 NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "started_at" timestamp with time zone,
    "finished_at" timestamp with time zone,
    "published_at" timestamp with time zone,
    "organization_id" "uuid",
    "pk_columns" "text"[] DEFAULT '{id}'::"text"[] NOT NULL,
    "snapshot" "jsonb",
    "pk_encoding" "text" DEFAULT 'json_v1'::"text" NOT NULL
);


ALTER TABLE "public"."ingest_runs" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."ingest_source_secrets" (
    "source_id" "uuid" NOT NULL,
    "headers" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."ingest_source_secrets" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."ingest_sources" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "owner_id" "uuid" NOT NULL,
    "name" "text" NOT NULL,
    "kind" "text" DEFAULT 'rest'::"text" NOT NULL,
    "target_table" "text" NOT NULL,
    "config" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "token_hash" "text",
    "last_run_at" timestamp with time zone,
    "last_status" "text",
    "last_rows" integer DEFAULT 0 NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "organization_id" "uuid",
    "pk_columns" "text"[] DEFAULT '{id}'::"text"[] NOT NULL,
    "enabled" boolean DEFAULT true NOT NULL,
    "adapter_version" integer DEFAULT 1 NOT NULL
);


ALTER TABLE "public"."ingest_sources" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."ingest_tables" (
    "owner_id" "uuid" NOT NULL,
    "name" "text" NOT NULL,
    "current_run_id" "uuid",
    "row_count" bigint DEFAULT 0 NOT NULL,
    "published_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "organization_id" "uuid"
);


ALTER TABLE "public"."ingest_tables" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."ingest_workers" (
    "id" "text" NOT NULL,
    "source_id" "uuid" NOT NULL,
    "owner_id" "uuid" NOT NULL,
    "host" "text",
    "registered_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "heartbeat_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."ingest_workers" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."org_members" (
    "org_id" "uuid" NOT NULL,
    "user_id" "uuid" NOT NULL
);


ALTER TABLE "public"."org_members" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."organizations" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "name" "text" NOT NULL,
    "status" "text" DEFAULT 'active'::"text" NOT NULL,
    "compute_quota_hours" integer DEFAULT 100 NOT NULL,
    "storage_quota_mb" integer DEFAULT 1024 NOT NULL,
    "infra" "text" DEFAULT 'shared'::"text" NOT NULL,
    "created_by" "uuid" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."organizations" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."platform_settings" (
    "key" "text" NOT NULL,
    "value" "jsonb" NOT NULL,
    "updated_by" "uuid",
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."platform_settings" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."profiles" (
    "id" "uuid" NOT NULL,
    "email" "text",
    "display_name" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."profiles" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."rate_limit_windows" (
    "key" "text" NOT NULL,
    "window_start" timestamp with time zone NOT NULL,
    "hits" integer NOT NULL,
    CONSTRAINT "rate_limit_windows_hits_check" CHECK (("hits" >= 0)),
    CONSTRAINT "rate_limit_windows_key_check" CHECK ((("length"("key") >= 1) AND ("length"("key") <= 200)))
);


ALTER TABLE "public"."rate_limit_windows" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."row_policies" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "table_name" "text" NOT NULL,
    "role" "public"."app_role" NOT NULL,
    "filter_sql" "text" NOT NULL,
    "created_by" "uuid" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "column_name" "text",
    "op" "text" DEFAULT '='::"text" NOT NULL,
    "value" "text",
    "organization_id" "uuid"
);


ALTER TABLE "public"."row_policies" OWNER TO "postgres";


COMMENT ON COLUMN "public"."row_policies"."filter_sql" IS 'DEPRECATED: display only; enforcement uses column_name/op/value';



CREATE TABLE IF NOT EXISTS "public"."schema_subjects" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "owner_id" "uuid" NOT NULL,
    "organization_id" "uuid",
    "name" "text" NOT NULL,
    "format" "text" DEFAULT 'avro'::"text" NOT NULL,
    "compatibility" "text" DEFAULT 'BACKWARD'::"text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "schema_subjects_compatibility_check" CHECK (("compatibility" = ANY (ARRAY['NONE'::"text", 'BACKWARD'::"text", 'BACKWARD_TRANSITIVE'::"text", 'FORWARD'::"text", 'FORWARD_TRANSITIVE'::"text", 'FULL'::"text", 'FULL_TRANSITIVE'::"text"]))),
    CONSTRAINT "schema_subjects_format_check" CHECK (("format" = 'avro'::"text")),
    CONSTRAINT "schema_subjects_name_check" CHECK (("name" ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$'::"text"))
);


ALTER TABLE "public"."schema_subjects" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."schema_versions" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "subject_id" "uuid" NOT NULL,
    "version" integer NOT NULL,
    "schema" "jsonb" NOT NULL,
    "fingerprint" "text" NOT NULL,
    "created_by" "uuid" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "schema_versions_fingerprint_check" CHECK (("fingerprint" ~ '^[a-f0-9]{64}$'::"text")),
    CONSTRAINT "schema_versions_version_check" CHECK (("version" >= 1))
);


ALTER TABLE "public"."schema_versions" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."storage_reconciliation_log" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "run_id" "uuid" NOT NULL,
    "dry_run" boolean NOT NULL,
    "bucket" "text" NOT NULL,
    "object_key" "text" NOT NULL,
    "kind" "text" NOT NULL,
    "action" "text" NOT NULL,
    "bytes" bigint,
    "object_created_at" timestamp with time zone,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "storage_reconciliation_log_action_check" CHECK (("action" = ANY (ARRAY['would_delete'::"text", 'deleted'::"text", 'delete_failed'::"text"]))),
    CONSTRAINT "storage_reconciliation_log_kind_check" CHECK (("kind" = ANY (ARRAY['dead_letter'::"text", 'landing'::"text", 'manifest'::"text"])))
);


ALTER TABLE "public"."storage_reconciliation_log" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."table_versions" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "table_name" "text" NOT NULL,
    "version" integer NOT NULL,
    "operation" "text" DEFAULT 'write'::"text" NOT NULL,
    "columns" "jsonb" DEFAULT '[]'::"jsonb" NOT NULL,
    "rows" "jsonb" DEFAULT '[]'::"jsonb" NOT NULL,
    "row_count" integer DEFAULT 0 NOT NULL,
    "created_by" "uuid" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."table_versions" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."user_roles" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "user_id" "uuid" NOT NULL,
    "role" "public"."app_role" NOT NULL
);


ALTER TABLE "public"."user_roles" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."user_tables" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "name" "text" NOT NULL,
    "columns" "jsonb" DEFAULT '[]'::"jsonb" NOT NULL,
    "rows" "jsonb" DEFAULT '[]'::"jsonb" NOT NULL,
    "row_count" integer DEFAULT 0 NOT NULL,
    "owner_id" "uuid" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "organization_id" "uuid"
);


ALTER TABLE "public"."user_tables" OWNER TO "postgres";


ALTER TABLE ONLY "public"."admin_audit" ALTER COLUMN "id" SET DEFAULT "nextval"('"public"."admin_audit_id_seq"'::"regclass");



ALTER TABLE ONLY "core"."dataset_versions"
    ADD CONSTRAINT "dataset_versions_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "core"."dataset_versions"
    ADD CONSTRAINT "dataset_versions_source_version_id_key" UNIQUE ("source_version_id");



ALTER TABLE ONLY "core"."datasets"
    ADD CONSTRAINT "datasets_namespace_source_id_key" UNIQUE ("namespace", "source_id");



ALTER TABLE ONLY "core"."datasets"
    ADD CONSTRAINT "datasets_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."access_policies"
    ADD CONSTRAINT "access_policies_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."admin_audit"
    ADD CONSTRAINT "admin_audit_pkey" PRIMARY KEY ("id");



ALTER TABLE "public"."alert_settings"
    ADD CONSTRAINT "alert_settings_alert_email_single_address" CHECK ((("alert_email" IS NULL) OR (("length"("alert_email") <= 254) AND ("alert_email" ~ '^[A-Za-z0-9.!#$%&''*+/=?^_`{|}~-]+@[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$'::"text")))) NOT VALID;



ALTER TABLE ONLY "public"."alert_settings"
    ADD CONSTRAINT "alert_settings_pkey" PRIMARY KEY ("user_id");



ALTER TABLE ONLY "public"."audit_log"
    ADD CONSTRAINT "audit_log_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."change_requests"
    ADD CONSTRAINT "change_requests_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."connect_attempts"
    ADD CONSTRAINT "connect_attempts_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."connect_attempts"
    ADD CONSTRAINT "connect_attempts_run_id_attempt_key" UNIQUE ("run_id", "attempt");



ALTER TABLE ONLY "public"."connect_commits"
    ADD CONSTRAINT "connect_commits_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."connect_commits"
    ADD CONSTRAINT "connect_commits_run_id_stream_id_sequence_key" UNIQUE ("run_id", "stream_id", "sequence");



ALTER TABLE ONLY "public"."connect_connections"
    ADD CONSTRAINT "connect_connections_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."connect_connections"
    ADD CONSTRAINT "connect_connections_source_id_key" UNIQUE ("source_id");



ALTER TABLE ONLY "public"."connect_dead_letters"
    ADD CONSTRAINT "connect_dead_letters_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."connect_dead_letters"
    ADD CONSTRAINT "connect_dead_letters_stream_id_position_key" UNIQUE ("stream_id", "position");



ALTER TABLE ONLY "public"."connect_events"
    ADD CONSTRAINT "connect_events_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."connect_run_streams"
    ADD CONSTRAINT "connect_run_streams_pkey" PRIMARY KEY ("run_id", "stream_id");



ALTER TABLE ONLY "public"."connect_runs"
    ADD CONSTRAINT "connect_runs_connection_id_request_key_key" UNIQUE ("connection_id", "request_key");



ALTER TABLE ONLY "public"."connect_runs"
    ADD CONSTRAINT "connect_runs_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."connect_streams"
    ADD CONSTRAINT "connect_streams_connection_id_namespace_name_key" UNIQUE ("connection_id", "namespace", "name");



ALTER TABLE ONLY "public"."connect_streams"
    ADD CONSTRAINT "connect_streams_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."connection_secrets"
    ADD CONSTRAINT "connection_secrets_pkey" PRIMARY KEY ("connection_id");



ALTER TABLE ONLY "public"."connections"
    ADD CONSTRAINT "connections_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."entitlements"
    ADD CONSTRAINT "entitlements_pkey" PRIMARY KEY ("organization_id", "feature");



ALTER TABLE ONLY "public"."execution_engines"
    ADD CONSTRAINT "execution_engines_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."file_tables"
    ADD CONSTRAINT "file_tables_owner_id_name_key" UNIQUE ("owner_id", "name");



ALTER TABLE ONLY "public"."file_tables"
    ADD CONSTRAINT "file_tables_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."ingest_dead_letters"
    ADD CONSTRAINT "ingest_dead_letters_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."ingest_landing_batches"
    ADD CONSTRAINT "ingest_landing_batches_object_key_key" UNIQUE ("object_key");



ALTER TABLE ONLY "public"."ingest_landing_batches"
    ADD CONSTRAINT "ingest_landing_batches_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."ingest_landing_batches"
    ADD CONSTRAINT "ingest_landing_batches_source_id_batch_key_key" UNIQUE ("source_id", "batch_key");



ALTER TABLE ONLY "public"."ingest_partitions"
    ADD CONSTRAINT "ingest_partitions_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."ingest_partitions"
    ADD CONSTRAINT "ingest_partitions_run_id_idx_key" UNIQUE ("run_id", "idx");



ALTER TABLE ONLY "public"."ingest_rows"
    ADD CONSTRAINT "ingest_rows_pkey" PRIMARY KEY ("run_id", "pk");



ALTER TABLE ONLY "public"."ingest_runs"
    ADD CONSTRAINT "ingest_runs_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."ingest_source_secrets"
    ADD CONSTRAINT "ingest_source_secrets_pkey" PRIMARY KEY ("source_id");



ALTER TABLE ONLY "public"."ingest_sources"
    ADD CONSTRAINT "ingest_sources_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."ingest_tables"
    ADD CONSTRAINT "ingest_tables_pkey" PRIMARY KEY ("owner_id", "name");



ALTER TABLE ONLY "public"."ingest_workers"
    ADD CONSTRAINT "ingest_workers_pkey" PRIMARY KEY ("source_id", "id");



ALTER TABLE ONLY "public"."org_members"
    ADD CONSTRAINT "org_members_pkey" PRIMARY KEY ("org_id", "user_id");



ALTER TABLE ONLY "public"."organizations"
    ADD CONSTRAINT "organizations_name_key" UNIQUE ("name");



ALTER TABLE ONLY "public"."organizations"
    ADD CONSTRAINT "organizations_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."platform_settings"
    ADD CONSTRAINT "platform_settings_pkey" PRIMARY KEY ("key");



ALTER TABLE ONLY "public"."profiles"
    ADD CONSTRAINT "profiles_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."rate_limit_windows"
    ADD CONSTRAINT "rate_limit_windows_pkey" PRIMARY KEY ("key");



ALTER TABLE ONLY "public"."row_policies"
    ADD CONSTRAINT "row_policies_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."schema_subjects"
    ADD CONSTRAINT "schema_subjects_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."schema_versions"
    ADD CONSTRAINT "schema_versions_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."schema_versions"
    ADD CONSTRAINT "schema_versions_subject_id_fingerprint_key" UNIQUE ("subject_id", "fingerprint");



ALTER TABLE ONLY "public"."schema_versions"
    ADD CONSTRAINT "schema_versions_subject_id_version_key" UNIQUE ("subject_id", "version");



ALTER TABLE ONLY "public"."storage_reconciliation_log"
    ADD CONSTRAINT "storage_reconciliation_log_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."table_versions"
    ADD CONSTRAINT "table_versions_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."table_versions"
    ADD CONSTRAINT "table_versions_table_name_version_key" UNIQUE ("table_name", "version");



ALTER TABLE ONLY "public"."user_roles"
    ADD CONSTRAINT "user_roles_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."user_roles"
    ADD CONSTRAINT "user_roles_user_id_role_key" UNIQUE ("user_id", "role");



ALTER TABLE ONLY "public"."user_tables"
    ADD CONSTRAINT "user_tables_name_key" UNIQUE ("name");



ALTER TABLE ONLY "public"."user_tables"
    ADD CONSTRAINT "user_tables_pkey" PRIMARY KEY ("id");



CREATE UNIQUE INDEX "core_dataset_identity" ON "core"."datasets" USING "btree" ("owner_id", COALESCE("organization_id", '00000000-0000-0000-0000-000000000000'::"uuid"), "namespace", "name");



CREATE UNIQUE INDEX "access_policies_scope_dataset_column_key" ON "public"."access_policies" USING "btree" (COALESCE("organization_id", '00000000-0000-0000-0000-000000000000'::"uuid"), "dataset", "column_name");



CREATE INDEX "connect_dead_letters_open" ON "public"."connect_dead_letters" USING "btree" ("connection_id", "status");



CREATE UNIQUE INDEX "connect_one_active_run" ON "public"."connect_runs" USING "btree" ("connection_id") WHERE ("status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'retry_wait'::"text"]));



CREATE INDEX "ingest_dead_letters_open" ON "public"."ingest_dead_letters" USING "btree" ("source_id", "status");



CREATE UNIQUE INDEX "ingest_dead_letters_position_payload" ON "public"."ingest_dead_letters" USING "btree" ("source_id", "topic", "partition", "offset", "payload_sha256") NULLS NOT DISTINCT;



CREATE INDEX "ingest_rows_part_idx" ON "public"."ingest_rows" USING "btree" ("run_id", "partition_idx");



CREATE INDEX "ingest_runs_source_idx" ON "public"."ingest_runs" USING "btree" ("source_id", "created_at" DESC);



CREATE INDEX "ingest_sources_token_idx" ON "public"."ingest_sources" USING "btree" ("token_hash");



CREATE UNIQUE INDEX "schema_subjects_scope_name" ON "public"."schema_subjects" USING "btree" (COALESCE("organization_id", "owner_id"), "name");



CREATE INDEX "storage_reconciliation_log_run" ON "public"."storage_reconciliation_log" USING "btree" ("run_id");



CREATE OR REPLACE TRIGGER "core_dataset_version_immutable" BEFORE UPDATE ON "core"."dataset_versions" FOR EACH ROW EXECUTE FUNCTION "core"."immutable_version"();



CREATE OR REPLACE TRIGGER "admin_audit_chain" BEFORE INSERT ON "public"."admin_audit" FOR EACH ROW EXECUTE FUNCTION "public"."admin_audit_chain"();



CREATE OR REPLACE TRIGGER "admin_audit_no_change" BEFORE DELETE OR UPDATE ON "public"."admin_audit" FOR EACH ROW EXECUTE FUNCTION "public"."admin_audit_block"();



CREATE OR REPLACE TRIGGER "audit_access_policies" AFTER INSERT OR DELETE OR UPDATE ON "public"."access_policies" FOR EACH ROW EXECUTE FUNCTION "public"."log_admin_change"();



CREATE OR REPLACE TRIGGER "audit_entitlements" AFTER INSERT OR DELETE OR UPDATE ON "public"."entitlements" FOR EACH ROW EXECUTE FUNCTION "public"."log_admin_change"();



CREATE OR REPLACE TRIGGER "audit_org_members" AFTER INSERT OR DELETE ON "public"."org_members" FOR EACH ROW EXECUTE FUNCTION "public"."log_admin_change"();



CREATE OR REPLACE TRIGGER "audit_orgs" AFTER INSERT OR DELETE OR UPDATE ON "public"."organizations" FOR EACH ROW EXECUTE FUNCTION "public"."log_admin_change"();



CREATE OR REPLACE TRIGGER "audit_row_policies" AFTER INSERT OR DELETE OR UPDATE ON "public"."row_policies" FOR EACH ROW EXECUTE FUNCTION "public"."log_admin_change"();



CREATE OR REPLACE TRIGGER "audit_settings" AFTER INSERT OR DELETE OR UPDATE ON "public"."platform_settings" FOR EACH ROW EXECUTE FUNCTION "public"."log_admin_change"();



CREATE OR REPLACE TRIGGER "audit_user_roles" AFTER INSERT OR DELETE OR UPDATE ON "public"."user_roles" FOR EACH ROW EXECUTE FUNCTION "public"."log_admin_change"();



CREATE OR REPLACE TRIGGER "connect_commit_immutable" BEFORE DELETE OR UPDATE ON "public"."connect_commits" FOR EACH ROW EXECUTE FUNCTION "public"."reject_row_mutation"();



CREATE OR REPLACE TRIGGER "connect_event_immutable" BEFORE DELETE OR UPDATE ON "public"."connect_events" FOR EACH ROW EXECUTE FUNCTION "public"."reject_row_mutation"();



CREATE OR REPLACE TRIGGER "connect_guard_stream" BEFORE INSERT OR UPDATE ON "public"."connect_streams" FOR EACH ROW EXECUTE FUNCTION "public"."connect_guard_stream"();



CREATE OR REPLACE TRIGGER "guard_connection_identity" BEFORE UPDATE ON "public"."connections" FOR EACH ROW EXECUTE FUNCTION "public"."guard_resource_identity_update"();



CREATE OR REPLACE TRIGGER "ingest_credentials_guard" BEFORE INSERT OR UPDATE ON "public"."ingest_sources" FOR EACH ROW EXECUTE FUNCTION "public"."guard_ingest_credentials"();



CREATE OR REPLACE TRIGGER "schema_version_immutable" BEFORE DELETE OR UPDATE ON "public"."schema_versions" FOR EACH ROW EXECUTE FUNCTION "public"."reject_row_mutation"();



CREATE OR REPLACE TRIGGER "storage_reconciliation_log_append_only" BEFORE DELETE OR UPDATE ON "public"."storage_reconciliation_log" FOR EACH ROW EXECUTE FUNCTION "public"."reject_row_mutation"();



CREATE OR REPLACE TRIGGER "validate_file_table_scope" BEFORE INSERT OR UPDATE ON "public"."file_tables" FOR EACH ROW EXECUTE FUNCTION "public"."validate_file_table_scope"();



CREATE OR REPLACE TRIGGER "validate_partition_creation" BEFORE INSERT ON "public"."ingest_partitions" FOR EACH ROW EXECUTE FUNCTION "public"."validate_partition_creation"();



ALTER TABLE ONLY "core"."datasets"
    ADD CONSTRAINT "core_dataset_current" FOREIGN KEY ("current_version_id") REFERENCES "core"."dataset_versions"("id");



ALTER TABLE ONLY "core"."dataset_versions"
    ADD CONSTRAINT "dataset_versions_dataset_id_fkey" FOREIGN KEY ("dataset_id") REFERENCES "core"."datasets"("id");



ALTER TABLE ONLY "core"."datasets"
    ADD CONSTRAINT "datasets_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id");



ALTER TABLE ONLY "public"."access_policies"
    ADD CONSTRAINT "access_policies_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id");



ALTER TABLE ONLY "public"."alert_settings"
    ADD CONSTRAINT "alert_settings_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."connect_attempts"
    ADD CONSTRAINT "connect_attempts_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "public"."connect_runs"("id");



ALTER TABLE ONLY "public"."connect_commits"
    ADD CONSTRAINT "connect_commits_batch_id_fkey" FOREIGN KEY ("batch_id") REFERENCES "public"."ingest_landing_batches"("id");



ALTER TABLE ONLY "public"."connect_commits"
    ADD CONSTRAINT "connect_commits_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "public"."connect_runs"("id");



ALTER TABLE ONLY "public"."connect_commits"
    ADD CONSTRAINT "connect_commits_stream_id_fkey" FOREIGN KEY ("stream_id") REFERENCES "public"."connect_streams"("id");



ALTER TABLE ONLY "public"."connect_connections"
    ADD CONSTRAINT "connect_connections_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id");



ALTER TABLE ONLY "public"."connect_connections"
    ADD CONSTRAINT "connect_connections_source_id_fkey" FOREIGN KEY ("source_id") REFERENCES "public"."ingest_sources"("id");



ALTER TABLE ONLY "public"."connect_dead_letters"
    ADD CONSTRAINT "connect_dead_letters_connection_id_fkey" FOREIGN KEY ("connection_id") REFERENCES "public"."connect_connections"("id");



ALTER TABLE ONLY "public"."connect_dead_letters"
    ADD CONSTRAINT "connect_dead_letters_replay_run_id_fkey" FOREIGN KEY ("replay_run_id") REFERENCES "public"."connect_runs"("id");



ALTER TABLE ONLY "public"."connect_dead_letters"
    ADD CONSTRAINT "connect_dead_letters_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "public"."connect_runs"("id");



ALTER TABLE ONLY "public"."connect_dead_letters"
    ADD CONSTRAINT "connect_dead_letters_stream_id_fkey" FOREIGN KEY ("stream_id") REFERENCES "public"."connect_streams"("id");



ALTER TABLE ONLY "public"."connect_events"
    ADD CONSTRAINT "connect_events_connection_id_fkey" FOREIGN KEY ("connection_id") REFERENCES "public"."connect_connections"("id");



ALTER TABLE ONLY "public"."connect_events"
    ADD CONSTRAINT "connect_events_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "public"."connect_runs"("id");



ALTER TABLE ONLY "public"."connect_run_streams"
    ADD CONSTRAINT "connect_run_streams_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "public"."connect_runs"("id");



ALTER TABLE ONLY "public"."connect_run_streams"
    ADD CONSTRAINT "connect_run_streams_stream_id_fkey" FOREIGN KEY ("stream_id") REFERENCES "public"."connect_streams"("id");



ALTER TABLE ONLY "public"."connect_runs"
    ADD CONSTRAINT "connect_runs_connection_id_fkey" FOREIGN KEY ("connection_id") REFERENCES "public"."connect_connections"("id");



ALTER TABLE ONLY "public"."connect_streams"
    ADD CONSTRAINT "connect_streams_connection_id_fkey" FOREIGN KEY ("connection_id") REFERENCES "public"."connect_connections"("id");



ALTER TABLE ONLY "public"."connection_secrets"
    ADD CONSTRAINT "connection_secrets_connection_id_fkey" FOREIGN KEY ("connection_id") REFERENCES "public"."connections"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."connections"
    ADD CONSTRAINT "connections_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."entitlements"
    ADD CONSTRAINT "entitlements_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."execution_engines"
    ADD CONSTRAINT "execution_engines_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id");



ALTER TABLE ONLY "public"."ingest_dead_letters"
    ADD CONSTRAINT "ingest_dead_letters_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."ingest_dead_letters"
    ADD CONSTRAINT "ingest_dead_letters_source_id_fkey" FOREIGN KEY ("source_id") REFERENCES "public"."ingest_sources"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."ingest_landing_batches"
    ADD CONSTRAINT "ingest_landing_batches_source_id_fkey" FOREIGN KEY ("source_id") REFERENCES "public"."ingest_sources"("id");



ALTER TABLE ONLY "public"."ingest_partitions"
    ADD CONSTRAINT "ingest_partitions_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "public"."ingest_runs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."ingest_rows"
    ADD CONSTRAINT "ingest_rows_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "public"."ingest_runs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."ingest_runs"
    ADD CONSTRAINT "ingest_runs_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."ingest_runs"
    ADD CONSTRAINT "ingest_runs_source_fk" FOREIGN KEY ("source_id") REFERENCES "public"."ingest_sources"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."ingest_source_secrets"
    ADD CONSTRAINT "ingest_source_secrets_source_id_fkey" FOREIGN KEY ("source_id") REFERENCES "public"."ingest_sources"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."ingest_sources"
    ADD CONSTRAINT "ingest_sources_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."ingest_tables"
    ADD CONSTRAINT "ingest_tables_current_run_id_fkey" FOREIGN KEY ("current_run_id") REFERENCES "public"."ingest_runs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."ingest_tables"
    ADD CONSTRAINT "ingest_tables_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."ingest_workers"
    ADD CONSTRAINT "ingest_workers_source_id_fkey" FOREIGN KEY ("source_id") REFERENCES "public"."ingest_sources"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."org_members"
    ADD CONSTRAINT "org_members_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "public"."organizations"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."profiles"
    ADD CONSTRAINT "profiles_id_fkey" FOREIGN KEY ("id") REFERENCES "auth"."users"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."row_policies"
    ADD CONSTRAINT "row_policies_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id");



ALTER TABLE ONLY "public"."schema_subjects"
    ADD CONSTRAINT "schema_subjects_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id");



ALTER TABLE ONLY "public"."schema_versions"
    ADD CONSTRAINT "schema_versions_subject_id_fkey" FOREIGN KEY ("subject_id") REFERENCES "public"."schema_subjects"("id");



ALTER TABLE ONLY "public"."user_roles"
    ADD CONSTRAINT "user_roles_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."user_tables"
    ADD CONSTRAINT "user_tables_organization_id_fkey" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE SET NULL;



CREATE POLICY "Admins grant roles" ON "public"."user_roles" FOR INSERT TO "authenticated" WITH CHECK ("public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role"));



CREATE POLICY "Admins manage members" ON "public"."org_members" TO "authenticated" USING ("public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role")) WITH CHECK ("public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role"));



CREATE POLICY "Admins manage orgs" ON "public"."organizations" TO "authenticated" USING ("public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role")) WITH CHECK ("public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role"));



CREATE POLICY "Admins read admin audit" ON "public"."admin_audit" FOR SELECT TO "authenticated" USING ("public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role"));



CREATE POLICY "Admins revoke roles" ON "public"."user_roles" FOR DELETE TO "authenticated" USING ("public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role"));



CREATE POLICY "Admins write settings" ON "public"."platform_settings" TO "authenticated" USING ("public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role")) WITH CHECK ("public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role"));



CREATE POLICY "Engine authorized reads" ON "public"."execution_engines" FOR SELECT TO "authenticated" USING ("public"."can_access"("owner_id", "organization_id"));



CREATE POLICY "Engineers create connections" ON "public"."connections" FOR INSERT TO "authenticated" WITH CHECK ((("auth"."uid"() = "created_by") AND ("public"."has_role"("auth"."uid"(), 'engineer'::"public"."app_role") OR "public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role")) AND (("organization_id" IS NULL) OR "public"."is_org_member"("organization_id"))));



CREATE POLICY "Governors add row policies" ON "public"."row_policies" FOR INSERT TO "authenticated" WITH CHECK ((("auth"."uid"() = "created_by") AND "public"."can_govern"("organization_id")));



CREATE POLICY "Governors delete row policies" ON "public"."row_policies" FOR DELETE TO "authenticated" USING ("public"."can_govern"("organization_id"));



CREATE POLICY "Governors manage access policies" ON "public"."access_policies" TO "authenticated" USING ("public"."can_govern"("organization_id")) WITH CHECK ("public"."can_govern"("organization_id"));



CREATE POLICY "Landing metadata authorized reads" ON "public"."ingest_landing_batches" FOR SELECT TO "authenticated" USING ("public"."can_access"("owner_id", "organization_id"));



CREATE POLICY "Members read entitlements" ON "public"."entitlements" FOR SELECT TO "authenticated" USING (("public"."is_org_member"("organization_id") OR "public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role")));



CREATE POLICY "Members view members" ON "public"."org_members" FOR SELECT TO "authenticated" USING ((("user_id" = "auth"."uid"()) OR "public"."is_org_member"("org_id") OR "public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role")));



CREATE POLICY "Members view orgs" ON "public"."organizations" FOR SELECT TO "authenticated" USING (("public"."is_org_member"("id") OR "public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role")));



CREATE POLICY "Owners add tables" ON "public"."user_tables" FOR INSERT TO "authenticated" WITH CHECK ((("auth"."uid"() = "owner_id") AND (("organization_id" IS NULL) OR "public"."is_org_member"("organization_id"))));



CREATE POLICY "Owners and org stewards view raw tables" ON "public"."user_tables" FOR SELECT TO "authenticated" USING ((("auth"."uid"() = "owner_id") OR "public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role") OR ("public"."has_role"("auth"."uid"(), 'steward'::"public"."app_role") AND "public"."is_org_member"("organization_id"))));



CREATE POLICY "Owners and org stewards view versions" ON "public"."table_versions" FOR SELECT TO "authenticated" USING ((("auth"."uid"() = "created_by") OR "public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role") OR ("public"."has_role"("auth"."uid"(), 'steward'::"public"."app_role") AND "public"."shares_org_with"("created_by"))));



CREATE POLICY "Owners delete connections" ON "public"."connections" FOR DELETE TO "authenticated" USING ((("auth"."uid"() = "created_by") OR "public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role")));



CREATE POLICY "Owners manage connections" ON "public"."connections" FOR UPDATE TO "authenticated" USING (("public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role") OR (("created_by" = "auth"."uid"()) AND (("organization_id" IS NULL) OR "public"."is_org_member"("organization_id"))))) WITH CHECK (("public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role") OR (("created_by" = "auth"."uid"()) AND (("organization_id" IS NULL) OR "public"."is_org_member"("organization_id")))));



CREATE POLICY "Owners or admins delete tables" ON "public"."user_tables" FOR DELETE TO "authenticated" USING ((("auth"."uid"() = "owner_id") OR "public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role")));



CREATE POLICY "Owners or admins update tables" ON "public"."user_tables" FOR UPDATE TO "authenticated" USING ((("owner_id" = "auth"."uid"()) OR "public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role"))) WITH CHECK (((("owner_id" = "auth"."uid"()) AND (("organization_id" IS NULL) OR "public"."is_org_member"("organization_id"))) OR "public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role")));



CREATE POLICY "Signed-in read settings" ON "public"."platform_settings" FOR SELECT TO "authenticated" USING (true);



CREATE POLICY "Stewards review others' requests" ON "public"."change_requests" FOR UPDATE TO "authenticated" USING (("public"."is_steward_for"("submitted_by", NULL::"uuid") AND ("submitted_by" <> "auth"."uid"()) AND ("status" = 'pending'::"text"))) WITH CHECK ((("reviewed_by" = "auth"."uid"()) AND ("submitted_by" <> "auth"."uid"()) AND ("status" = ANY (ARRAY['approved'::"text", 'rejected'::"text"]))));



CREATE POLICY "Tenant read audit" ON "public"."audit_log" FOR SELECT TO "authenticated" USING ((("actor_id" = "auth"."uid"()) OR "public"."has_role"("auth"."uid"(), 'admin'::"public"."app_role") OR ("public"."has_role"("auth"."uid"(), 'steward'::"public"."app_role") AND "public"."shares_org_with"("actor_id"))));



CREATE POLICY "Tenant view access policies" ON "public"."access_policies" FOR SELECT TO "authenticated" USING ((("organization_id" IS NULL) OR "public"."can_access"(NULL::"uuid", "organization_id")));



CREATE POLICY "Tenant view connections" ON "public"."connections" FOR SELECT TO "authenticated" USING ("public"."can_access"("created_by", "organization_id"));



CREATE POLICY "Tenant view profiles" ON "public"."profiles" FOR SELECT TO "authenticated" USING ("public"."can_see_user"("id"));



CREATE POLICY "Tenant view requests" ON "public"."change_requests" FOR SELECT TO "authenticated" USING ("public"."can_see_user"("submitted_by"));



CREATE POLICY "Tenant view roles" ON "public"."user_roles" FOR SELECT TO "authenticated" USING ("public"."can_see_user"("user_id"));



CREATE POLICY "Tenant view row policies" ON "public"."row_policies" FOR SELECT TO "authenticated" USING ((("organization_id" IS NULL) OR "public"."can_access"(NULL::"uuid", "organization_id")));



CREATE POLICY "Users delete own alert settings" ON "public"."alert_settings" FOR DELETE TO "authenticated" USING (("auth"."uid"() = "user_id"));



CREATE POLICY "Users insert own alert settings" ON "public"."alert_settings" FOR INSERT TO "authenticated" WITH CHECK (("auth"."uid"() = "user_id"));



CREATE POLICY "Users read own alert settings" ON "public"."alert_settings" FOR SELECT TO "authenticated" USING (("auth"."uid"() = "user_id"));



CREATE POLICY "Users record versions" ON "public"."table_versions" FOR INSERT TO "authenticated" WITH CHECK (("auth"."uid"() = "created_by"));



CREATE POLICY "Users submit own pending requests" ON "public"."change_requests" FOR INSERT TO "authenticated" WITH CHECK ((("auth"."uid"() = "submitted_by") AND ("status" = 'pending'::"text") AND ("reviewed_by" IS NULL)));



CREATE POLICY "Users update own alert settings" ON "public"."alert_settings" FOR UPDATE TO "authenticated" USING (("auth"."uid"() = "user_id")) WITH CHECK (("auth"."uid"() = "user_id"));



CREATE POLICY "Users update own profile" ON "public"."profiles" FOR UPDATE TO "authenticated" USING (("auth"."uid"() = "id")) WITH CHECK (("auth"."uid"() = "id"));



CREATE POLICY "Users write own audit" ON "public"."audit_log" FOR INSERT TO "authenticated" WITH CHECK (("auth"."uid"() = "actor_id"));



ALTER TABLE "public"."access_policies" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."admin_audit" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."alert_settings" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."audit_log" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."change_requests" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "connect_attempt_read" ON "public"."connect_attempts" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM ("public"."connect_runs" "r"
     JOIN "public"."connect_connections" "c" ON (("c"."id" = "r"."connection_id")))
  WHERE (("r"."id" = "connect_attempts"."run_id") AND "public"."can_read_scoped"("c"."owner_id", "c"."organization_id")))));



ALTER TABLE "public"."connect_attempts" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "connect_commit_read" ON "public"."connect_commits" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM ("public"."connect_runs" "r"
     JOIN "public"."connect_connections" "c" ON (("c"."id" = "r"."connection_id")))
  WHERE (("r"."id" = "connect_commits"."run_id") AND "public"."can_read_scoped"("c"."owner_id", "c"."organization_id")))));



ALTER TABLE "public"."connect_commits" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "connect_connection_read" ON "public"."connect_connections" FOR SELECT TO "authenticated" USING ("public"."can_read_scoped"("owner_id", "organization_id"));



ALTER TABLE "public"."connect_connections" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "connect_dead_letter_read" ON "public"."connect_dead_letters" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "public"."connect_connections" "c"
  WHERE (("c"."id" = "connect_dead_letters"."connection_id") AND "public"."can_read_scoped"("c"."owner_id", "c"."organization_id")))));



ALTER TABLE "public"."connect_dead_letters" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "connect_event_read" ON "public"."connect_events" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "public"."connect_connections" "c"
  WHERE (("c"."id" = "connect_events"."connection_id") AND "public"."can_read_scoped"("c"."owner_id", "c"."organization_id")))));



ALTER TABLE "public"."connect_events" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "connect_run_read" ON "public"."connect_runs" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "public"."connect_connections" "c"
  WHERE (("c"."id" = "connect_runs"."connection_id") AND "public"."can_read_scoped"("c"."owner_id", "c"."organization_id")))));



CREATE POLICY "connect_run_stream_read" ON "public"."connect_run_streams" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM ("public"."connect_runs" "r"
     JOIN "public"."connect_connections" "c" ON (("c"."id" = "r"."connection_id")))
  WHERE (("r"."id" = "connect_run_streams"."run_id") AND "public"."can_read_scoped"("c"."owner_id", "c"."organization_id")))));



ALTER TABLE "public"."connect_run_streams" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."connect_runs" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "connect_stream_read" ON "public"."connect_streams" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "public"."connect_connections" "c"
  WHERE (("c"."id" = "connect_streams"."connection_id") AND "public"."can_read_scoped"("c"."owner_id", "c"."organization_id")))));



ALTER TABLE "public"."connect_streams" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."connection_secrets" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."connections" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."entitlements" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."execution_engines" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."file_tables" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "file_tables delete own" ON "public"."file_tables" FOR DELETE TO "authenticated" USING (("owner_id" = "auth"."uid"()));



CREATE POLICY "file_tables insert own" ON "public"."file_tables" FOR INSERT TO "authenticated" WITH CHECK ((("owner_id" = "auth"."uid"()) AND (("organization_id" IS NULL) OR "public"."is_org_member"("organization_id")) AND ("split_part"("manifest_prefix", '/'::"text", 1) = ("auth"."uid"())::"text")));



CREATE POLICY "file_tables read" ON "public"."file_tables" FOR SELECT TO "authenticated" USING ("public"."can_access"("owner_id", "organization_id"));



CREATE POLICY "ingest_dead_letter_read" ON "public"."ingest_dead_letters" FOR SELECT TO "authenticated" USING ("public"."can_read_scoped"("owner_id", "organization_id"));



ALTER TABLE "public"."ingest_dead_letters" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."ingest_landing_batches" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."ingest_partitions" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."ingest_rows" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."ingest_runs" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."ingest_source_secrets" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."ingest_sources" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."ingest_tables" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."ingest_workers" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "org published read" ON "public"."ingest_tables" FOR SELECT TO "authenticated" USING ("public"."is_org_member"("organization_id"));



CREATE POLICY "org runs read" ON "public"."ingest_runs" FOR SELECT TO "authenticated" USING ("public"."can_access"("owner_id", "organization_id"));



ALTER TABLE "public"."org_members" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."organizations" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "own partitions" ON "public"."ingest_partitions" TO "authenticated" USING (("owner_id" = "auth"."uid"())) WITH CHECK (("owner_id" = "auth"."uid"()));



CREATE POLICY "own rows" ON "public"."ingest_rows" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "public"."ingest_runs" "r"
  WHERE (("r"."id" = "ingest_rows"."run_id") AND ("r"."owner_id" = "auth"."uid"())))));



CREATE POLICY "own runs" ON "public"."ingest_runs" TO "authenticated" USING (("owner_id" = "auth"."uid"())) WITH CHECK ((("owner_id" = "auth"."uid"()) AND (("organization_id" IS NULL) OR "public"."is_org_member"("organization_id")) AND (("source_id" IS NULL) OR (EXISTS ( SELECT 1
   FROM "public"."ingest_sources" "s"
  WHERE (("s"."id" = "ingest_runs"."source_id") AND ("s"."owner_id" = "auth"."uid"())))))));



CREATE POLICY "own sources delete" ON "public"."ingest_sources" FOR DELETE TO "authenticated" USING (("owner_id" = "auth"."uid"()));



CREATE POLICY "own sources insert" ON "public"."ingest_sources" FOR INSERT TO "authenticated" WITH CHECK ((("owner_id" = "auth"."uid"()) AND (("organization_id" IS NULL) OR "public"."is_org_member"("organization_id"))));



CREATE POLICY "own sources select" ON "public"."ingest_sources" FOR SELECT TO "authenticated" USING (("owner_id" = "auth"."uid"()));



CREATE POLICY "own sources update" ON "public"."ingest_sources" FOR UPDATE TO "authenticated" USING (("owner_id" = "auth"."uid"())) WITH CHECK ((("owner_id" = "auth"."uid"()) AND (("organization_id" IS NULL) OR "public"."is_org_member"("organization_id"))));



CREATE POLICY "own tables" ON "public"."ingest_tables" FOR SELECT TO "authenticated" USING (("owner_id" = "auth"."uid"()));



CREATE POLICY "own workers" ON "public"."ingest_workers" FOR SELECT TO "authenticated" USING (("owner_id" = "auth"."uid"()));



ALTER TABLE "public"."platform_settings" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."profiles" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."rate_limit_windows" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."row_policies" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "schema_subject_read" ON "public"."schema_subjects" FOR SELECT TO "authenticated" USING ("public"."can_access"("owner_id", "organization_id"));



ALTER TABLE "public"."schema_subjects" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "schema_version_read" ON "public"."schema_versions" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "public"."schema_subjects" "s"
  WHERE (("s"."id" = "schema_versions"."subject_id") AND "public"."can_access"("s"."owner_id", "s"."organization_id")))));



ALTER TABLE "public"."schema_versions" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."storage_reconciliation_log" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."table_versions" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."user_roles" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."user_tables" ENABLE ROW LEVEL SECURITY;




ALTER PUBLICATION "supabase_realtime" OWNER TO "postgres";


GRANT USAGE ON SCHEMA "core" TO "service_role";



GRANT USAGE ON SCHEMA "public" TO "postgres";
GRANT USAGE ON SCHEMA "public" TO "anon";
GRANT USAGE ON SCHEMA "public" TO "authenticated";
GRANT USAGE ON SCHEMA "public" TO "service_role";






















































































































































REVOKE ALL ON FUNCTION "public"."admin_audit_block"() FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."admin_audit_block"() TO "service_role";



REVOKE ALL ON FUNCTION "public"."admin_audit_chain"() FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."admin_audit_chain"() TO "service_role";



REVOKE ALL ON FUNCTION "public"."apply_file_table_policies"("_table" "uuid", "_rows" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."apply_file_table_policies"("_table" "uuid", "_rows" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."apply_file_table_policies"("_table" "uuid", "_rows" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."apply_licence_entitlements"("_actor" "uuid", "_licence" "text", "_org" "uuid", "_features" "text"[], "_expires" timestamp with time zone) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."apply_licence_entitlements"("_actor" "uuid", "_licence" "text", "_org" "uuid", "_features" "text"[], "_expires" timestamp with time zone) TO "service_role";



REVOKE ALL ON FUNCTION "public"."bootstrap_platform_admin"("_email" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."bootstrap_platform_admin"("_email" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."can_access"("_owner" "uuid", "_org" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."can_access"("_owner" "uuid", "_org" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."can_access"("_owner" "uuid", "_org" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."can_govern"("_org" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."can_govern"("_org" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."can_govern"("_org" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."can_read_file_object"("_name" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."can_read_file_object"("_name" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."can_read_file_object"("_name" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."can_read_scoped"("_owner" "uuid", "_org" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."can_read_scoped"("_owner" "uuid", "_org" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."can_read_scoped"("_owner" "uuid", "_org" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."can_see_user"("_user" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."can_see_user"("_user" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."can_see_user"("_user" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cancel_ingest_run"("_run" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cancel_ingest_run"("_run" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cancel_ingest_run"("_run" "uuid") TO "service_role";



GRANT SELECT ON TABLE "public"."ingest_partitions" TO "authenticated";
GRANT ALL ON TABLE "public"."ingest_partitions" TO "service_role";



REVOKE ALL ON FUNCTION "public"."claim_ingest_partition"("_run" "uuid", "_worker" "text", "_lease_seconds" integer) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."claim_ingest_partition"("_run" "uuid", "_worker" "text", "_lease_seconds" integer) TO "service_role";



REVOKE ALL ON FUNCTION "public"."claim_specific_partition"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_seconds" integer) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."claim_specific_partition"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_seconds" integer) TO "service_role";



REVOKE ALL ON FUNCTION "public"."connect_accept_offset_gap"("_stream" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."connect_accept_offset_gap"("_stream" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."connect_accept_offset_gap"("_stream" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."connect_cancel"("_run" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."connect_cancel"("_run" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."connect_cancel"("_run" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."connect_claim"("_connection" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."connect_claim"("_connection" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."connect_commit"("_run" "uuid", "_token" "uuid", "_stream" "uuid", "_sequence" integer, "_batch" "uuid", "_manifest" "text", "_sha" "text", "_rows" integer, "_bytes" integer, "_checkpoint" "jsonb", "_completed" boolean) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."connect_commit"("_run" "uuid", "_token" "uuid", "_stream" "uuid", "_sequence" integer, "_batch" "uuid", "_manifest" "text", "_sha" "text", "_rows" integer, "_bytes" integer, "_checkpoint" "jsonb", "_completed" boolean) TO "service_role";



REVOKE ALL ON FUNCTION "public"."connect_discard_dead_letters"("_connection" "uuid", "_dead_letters" "uuid"[]) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."connect_discard_dead_letters"("_connection" "uuid", "_dead_letters" "uuid"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."connect_discard_dead_letters"("_connection" "uuid", "_dead_letters" "uuid"[]) TO "service_role";



REVOKE ALL ON FUNCTION "public"."connect_enqueue"("_connection" "uuid", "_request" "uuid", "_operation" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."connect_enqueue"("_connection" "uuid", "_request" "uuid", "_operation" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."connect_enqueue"("_connection" "uuid", "_request" "uuid", "_operation" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."connect_enqueue_replay"("_connection" "uuid", "_request" "uuid", "_dead_letters" "uuid"[]) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."connect_enqueue_replay"("_connection" "uuid", "_request" "uuid", "_dead_letters" "uuid"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."connect_enqueue_replay"("_connection" "uuid", "_request" "uuid", "_dead_letters" "uuid"[]) TO "service_role";



REVOKE ALL ON FUNCTION "public"."connect_finish"("_run" "uuid", "_token" "uuid", "_ok" boolean, "_code" "text", "_catalog" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."connect_finish"("_run" "uuid", "_token" "uuid", "_ok" boolean, "_code" "text", "_catalog" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."connect_guard_stream"() FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."connect_guard_stream"() TO "service_role";



REVOKE ALL ON FUNCTION "public"."connect_heartbeat"("_run" "uuid", "_token" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."connect_heartbeat"("_run" "uuid", "_token" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."connect_record_dead_letters"("_run" "uuid", "_token" "uuid", "_stream" "uuid", "_sequence" integer, "_object" "text", "_entries" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."connect_record_dead_letters"("_run" "uuid", "_token" "uuid", "_stream" "uuid", "_sequence" integer, "_object" "text", "_entries" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."connect_report"("_run" "uuid", "_token" "uuid", "_code" "text", "_detail" "jsonb", "_stream" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."connect_report"("_run" "uuid", "_token" "uuid", "_code" "text", "_detail" "jsonb", "_stream" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."connect_rewind_stream"("_stream" "uuid", "_state" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."connect_rewind_stream"("_stream" "uuid", "_state" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."connect_rewind_stream"("_stream" "uuid", "_state" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."consume_rate_limit"("_key" "text", "_limit" integer, "_window_seconds" integer) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."consume_rate_limit"("_key" "text", "_limit" integer, "_window_seconds" integer) TO "service_role";



REVOKE ALL ON FUNCTION "public"."create_demo_ingest_run_verified"("_actor" "uuid", "_target" "text", "_total" bigint, "_batch" integer, "_concurrency" integer, "_plan" "jsonb", "_partitions" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."create_demo_ingest_run_verified"("_actor" "uuid", "_target" "text", "_total" bigint, "_batch" integer, "_concurrency" integer, "_plan" "jsonb", "_partitions" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."create_ingest_run_atomic"("_source" "uuid", "_target" "text", "_total" bigint, "_batch" integer, "_concurrency" integer, "_plan" "jsonb", "_partitions" "jsonb", "_pk" "text"[]) FROM PUBLIC;



REVOKE ALL ON FUNCTION "public"."create_ingest_run_verified"("_actor" "uuid", "_source" "uuid", "_target" "text", "_total" bigint, "_batch" integer, "_concurrency" integer, "_plan" "jsonb", "_partitions" "jsonb", "_pk" "text"[]) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."create_ingest_run_verified"("_actor" "uuid", "_source" "uuid", "_target" "text", "_total" bigint, "_batch" integer, "_concurrency" integer, "_plan" "jsonb", "_partitions" "jsonb", "_pk" "text"[]) TO "service_role";



REVOKE ALL ON FUNCTION "public"."create_ingest_source_verified"("_actor" "uuid", "_name" "text", "_kind" "text", "_target" "text", "_config" "jsonb", "_headers" "jsonb", "_hash" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."create_ingest_source_verified"("_actor" "uuid", "_name" "text", "_kind" "text", "_target" "text", "_config" "jsonb", "_headers" "jsonb", "_hash" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."dataset_policy_decision"("_subject" "uuid", "_dataset" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."dataset_policy_decision"("_subject" "uuid", "_dataset" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."fail_ingest_partition_fenced"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_token" "uuid", "_error" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."fail_ingest_partition_fenced"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_token" "uuid", "_error" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."file_object_is_committed"("_name" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."file_object_is_committed"("_name" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."file_object_is_committed"("_name" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."guard_ingest_credentials"() FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."guard_ingest_credentials"() TO "service_role";



REVOKE ALL ON FUNCTION "public"."guard_resource_identity_update"() FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."guard_resource_identity_update"() TO "service_role";



REVOKE ALL ON FUNCTION "public"."handle_new_user"() FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."handle_new_user"() TO "service_role";



REVOKE ALL ON FUNCTION "public"."has_entitlement"("_org" "uuid", "_feature" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."has_entitlement"("_org" "uuid", "_feature" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."has_entitlement"("_org" "uuid", "_feature" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."has_role"("_user_id" "uuid", "_role" "public"."app_role") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."has_role"("_user_id" "uuid", "_role" "public"."app_role") TO "authenticated";
GRANT ALL ON FUNCTION "public"."has_role"("_user_id" "uuid", "_role" "public"."app_role") TO "service_role";



REVOKE ALL ON FUNCTION "public"."heartbeat_ingest_partition"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_seconds" integer) FROM PUBLIC;



REVOKE ALL ON FUNCTION "public"."heartbeat_ingest_partition_fenced"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_token" "uuid", "_lease_seconds" integer) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."heartbeat_ingest_partition_fenced"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_token" "uuid", "_lease_seconds" integer) TO "service_role";



REVOKE ALL ON FUNCTION "public"."is_org_member"("_org" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."is_org_member"("_org" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."is_org_member"("_org" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."is_steward_for"("_user" "uuid", "_org" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."is_steward_for"("_user" "uuid", "_org" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."is_steward_for"("_user" "uuid", "_org" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."log_admin_change"() FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."log_admin_change"() TO "service_role";



GRANT ALL ON FUNCTION "public"."mask_text"("_v" "text", "_mode" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."mask_text"("_v" "text", "_mode" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."mask_text"("_v" "text", "_mode" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."publish_ingest_run"("_run" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."publish_ingest_run"("_run" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."publish_ingest_run"("_run" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."read_ingest_table"("_name" "text", "_after" "text", "_limit" integer) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."read_ingest_table"("_name" "text", "_after" "text", "_limit" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."read_ingest_table"("_name" "text", "_after" "text", "_limit" integer) TO "service_role";



REVOKE ALL ON FUNCTION "public"."register_ingest_landing_batch"("_actor" "uuid", "_source" "uuid", "_key" "text", "_sha" "text", "_rows" integer, "_mode" "text", "_batch" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."register_ingest_landing_batch"("_actor" "uuid", "_source" "uuid", "_key" "text", "_sha" "text", "_rows" integer, "_mode" "text", "_batch" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."register_personal_verified_file_table"("_owner" "uuid", "_name" "text", "_prefix" "text", "_columns" "text"[], "_parts" integer, "_rows" bigint, "_source_sha" "text", "_manifest_sha" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."register_personal_verified_file_table"("_owner" "uuid", "_name" "text", "_prefix" "text", "_columns" "text"[], "_parts" integer, "_rows" bigint, "_source_sha" "text", "_manifest_sha" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."register_verified_file_table"("_owner" "uuid", "_org" "uuid", "_name" "text", "_prefix" "text", "_columns" "text"[], "_parts" integer, "_rows" bigint, "_source_sha" "text", "_manifest_sha" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."register_verified_file_table"("_owner" "uuid", "_org" "uuid", "_name" "text", "_prefix" "text", "_columns" "text"[], "_parts" integer, "_rows" bigint, "_source_sha" "text", "_manifest_sha" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."reject_row_mutation"() FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."reject_row_mutation"() TO "service_role";



GRANT ALL ON FUNCTION "public"."row_passes"("_row" "jsonb", "_col" "text", "_op" "text", "_val" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."row_passes"("_row" "jsonb", "_col" "text", "_op" "text", "_val" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."row_passes"("_row" "jsonb", "_col" "text", "_op" "text", "_val" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."schema_register_version"("_actor" "uuid", "_organization" "uuid", "_name" "text", "_schema" "jsonb", "_fingerprint" "text", "_expected_latest" integer, "_compatibility" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."schema_register_version"("_actor" "uuid", "_organization" "uuid", "_name" "text", "_schema" "jsonb", "_fingerprint" "text", "_expected_latest" integer, "_compatibility" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."secure_tables"() FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."secure_tables"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."secure_tables"() TO "service_role";



REVOKE ALL ON FUNCTION "public"."serve_manifest"("_subject" "uuid", "_names" "text"[]) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."serve_manifest"("_subject" "uuid", "_names" "text"[]) TO "service_role";



REVOKE ALL ON FUNCTION "public"."shares_org_with"("_user" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."shares_org_with"("_user" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."shares_org_with"("_user" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."validate_file_table_scope"() FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."validate_file_table_scope"() TO "service_role";



REVOKE ALL ON FUNCTION "public"."validate_partition_creation"() FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."validate_partition_creation"() TO "service_role";



REVOKE ALL ON FUNCTION "public"."verify_admin_audit"() FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."verify_admin_audit"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."verify_admin_audit"() TO "service_role";



REVOKE ALL ON FUNCTION "public"."write_ingest_batch"("_run" "uuid", "_idx" integer, "_worker" "text", "_rows" "jsonb", "_checkpoint" "jsonb", "_done" boolean, "_lease_seconds" integer) FROM PUBLIC;



REVOKE ALL ON FUNCTION "public"."write_ingest_batch_fenced"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_token" "uuid", "_rows" "jsonb", "_checkpoint" "jsonb", "_done" boolean, "_lease_seconds" integer) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."write_ingest_batch_fenced"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_token" "uuid", "_rows" "jsonb", "_checkpoint" "jsonb", "_done" boolean, "_lease_seconds" integer) TO "service_role";



REVOKE ALL ON FUNCTION "public"."yield_ingest_partition_fenced"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_token" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."yield_ingest_partition_fenced"("_run" "uuid", "_idx" integer, "_worker" "text", "_lease_token" "uuid") TO "service_role";












GRANT ALL ON TABLE "core"."dataset_versions" TO "service_role";



GRANT ALL ON TABLE "core"."datasets" TO "service_role";









GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."access_policies" TO "authenticated";
GRANT ALL ON TABLE "public"."access_policies" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."admin_audit" TO "authenticated";
GRANT ALL ON TABLE "public"."admin_audit" TO "service_role";



GRANT ALL ON SEQUENCE "public"."admin_audit_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."admin_audit_id_seq" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."alert_settings" TO "authenticated";
GRANT ALL ON TABLE "public"."alert_settings" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."audit_log" TO "authenticated";
GRANT ALL ON TABLE "public"."audit_log" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."change_requests" TO "authenticated";
GRANT ALL ON TABLE "public"."change_requests" TO "service_role";



GRANT ALL ON TABLE "public"."connect_attempts" TO "service_role";



GRANT SELECT("id") ON TABLE "public"."connect_attempts" TO "authenticated";



GRANT SELECT("run_id") ON TABLE "public"."connect_attempts" TO "authenticated";



GRANT SELECT("attempt") ON TABLE "public"."connect_attempts" TO "authenticated";



GRANT SELECT("status") ON TABLE "public"."connect_attempts" TO "authenticated";



GRANT SELECT("started_at") ON TABLE "public"."connect_attempts" TO "authenticated";



GRANT SELECT("finished_at") ON TABLE "public"."connect_attempts" TO "authenticated";



GRANT SELECT("error_code") ON TABLE "public"."connect_attempts" TO "authenticated";



GRANT SELECT ON TABLE "public"."connect_commits" TO "authenticated";
GRANT ALL ON TABLE "public"."connect_commits" TO "service_role";



GRANT SELECT ON TABLE "public"."connect_connections" TO "authenticated";
GRANT ALL ON TABLE "public"."connect_connections" TO "service_role";



GRANT SELECT ON TABLE "public"."connect_dead_letters" TO "authenticated";
GRANT ALL ON TABLE "public"."connect_dead_letters" TO "service_role";



GRANT SELECT ON TABLE "public"."connect_events" TO "authenticated";
GRANT ALL ON TABLE "public"."connect_events" TO "service_role";



GRANT SELECT ON TABLE "public"."connect_run_streams" TO "authenticated";
GRANT ALL ON TABLE "public"."connect_run_streams" TO "service_role";



GRANT ALL ON TABLE "public"."connect_runs" TO "service_role";



GRANT SELECT("id") ON TABLE "public"."connect_runs" TO "authenticated";



GRANT SELECT("connection_id") ON TABLE "public"."connect_runs" TO "authenticated";



GRANT SELECT("request_key") ON TABLE "public"."connect_runs" TO "authenticated";



GRANT SELECT("operation") ON TABLE "public"."connect_runs" TO "authenticated";



GRANT SELECT("status") ON TABLE "public"."connect_runs" TO "authenticated";



GRANT SELECT("attempts") ON TABLE "public"."connect_runs" TO "authenticated";



GRANT SELECT("max_attempts") ON TABLE "public"."connect_runs" TO "authenticated";



GRANT SELECT("next_attempt_at") ON TABLE "public"."connect_runs" TO "authenticated";



GRANT SELECT("rows_committed") ON TABLE "public"."connect_runs" TO "authenticated";



GRANT SELECT("bytes_committed") ON TABLE "public"."connect_runs" TO "authenticated";



GRANT SELECT("error_code") ON TABLE "public"."connect_runs" TO "authenticated";



GRANT SELECT("created_at") ON TABLE "public"."connect_runs" TO "authenticated";



GRANT SELECT("finished_at") ON TABLE "public"."connect_runs" TO "authenticated";



GRANT SELECT ON TABLE "public"."connect_streams" TO "authenticated";
GRANT ALL ON TABLE "public"."connect_streams" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."connection_secrets" TO "authenticated";
GRANT ALL ON TABLE "public"."connection_secrets" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."connections" TO "authenticated";
GRANT ALL ON TABLE "public"."connections" TO "service_role";



GRANT ALL ON TABLE "public"."entitlements" TO "service_role";
GRANT SELECT ON TABLE "public"."entitlements" TO "authenticated";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."execution_engines" TO "authenticated";
GRANT ALL ON TABLE "public"."execution_engines" TO "service_role";



GRANT SELECT,DELETE,UPDATE ON TABLE "public"."file_tables" TO "authenticated";
GRANT ALL ON TABLE "public"."file_tables" TO "service_role";



GRANT SELECT ON TABLE "public"."ingest_dead_letters" TO "authenticated";
GRANT ALL ON TABLE "public"."ingest_dead_letters" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."ingest_landing_batches" TO "authenticated";
GRANT ALL ON TABLE "public"."ingest_landing_batches" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."ingest_rows" TO "authenticated";
GRANT ALL ON TABLE "public"."ingest_rows" TO "service_role";



GRANT SELECT,DELETE ON TABLE "public"."ingest_runs" TO "authenticated";
GRANT ALL ON TABLE "public"."ingest_runs" TO "service_role";



GRANT ALL ON TABLE "public"."ingest_source_secrets" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."ingest_sources" TO "authenticated";
GRANT ALL ON TABLE "public"."ingest_sources" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."ingest_tables" TO "authenticated";
GRANT ALL ON TABLE "public"."ingest_tables" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."ingest_workers" TO "authenticated";
GRANT ALL ON TABLE "public"."ingest_workers" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."org_members" TO "authenticated";
GRANT ALL ON TABLE "public"."org_members" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."organizations" TO "authenticated";
GRANT ALL ON TABLE "public"."organizations" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."platform_settings" TO "authenticated";
GRANT ALL ON TABLE "public"."platform_settings" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."profiles" TO "authenticated";
GRANT ALL ON TABLE "public"."profiles" TO "service_role";



GRANT ALL ON TABLE "public"."rate_limit_windows" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."row_policies" TO "authenticated";
GRANT ALL ON TABLE "public"."row_policies" TO "service_role";



GRANT SELECT ON TABLE "public"."schema_subjects" TO "authenticated";
GRANT ALL ON TABLE "public"."schema_subjects" TO "service_role";



GRANT SELECT ON TABLE "public"."schema_versions" TO "authenticated";
GRANT ALL ON TABLE "public"."schema_versions" TO "service_role";



GRANT ALL ON TABLE "public"."storage_reconciliation_log" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."table_versions" TO "authenticated";
GRANT ALL ON TABLE "public"."table_versions" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."user_roles" TO "authenticated";
GRANT ALL ON TABLE "public"."user_roles" TO "service_role";



GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "public"."user_tables" TO "authenticated";
GRANT ALL ON TABLE "public"."user_tables" TO "service_role";









ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES  TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES  TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES  TO "service_role";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS  TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS  TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS  TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS  TO "service_role";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES  TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT SELECT,INSERT,DELETE,UPDATE ON TABLES  TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES  TO "service_role";
































INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types) VALUES ('ingest-files', 'ingest-files', 'f', NULL, NULL) ON CONFLICT (id) DO NOTHING;
CREATE POLICY "ingest files own delete" ON storage.objects AS PERMISSIVE FOR DELETE TO authenticated USING (((bucket_id = 'ingest-files'::text) AND ((storage.foldername(name))[1] = (auth.uid())::text) AND ((storage.foldername(name))[2] IS DISTINCT FROM 'published'::text) AND (NOT public.file_object_is_committed(name))));
CREATE POLICY "ingest files own insert" ON storage.objects AS PERMISSIVE FOR INSERT TO authenticated WITH CHECK (((bucket_id = 'ingest-files'::text) AND ((storage.foldername(name))[1] = (auth.uid())::text) AND ((storage.foldername(name))[2] IS DISTINCT FROM 'published'::text) AND (NOT public.file_object_is_committed(name))));
CREATE POLICY "ingest files own read" ON storage.objects AS PERMISSIVE FOR SELECT TO authenticated USING (((bucket_id = 'ingest-files'::text) AND ((storage.foldername(name))[1] = (auth.uid())::text)));
CREATE POLICY "ingest files own update" ON storage.objects AS PERMISSIVE FOR UPDATE TO authenticated USING (((bucket_id = 'ingest-files'::text) AND ((storage.foldername(name))[1] = (auth.uid())::text) AND ((storage.foldername(name))[2] IS DISTINCT FROM 'published'::text) AND (NOT public.file_object_is_committed(name)))) WITH CHECK (((bucket_id = 'ingest-files'::text) AND ((storage.foldername(name))[1] = (auth.uid())::text) AND ((storage.foldername(name))[2] IS DISTINCT FROM 'published'::text) AND (NOT public.file_object_is_committed(name))));
