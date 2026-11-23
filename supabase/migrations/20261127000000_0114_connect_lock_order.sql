-- 0114: connection locks no longer deadlock with commits and task finishes.
-- Claims, enqueues and stream edits lock the connection row and then a run row. Commits and
-- other leased calls lock the run row first, then reach the connection row through
-- connect_events foreign keys (FOR KEY SHARE) and the stream guard trigger (FOR UPDATE).
-- Concurrent shared-Kafka claims and commits deadlocked; the agent saw a 409 and exited.
--
-- 1. Connection locks use FOR NO KEY UPDATE: still exclusive between claims, enqueues and
--    connection edits, but compatible with foreign-key checks.
-- 2. The stream guard locks the connection only for inserts and configuration changes;
--    checkpoint state updates read nothing from it.

CREATE OR REPLACE FUNCTION public.connect_guard_stream() RETURNS trigger
 LANGUAGE plpgsql SET search_path=public AS $$
DECLARE source_version text; configured boolean;
BEGIN
 configured:=TG_OP='UPDATE' AND (NEW.namespace<>OLD.namespace OR NEW.name<>OLD.name OR NEW.primary_key<>OLD.primary_key OR NEW.key_columns IS DISTINCT FROM OLD.key_columns OR NEW.cursor_field<>OLD.cursor_field OR NEW.sync_mode<>OLD.sync_mode OR NEW.schema_json IS DISTINCT FROM OLD.schema_json);
 IF TG_OP='INSERT' OR configured OR NEW.connection_id IS DISTINCT FROM OLD.connection_id THEN
  SELECT c.source_version INTO source_version FROM public.connect_connections c WHERE c.id=NEW.connection_id FOR NO KEY UPDATE;
 END IF;
 IF TG_OP='INSERT' AND NEW.key_columns IS NULL THEN NEW.key_columns:=string_to_array(NEW.primary_key,','); END IF;
 IF configured THEN
  IF EXISTS(SELECT 1 FROM public.connect_runs WHERE connection_id=NEW.connection_id AND status IN ('queued','running','retry_wait')) THEN RAISE EXCEPTION 'Active replication configuration is immutable'; END IF;
  IF NEW.connection_id=OLD.connection_id AND source_version IN ('kafka-v1','spark-kafka-v1')
   AND NEW.namespace IS NOT DISTINCT FROM OLD.namespace AND NEW.name IS NOT DISTINCT FROM OLD.name
   AND NEW.primary_key IS NOT DISTINCT FROM OLD.primary_key AND NEW.key_columns IS NOT DISTINCT FROM OLD.key_columns
   AND NEW.cursor_field IS NOT DISTINCT FROM OLD.cursor_field AND NEW.sync_mode IS NOT DISTINCT FROM OLD.sync_mode
   AND jsonb_typeof(NEW.schema_json)='object' AND jsonb_typeof(OLD.schema_json)='object'
   AND (NEW.schema_json-ARRAY['x-avro-reader-schema','x-avro-reader-version']) IS NOT DISTINCT FROM (OLD.schema_json-ARRAY['x-avro-reader-schema','x-avro-reader-version']) THEN
   -- Even a caller-supplied replacement checkpoint/generation cannot discard committed offsets.
   NEW.state:=OLD.state; NEW.state_generation:=OLD.state_generation;
  ELSE
   NEW.state:='{}'; NEW.state_generation:=OLD.state_generation+1;
  END IF;
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION public.connect_guard_stream() FROM PUBLIC,anon,authenticated;

-- Rewrite only the connection lock clause of each function, keeping every other line and the
-- function's grants (CREATE OR REPLACE keeps ACLs). Each expected rewrite must match exactly.
DO $$
DECLARE
 expected constant jsonb := '{"connect_accept_offset_gap":1,"connect_claim":1,"connect_claim_pool_group_task":1,
  "connect_claim_pool_task":1,"connect_claim_selected_task":1,"connect_enqueue":1,"connect_enqueue_replay":1,
  "connect_retire_stream":1,"connect_rewind_stream":1,"connect_schedule_due":1}';
 lock constant text := '(connect_connections[^;]*?)FOR UPDATE';
 f record; def text; found integer;
BEGIN
 FOR f IN SELECT p.oid,p.proname FROM pg_proc p WHERE p.pronamespace='public'::regnamespace
   AND p.proname IN (SELECT jsonb_object_keys(expected)) LOOP
  def:=pg_get_functiondef(f.oid);
  found:=(SELECT count(*) FROM regexp_matches(def,lock,'g'));
  IF found<>(expected->>f.proname)::integer THEN
   RAISE EXCEPTION '0114: % has % connection locks, expected %',f.proname,found,expected->>f.proname;
  END IF;
  EXECUTE regexp_replace(def,lock,'\1FOR NO KEY UPDATE','g');
 END LOOP;
 IF EXISTS(SELECT 1 FROM pg_proc p WHERE p.pronamespace='public'::regnamespace
   AND p.prosrc ~* 'connect_connections[^;]*?FOR UPDATE') THEN
  RAISE EXCEPTION '0114: a connection lock still blocks foreign-key checks';
 END IF;
END $$;
