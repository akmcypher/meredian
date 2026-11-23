-- 0112: changing the Avro reader projection does not change Kafka source positions.
-- Keep active-run immutability and every other replication reset from 0053.
CREATE OR REPLACE FUNCTION public.connect_guard_stream() RETURNS trigger
 LANGUAGE plpgsql SET search_path=public AS $$
DECLARE source_version text;
BEGIN
 SELECT c.source_version INTO source_version FROM public.connect_connections c WHERE c.id=NEW.connection_id FOR UPDATE;
 IF TG_OP='INSERT' AND NEW.key_columns IS NULL THEN NEW.key_columns:=string_to_array(NEW.primary_key,','); END IF;
 IF TG_OP='UPDATE' AND (NEW.namespace<>OLD.namespace OR NEW.name<>OLD.name OR NEW.primary_key<>OLD.primary_key OR NEW.key_columns IS DISTINCT FROM OLD.key_columns OR NEW.cursor_field<>OLD.cursor_field OR NEW.sync_mode<>OLD.sync_mode OR NEW.schema_json IS DISTINCT FROM OLD.schema_json) THEN
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
