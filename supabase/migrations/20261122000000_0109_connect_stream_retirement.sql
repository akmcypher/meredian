-- Retire streams without deleting immutable Bronze or leaving CDC slots silently orphaned.
CREATE OR REPLACE FUNCTION public.connect_retire_stream(_stream uuid) RETURNS uuid
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE c public.connect_connections; s public.connect_streams; rid uuid; cp jsonb;
BEGIN
 SELECT conn.* INTO c FROM public.connect_connections conn JOIN public.connect_streams st ON st.connection_id=conn.id
 WHERE st.id=_stream AND conn.owner_id=auth.uid() FOR UPDATE OF conn;
 IF c.id IS NULL OR (c.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members WHERE org_id=c.organization_id AND user_id=auth.uid())) THEN RAISE EXCEPTION 'Connection owner required'; END IF;
 IF NOT c.enabled OR NOT EXISTS(SELECT 1 FROM public.ingest_sources src WHERE src.id=c.source_id AND src.enabled AND src.owner_id=c.owner_id AND src.organization_id IS NOT DISTINCT FROM c.organization_id) THEN RAISE EXCEPTION 'Enable connection and source for cleanup'; END IF;
 IF EXISTS(SELECT 1 FROM public.connect_runs WHERE connection_id=c.id AND status IN ('queued','running','retry_wait')) THEN RAISE EXCEPTION 'Stop active run before retiring a stream'; END IF;
 -- Cancellation fences publication immediately; allow the existing 60s watchdog grace
 -- plus transport margin before source cleanup, so a cancelled child can no longer start.
 IF EXISTS(SELECT 1 FROM public.connect_runs WHERE connection_id=c.id AND attempts>0 AND finished_at>now()-interval '90 seconds') THEN RAISE EXCEPTION 'Wait 90 seconds after the last worker run before source cleanup'; END IF;
 SELECT * INTO s FROM public.connect_streams WHERE id=_stream FOR UPDATE;
 IF s.state->'retirement'->>'status'='done' THEN RETURN NULL; END IF;
 cp:=s.state||jsonb_build_object('version',1,'completed',false,'retirement',jsonb_build_object('status',CASE WHEN c.source_version='postgres-cdc-v1' THEN 'pending' ELSE 'done' END));
 UPDATE public.connect_streams SET enabled=false,state=cp,state_generation=state_generation+1 WHERE id=s.id RETURNING * INTO s;
 IF c.source_version='postgres-cdc-v1' THEN
  INSERT INTO public.connect_runs(connection_id,request_key,operation) VALUES(c.id,gen_random_uuid(),'sync') RETURNING id INTO rid;
  INSERT INTO public.connect_run_streams(run_id,stream_id,initial_state,checkpoint,schema_json,generation)
   VALUES(rid,s.id,cp,cp,s.schema_json,s.state_generation);
 END IF;
 INSERT INTO public.connect_events(connection_id,run_id,code) VALUES(c.id,rid,'stream_retired');
 RETURN rid;
END $$;
REVOKE ALL ON FUNCTION public.connect_retire_stream(uuid) FROM PUBLIC,anon;
GRANT EXECUTE ON FUNCTION public.connect_retire_stream(uuid) TO authenticated;

CREATE OR REPLACE FUNCTION public.connect_retired_stream_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path=public AS $$
BEGIN
 IF OLD.state ? 'retirement' AND (NEW.enabled OR NEW.connection_id<>OLD.connection_id OR NEW.namespace<>OLD.namespace OR NEW.name<>OLD.name OR NEW.schema_json<>OLD.schema_json OR NEW.sync_mode<>OLD.sync_mode OR NEW.key_columns<>OLD.key_columns OR NEW.primary_key<>OLD.primary_key OR NEW.cursor_field<>OLD.cursor_field OR NEW.schema_policy<>OLD.schema_policy OR NOT (NEW.state ? 'retirement')) THEN
  RAISE EXCEPTION 'Retired streams cannot be reconfigured or resumed';
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION public.connect_retired_stream_guard() FROM PUBLIC,anon,authenticated;
CREATE TRIGGER connect_retired_stream_guard BEFORE UPDATE ON public.connect_streams
 FOR EACH ROW EXECUTE FUNCTION public.connect_retired_stream_guard();
