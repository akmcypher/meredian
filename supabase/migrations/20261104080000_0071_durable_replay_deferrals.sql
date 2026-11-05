-- Replay deferrals are durable run evidence, not ephemeral claim-response metadata.
ALTER TABLE public.connect_run_streams ADD COLUMN replay_deferred integer NOT NULL DEFAULT 0 CHECK (replay_deferred>=0);
CREATE FUNCTION public.connect_defer_replay(_run uuid,_token uuid,_stream uuid,_ids uuid[]) RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE r public.connect_runs; rs public.connect_run_streams; n integer; total integer;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 IF _ids IS NULL OR cardinality(_ids) NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'Invalid deferred selection'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run FOR UPDATE;
 IF r.id IS NULL OR r.operation<>'replay' OR r.status<>'running' OR r.lease_token IS DISTINCT FROM _token OR r.lease_until<=now()
  OR NOT EXISTS(SELECT 1 FROM public.connect_connections c JOIN public.ingest_sources s ON s.id=c.source_id
    WHERE c.id=r.connection_id AND c.enabled AND s.enabled AND c.owner_id=s.owner_id
      AND c.organization_id IS NOT DISTINCT FROM s.organization_id
      AND (c.organization_id IS NULL OR EXISTS(SELECT 1 FROM public.org_members m WHERE m.org_id=c.organization_id AND m.user_id=c.owner_id)))
 THEN RAISE EXCEPTION 'Stale replay attempt'; END IF;
 SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id AND stream_id=_stream FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Invalid replay stream'; END IF;
 WITH released AS (UPDATE public.connect_dead_letters SET replay_run_id=NULL
   WHERE replay_run_id=r.id AND stream_id=_stream AND id=ANY(_ids) AND status='open' RETURNING id)
 SELECT count(*)::integer INTO n FROM released;
 UPDATE public.connect_run_streams SET replay_deferred=replay_deferred+n
   WHERE run_id=r.id AND stream_id=_stream RETURNING replay_deferred INTO total;
 IF n>0 THEN INSERT INTO public.connect_events(connection_id,run_id,code,detail)
   VALUES(r.connection_id,r.id,'replay_deferred',jsonb_build_object('stream_id',_stream,'deferred',total)); END IF;
 RETURN total;
END $$;
REVOKE ALL ON FUNCTION public.connect_defer_replay(uuid,uuid,uuid,uuid[]) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.connect_defer_replay(uuid,uuid,uuid,uuid[]) TO service_role;

CREATE OR REPLACE FUNCTION public.connect_claim(_connection uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE c public.connect_connections; r public.connect_runs; token uuid; payload jsonb;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Agent service only'; END IF;
 SELECT * INTO c FROM public.connect_connections WHERE id=_connection FOR UPDATE;
 IF NOT FOUND OR NOT c.enabled OR NOT EXISTS(SELECT 1 FROM public.ingest_sources s WHERE s.id=c.source_id AND s.enabled AND s.owner_id=c.owner_id AND s.organization_id IS NOT DISTINCT FROM c.organization_id) OR (c.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members WHERE org_id=c.organization_id AND user_id=c.owner_id)) THEN RETURN NULL; END IF;
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
 SELECT coalesce(jsonb_agg(jsonb_build_object('id',s.id,'namespace',s.namespace,'name',s.name,'primary_key',s.primary_key,'primary_keys',to_jsonb(s.key_columns),'cursor_field',s.cursor_field,'sync_mode',CASE WHEN r.operation IN ('reset','backfill') THEN 'full_refresh' ELSE s.sync_mode END,'schema',rs.schema_json,'state',rs.checkpoint,'sequence',rs.sequence,'completed',rs.completed,'generation',rs.generation,'deferred',rs.replay_deferred,'dead_letters',CASE WHEN r.operation='replay' THEN (SELECT coalesce(jsonb_agg(jsonb_build_object('id',x.id,'position',x.position,'error_code',x.error_code,'object_key',x.object_key,'line',x.line) ORDER BY x.created_at),'[]') FROM public.connect_dead_letters x WHERE x.replay_run_id=r.id AND x.stream_id=s.id AND x.status='open') END)), '[]') INTO payload FROM public.connect_run_streams rs JOIN public.connect_streams s ON s.id=rs.stream_id WHERE rs.run_id=r.id;
 INSERT INTO public.connect_events(connection_id,run_id,code) VALUES(c.id,r.id,'attempt_started');
 RETURN jsonb_build_object('run_id',r.id,'lease_token',token,'attempt',r.attempts,'operation',r.operation,'streams',payload,'credential_ref',c.credential_ref,'source_version',c.source_version,'destination_version',c.destination_version,'operation_timeout_seconds',c.operation_timeout_seconds);
END $function$;

CREATE OR REPLACE FUNCTION public.connect_commit(_run uuid, _token uuid, _stream uuid, _sequence integer, _batch uuid, _manifest text, _sha text, _rows integer, _bytes integer, _checkpoint jsonb, _completed boolean)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE r public.connect_runs; c public.connect_connections; rs public.connect_run_streams; s public.connect_streams; prior public.connect_commits; b public.ingest_landing_batches; cid uuid;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run FOR UPDATE; SELECT * INTO c FROM public.connect_connections WHERE id=r.connection_id;
 IF r.id IS NULL OR r.status<>'running' OR r.lease_token IS DISTINCT FROM _token OR r.lease_until<=now() OR NOT c.enabled OR (c.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members WHERE org_id=c.organization_id AND user_id=c.owner_id)) OR NOT EXISTS(SELECT 1 FROM public.ingest_sources src WHERE src.id=c.source_id AND src.enabled AND src.owner_id=c.owner_id AND src.organization_id IS NOT DISTINCT FROM c.organization_id) THEN RAISE EXCEPTION 'Stale or unauthorized commit'; END IF;
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
 INSERT INTO public.connect_events(connection_id,run_id,code) VALUES(c.id,r.id,'batch_committed'); RETURN cid;
END $function$;

CREATE OR REPLACE FUNCTION public.connect_finish(_run uuid, _token uuid, _ok boolean, _code text, _catalog jsonb DEFAULT NULL::jsonb)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE r public.connect_runs; wait_seconds integer;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run FOR UPDATE;
 IF r.status<>'running' OR r.lease_token IS DISTINCT FROM _token OR r.lease_until<=now() THEN RAISE EXCEPTION 'Stale completion'; END IF;
 IF _code NOT IN ('ok','auth_failed','source_unavailable','schema_drift','connector_failed','destination_failed','timeout','dead_letter_budget','offset_out_of_range','source_changed','record_too_large','replay_incomplete') THEN RAISE EXCEPTION 'Unknown safe error code'; END IF;
 IF _ok AND r.operation='replay' AND EXISTS(SELECT 1 FROM public.connect_run_streams WHERE run_id=r.id AND replay_deferred>0) THEN _ok:=false; _code:='replay_incomplete'; END IF;
 IF _ok AND r.operation IN ('sync','reset','backfill','replay') AND EXISTS(SELECT 1 FROM public.connect_run_streams WHERE run_id=r.id AND NOT completed) THEN RAISE EXCEPTION 'Incomplete streams'; END IF;
 IF _ok AND r.operation='discover' THEN IF jsonb_typeof(_catalog)<>'array' OR jsonb_array_length(_catalog)>1000 THEN RAISE EXCEPTION 'Invalid discovery'; END IF; UPDATE public.connect_connections SET discovered_catalog=_catalog,health='reachable' WHERE id=r.connection_id; END IF;
 IF _ok AND r.operation='check' THEN UPDATE public.connect_connections SET health='reachable' WHERE id=r.connection_id; END IF;
 wait_seconds:=least(300,5*(2^r.attempts)::integer)+floor(random()*5)::integer;
 UPDATE public.connect_runs SET status=CASE WHEN _ok THEN 'succeeded' WHEN attempts>=max_attempts OR _code IN ('auth_failed','schema_drift','dead_letter_budget','offset_out_of_range','source_changed','record_too_large','replay_incomplete') THEN 'failed' ELSE 'retry_wait' END,error_code=CASE WHEN _ok THEN NULL ELSE _code END,lease_token=NULL,lease_until=NULL,next_attempt_at=now()+make_interval(secs=>wait_seconds),finished_at=CASE WHEN _ok OR attempts>=max_attempts OR _code IN ('auth_failed','schema_drift','dead_letter_budget','offset_out_of_range','source_changed','record_too_large','replay_incomplete') THEN now() ELSE NULL END WHERE id=r.id;
 UPDATE public.connect_attempts SET status=CASE WHEN _ok THEN 'succeeded' ELSE 'failed' END,finished_at=now(),error_code=CASE WHEN _ok THEN NULL ELSE _code END WHERE run_id=r.id AND attempt=r.attempts;
 INSERT INTO public.connect_events(connection_id,run_id,level,code) VALUES(r.connection_id,r.id,CASE WHEN _ok THEN 'INFO' ELSE 'ERROR' END,CASE WHEN _ok THEN 'run_succeeded' ELSE _code END); RETURN true;
END $function$;

