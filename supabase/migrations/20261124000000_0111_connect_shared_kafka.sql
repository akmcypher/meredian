-- 0111: broker membership selects existing Postgres task leases; no broker offset authority.
ALTER TABLE public.connect_run_streams ADD COLUMN handoffs integer NOT NULL DEFAULT 0
 CHECK (handoffs>=0 AND handoffs<=attempts);
GRANT SELECT (handoffs) ON public.connect_run_streams TO authenticated;

-- Metadata only. Recheck source, workspace, verified owner and optional pool scope in SQL.
CREATE FUNCTION public.connect_group_context(_connection uuid,_pool uuid DEFAULT NULL) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE c public.connect_connections; p public.connect_pools; payload jsonb;
BEGIN
 IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO c FROM public.connect_connections WHERE id=_connection;
 IF c.id IS NULL OR NOT c.enabled OR c.source_version<>'kafka-v1' THEN RAISE EXCEPTION 'Shared Kafka connection unavailable'; END IF;
 IF NOT EXISTS(SELECT 1 FROM public.ingest_sources s WHERE s.id=c.source_id AND s.enabled AND s.owner_id=c.owner_id AND s.organization_id IS NOT DISTINCT FROM c.organization_id)
  OR NOT EXISTS(SELECT 1 FROM auth.users u WHERE u.id=c.owner_id AND u.email_confirmed_at IS NOT NULL AND (u.banned_until IS NULL OR u.banned_until<=now()))
  OR (c.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members m JOIN public.organizations o ON o.id=m.org_id AND o.status='active' WHERE m.org_id=c.organization_id AND m.user_id=c.owner_id)) THEN RAISE EXCEPTION 'Connection scope unavailable'; END IF;
 IF _pool IS NOT NULL THEN
  SELECT * INTO p FROM public.connect_pools WHERE id=_pool;
  IF p.id IS NULL OR NOT p.enabled OR c.pool_id IS DISTINCT FROM p.id OR c.organization_id IS DISTINCT FROM p.organization_id OR (p.organization_id IS NULL AND c.owner_id<>p.owner_id)
   OR NOT EXISTS(SELECT 1 FROM auth.users u WHERE u.id=p.owner_id AND u.email_confirmed_at IS NOT NULL AND (u.banned_until IS NULL OR u.banned_until<=now()))
   OR (p.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members m WHERE m.org_id=p.organization_id AND m.user_id=p.owner_id)) THEN RAISE EXCEPTION 'Pool scope mismatch'; END IF;
 END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_object('id',x.id,'topic',x.topic) ORDER BY x.id,x.topic),'[]'::jsonb) INTO payload FROM (
  SELECT s.id,coalesce(nullif(s.schema_json->>'x-kafka-topic',''),s.name) topic FROM public.connect_streams s WHERE s.connection_id=c.id AND s.enabled
   AND NOT EXISTS(SELECT 1 FROM public.connect_run_streams rs JOIN public.connect_runs r ON r.id=rs.run_id WHERE rs.stream_id=s.id AND r.connection_id=c.id AND r.status IN ('queued','running','retry_wait'))
  UNION
  SELECT s.id,coalesce(nullif(rs.schema_json->>'x-kafka-topic',''),s.name) topic
   FROM public.connect_run_streams rs JOIN public.connect_runs r ON r.id=rs.run_id JOIN public.connect_streams s ON s.id=rs.stream_id
   WHERE r.connection_id=c.id AND r.status IN ('queued','running','retry_wait')
 ) x;
 RETURN jsonb_build_object('id',c.id,'source_version',c.source_version,'credential_ref',c.credential_ref,'streams',payload,
  'pending',EXISTS(SELECT 1 FROM public.connect_runs r WHERE r.connection_id=c.id AND r.status IN ('queued','running','retry_wait')));
END $$;

CREATE FUNCTION public.connect_claim_selected_task(_connection uuid,_worker text DEFAULT NULL,_assignments jsonb DEFAULT NULL) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE c public.connect_connections; r public.connect_runs; rs public.connect_run_streams; token uuid; payload jsonb;
BEGIN
 IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'Agent service only'; END IF;
 IF _assignments IS NOT NULL THEN
  IF jsonb_typeof(_assignments) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'Invalid assignments'; END IF;
  IF (SELECT count(*) FROM jsonb_object_keys(_assignments))>1000 THEN RAISE EXCEPTION 'Invalid assignments'; END IF;
  IF (SELECT count(*)<>count(DISTINCT lower(key)) FROM jsonb_each(_assignments)) THEN RAISE EXCEPTION 'Invalid assignments'; END IF;
  SELECT coalesce(jsonb_object_agg(lower(key),value),'{}'::jsonb) INTO _assignments FROM jsonb_each(_assignments);
  IF EXISTS(SELECT 1 FROM jsonb_each(_assignments) e WHERE e.key !~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' OR jsonb_typeof(e.value)<>'array') THEN RAISE EXCEPTION 'Invalid assignments'; END IF;
  IF EXISTS(SELECT 1 FROM jsonb_each(_assignments) e WHERE jsonb_array_length(e.value)>1024
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(e.value) v WHERE jsonb_typeof(v)<>'number' OR v::text !~ '^[0-9]{1,6}$')
    OR jsonb_array_length(e.value)<>(SELECT count(DISTINCT v) FROM jsonb_array_elements(e.value) v)) THEN RAISE EXCEPTION 'Invalid assignments'; END IF;
 END IF;
 SELECT * INTO c FROM public.connect_connections WHERE id=_connection FOR UPDATE;
 IF _assignments IS NOT NULL THEN PERFORM public.connect_group_context(_connection); END IF;
 IF NOT FOUND OR NOT c.enabled OR NOT EXISTS(SELECT 1 FROM public.ingest_sources s WHERE s.id=c.source_id AND s.enabled AND s.owner_id=c.owner_id AND s.organization_id IS NOT DISTINCT FROM c.organization_id) OR (c.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members WHERE org_id=c.organization_id AND user_id=c.owner_id)) THEN RETURN NULL; END IF;
 -- 0108: a connection's limit counts its live task leases and live whole-run leases.
 IF c.max_parallel_tasks IS NOT NULL AND public.connect_running_tasks(c.id)>=c.max_parallel_tasks THEN RETURN NULL; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE connection_id=c.id AND status IN ('queued','retry_wait','running') ORDER BY created_at LIMIT 1 FOR UPDATE;
 IF r.id IS NULL THEN RETURN NULL; END IF;
 IF r.operation IN ('check','discover') OR (r.lease_mode='run' AND _assignments IS NULL) THEN RETURN public.connect_claim(_connection); END IF;
 IF r.lease_mode='run' THEN RETURN NULL; END IF;
 IF r.status<>'running' AND r.next_attempt_at>now() THEN RETURN NULL; END IF;
 IF r.lease_mode IS NULL OR r.status<>'running' THEN
  UPDATE public.connect_runs SET status='running',lease_mode='task',attempts=attempts+1,lease_token=NULL,lease_until=NULL WHERE id=r.id RETURNING * INTO r;
  INSERT INTO public.connect_attempts(run_id,attempt,token) VALUES(r.id,r.attempts,gen_random_uuid());
  INSERT INTO public.connect_events(connection_id,run_id,code) VALUES(c.id,r.id,'attempt_started');
 END IF;
 SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id
   AND (task_status='queued' OR (task_status='retry_wait' AND next_attempt_at<=now()) OR (task_status='running' AND lease_until<=now()))
   AND (_assignments IS NULL OR (jsonb_array_length(coalesce(_assignments->stream_id::text,'[]'::jsonb))>0 AND
     CASE WHEN shard='' THEN true ELSE (_assignments->stream_id::text) @> jsonb_build_array(shard::integer) END))
   ORDER BY stream_id,shard='' DESC,length(shard),shard LIMIT 1 FOR UPDATE SKIP LOCKED;
 IF NOT FOUND THEN RETURN NULL; END IF;
 IF rs.task_status='running' THEN
  INSERT INTO public.connect_events(connection_id,run_id,level,code) VALUES(c.id,r.id,'WARN','task_lease_expired');
 END IF;
 IF rs.attempts-rs.handoffs>=r.max_attempts THEN
  PERFORM public.connect_fail_task_run(r.id,'attempts_exhausted');
  RETURN NULL;
 END IF;
 token:=gen_random_uuid();
 UPDATE public.connect_run_streams SET task_status='running',lease_token=token,lease_until=now()+interval '90 seconds',
   attempts=attempts+1,worker=left(_worker,200),started_at=coalesce(started_at,now()),error_code=NULL
  WHERE run_id=rs.run_id AND stream_id=rs.stream_id AND shard=rs.shard RETURNING * INTO rs;
 SELECT coalesce(jsonb_agg(jsonb_build_object('id',s.id,'namespace',s.namespace,'name',s.name,'primary_key',s.primary_key,'primary_keys',to_jsonb(s.key_columns),'cursor_field',s.cursor_field,'sync_mode',CASE WHEN r.operation IN ('reset','backfill') THEN 'full_refresh' ELSE s.sync_mode END,'schema',x.schema_json,'state',x.checkpoint,'sequence',x.sequence,'completed',x.completed,'generation',x.generation,'deferred',x.replay_deferred,
   'shard',x.shard,
   'splittable',(x.shard='' AND x.sequence=0 AND r.operation IN ('sync','reset','backfill') AND c.source_version LIKE 'kafka-%'),
   'dead_letters',CASE WHEN r.operation='replay' THEN (SELECT coalesce(jsonb_agg(jsonb_build_object('id',d.id,'position',d.position,'error_code',d.error_code,'object_key',d.object_key,'line',d.line) ORDER BY d.created_at),'[]') FROM public.connect_dead_letters d WHERE d.replay_run_id=r.id AND d.stream_id=s.id AND d.status='open') END)),'[]')
  INTO payload FROM public.connect_run_streams x JOIN public.connect_streams s ON s.id=x.stream_id WHERE x.run_id=r.id AND x.stream_id=rs.stream_id AND x.shard=rs.shard;
 RETURN jsonb_build_object('run_id',r.id,'lease_token',token,'attempt',rs.attempts,'task',true,'operation',r.operation,'streams',payload,'credential_ref',c.credential_ref,'source_version',c.source_version,'destination_version',c.destination_version,'operation_timeout_seconds',c.operation_timeout_seconds);
END $$;

CREATE OR REPLACE FUNCTION public.connect_finish_task(_run uuid,_token uuid,_ok boolean,_code text) RETURNS boolean
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE r public.connect_runs; rs public.connect_run_streams; final_ok boolean; final_code text; wait_seconds integer;
BEGIN
 IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run FOR UPDATE;
 IF NOT public.connect_lease_valid(r,_token,NULL) THEN RAISE EXCEPTION 'Stale completion'; END IF;
 IF _code NOT IN ('ok','auth_failed','source_unavailable','schema_drift','connector_failed','destination_failed','timeout','dead_letter_budget','offset_out_of_range','source_changed','record_too_large','replay_incomplete') THEN RAISE EXCEPTION 'Unknown safe error code'; END IF;
 SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id AND lease_token=_token FOR UPDATE;
 IF _ok THEN
  IF NOT rs.completed THEN RAISE EXCEPTION 'Incomplete streams'; END IF;
  UPDATE public.connect_run_streams SET task_status='done',lease_token=NULL,lease_until=NULL,finished_at=now(),error_code=NULL
   WHERE run_id=rs.run_id AND stream_id=rs.stream_id AND shard=rs.shard;
  IF EXISTS(SELECT 1 FROM public.connect_run_streams WHERE run_id=r.id AND task_status<>'done') THEN RETURN true; END IF;
  final_ok:=true; final_code:='ok';
  IF r.operation='replay' AND EXISTS(SELECT 1 FROM public.connect_run_streams WHERE run_id=r.id AND replay_deferred>0) THEN final_ok:=false; final_code:='replay_incomplete'; END IF;
  IF NOT final_ok THEN PERFORM public.connect_fail_task_run(r.id,final_code); RETURN true; END IF;
  UPDATE public.connect_runs SET status='succeeded',error_code=NULL,lease_token=NULL,lease_until=NULL,finished_at=now() WHERE id=r.id;
  UPDATE public.connect_attempts SET status='succeeded',finished_at=now(),error_code=NULL WHERE run_id=r.id AND attempt=r.attempts;
  INSERT INTO public.connect_events(connection_id,run_id,level,code) VALUES(r.connection_id,r.id,'INFO','run_succeeded');
  RETURN true;
 END IF;
 IF rs.attempts-rs.handoffs>=r.max_attempts OR _code IN ('auth_failed','schema_drift','dead_letter_budget','offset_out_of_range','source_changed','record_too_large','replay_incomplete') THEN
  UPDATE public.connect_run_streams SET task_status='failed',lease_token=NULL,lease_until=NULL,finished_at=now(),error_code=_code
   WHERE run_id=rs.run_id AND stream_id=rs.stream_id AND shard=rs.shard;
  PERFORM public.connect_fail_task_run(r.id,_code);
  RETURN true;
 END IF;
 wait_seconds:=least(300,5*(2^least(rs.attempts-rs.handoffs,6))::integer)+floor(random()*5)::integer;
 UPDATE public.connect_run_streams SET task_status='retry_wait',lease_token=NULL,lease_until=NULL,error_code=_code,next_attempt_at=now()+make_interval(secs=>wait_seconds)
  WHERE run_id=rs.run_id AND stream_id=rs.stream_id AND shard=rs.shard;
 INSERT INTO public.connect_events(connection_id,run_id,level,code) VALUES(r.connection_id,r.id,'WARN',_code);
 RETURN true;
END $$;

-- Keep old polling semantics and quotas exactly; only shared claims supply a selector.
CREATE OR REPLACE FUNCTION public.connect_claim_task(_connection uuid,_worker text DEFAULT NULL) RETURNS jsonb
 LANGUAGE sql SECURITY DEFINER SET search_path=public AS $$
 SELECT public.connect_claim_selected_task(_connection,_worker,NULL)
$$;

CREATE FUNCTION public.connect_claim_pool_group_task(_pool uuid,_connection uuid,_assignments jsonb,
 _worker text DEFAULT NULL,_agent text DEFAULT NULL,_slots integer DEFAULT NULL,_version text DEFAULT NULL) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE claim jsonb;
BEGIN
 IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 -- Serialize the selected connection against reassignment and enforce the existing limit.
 PERFORM 1 FROM public.connect_connections WHERE id=_connection FOR UPDATE;
 PERFORM public.connect_group_context(_connection,_pool);
 IF _assignments IS NULL THEN RAISE EXCEPTION 'Invalid assignments'; END IF;
 IF _agent IS NOT NULL THEN
  INSERT INTO public.connect_workers(pool_id,agent,version,slots) VALUES(_pool,left(_agent,200),_version,_slots)
   ON CONFLICT(pool_id,agent) DO UPDATE SET last_seen_at=now(),version=EXCLUDED.version,slots=EXCLUDED.slots;
 END IF;
 claim:=public.connect_claim_selected_task(_connection,_worker,_assignments);
 IF claim IS NOT NULL THEN
  UPDATE public.connect_connections SET last_claimed_at=now() WHERE id=_connection;
  RETURN claim||jsonb_build_object('connection_id',_connection);
 END IF;
 RETURN NULL;
END $$;

-- Run-before-task locks match commit/finish. A landed completed checkpoint wins the race.
CREATE FUNCTION public.connect_abandon_task(_run uuid,_token uuid) RETURNS boolean
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE r public.connect_runs; rs public.connect_run_streams;
BEGIN
 IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run FOR UPDATE;
 IF r.id IS NULL OR r.lease_mode IS DISTINCT FROM 'task' OR NOT public.connect_lease_valid(r,_token,NULL) THEN RAISE EXCEPTION 'Stale handoff'; END IF;
 PERFORM public.connect_group_context(r.connection_id);
 SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id AND lease_token=_token FOR UPDATE;
 IF NOT FOUND OR rs.task_status<>'running' OR rs.lease_until<=now() THEN RAISE EXCEPTION 'Stale handoff'; END IF;
 IF rs.completed THEN RETURN public.connect_finish_task(r.id,_token,true,'ok'); END IF;
 UPDATE public.connect_run_streams SET task_status='queued',lease_token=NULL,lease_until=NULL,
  worker=NULL,error_code=NULL,next_attempt_at=now(),handoffs=handoffs+1
 WHERE run_id=rs.run_id AND stream_id=rs.stream_id AND shard=rs.shard;
 INSERT INTO public.connect_events(connection_id,run_id,code) VALUES(r.connection_id,r.id,'task_handoff');
 RETURN true;
END $$;
REVOKE ALL ON FUNCTION public.connect_group_context(uuid,uuid) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.connect_group_context(uuid,uuid) TO service_role;
REVOKE ALL ON FUNCTION public.connect_claim_selected_task(uuid,text,jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.connect_claim_selected_task(uuid,text,jsonb) TO service_role;
REVOKE ALL ON FUNCTION public.connect_claim_pool_group_task(uuid,uuid,jsonb,text,text,integer,text) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.connect_claim_pool_group_task(uuid,uuid,jsonb,text,text,integer,text) TO service_role;
REVOKE ALL ON FUNCTION public.connect_abandon_task(uuid,uuid) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.connect_abandon_task(uuid,uuid) TO service_role;
