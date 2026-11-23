-- 0110: durable Connect scheduling, independent of agent polling.
-- The existing authenticated jobs tick calls this service-only RPC. A tick queues work;
-- it does not acquire execution leases or need any online workers.
CREATE FUNCTION public.connect_schedule_due(_limit integer DEFAULT 50) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE c public.connect_connections; rid uuid; queued integer := 0;
BEGIN
 IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'Scheduler service only'; END IF;
 IF _limit IS NULL OR _limit<1 OR _limit>1000 THEN RAISE EXCEPTION 'Invalid scheduler batch limit'; END IF;
 -- Match claims: connection first, then run, then streams. Skip busy connections so
 -- concurrent ticks and agent claims cannot hold one another up or queue duplicates.
 FOR c IN
  SELECT x.* FROM public.connect_connections x
  WHERE x.enabled AND x.interval_minutes>0 AND (x.next_run_at IS NULL OR x.next_run_at<=now())
   AND EXISTS(SELECT 1 FROM public.ingest_sources s WHERE s.id=x.source_id AND s.enabled
     AND s.owner_id=x.owner_id AND s.organization_id IS NOT DISTINCT FROM x.organization_id)
   AND EXISTS(SELECT 1 FROM auth.users u WHERE u.id=x.owner_id AND u.email_confirmed_at IS NOT NULL
     AND (u.banned_until IS NULL OR u.banned_until<=now()))
   AND (x.organization_id IS NULL OR EXISTS(SELECT 1 FROM public.org_members m
     JOIN public.organizations o ON o.id=m.org_id AND o.status='active'
     WHERE m.org_id=x.organization_id AND m.user_id=x.owner_id))
   AND EXISTS(SELECT 1 FROM public.connect_streams s WHERE s.connection_id=x.id AND s.enabled)
   AND NOT EXISTS(SELECT 1 FROM public.connect_runs r WHERE r.connection_id=x.id
     AND r.status IN ('queued','running','retry_wait'))
  ORDER BY x.next_run_at NULLS FIRST,x.id LIMIT _limit FOR UPDATE OF x SKIP LOCKED
 LOOP
  -- Recheck under the connection lock; every manual enqueue and agent claim takes
  -- this same lock. The partial unique active-run index is the final invariant.
  PERFORM 1 FROM public.connect_runs WHERE connection_id=c.id
   AND status IN ('queued','running','retry_wait') FOR UPDATE;
  IF FOUND THEN CONTINUE; END IF;
  PERFORM 1 FROM public.connect_streams WHERE connection_id=c.id AND enabled ORDER BY id FOR UPDATE;
  IF NOT FOUND THEN CONTINUE; END IF;
  INSERT INTO public.connect_runs(connection_id,request_key,operation)
   VALUES(c.id,gen_random_uuid(),'sync') RETURNING id INTO rid;
  INSERT INTO public.connect_run_streams(run_id,stream_id,initial_state,checkpoint,schema_json,generation)
   SELECT rid,s.id,CASE WHEN s.sync_mode='full_refresh' THEN '{}'::jsonb ELSE s.state END,
    CASE WHEN s.sync_mode='full_refresh' THEN '{}'::jsonb ELSE s.state END,s.schema_json,s.state_generation
   FROM public.connect_streams s WHERE s.connection_id=c.id AND s.enabled;
  -- Coalesce missed intervals into one run; never replay intervals missed during downtime.
  UPDATE public.connect_connections SET next_run_at=now()+make_interval(mins=>c.interval_minutes) WHERE id=c.id;
  INSERT INTO public.connect_events(connection_id,run_id,code) VALUES(c.id,rid,'schedule_queued');
  queued:=queued+1;
 END LOOP;
 RETURN jsonb_build_object('queued',queued);
END $$;
REVOKE ALL ON FUNCTION public.connect_schedule_due(integer) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.connect_schedule_due(integer) TO service_role;

-- Claims only consume persisted runs; a worker polling must not become a second scheduler.
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
 IF r.id IS NULL THEN RETURN NULL; END IF;
 IF r.lease_mode='task' THEN RETURN NULL; END IF;
 IF r.status='running' AND r.lease_until>now() THEN RETURN NULL; END IF;
 IF r.status='running' THEN UPDATE public.connect_attempts SET status='interrupted',finished_at=now(),error_code='lease_expired' WHERE run_id=r.id AND attempt=r.attempts; END IF;
 IF r.attempts>=r.max_attempts THEN UPDATE public.connect_runs SET status='failed',error_code='attempts_exhausted',finished_at=now(),lease_token=NULL WHERE id=r.id; RETURN NULL; END IF;
 IF r.next_attempt_at>now() THEN RETURN NULL; END IF;
 token:=gen_random_uuid(); UPDATE public.connect_runs SET status='running',lease_mode='run',attempts=attempts+1,lease_token=token,lease_until=now()+interval '90 seconds' WHERE id=r.id RETURNING * INTO r;
 INSERT INTO public.connect_attempts(run_id,attempt,token) VALUES(r.id,r.attempts,token);
 SELECT coalesce(jsonb_agg(jsonb_build_object('id',s.id,'namespace',s.namespace,'name',s.name,'primary_key',s.primary_key,'primary_keys',to_jsonb(s.key_columns),'cursor_field',s.cursor_field,'sync_mode',CASE WHEN r.operation IN ('reset','backfill') THEN 'full_refresh' ELSE s.sync_mode END,'schema',rs.schema_json,'state',rs.checkpoint,'sequence',rs.sequence,'completed',rs.completed,'generation',rs.generation,'deferred',rs.replay_deferred,'dead_letters',CASE WHEN r.operation='replay' THEN (SELECT coalesce(jsonb_agg(jsonb_build_object('id',x.id,'position',x.position,'error_code',x.error_code,'object_key',x.object_key,'line',x.line) ORDER BY x.created_at),'[]') FROM public.connect_dead_letters x WHERE x.replay_run_id=r.id AND x.stream_id=s.id AND x.status='open') END)), '[]') INTO payload FROM public.connect_run_streams rs JOIN public.connect_streams s ON s.id=rs.stream_id WHERE rs.run_id=r.id;
 INSERT INTO public.connect_events(connection_id,run_id,code) VALUES(c.id,r.id,'attempt_started');
 RETURN jsonb_build_object('run_id',r.id,'lease_token',token,'attempt',r.attempts,'operation',r.operation,'streams',payload,'credential_ref',c.credential_ref,'source_version',c.source_version,'destination_version',c.destination_version,'operation_timeout_seconds',c.operation_timeout_seconds);
END $function$;
CREATE OR REPLACE FUNCTION public.connect_claim_task(_connection uuid,_worker text DEFAULT NULL) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE c public.connect_connections; r public.connect_runs; rs public.connect_run_streams; token uuid; payload jsonb;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Agent service only'; END IF;
 SELECT * INTO c FROM public.connect_connections WHERE id=_connection FOR UPDATE;
 IF NOT FOUND OR NOT c.enabled OR NOT EXISTS(SELECT 1 FROM public.ingest_sources s WHERE s.id=c.source_id AND s.enabled AND s.owner_id=c.owner_id AND s.organization_id IS NOT DISTINCT FROM c.organization_id) OR (c.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members WHERE org_id=c.organization_id AND user_id=c.owner_id)) THEN RETURN NULL; END IF;
 -- 0108: a connection's limit counts its live task leases and live whole-run leases.
 IF c.max_parallel_tasks IS NOT NULL AND public.connect_running_tasks(c.id)>=c.max_parallel_tasks THEN RETURN NULL; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE connection_id=c.id AND status IN ('queued','retry_wait','running') ORDER BY created_at LIMIT 1 FOR UPDATE;
 IF r.id IS NULL THEN RETURN NULL; END IF;
 IF r.lease_mode='run' OR r.operation IN ('check','discover') THEN RETURN public.connect_claim(_connection); END IF;
 IF r.status<>'running' AND r.next_attempt_at>now() THEN RETURN NULL; END IF;
 IF r.lease_mode IS NULL OR r.status<>'running' THEN
  UPDATE public.connect_runs SET status='running',lease_mode='task',attempts=attempts+1,lease_token=NULL,lease_until=NULL WHERE id=r.id RETURNING * INTO r;
  INSERT INTO public.connect_attempts(run_id,attempt,token) VALUES(r.id,r.attempts,gen_random_uuid());
  INSERT INTO public.connect_events(connection_id,run_id,code) VALUES(c.id,r.id,'attempt_started');
 END IF;
 SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id
   AND (task_status='queued' OR (task_status='retry_wait' AND next_attempt_at<=now()) OR (task_status='running' AND lease_until<=now()))
   ORDER BY stream_id,shard='' DESC,length(shard),shard LIMIT 1 FOR UPDATE SKIP LOCKED;
 IF NOT FOUND THEN RETURN NULL; END IF;
 IF rs.task_status='running' THEN
  INSERT INTO public.connect_events(connection_id,run_id,level,code) VALUES(c.id,r.id,'WARN','task_lease_expired');
 END IF;
 IF rs.attempts>=r.max_attempts THEN
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
