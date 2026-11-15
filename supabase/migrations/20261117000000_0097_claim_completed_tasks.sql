-- 0097: a task whose stream is already complete can still be claimed.
-- A task that commits its last batch (completed) and then fails to finish (lost response, lease lost,
-- an agent or Spark driver crash after the final commit) goes back to retry_wait with
-- completed=true. connect_claim_task skipped completed rows, so nobody could claim it again and the
-- run stayed running forever. Such a task is now claimed like any other: agents skip completed
-- streams and finish, so the run completes without re-reading anything.

CREATE OR REPLACE FUNCTION public.connect_claim_task(_connection uuid,_worker text DEFAULT NULL) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE c public.connect_connections; r public.connect_runs; rs public.connect_run_streams; token uuid; payload jsonb;
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
