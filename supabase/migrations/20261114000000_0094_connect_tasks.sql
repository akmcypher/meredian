-- 0094: Connect tasks (Community ingestion Phase 1, milestone M1).
-- A run's streams become individually leased tasks, so several agents (or agent slots) can work on
-- one run at once. Fencing moves from the run to the task: every commit, dead-letter record,
-- replay deferral, report, heartbeat and finish must present a live lease for the task that owns
-- the stream. Pre-task agents keep working unchanged: a run is leased either as a whole ('run',
-- by connect_claim) or per task ('task', by connect_claim_task), decided at its first claim, and
-- the two paths never share a run. check/discover runs have no streams and stay run-leased.

ALTER TABLE public.connect_runs
  ADD COLUMN lease_mode text CHECK (lease_mode IN ('run','task'));

ALTER TABLE public.connect_run_streams
  ADD COLUMN task_status text NOT NULL DEFAULT 'queued'
    CHECK (task_status IN ('queued','running','retry_wait','done','failed','cancelled')),
  ADD COLUMN lease_token uuid,
  ADD COLUMN lease_until timestamptz,
  ADD COLUMN attempts integer NOT NULL DEFAULT 0 CHECK (attempts>=0),
  ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN worker text CHECK (worker IS NULL OR length(worker)<=200),
  ADD COLUMN error_code text,
  ADD COLUMN started_at timestamptz,
  ADD COLUMN finished_at timestamptz,
  ADD CONSTRAINT connect_run_streams_task_lease CHECK ((task_status='running')=(lease_token IS NOT NULL));

-- Streams already finished under the run model count as done tasks; nothing else changes for them.
UPDATE public.connect_run_streams SET task_status='done' WHERE completed;
CREATE UNIQUE INDEX connect_run_streams_lease_token ON public.connect_run_streams(lease_token) WHERE lease_token IS NOT NULL;
CREATE INDEX connect_run_streams_claimable ON public.connect_run_streams(run_id) WHERE NOT completed AND task_status IN ('queued','running','retry_wait');

-- The one lease rule. Run-leased: the run's token and lease. Task-leased: a running task of this
-- run with this token and a live lease, and (when a stream is named) that task owns the stream.
CREATE FUNCTION public.connect_lease_valid(r public.connect_runs,_token uuid,_stream uuid) RETURNS boolean
 LANGUAGE sql STABLE SET search_path=public AS $$
 SELECT r.id IS NOT NULL AND r.status='running' AND _token IS NOT NULL AND CASE
   WHEN r.lease_mode='task' THEN EXISTS(SELECT 1 FROM public.connect_run_streams rs
     WHERE rs.run_id=r.id AND rs.task_status='running' AND rs.lease_token=_token AND rs.lease_until>now()
       AND (_stream IS NULL OR rs.stream_id=_stream))
   ELSE r.lease_token IS NOT DISTINCT FROM _token AND r.lease_until>now() END
$$;

-- Read-only lease check for the agent API, which rejects stale attempts before doing any work.
CREATE FUNCTION public.connect_lease_check(_run uuid,_token uuid,_stream uuid DEFAULT NULL) RETURNS boolean
 LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=public AS $$
DECLARE r public.connect_runs;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run;
 RETURN coalesce(public.connect_lease_valid(r,_token,_stream),false);
END $$;

CREATE OR REPLACE FUNCTION public.connect_heartbeat(_run uuid,_token uuid) RETURNS boolean
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE r public.connect_runs;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run;
 IF r.id IS NULL OR r.status<>'running' OR NOT EXISTS(SELECT 1 FROM public.connect_connections c WHERE c.id=r.connection_id AND c.enabled) THEN RETURN false; END IF;
 IF r.lease_mode='task' THEN
  UPDATE public.connect_run_streams SET lease_until=now()+interval '90 seconds'
   WHERE run_id=r.id AND task_status='running' AND lease_token=_token AND lease_until>now();
  RETURN FOUND;
 END IF;
 UPDATE public.connect_runs SET lease_until=now()+interval '90 seconds'
  WHERE id=r.id AND status='running' AND lease_token=_token AND lease_until>now();
 RETURN FOUND;
END $$;

-- Claims one task (one stream) of the connection's oldest active run. Scheduled runs are created
-- here as in connect_claim. Runs with no streams (check/discover) and runs already leased as a
-- whole are handed to connect_claim, so a task-capable agent can serve every run.
CREATE FUNCTION public.connect_claim_task(_connection uuid,_worker text DEFAULT NULL) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE c public.connect_connections; r public.connect_runs; rs public.connect_run_streams; token uuid; payload jsonb;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Agent service only'; END IF;
 SELECT * INTO c FROM public.connect_connections WHERE id=_connection FOR UPDATE;
 IF NOT FOUND OR NOT c.enabled OR NOT EXISTS(SELECT 1 FROM public.ingest_sources s WHERE s.id=c.source_id AND s.enabled AND s.owner_id=c.owner_id AND s.organization_id IS NOT DISTINCT FROM c.organization_id) OR (c.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members WHERE org_id=c.organization_id AND user_id=c.owner_id)) THEN RETURN NULL; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE connection_id=c.id AND status IN ('queued','retry_wait','running') ORDER BY created_at LIMIT 1 FOR UPDATE;
 -- Scheduled runs, exactly as connect_claim creates them.
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
 SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id AND NOT completed
   AND (task_status='queued' OR (task_status='retry_wait' AND next_attempt_at<=now()) OR (task_status='running' AND lease_until<=now()))
   ORDER BY stream_id LIMIT 1 FOR UPDATE SKIP LOCKED;
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
  WHERE run_id=rs.run_id AND stream_id=rs.stream_id RETURNING * INTO rs;
 SELECT coalesce(jsonb_agg(jsonb_build_object('id',s.id,'namespace',s.namespace,'name',s.name,'primary_key',s.primary_key,'primary_keys',to_jsonb(s.key_columns),'cursor_field',s.cursor_field,'sync_mode',CASE WHEN r.operation IN ('reset','backfill') THEN 'full_refresh' ELSE s.sync_mode END,'schema',x.schema_json,'state',x.checkpoint,'sequence',x.sequence,'completed',x.completed,'generation',x.generation,'deferred',x.replay_deferred,'dead_letters',CASE WHEN r.operation='replay' THEN (SELECT coalesce(jsonb_agg(jsonb_build_object('id',d.id,'position',d.position,'error_code',d.error_code,'object_key',d.object_key,'line',d.line) ORDER BY d.created_at),'[]') FROM public.connect_dead_letters d WHERE d.replay_run_id=r.id AND d.stream_id=s.id AND d.status='open') END)),'[]')
  INTO payload FROM public.connect_run_streams x JOIN public.connect_streams s ON s.id=x.stream_id WHERE x.run_id=r.id AND x.stream_id=rs.stream_id;
 RETURN jsonb_build_object('run_id',r.id,'lease_token',token,'attempt',rs.attempts,'task',true,'operation',r.operation,'streams',payload,'credential_ref',c.credential_ref,'source_version',c.source_version,'destination_version',c.destination_version,'operation_timeout_seconds',c.operation_timeout_seconds);
END $$;

-- Ends a task-mode run as failed and fences every outstanding task of it.
CREATE FUNCTION public.connect_fail_task_run(_run uuid,_code text) RETURNS void
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE r public.connect_runs;
BEGIN
 SELECT * INTO r FROM public.connect_runs WHERE id=_run FOR UPDATE;
 UPDATE public.connect_run_streams SET task_status='cancelled',lease_token=NULL,lease_until=NULL,finished_at=now()
  WHERE run_id=r.id AND task_status IN ('queued','running','retry_wait');
 UPDATE public.connect_runs SET status='failed',error_code=_code,lease_token=NULL,lease_until=NULL,finished_at=now() WHERE id=r.id;
 UPDATE public.connect_attempts SET status='failed',finished_at=now(),error_code=_code WHERE run_id=r.id AND attempt=r.attempts;
 INSERT INTO public.connect_events(connection_id,run_id,level,code) VALUES(r.connection_id,r.id,'ERROR',_code);
END $$;

-- Task-mode completion (reached through connect_finish). Success marks the task done; the run
-- succeeds when every task is done. A retryable failure puts only that task back with backoff;
-- a terminal one, or exhausted attempts, fails the run and fences its other tasks.
CREATE FUNCTION public.connect_finish_task(_run uuid,_token uuid,_ok boolean,_code text) RETURNS boolean
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE r public.connect_runs; rs public.connect_run_streams; final_ok boolean; final_code text; wait_seconds integer;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run FOR UPDATE;
 IF NOT public.connect_lease_valid(r,_token,NULL) THEN RAISE EXCEPTION 'Stale completion'; END IF;
 IF _code NOT IN ('ok','auth_failed','source_unavailable','schema_drift','connector_failed','destination_failed','timeout','dead_letter_budget','offset_out_of_range','source_changed','record_too_large','replay_incomplete') THEN RAISE EXCEPTION 'Unknown safe error code'; END IF;
 SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id AND lease_token=_token FOR UPDATE;
 IF _ok THEN
  IF NOT rs.completed THEN RAISE EXCEPTION 'Incomplete streams'; END IF;
  UPDATE public.connect_run_streams SET task_status='done',lease_token=NULL,lease_until=NULL,finished_at=now(),error_code=NULL
   WHERE run_id=rs.run_id AND stream_id=rs.stream_id;
  IF EXISTS(SELECT 1 FROM public.connect_run_streams WHERE run_id=r.id AND task_status<>'done') THEN RETURN true; END IF;
  final_ok:=true; final_code:='ok';
  IF r.operation='replay' AND EXISTS(SELECT 1 FROM public.connect_run_streams WHERE run_id=r.id AND replay_deferred>0) THEN final_ok:=false; final_code:='replay_incomplete'; END IF;
  IF NOT final_ok THEN PERFORM public.connect_fail_task_run(r.id,final_code); RETURN true; END IF;
  UPDATE public.connect_runs SET status='succeeded',error_code=NULL,lease_token=NULL,lease_until=NULL,finished_at=now() WHERE id=r.id;
  UPDATE public.connect_attempts SET status='succeeded',finished_at=now(),error_code=NULL WHERE run_id=r.id AND attempt=r.attempts;
  INSERT INTO public.connect_events(connection_id,run_id,level,code) VALUES(r.connection_id,r.id,'INFO','run_succeeded');
  RETURN true;
 END IF;
 IF rs.attempts>=r.max_attempts OR _code IN ('auth_failed','schema_drift','dead_letter_budget','offset_out_of_range','source_changed','record_too_large','replay_incomplete') THEN
  UPDATE public.connect_run_streams SET task_status='failed',lease_token=NULL,lease_until=NULL,finished_at=now(),error_code=_code
   WHERE run_id=rs.run_id AND stream_id=rs.stream_id;
  PERFORM public.connect_fail_task_run(r.id,_code);
  RETURN true;
 END IF;
 wait_seconds:=least(300,5*(2^rs.attempts)::integer)+floor(random()*5)::integer;
 UPDATE public.connect_run_streams SET task_status='retry_wait',lease_token=NULL,lease_until=NULL,error_code=_code,next_attempt_at=now()+make_interval(secs=>wait_seconds)
  WHERE run_id=rs.run_id AND stream_id=rs.stream_id;
 INSERT INTO public.connect_events(connection_id,run_id,level,code) VALUES(r.connection_id,r.id,'WARN',_code);
 RETURN true;
END $$;

-- Existing fenced RPCs, unchanged except the lease predicate (and the two mode switches).
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
 IF r.id IS NULL OR r.status<>'running' OR NOT public.connect_lease_valid(r,_token,_stream) OR NOT c.enabled OR (c.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members WHERE org_id=c.organization_id AND user_id=c.owner_id)) OR NOT EXISTS(SELECT 1 FROM public.ingest_sources src WHERE src.id=c.source_id AND src.enabled AND src.owner_id=c.owner_id AND src.organization_id IS NOT DISTINCT FROM c.organization_id) THEN RAISE EXCEPTION 'Stale or unauthorized commit'; END IF;
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

CREATE OR REPLACE FUNCTION public.connect_record_dead_letters(_run uuid,_token uuid,_stream uuid,_sequence integer,_object text,_entries jsonb) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE r public.connect_runs; c public.connect_connections; rs public.connect_run_streams; n integer;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run FOR UPDATE; SELECT * INTO c FROM public.connect_connections WHERE id=r.connection_id;
 IF r.id IS NULL OR r.status<>'running' OR r.operation NOT IN ('sync','reset','backfill') OR NOT public.connect_lease_valid(r,_token,_stream) OR NOT c.enabled THEN RAISE EXCEPTION 'Stale or unauthorized dead letter'; END IF;
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

CREATE OR REPLACE FUNCTION public.connect_defer_replay(_run uuid,_token uuid,_stream uuid,_ids uuid[]) RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE r public.connect_runs; rs public.connect_run_streams; n integer; total integer;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 IF _ids IS NULL OR cardinality(_ids) NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'Invalid deferred selection'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run FOR UPDATE;
 IF r.id IS NULL OR r.operation<>'replay' OR r.status<>'running' OR NOT public.connect_lease_valid(r,_token,_stream)
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
   VALUES(r.connection_id,r.id,'replay_deferred',jsonb_build_array(jsonb_build_object('stream_id',_stream,'deferred',total))); END IF;
 RETURN total;
END $$;

CREATE OR REPLACE FUNCTION public.connect_report(_run uuid,_token uuid,_code text,_detail jsonb,_stream uuid DEFAULT NULL) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE r public.connect_runs; tagged jsonb;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run;
 IF r.id IS NULL OR r.status<>'running' OR NOT public.connect_lease_valid(r,_token,_stream) THEN RAISE EXCEPTION 'Stale report'; END IF;
 IF _code NOT IN ('offset_out_of_range','source_changed','record_too_large','replay_incomplete') OR jsonb_typeof(_detail)<>'array' OR jsonb_array_length(_detail) NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'Invalid report'; END IF;
 IF _stream IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.connect_run_streams WHERE run_id=r.id AND stream_id=_stream) THEN RAISE EXCEPTION 'Stream not in run'; END IF;
 SELECT jsonb_agg(CASE WHEN _stream IS NULL THEN g ELSE g||jsonb_build_object('stream_id',_stream) END) INTO tagged FROM jsonb_array_elements(_detail) g;
 INSERT INTO public.connect_events(connection_id,run_id,level,code,detail) VALUES(r.connection_id,r.id,'ERROR',_code,tagged); RETURN true;
END $$;

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
 IF r.lease_mode='task' THEN RETURN public.connect_finish_task(r.id,_token,_ok,_code); END IF;
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

-- Every function here is for the agent API (service role) only.
REVOKE ALL ON FUNCTION public.connect_lease_valid(public.connect_runs,uuid,uuid) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION public.connect_lease_check(uuid,uuid,uuid) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION public.connect_claim_task(uuid,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION public.connect_fail_task_run(uuid,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION public.connect_finish_task(uuid,uuid,boolean,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION public.connect_heartbeat(uuid,uuid) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.connect_lease_check(uuid,uuid,uuid) TO service_role;
GRANT EXECUTE ON FUNCTION public.connect_claim_task(uuid,text) TO service_role;
GRANT EXECUTE ON FUNCTION public.connect_heartbeat(uuid,uuid) TO service_role;

-- Task lease tokens are agent credentials, like connect_runs.lease_token: owners can see a task's
-- progress but never its token. connect_run_streams had a table-wide client SELECT grant; replace it
-- with per-column grants that leave lease_token out.
REVOKE SELECT ON public.connect_run_streams FROM authenticated;
GRANT SELECT (run_id,stream_id,initial_state,checkpoint,schema_json,sequence,completed,generation,replay_deferred,
  task_status,lease_until,attempts,next_attempt_at,worker,error_code,started_at,finished_at)
  ON public.connect_run_streams TO authenticated;
GRANT SELECT (lease_mode) ON public.connect_runs TO authenticated;
