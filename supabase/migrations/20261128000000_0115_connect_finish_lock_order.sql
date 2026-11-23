-- connect_finish locked the run, then updated connect_connections for a successful check or
-- discover. Claims, enqueues and stream edits lock the connection first and the run second (0114),
-- so a finishing check could deadlock with a claim on the same connection. Lock the connection
-- before the run for those operations. connection_id and operation never change after a run is
-- created, so reading them unlocked only chooses the lock; the locked row is checked again.
CREATE OR REPLACE FUNCTION public.connect_finish(_run uuid, _token uuid, _ok boolean, _code text, _catalog jsonb DEFAULT NULL::jsonb)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE r public.connect_runs; wait_seconds integer; conn uuid; op text;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT connection_id,operation INTO conn,op FROM public.connect_runs WHERE id=_run;
 IF op IN ('check','discover') THEN PERFORM 1 FROM public.connect_connections WHERE id=conn FOR NO KEY UPDATE; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run FOR UPDATE;
 IF r.connection_id IS DISTINCT FROM conn OR r.operation IS DISTINCT FROM op THEN RAISE EXCEPTION 'Stale completion'; END IF;
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

REVOKE ALL ON FUNCTION public.connect_finish(uuid,uuid,boolean,text,jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.connect_finish(uuid,uuid,boolean,text,jsonb) TO service_role;
