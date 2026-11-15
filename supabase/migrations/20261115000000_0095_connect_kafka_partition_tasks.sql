-- 0095: Kafka per-partition tasks (Community ingestion Phase 1, milestone M4, Kafka part).
-- A task-leased Kafka stream is split into one task per topic partition, so several workers read
-- one topic at once. The first task of the stream (shard '') plans: the agent lists the topic's
-- partitions and calls connect_split_task, which replaces that task with one task per partition.
-- Each partition task starts from the stream checkpoint restricted to its own partition, commits
-- with its own sequence, and merges only its own partition's offset into the stream state.
--
-- Commits keep one sequence per (run, stream), assigned by the control plane, so Bronze readers
-- (serve_connect_bronze, dataset registration) page through a split stream exactly as before. The
-- task's own sequence is shard_sequence; (run, stream, shard, shard_sequence) is the idempotency key.
-- Unsplit streams use shard '' and shard_sequence = sequence, so nothing changes for them.

ALTER TABLE public.connect_run_streams
  ADD COLUMN shard text NOT NULL DEFAULT '' CHECK (shard='' OR shard ~ '^[0-9]{1,6}$');
ALTER TABLE public.connect_run_streams DROP CONSTRAINT connect_run_streams_pkey;
ALTER TABLE public.connect_run_streams ADD PRIMARY KEY (run_id, stream_id, shard);

-- Commit rows are immutable (connect_commit_immutable), so existing rows are not rewritten:
-- a NULL shard_sequence means the commit predates shards and equals sequence.
ALTER TABLE public.connect_commits
  ADD COLUMN shard text NOT NULL DEFAULT '' CHECK (shard='' OR shard ~ '^[0-9]{1,6}$'),
  ADD COLUMN shard_sequence integer CHECK (shard_sequence IS NULL OR shard_sequence>=0);
CREATE UNIQUE INDEX connect_commits_task_sequence
  ON public.connect_commits(run_id, stream_id, shard, coalesce(shard_sequence, sequence));

-- Replaces the planning task (shard '') of a Kafka stream with one queued task per partition. Only
-- the holder of that task's live lease may split, only before it committed anything, and only for
-- sync/reset/backfill runs of a Kafka connection. Each partition task gets the stream checkpoint
-- with positions restricted to its partition; identity, gap approval and generation are kept.
CREATE FUNCTION public.connect_split_task(_run uuid,_token uuid,_shards text[]) RETURNS integer
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE r public.connect_runs; c public.connect_connections; rs public.connect_run_streams; n integer;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run FOR UPDATE;
 SELECT * INTO c FROM public.connect_connections WHERE id=r.connection_id;
 IF r.id IS NULL OR r.lease_mode IS DISTINCT FROM 'task' OR NOT public.connect_lease_valid(r,_token,NULL) OR NOT c.enabled THEN RAISE EXCEPTION 'Stale split'; END IF;
 IF r.operation NOT IN ('sync','reset','backfill') OR c.source_version NOT LIKE 'kafka-%' THEN RAISE EXCEPTION 'Stream cannot be split'; END IF;
 IF _shards IS NULL OR cardinality(_shards) NOT BETWEEN 1 AND 1024
   OR EXISTS(SELECT 1 FROM unnest(_shards) x WHERE x IS NULL OR x !~ '^[0-9]{1,6}$')
   OR (SELECT count(DISTINCT x) FROM unnest(_shards) x)<>cardinality(_shards) THEN RAISE EXCEPTION 'Invalid shards'; END IF;
 SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id AND lease_token=_token FOR UPDATE;
 IF rs.run_id IS NULL OR rs.shard<>'' OR rs.sequence<>0 OR rs.completed THEN RAISE EXCEPTION 'Stream cannot be split'; END IF;
 INSERT INTO public.connect_run_streams(run_id,stream_id,shard,initial_state,checkpoint,schema_json,generation,task_status)
  SELECT rs.run_id,rs.stream_id,x,
    CASE WHEN rs.initial_state ? 'positions' THEN rs.initial_state||jsonb_build_object('positions',
      CASE WHEN rs.initial_state->'positions' ? x THEN jsonb_build_object(x,rs.initial_state->'positions'->x) ELSE '{}'::jsonb END) ELSE rs.initial_state END,
    CASE WHEN rs.checkpoint ? 'positions' THEN rs.checkpoint||jsonb_build_object('positions',
      CASE WHEN rs.checkpoint->'positions' ? x THEN jsonb_build_object(x,rs.checkpoint->'positions'->x) ELSE '{}'::jsonb END) ELSE rs.checkpoint END,
    rs.schema_json,rs.generation,'queued'
  FROM unnest(_shards) x;
 GET DIAGNOSTICS n=ROW_COUNT;
 DELETE FROM public.connect_run_streams WHERE run_id=rs.run_id AND stream_id=rs.stream_id AND shard='';
 INSERT INTO public.connect_events(connection_id,run_id,code,detail)
  VALUES(c.id,r.id,'task_split',jsonb_build_array(jsonb_build_object('stream_id',rs.stream_id,'tasks',n)));
 RETURN n;
END $$;

-- Claim: as in 0094, plus the task's shard and whether the agent should plan a split first.
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
 SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id AND NOT completed
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

-- Finish: as in 0094, addressing the task row by its shard.
CREATE OR REPLACE FUNCTION public.connect_finish_task(_run uuid,_token uuid,_ok boolean,_code text) RETURNS boolean
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
 IF rs.attempts>=r.max_attempts OR _code IN ('auth_failed','schema_drift','dead_letter_budget','offset_out_of_range','source_changed','record_too_large','replay_incomplete') THEN
  UPDATE public.connect_run_streams SET task_status='failed',lease_token=NULL,lease_until=NULL,finished_at=now(),error_code=_code
   WHERE run_id=rs.run_id AND stream_id=rs.stream_id AND shard=rs.shard;
  PERFORM public.connect_fail_task_run(r.id,_code);
  RETURN true;
 END IF;
 wait_seconds:=least(300,5*(2^rs.attempts)::integer)+floor(random()*5)::integer;
 UPDATE public.connect_run_streams SET task_status='retry_wait',lease_token=NULL,lease_until=NULL,error_code=_code,next_attempt_at=now()+make_interval(secs=>wait_seconds)
  WHERE run_id=rs.run_id AND stream_id=rs.stream_id AND shard=rs.shard;
 INSERT INTO public.connect_events(connection_id,run_id,level,code) VALUES(r.connection_id,r.id,'WARN',_code);
 RETURN true;
END $$;

-- Commit: the task row is the one holding this lease (task mode) or the stream's only row (run
-- mode). A partition task's commit gets the next stream-wide sequence and merges only its own
-- partition offsets into the stream state; everything else is checked exactly as before.
CREATE OR REPLACE FUNCTION public.connect_commit(_run uuid, _token uuid, _stream uuid, _sequence integer, _batch uuid, _manifest text, _sha text, _rows integer, _bytes integer, _checkpoint jsonb, _completed boolean)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE r public.connect_runs; c public.connect_connections; rs public.connect_run_streams; s public.connect_streams; prior public.connect_commits; b public.ingest_landing_batches; cid uuid; seq integer;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run FOR UPDATE; SELECT * INTO c FROM public.connect_connections WHERE id=r.connection_id;
 IF r.id IS NULL OR r.status<>'running' OR NOT public.connect_lease_valid(r,_token,_stream) OR NOT c.enabled OR (c.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members WHERE org_id=c.organization_id AND user_id=c.owner_id)) OR NOT EXISTS(SELECT 1 FROM public.ingest_sources src WHERE src.id=c.source_id AND src.enabled AND src.owner_id=c.owner_id AND src.organization_id IS NOT DISTINCT FROM c.organization_id) THEN RAISE EXCEPTION 'Stale or unauthorized commit'; END IF;
 IF r.lease_mode='task' THEN SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id AND lease_token=_token AND stream_id=_stream FOR UPDATE;
 ELSE SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id AND stream_id=_stream AND shard='' FOR UPDATE; END IF;
 IF rs.run_id IS NULL THEN RAISE EXCEPTION 'Stream sequence or generation conflict'; END IF;
 SELECT * INTO prior FROM public.connect_commits WHERE run_id=r.id AND stream_id=_stream AND shard=rs.shard AND coalesce(shard_sequence,sequence)=_sequence;
 IF FOUND THEN IF prior.sha256<>_sha OR prior.checkpoint IS DISTINCT FROM _checkpoint OR prior.row_count<>_rows OR prior.byte_count<>_bytes THEN RAISE EXCEPTION 'Commit identity conflict'; END IF; RETURN prior.id; END IF;
 SELECT * INTO s FROM public.connect_streams WHERE id=_stream;
 IF rs.completed OR rs.sequence<>_sequence OR s.state_generation<>rs.generation THEN RAISE EXCEPTION 'Stream sequence or generation conflict'; END IF;
 -- A partition checkpoint names at most its own partition's offset (none yet on a new stream).
 IF rs.shard<>'' AND _checkpoint ? 'positions' AND (jsonb_typeof(_checkpoint->'positions')<>'object' OR EXISTS(SELECT 1 FROM jsonb_object_keys(_checkpoint->'positions') k WHERE k<>rs.shard)) THEN RAISE EXCEPTION 'Checkpoint outside the task partition'; END IF;
 SELECT * INTO b FROM public.ingest_landing_batches WHERE id=_batch AND source_id=c.source_id AND owner_id=c.owner_id;
 IF b.id IS NULL OR b.sha256<>_sha OR b.row_count<>_rows OR _bytes NOT BETWEEN 0 AND 8388608 OR _manifest<>c.owner_id::text||'/published/'||b.batch_key::text||'/manifest.json' THEN RAISE EXCEPTION 'Invalid destination attestation'; END IF;
 -- The run row is locked above, so the stream-wide sequence is assigned without races.
 IF rs.shard='' THEN seq:=_sequence;
 ELSE SELECT coalesce(max(sequence)+1,0) INTO seq FROM public.connect_commits WHERE run_id=r.id AND stream_id=_stream; END IF;
 INSERT INTO public.connect_commits(run_id,stream_id,sequence,shard,shard_sequence,batch_id,manifest_key,sha256,row_count,byte_count,checkpoint) VALUES(r.id,s.id,seq,rs.shard,_sequence,b.id,_manifest,_sha,_rows,_bytes,_checkpoint) RETURNING id INTO cid;
 UPDATE public.connect_run_streams SET checkpoint=_checkpoint,sequence=sequence+1,completed=_completed WHERE run_id=r.id AND stream_id=s.id AND shard=rs.shard;
 IF r.operation='replay' THEN UPDATE public.connect_dead_letters SET status='replayed',resolved_at=now() WHERE replay_run_id=r.id AND stream_id=s.id AND status='open' AND id::text IN (SELECT jsonb_array_elements_text(coalesce(_checkpoint->'replayed','[]')));
 ELSIF rs.shard='' THEN UPDATE public.connect_streams SET state=_checkpoint,last_success_at=now() WHERE id=s.id;
 -- Partition tasks: this partition's offset joins the others. Per-run keys behave as for unsplit
 -- streams: the consumed gap approval is dropped (every partition task carries its own copy from the
 -- split), and gaps are those of the latest commit only.
 ELSE UPDATE public.connect_streams SET state=((coalesce(state,'{}'::jsonb)-'gap_approval'-'gaps')||(_checkpoint-'positions'))
   ||jsonb_build_object('positions',coalesce(state->'positions','{}'::jsonb)||coalesce(_checkpoint->'positions','{}'::jsonb)),last_success_at=now() WHERE id=s.id;
 END IF;
 UPDATE public.connect_runs SET rows_committed=rows_committed+_rows,bytes_committed=bytes_committed+_bytes WHERE id=r.id;
 INSERT INTO public.connect_events(connection_id,run_id,code) VALUES(c.id,r.id,'batch_committed'); RETURN cid;
END $function$;

-- Dead letters: the sequence check reads the task row holding this lease.
CREATE OR REPLACE FUNCTION public.connect_record_dead_letters(_run uuid,_token uuid,_stream uuid,_sequence integer,_object text,_entries jsonb) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE r public.connect_runs; c public.connect_connections; rs public.connect_run_streams; n integer;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run FOR UPDATE; SELECT * INTO c FROM public.connect_connections WHERE id=r.connection_id;
 IF r.id IS NULL OR r.status<>'running' OR r.operation NOT IN ('sync','reset','backfill') OR NOT public.connect_lease_valid(r,_token,_stream) OR NOT c.enabled THEN RAISE EXCEPTION 'Stale or unauthorized dead letter'; END IF;
 IF r.lease_mode='task' THEN SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id AND lease_token=_token AND stream_id=_stream;
 ELSE SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id AND stream_id=_stream AND shard=''; END IF;
 IF rs.run_id IS NULL OR rs.sequence<>_sequence OR _object NOT LIKE c.owner_id::text||'/dead-letters/%' OR jsonb_typeof(_entries)<>'array' OR jsonb_array_length(_entries) NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'Invalid dead letter batch'; END IF;
 INSERT INTO public.connect_dead_letters(connection_id,stream_id,run_id,sequence,position,error_code,object_key,line)
  SELECT c.id,_stream,r.id,_sequence,e->>'position',e->>'error_code',_object,(e->>'line')::integer FROM jsonb_array_elements(_entries) e
  ON CONFLICT(stream_id,position) DO NOTHING;
 SELECT count(*) INTO n FROM public.connect_dead_letters WHERE run_id=r.id;
 IF n>c.dead_letter_budget THEN RAISE EXCEPTION 'dead_letter_budget'; END IF;
 INSERT INTO public.connect_events(connection_id,run_id,level,code) VALUES(c.id,r.id,'WARN','dead_letters_recorded'); RETURN n;
END $$;

-- Dataset registration read one run-stream row STRICTly; a split stream has one row per partition.
CREATE OR REPLACE FUNCTION core.register_connect_commit(_commit uuid)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'core'
AS $function$
DECLARE c public.connect_commits; r public.connect_runs; s public.connect_streams;
  conn public.connect_connections; b public.ingest_landing_batches; snapshot jsonb; did uuid; vid uuid;
BEGIN
  SELECT * INTO STRICT c FROM public.connect_commits WHERE id=_commit;
  SELECT * INTO STRICT r FROM public.connect_runs WHERE id=c.run_id;
  SELECT * INTO STRICT s FROM public.connect_streams WHERE id=c.stream_id;
  SELECT * INTO STRICT conn FROM public.connect_connections WHERE id=r.connection_id;
  SELECT * INTO STRICT b FROM public.ingest_landing_batches WHERE id=c.batch_id;
  SELECT schema_json INTO STRICT snapshot FROM public.connect_run_streams WHERE run_id=r.id AND stream_id=s.id AND shard=c.shard;
  INSERT INTO core.datasets(owner_id,organization_id,namespace,name,layer,source_id)
    VALUES(conn.owner_id,conn.organization_id,'connect','connect_'||replace(s.id::text,'-',''),'bronze',s.id)
    ON CONFLICT(namespace,source_id) DO NOTHING;
  SELECT id INTO STRICT did FROM core.datasets WHERE namespace='connect' AND source_id=s.id;
  INSERT INTO core.dataset_versions(dataset_id,source_version_id,format,bucket,object_key,sha256,
    byte_count,row_count,schema,produced_by,evidence_class,created_at)
    VALUES(did,c.id,'ndjson','ingest-files',b.object_key,c.sha256,c.byte_count,c.row_count,snapshot,
      jsonb_build_object('product','ingest','run_id',r.id,'task_id',s.id,'attempt',NULL),
      'server_verified',c.created_at)
    ON CONFLICT(source_version_id) DO NOTHING;
  SELECT id INTO STRICT vid FROM core.dataset_versions WHERE source_version_id=c.id;
  UPDATE core.datasets SET current_version_id=vid WHERE id=did;
END $function$;

REVOKE ALL ON FUNCTION public.connect_split_task(uuid,uuid,text[]) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.connect_split_task(uuid,uuid,text[]) TO service_role;
GRANT SELECT (shard) ON public.connect_run_streams TO authenticated;
