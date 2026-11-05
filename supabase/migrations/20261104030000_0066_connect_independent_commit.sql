-- Connect publication must work without optional catalog and lineage products.
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
 IF r.id IS NULL OR r.status<>'running' OR r.lease_token IS DISTINCT FROM _token OR r.lease_until<=now() OR NOT c.enabled OR (c.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members WHERE org_id=c.organization_id AND user_id=c.owner_id)) THEN RAISE EXCEPTION 'Stale or unauthorized commit'; END IF;
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
END $function$

