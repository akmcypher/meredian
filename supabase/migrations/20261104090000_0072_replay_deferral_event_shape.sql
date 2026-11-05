-- Preserve the existing bounded array event-detail contract.
CREATE OR REPLACE FUNCTION public.connect_defer_replay(_run uuid,_token uuid,_stream uuid,_ids uuid[]) RETURNS integer
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
   VALUES(r.connection_id,r.id,'replay_deferred',jsonb_build_array(jsonb_build_object('stream_id',_stream,'deferred',total))); END IF;
 RETURN total;
END $$;
