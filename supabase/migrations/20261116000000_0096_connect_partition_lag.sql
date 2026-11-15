-- 0096: Kafka per-partition lag (Community ingestion Phase 3, milestone K2).
-- How far each partition's committed position is behind the partition's end offset, as last
-- observed by the agent. Kept out of checkpoints on purpose: a retried commit must reproduce its
-- checkpoint byte for byte, and end offsets keep moving between attempts. Reports are fenced by
-- the reporting task's lease like every other agent write, and owners read them under RLS.

CREATE TABLE public.connect_partition_lag (
  stream_id uuid NOT NULL REFERENCES public.connect_streams(id) ON DELETE CASCADE,
  partition integer NOT NULL CHECK (partition BETWEEN 0 AND 999999),
  position numeric(20,0) NOT NULL CHECK (position>=0),
  end_offset numeric(20,0) NOT NULL CHECK (end_offset>=0),
  lag numeric(20,0) GENERATED ALWAYS AS (greatest(end_offset-position,0)) STORED,
  run_id uuid NOT NULL REFERENCES public.connect_runs(id),
  observed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (stream_id, partition)
);
ALTER TABLE public.connect_partition_lag ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Verified active accounts" ON public.connect_partition_lag AS RESTRICTIVE FOR ALL TO authenticated
  USING (public.is_active_user()) WITH CHECK (public.is_active_user());
CREATE POLICY connect_partition_lag_read ON public.connect_partition_lag FOR SELECT TO authenticated
  USING (EXISTS (SELECT 1 FROM public.connect_streams s JOIN public.connect_connections c ON c.id=s.connection_id
    WHERE s.id=connect_partition_lag.stream_id AND public.can_read_scoped(c.owner_id,c.organization_id)));
REVOKE ALL ON public.connect_partition_lag FROM PUBLIC, anon, authenticated;
GRANT SELECT ON public.connect_partition_lag TO authenticated;

-- Records the agent's latest observation for some partitions of a stream. The caller must hold a
-- live lease covering the stream; a partition task (shard) may report only its own partition.
-- Offsets arrive as decimal strings (the agent API parses JSON numbers as 64-bit floats).
CREATE FUNCTION public.connect_record_lag(_run uuid,_token uuid,_stream uuid,_partitions jsonb) RETURNS integer
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE r public.connect_runs; rs public.connect_run_streams; n integer;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Service only'; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE id=_run;
 IF r.id IS NULL OR r.status<>'running' OR NOT public.connect_lease_valid(r,_token,_stream) THEN RAISE EXCEPTION 'Stale lag report'; END IF;
 IF r.lease_mode='task' THEN SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id AND lease_token=_token AND stream_id=_stream;
 ELSE SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id AND stream_id=_stream AND shard=''; END IF;
 IF rs.run_id IS NULL THEN RAISE EXCEPTION 'Stream not in run'; END IF;
 IF jsonb_typeof(_partitions)<>'array' OR jsonb_array_length(_partitions) NOT BETWEEN 1 AND 1024
   OR EXISTS(SELECT 1 FROM jsonb_array_elements(_partitions) p WHERE jsonb_typeof(p)<>'object'
     OR coalesce(p->>'partition','') !~ '^[0-9]{1,6}$' OR coalesce(p->>'position','') !~ '^[0-9]{1,20}$' OR coalesce(p->>'end','') !~ '^[0-9]{1,20}$'
     OR (rs.shard<>'' AND p->>'partition'<>rs.shard))
   OR (SELECT count(DISTINCT p->>'partition') FROM jsonb_array_elements(_partitions) p)<>jsonb_array_length(_partitions)
 THEN RAISE EXCEPTION 'Invalid lag report'; END IF;
 INSERT INTO public.connect_partition_lag(stream_id,partition,position,end_offset,run_id,observed_at)
  SELECT _stream,(p->>'partition')::integer,(p->>'position')::numeric,(p->>'end')::numeric,r.id,now() FROM jsonb_array_elements(_partitions) p
  ON CONFLICT (stream_id,partition) DO UPDATE SET position=excluded.position,end_offset=excluded.end_offset,run_id=excluded.run_id,observed_at=excluded.observed_at;
 GET DIAGNOSTICS n=ROW_COUNT;
 RETURN n;
END $$;
REVOKE ALL ON FUNCTION public.connect_record_lag(uuid,uuid,uuid,jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.connect_record_lag(uuid,uuid,uuid,jsonb) TO service_role;
