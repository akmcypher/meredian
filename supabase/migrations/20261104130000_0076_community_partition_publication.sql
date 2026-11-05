-- Community partition publication is self-contained: catalog metadata is an optional product.
-- Preserve tenant/owner authorization, row-count checks and the atomic snapshot swap.
CREATE OR REPLACE FUNCTION "public"."publish_ingest_run"("_run" "uuid") RETURNS bigint
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE r public.ingest_runs; n bigint; pending int; total int;
BEGIN
  SELECT * INTO r FROM public.ingest_runs WHERE id = _run FOR UPDATE;
  IF r.id IS NULL OR r.owner_id IS DISTINCT FROM auth.uid() THEN RAISE EXCEPTION 'not found'; END IF;
  IF r.status IN ('cancelled','published') THEN RAISE EXCEPTION 'run is %', r.status; END IF;
  IF r.organization_id IS NOT NULL AND NOT public.is_org_member(r.organization_id) THEN RAISE EXCEPTION 'not a member of the run organization'; END IF;
  SELECT count(*), count(*) FILTER (WHERE status <> 'done') INTO total, pending FROM public.ingest_partitions WHERE run_id = _run;
  IF total = 0 THEN RAISE EXCEPTION 'run has no partitions'; END IF;
  IF pending > 0 THEN RAISE EXCEPTION '% partitions not finished', pending; END IF;
  SELECT count(*) INTO n FROM public.ingest_rows WHERE run_id = _run;
  IF r.expected_rows > 0 AND n <> r.expected_rows THEN RAISE EXCEPTION 'row count % does not match expected %', n, r.expected_rows; END IF;
  PERFORM pg_advisory_xact_lock(hashtext('publish_ingest_scope:'||r.target_table));
 IF EXISTS (SELECT 1 FROM public.ingest_tables t WHERE t.owner_id=r.owner_id AND t.name=r.target_table AND t.organization_id IS DISTINCT FROM r.organization_id) THEN RAISE EXCEPTION 'published table scope conflict'; END IF;
 INSERT INTO public.ingest_tables(owner_id, name, current_run_id, row_count, published_at, organization_id)
  VALUES (r.owner_id, r.target_table, _run, n, now(), r.organization_id)
  ON CONFLICT (owner_id, name) DO UPDATE SET current_run_id = EXCLUDED.current_run_id, row_count = EXCLUDED.row_count, published_at = now();
  UPDATE public.ingest_runs SET status = 'published', published_at = now(), finished_at = coalesce(finished_at, now()), written_rows = n WHERE id = _run;
  RETURN n;
END $$;

REVOKE ALL ON FUNCTION public.publish_ingest_run(uuid) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.publish_ingest_run(uuid) TO authenticated, service_role;
