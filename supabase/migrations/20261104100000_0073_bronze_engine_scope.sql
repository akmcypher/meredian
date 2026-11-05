-- Bind raw Bronze disclosure to the actual registered engine and exact source organization.
DROP FUNCTION public.serve_connect_bronze(uuid,uuid,uuid,integer);
CREATE OR REPLACE FUNCTION public.serve_connect_bronze(_subject uuid, _stream uuid, _run uuid, _engine uuid, _after integer DEFAULT '-1'::integer)
 RETURNS TABLE(dataset_id uuid, namespace text, name text, layer text, version_id uuid, format text, bucket text, object_key text, sha256 text, byte_count bigint, row_count bigint, schema jsonb, produced_by jsonb, evidence_class text, created_at timestamp with time zone, sequence integer)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'core'
AS $function$
DECLARE conn public.connect_connections; snapshot jsonb;
BEGIN
  IF _after < -1 THEN RAISE EXCEPTION 'Invalid cursor'; END IF;
  SELECT c.* INTO conn FROM public.connect_connections c
    JOIN public.connect_streams s ON s.connection_id=c.id
    JOIN public.ingest_sources src ON src.id=c.source_id
    JOIN public.execution_engines e ON e.id=_engine AND e.enabled
      AND e.organization_id IS NOT DISTINCT FROM c.organization_id
      AND (e.organization_id IS NOT NULL OR e.owner_id=_subject)
    WHERE s.id=_stream AND c.owner_id=_subject AND src.owner_id=_subject AND c.enabled AND src.enabled
      AND src.organization_id IS NOT DISTINCT FROM c.organization_id
      AND (c.organization_id IS NULL OR EXISTS(SELECT 1 FROM public.org_members m
        WHERE m.org_id=c.organization_id AND m.user_id=_subject));
  IF conn.id IS NULL THEN RETURN; END IF;
  SELECT rs.schema_json INTO snapshot FROM public.connect_run_streams rs
    JOIN public.connect_runs r ON r.id=rs.run_id WHERE rs.stream_id=_stream AND r.id=_run
    AND r.connection_id=conn.id AND r.status='succeeded' AND rs.completed
    AND r.operation IN ('sync','reset','backfill');
  IF NOT FOUND THEN RETURN; END IF;
  IF octet_length(snapshot::text)>65536 THEN RAISE EXCEPTION 'Schema exceeds serving limit'; END IF;
  RETURN QUERY SELECT d.id,d.namespace,d.name,d.layer,v.id,v.format,v.bucket,v.object_key,v.sha256,
    v.byte_count,v.row_count,v.schema,(v.produced_by || jsonb_build_object('attempt',NULL)),v.evidence_class,v.created_at,c.sequence
    FROM public.connect_commits c JOIN core.dataset_versions v ON v.source_version_id=c.id
    JOIN core.datasets d ON d.id=v.dataset_id
    WHERE c.run_id=_run AND c.stream_id=_stream AND c.sequence>_after
      AND d.namespace='connect' AND d.source_id=_stream AND d.owner_id=_subject
      AND d.organization_id IS NOT DISTINCT FROM conn.organization_id
    ORDER BY c.sequence LIMIT 26;
END $function$;

REVOKE ALL ON FUNCTION public.serve_connect_bronze(uuid,uuid,uuid,uuid,integer) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.serve_connect_bronze(uuid,uuid,uuid,uuid,integer) TO service_role;
