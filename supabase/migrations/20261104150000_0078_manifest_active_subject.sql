-- Engine JWTs use service-role disclosure RPCs, so authenticated-role RLS is insufficient.
CREATE FUNCTION public.require_active_subject(_subject uuid) RETURNS void
 LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=public AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM auth.users u WHERE u.id=_subject AND u.email_confirmed_at IS NOT NULL
  AND (u.banned_until IS NULL OR u.banned_until<=now())) THEN
  RAISE EXCEPTION 'Active verified account required' USING ERRCODE='42501';
 END IF;
END $$;
REVOKE ALL ON FUNCTION public.require_active_subject(uuid) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.require_active_subject(uuid) TO service_role;

CREATE OR REPLACE FUNCTION "public"."serve_manifest"("_subject" "uuid", "_names" "text"[]) RETURNS TABLE("dataset_id" "uuid", "name" "text", "layer" "text", "version_id" "uuid", "bucket" "text", "object_key" "text", "sha256" "text", "byte_count" bigint, "row_count" bigint, "schema" "jsonb", "produced_by" "jsonb", "evidence_class" "text", "created_at" timestamp with time zone, "policy" "jsonb")
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'core'
    AS $$
BEGIN
 PERFORM public.require_active_subject(_subject);
 RETURN QUERY
 WITH visible AS (
  SELECT d.*, d.owner_id = _subject AS own FROM core.datasets d
  WHERE d.layer = 'gold' AND d.current_version_id IS NOT NULL AND lower(d.name) = ANY(_names)
   AND (d.owner_id = _subject OR (d.organization_id IS NOT NULL AND EXISTS (
    SELECT 1 FROM public.org_members m WHERE m.org_id = d.organization_id AND m.user_id = _subject)))
 ), ranked AS (
  SELECT v.*, count(*) FILTER (WHERE NOT v.own) OVER (PARTITION BY lower(v.name)) AS others,
   bool_or(v.own) OVER (PARTITION BY lower(v.name)) AS has_own FROM visible v
 )
 SELECT r.id, r.name, r.layer, x.id, x.bucket, x.object_key, x.sha256, x.byte_count, x.row_count, x.schema, x.produced_by, x.evidence_class, x.created_at,
  public.dataset_policy_decision(_subject, r.id)
 FROM ranked r JOIN core.dataset_versions x ON x.id = r.current_version_id
 WHERE (r.own AND NOT EXISTS (SELECT 1 FROM ranked o WHERE o.own AND lower(o.name) = lower(r.name) AND o.id <> r.id))
    OR (NOT r.has_own AND r.others = 1)
 ORDER BY r.name;
END
$$;
REVOKE ALL ON FUNCTION public.serve_manifest(uuid,text[]) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.serve_manifest(uuid,text[]) TO service_role;

CREATE OR REPLACE FUNCTION public.serve_connect_bronze(_subject uuid, _stream uuid, _run uuid, _engine uuid, _after integer DEFAULT '-1'::integer)
 RETURNS TABLE(dataset_id uuid, namespace text, name text, layer text, version_id uuid, format text, bucket text, object_key text, sha256 text, byte_count bigint, row_count bigint, schema jsonb, produced_by jsonb, evidence_class text, created_at timestamp with time zone, sequence integer)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'core'
AS $function$
DECLARE conn public.connect_connections; snapshot jsonb;
BEGIN
  PERFORM public.require_active_subject(_subject);
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
