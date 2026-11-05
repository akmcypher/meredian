-- Immutable per-batch versions, stable per-stream identity, and owner-scoped run snapshots.
ALTER TABLE core.datasets DROP CONSTRAINT datasets_namespace_check;
ALTER TABLE core.datasets ADD CONSTRAINT datasets_namespace_check CHECK (namespace IN ('transform','connect'));

CREATE FUNCTION core.register_connect_commit(_commit uuid) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,core AS $$
DECLARE c public.connect_commits; r public.connect_runs; s public.connect_streams;
  conn public.connect_connections; b public.ingest_landing_batches; snapshot jsonb; did uuid; vid uuid;
BEGIN
  SELECT * INTO STRICT c FROM public.connect_commits WHERE id=_commit;
  SELECT * INTO STRICT r FROM public.connect_runs WHERE id=c.run_id;
  SELECT * INTO STRICT s FROM public.connect_streams WHERE id=c.stream_id;
  SELECT * INTO STRICT conn FROM public.connect_connections WHERE id=r.connection_id;
  SELECT * INTO STRICT b FROM public.ingest_landing_batches WHERE id=c.batch_id;
  SELECT schema_json INTO STRICT snapshot FROM public.connect_run_streams WHERE run_id=r.id AND stream_id=s.id;
  INSERT INTO core.datasets(owner_id,organization_id,namespace,name,layer,source_id)
    VALUES(conn.owner_id,conn.organization_id,'connect','connect_'||replace(s.id::text,'-',''),'bronze',s.id)
    ON CONFLICT(namespace,source_id) DO NOTHING;
  SELECT id INTO STRICT did FROM core.datasets WHERE namespace='connect' AND source_id=s.id;
  INSERT INTO core.dataset_versions(dataset_id,source_version_id,format,bucket,object_key,sha256,
    byte_count,row_count,schema,produced_by,evidence_class,created_at)
    VALUES(did,c.id,'ndjson','ingest-files',b.object_key,c.sha256,c.byte_count,c.row_count,snapshot,
      jsonb_build_object('product','ingest','run_id',r.id,'task_id',s.id,'attempt',r.attempts),
      'server_verified',c.created_at)
    ON CONFLICT(source_version_id) DO NOTHING;
  SELECT id INTO STRICT vid FROM core.dataset_versions WHERE source_version_id=c.id;
  UPDATE core.datasets SET current_version_id=vid WHERE id=did;
END $$;
REVOKE ALL ON FUNCTION core.register_connect_commit(uuid) FROM PUBLIC,anon,authenticated;

CREATE FUNCTION core.connect_commit_registers_dataset() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,core AS $$
BEGIN PERFORM core.register_connect_commit(NEW.id); RETURN NEW; END $$;
REVOKE ALL ON FUNCTION core.connect_commit_registers_dataset() FROM PUBLIC,anon,authenticated;
CREATE TRIGGER connect_version_registers_dataset AFTER INSERT ON public.connect_commits
  FOR EACH ROW EXECUTE FUNCTION core.connect_commit_registers_dataset();

DO $$ DECLARE c record; BEGIN
  FOR c IN SELECT id FROM public.connect_commits ORDER BY created_at,id LOOP
    PERFORM core.register_connect_commit(c.id);
  END LOOP;
END $$;

CREATE FUNCTION public.serve_connect_bronze(_subject uuid,_stream uuid,_run uuid,_after integer DEFAULT -1)
RETURNS TABLE(dataset_id uuid,namespace text,name text,layer text,version_id uuid,format text,
  bucket text,object_key text,sha256 text,byte_count bigint,row_count bigint,schema jsonb,
  produced_by jsonb,evidence_class text,created_at timestamptz,sequence integer)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=public,core AS $$
DECLARE conn public.connect_connections; snapshot jsonb;
BEGIN
  IF _after < -1 THEN RAISE EXCEPTION 'Invalid cursor'; END IF;
  SELECT c.* INTO conn FROM public.connect_connections c
    JOIN public.connect_streams s ON s.connection_id=c.id
    JOIN public.ingest_sources src ON src.id=c.source_id
    WHERE s.id=_stream AND c.owner_id=_subject AND src.owner_id=_subject AND c.enabled AND src.enabled
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
    v.byte_count,v.row_count,v.schema,v.produced_by,v.evidence_class,v.created_at,c.sequence
    FROM public.connect_commits c JOIN core.dataset_versions v ON v.source_version_id=c.id
    JOIN core.datasets d ON d.id=v.dataset_id
    WHERE c.run_id=_run AND c.stream_id=_stream AND c.sequence>_after
      AND d.namespace='connect' AND d.source_id=_stream AND d.owner_id=_subject
    ORDER BY c.sequence LIMIT 26;
END $$;
REVOKE ALL ON FUNCTION public.serve_connect_bronze(uuid,uuid,uuid,integer) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.serve_connect_bronze(uuid,uuid,uuid,integer) TO service_role;
