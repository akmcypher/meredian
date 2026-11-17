-- 0106: Bronze parts as Parquet (Spark S6, deploy/connect/README.md, "Parquet Bronze parts (S6)").
-- A landing batch's object is landing.ndjson or landing.parquet (layout meridian-bronze-parquet/1,
-- deploy/connect/bronze_parquet.py); the dataset version records the format its key names, so
-- serve_connect_bronze hands readers the format of every part. Nothing else changes: the server
-- verifies a Parquet part's envelopes exactly as it verifies NDJSON before registering it.

CREATE OR REPLACE FUNCTION public.register_ingest_landing_batch(_actor uuid,_source uuid,_key text,_sha text,_rows integer,_mode text,_batch uuid) RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $function$
DECLARE src public.ingest_sources; out_id uuid;
BEGIN
 IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'service only'; END IF;
 SELECT * INTO src FROM public.ingest_sources WHERE id=_source AND owner_id=_actor FOR UPDATE;
 IF NOT FOUND OR (src.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members WHERE org_id=src.organization_id AND user_id=_actor)) THEN RAISE EXCEPTION 'source not authorized'; END IF;
 IF _key IS NULL OR _key NOT IN (_actor::text||'/published/'||_batch::text||'/landing.ndjson', _actor::text||'/published/'||_batch::text||'/landing.parquet') OR _sha IS NULL OR _sha !~ '^[a-f0-9]{64}$' OR _rows IS NULL OR _rows NOT BETWEEN 0 AND 10000 OR _mode NOT IN ('replace','append','upsert','cdc') THEN RAISE EXCEPTION 'invalid landing batch'; END IF;
 INSERT INTO public.ingest_landing_batches(source_id,owner_id,organization_id,object_key,sha256,row_count,mode,batch_key) VALUES(_source,_actor,src.organization_id,_key,_sha,_rows,_mode,_batch) ON CONFLICT(source_id,batch_key) DO UPDATE SET batch_key=EXCLUDED.batch_key WHERE public.ingest_landing_batches.sha256=EXCLUDED.sha256 AND public.ingest_landing_batches.mode=EXCLUDED.mode AND public.ingest_landing_batches.row_count=EXCLUDED.row_count RETURNING id INTO out_id;
 IF out_id IS NULL THEN RAISE EXCEPTION 'batch key payload conflict'; END IF;
 UPDATE public.ingest_sources SET last_run_at=now(),last_status='landed_pending_format_commit',last_rows=_rows WHERE id=_source;
 RETURN out_id;
END $function$;

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
    VALUES(did,c.id,CASE WHEN b.object_key LIKE '%/landing.parquet' THEN 'parquet' ELSE 'ndjson' END,'ingest-files',b.object_key,c.sha256,c.byte_count,c.row_count,snapshot,
      jsonb_build_object('product','ingest','run_id',r.id,'task_id',s.id,'attempt',NULL),
      'server_verified',c.created_at)
    ON CONFLICT(source_version_id) DO NOTHING;
  SELECT id INTO STRICT vid FROM core.dataset_versions WHERE source_version_id=c.id;
  UPDATE core.datasets SET current_version_id=vid WHERE id=did;
END $function$;
