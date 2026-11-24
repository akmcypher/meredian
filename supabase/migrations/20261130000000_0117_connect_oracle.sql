-- Add Oracle Database batch/incremental to the existing verified, workspace-scoped source authority.
-- Credentials remain worker-local and existing ingest config/RLS triggers stay unchanged.
CREATE OR REPLACE FUNCTION public.create_ingest_source_verified(_actor uuid,_name text,_kind text,_target text,_config jsonb,_headers jsonb,_hash text)
 RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE sid uuid; org uuid;
BEGIN
 IF _actor IS NULL OR _kind NOT IN ('rest','postgres','mysql','sqlserver','oracle','mongodb','kafka_cdc','kafka','s3','adls') THEN RAISE EXCEPTION 'Invalid source'; END IF;
 SELECT p.active_organization_id INTO org FROM public.profiles p WHERE p.id=_actor;
 IF org IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members m JOIN public.organizations o ON o.id=m.org_id WHERE m.user_id=_actor AND m.org_id=org AND o.status='active') THEN RAISE EXCEPTION 'Workspace access required' USING ERRCODE='42501'; END IF;
 INSERT INTO public.ingest_sources(owner_id,organization_id,name,kind,target_table,config,token_hash) VALUES(_actor,org,_name,_kind,_target,_config,_hash) RETURNING id INTO sid;
 IF _kind='rest' THEN INSERT INTO public.ingest_source_secrets(source_id,headers) VALUES(sid,coalesce(_headers,'{}')); END IF;
 RETURN sid;
END $$;
REVOKE ALL ON FUNCTION public.create_ingest_source_verified(uuid,text,text,text,jsonb,jsonb,text) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.create_ingest_source_verified(uuid,text,text,text,jsonb,jsonb,text) TO service_role;
