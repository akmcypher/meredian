BEGIN;
-- Personal tables must never inherit the audience of a potentially shared default org.
DO $$ DECLARE owner uuid; private_org uuid; BEGIN
 FOR owner IN SELECT DISTINCT t.owner_id FROM public.user_tables t WHERE t.organization_id IS NULL
  AND EXISTS(SELECT 1 FROM auth.users u WHERE u.id=t.owner_id) LOOP
  INSERT INTO public.organizations(name,created_by) VALUES('Personal legacy files',owner) RETURNING id INTO private_org;
  INSERT INTO public.org_members(org_id,user_id,workspace_role) VALUES(private_org,owner,'owner');
  UPDATE public.user_tables SET organization_id=private_org WHERE owner_id=owner AND organization_id IS NULL;
  UPDATE public.profiles SET default_organization_id=coalesce(default_organization_id,private_org),
   active_organization_id=coalesce(active_organization_id,private_org) WHERE id=owner;
 END LOOP;
END $$;
ALTER TABLE public.table_versions ADD COLUMN table_id uuid REFERENCES public.user_tables(id) ON DELETE SET NULL;
-- Deleted/reused names cannot prove identity. Preserve ambiguous history as owner-only orphans.
UPDATE public.table_versions v SET table_id=t.id FROM public.user_tables t
 WHERE t.name=v.table_name AND v.created_by=t.owner_id AND v.created_at>=t.created_at;
ALTER TABLE public.user_tables DROP CONSTRAINT user_tables_name_key;
CREATE UNIQUE INDEX user_tables_workspace_name ON public.user_tables(coalesce(organization_id,owner_id),name);
ALTER TABLE public.table_versions DROP CONSTRAINT table_versions_table_name_version_key;
CREATE UNIQUE INDEX table_versions_identity ON public.table_versions(table_id,version);
DROP POLICY "Owners and org stewards view versions" ON public.table_versions;
CREATE POLICY "Owners and org stewards view versions" ON public.table_versions FOR SELECT TO authenticated
 USING ((table_id IS NULL AND created_by=auth.uid()) OR EXISTS(SELECT 1 FROM public.user_tables t WHERE t.id=table_id));
DROP POLICY "Users record versions" ON public.table_versions;
CREATE POLICY "Users record versions" ON public.table_versions FOR INSERT TO authenticated
 WITH CHECK (created_by=auth.uid() AND EXISTS(SELECT 1 FROM public.user_tables t WHERE t.id=table_id AND t.owner_id=auth.uid() AND t.name=table_name));

-- One lock covers both the current value and its immutable version; failures roll back together.
CREATE FUNCTION public.write_native_table(_name text,_rows jsonb,_columns jsonb,_operation text,_table_id uuid DEFAULT NULL) RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE uid uuid:=auth.uid(); org uuid; target uuid; owner uuid; next_version integer;
BEGIN
 IF uid IS NULL OR NOT public.is_active_user() THEN RAISE EXCEPTION 'Verified sign-in required' USING ERRCODE='42501'; END IF;
 org:=public.provision_workspace_for_user(uid);
 IF length(_name) NOT BETWEEN 1 AND 60 OR jsonb_typeof(_rows) IS DISTINCT FROM 'array' OR jsonb_typeof(_columns) IS DISTINCT FROM 'array'
 OR jsonb_array_length(_rows)>5000 OR octet_length(_rows::text)>16777216 OR length(_operation)>200 THEN RAISE EXCEPTION 'Invalid native table payload'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(org::text||':'||_name,0));
 SELECT id,owner_id INTO target,owner FROM public.user_tables WHERE organization_id=org AND name=_name FOR UPDATE;
 IF _table_id IS NOT NULL AND target IS DISTINCT FROM _table_id THEN RAISE EXCEPTION 'Table is not in the active workspace' USING ERRCODE='42501'; END IF;
 IF target IS NOT NULL AND owner<>uid THEN RAISE EXCEPTION 'Only the table owner can write raw records' USING ERRCODE='42501'; END IF;
 IF target IS NULL THEN
  INSERT INTO public.user_tables(name,owner_id,organization_id,rows,columns,row_count)
  VALUES(_name,uid,org,_rows,_columns,jsonb_array_length(_rows)) RETURNING id INTO target;
 ELSE
  UPDATE public.user_tables SET rows=_rows,columns=_columns,row_count=jsonb_array_length(_rows) WHERE id=target;
 END IF;
 SELECT coalesce(max(version),0)+1 INTO next_version FROM public.table_versions WHERE table_id=target;
 INSERT INTO public.table_versions(table_id,table_name,version,operation,created_by,rows,columns,row_count)
 VALUES(target,_name,next_version,_operation,uid,_rows,_columns,jsonb_array_length(_rows));
 RETURN next_version;
END $$;
REVOKE ALL ON FUNCTION public.write_native_table(text,jsonb,jsonb,text,uuid) FROM PUBLIC,anon;
GRANT EXECUTE ON FUNCTION public.write_native_table(text,jsonb,jsonb,text,uuid) TO authenticated;
CREATE OR REPLACE FUNCTION "public"."secure_tables"() RETURNS TABLE("id" "uuid", "name" "text", "columns" "jsonb", "rows" "jsonb", "row_count" integer, "owner_id" "uuid", "created_at" timestamp with time zone, "filtered" boolean, "masked" "text"[])
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
  uid uuid := auth.uid(); privileged boolean; my_roles public.app_role[];
  total_bytes bigint := 0; total_rows bigint := 0; t record; r jsonb; out_rows jsonb; keep boolean; pol record; has_pol boolean; m text[]; ap record;
BEGIN
  IF uid IS NULL THEN RETURN; END IF;
  privileged := public.has_role(uid,'admin');
  SELECT coalesce(array_agg(ur.role), '{}') INTO my_roles FROM public.user_roles ur WHERE ur.user_id = uid;
  FOR t IN SELECT * FROM public.user_tables ut
           WHERE (ut.owner_id = uid OR privileged OR public.is_org_member(ut.organization_id))
           AND coalesce(ut.organization_id,ut.owner_id)=coalesce((SELECT p.active_organization_id FROM public.profiles p WHERE p.id=uid),uid)
           ORDER BY ut.created_at DESC LOOP
    SELECT EXISTS (SELECT 1 FROM public.row_policies p WHERE p.table_name = t.name AND (p.organization_id IS NULL OR p.organization_id = t.organization_id)) INTO has_pol;
    IF jsonb_typeof(t.rows) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'invalid legacy row array'; END IF;
 total_bytes := total_bytes + octet_length(t.rows::text);
 total_rows := total_rows + jsonb_array_length(t.rows);
 IF jsonb_array_length(t.rows)>5000 OR total_rows>20000 OR total_bytes>16777216 THEN RAISE EXCEPTION 'legacy warehouse exceeds read budget; migrate tables to paged storage'; END IF;
 out_rows := '[]'::jsonb; m := '{}';
    FOR r IN SELECT * FROM jsonb_array_elements(t.rows) LOOP
      keep := true;
      IF has_pol AND NOT privileged AND t.owner_id <> uid THEN
        keep := false;
        FOR pol IN SELECT * FROM public.row_policies p WHERE p.table_name = t.name AND p.role = ANY (my_roles) AND (p.organization_id IS NULL OR p.organization_id = t.organization_id) LOOP
          IF public.row_passes(r, pol.column_name, pol.op, pol.value) THEN keep := true; EXIT; END IF;
        END LOOP;
      END IF;
      IF keep THEN
        IF NOT privileged AND t.owner_id <> uid THEN
          FOR ap IN SELECT * FROM public.access_policies a WHERE (a.dataset = t.name OR a.dataset = '*') AND a.masking <> 'none' AND NOT (a.allowed_roles && my_roles)
                    AND (a.organization_id IS NULL OR a.organization_id = t.organization_id) LOOP
            IF r ? ap.column_name THEN
              r := jsonb_set(r, ARRAY[ap.column_name], to_jsonb(public.mask_text(r ->> ap.column_name, ap.masking)));
              IF NOT ap.column_name = ANY (m) THEN m := m || ap.column_name; END IF;
            END IF;
          END LOOP;
        END IF;
        out_rows := out_rows || jsonb_build_array(r);
      END IF;
    END LOOP;
    id := t.id; name := t.name; columns := t.columns; rows := out_rows; row_count := jsonb_array_length(out_rows);
    owner_id := t.owner_id; created_at := t.created_at; filtered := has_pol AND NOT privileged AND t.owner_id <> uid; masked := m;
    RETURN NEXT;
  END LOOP;
END $$;
COMMIT;
