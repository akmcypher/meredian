-- Verified self-activation reuses organizations and memberships; global roles are unchanged.
ALTER TABLE public.org_members ADD COLUMN workspace_role text NOT NULL DEFAULT 'member'
  CHECK (workspace_role IN ('owner','admin','engineer','member'));
ALTER TABLE public.organizations ADD COLUMN description text NOT NULL DEFAULT '';
ALTER TABLE public.organizations ADD COLUMN onboarding_settings jsonb NOT NULL DEFAULT '{}'
  CHECK (jsonb_typeof(onboarding_settings)='object');
ALTER TABLE public.profiles ADD COLUMN default_organization_id uuid REFERENCES public.organizations(id) ON DELETE SET NULL;
ALTER TABLE public.profiles ADD COLUMN active_organization_id uuid REFERENCES public.organizations(id) ON DELETE SET NULL;
ALTER TABLE public.profiles ADD COLUMN onboarding_step smallint NOT NULL DEFAULT 3 CHECK (onboarding_step BETWEEN 0 AND 3);
ALTER TABLE public.profiles ADD COLUMN onboarding_completed_at timestamptz DEFAULT now();
ALTER TABLE public.profiles ADD COLUMN onboarding_first_source text CHECK (onboarding_first_source IN ('postgres','mysql','kafka','skip'));
UPDATE public.org_members m SET workspace_role='owner'
 FROM public.organizations o WHERE o.id=m.org_id AND o.created_by=m.user_id;
CREATE UNIQUE INDEX org_one_workspace_owner ON public.org_members(org_id) WHERE workspace_role='owner';

CREATE FUNCTION public.is_active_user() RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path=public AS $$
 SELECT EXISTS(SELECT 1 FROM auth.users u WHERE u.id=auth.uid() AND u.email_confirmed_at IS NOT NULL
   AND (u.banned_until IS NULL OR u.banned_until<=now()));
$$;
REVOKE ALL ON FUNCTION public.is_active_user() FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.is_active_user() TO authenticated,service_role;

-- Definer RPCs bypass table RLS. Check every Data API request before dispatch,
-- preserving an installation's existing pre-request hook under the caller role.
DO $$
DECLARE previous_hook text; previous_oid regprocedure; hook_call text := ''; body text;
BEGIN
 SELECT substring(setting FROM length('pgrst.db_pre_request=')+1) INTO previous_hook
 FROM pg_db_role_setting s CROSS JOIN LATERAL unnest(s.setconfig) setting
 WHERE s.setrole='authenticator'::regrole AND s.setdatabase IN (0,(SELECT oid FROM pg_database WHERE datname=current_database()))
  AND setting LIKE 'pgrst.db_pre_request=%' ORDER BY s.setdatabase DESC LIMIT 1;
 IF nullif(previous_hook,'') IS NOT NULL AND previous_hook<>'public.enforce_verified_account' THEN
  previous_oid:=to_regprocedure(previous_hook||'()');
  IF previous_oid IS NULL THEN RAISE EXCEPTION 'Existing pre-request hook cannot be resolved'; END IF;
  SELECT format('PERFORM %I.%I();',n.nspname,p.proname) INTO hook_call
   FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE p.oid=previous_oid;
 END IF;
 body := 'BEGIN IF auth.role()=''authenticated'' AND NOT public.is_active_user() THEN RAISE EXCEPTION ''Verified active sign-in required'' USING ERRCODE=''42501''; END IF; ' || hook_call || ' END';
 EXECUTE format('CREATE FUNCTION public.enforce_verified_account() RETURNS void LANGUAGE plpgsql SECURITY INVOKER SET search_path=public AS %L',body);
 EXECUTE format('ALTER ROLE authenticator IN DATABASE %I SET pgrst.db_pre_request = %L',current_database(),'public.enforce_verified_account');
END $$;
REVOKE ALL ON FUNCTION public.enforce_verified_account() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.enforce_verified_account() TO anon,authenticated,service_role;
NOTIFY pgrst,'reload config';

CREATE OR REPLACE FUNCTION public.handle_new_user() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
BEGIN
 INSERT INTO public.profiles(id,email,display_name,onboarding_step,onboarding_completed_at)
 VALUES(NEW.id,NEW.email,left(coalesce(nullif(btrim(NEW.raw_user_meta_data->>'full_name'),''),split_part(NEW.email,'@',1)),80),0,NULL)
 ON CONFLICT(id) DO NOTHING;
 RETURN NEW;
END $$;

-- The Community baseline omitted Auth-schema triggers; preserve existing profiles and restore the lifecycle.
INSERT INTO public.profiles(id,email,display_name,onboarding_step,onboarding_completed_at)
 SELECT id,email,left(coalesce(nullif(btrim(raw_user_meta_data->>'full_name'),''),split_part(email,'@',1)),80),
  CASE WHEN email_confirmed_at IS NULL THEN 0 ELSE 3 END,
  CASE WHEN email_confirmed_at IS NULL THEN NULL ELSE now() END FROM auth.users
 ON CONFLICT(id) DO NOTHING;
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='auth.users'::regclass AND tgname='on_auth_user_created') THEN
  CREATE TRIGGER on_auth_user_created AFTER INSERT ON auth.users FOR EACH ROW EXECUTE FUNCTION public.handle_new_user();
 END IF;
END $$;

-- Only trusted triggers / server callers may supply a user id to this helper.
CREATE FUNCTION public.provision_workspace_for_user(_user uuid) RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE p public.profiles; org uuid; org_status text;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM auth.users u WHERE u.id=_user AND u.email_confirmed_at IS NOT NULL
   AND (u.banned_until IS NULL OR u.banned_until<=now())) THEN
  RAISE EXCEPTION 'Verify your email before continuing' USING ERRCODE='42501';
 END IF;
 SELECT * INTO p FROM public.profiles WHERE id=_user FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Account profile unavailable'; END IF;
 SELECT o.id,o.status INTO org,org_status FROM public.org_members m JOIN public.organizations o ON o.id=m.org_id
  WHERE m.user_id=_user AND o.id=p.active_organization_id FOR SHARE OF m;
 IF org IS NULL THEN
  SELECT o.id,o.status INTO org,org_status FROM public.org_members m JOIN public.organizations o ON o.id=m.org_id
   WHERE m.user_id=_user ORDER BY (o.status='active') DESC,o.created_at,o.id LIMIT 1 FOR SHARE OF m;
 END IF;
 IF org IS NULL THEN
  IF p.default_organization_id IS NOT NULL THEN RAISE EXCEPTION 'Workspace access is unavailable' USING ERRCODE='42501'; END IF;
  INSERT INTO public.organizations(name,created_by)
   VALUES(left(coalesce(nullif(p.display_name,''),'My'),60)||'''s workspace',_user) RETURNING id INTO org;
  INSERT INTO public.org_members(org_id,user_id,workspace_role) VALUES(org,_user,'owner');
  org_status:='active';
 END IF;
 IF org_status<>'active' THEN RAISE EXCEPTION 'Workspace is inactive' USING ERRCODE='42501'; END IF;
 UPDATE public.profiles SET default_organization_id=coalesce(default_organization_id,org),active_organization_id=org WHERE id=_user;
 INSERT INTO public.alert_settings(user_id,alert_email) SELECT id,email FROM public.profiles WHERE id=_user ON CONFLICT(user_id) DO NOTHING;
 RETURN org;
END $$;
REVOKE ALL ON FUNCTION public.provision_workspace_for_user(uuid) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.provision_workspace_for_user(uuid) TO service_role;

CREATE FUNCTION public.provision_verified_user() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
BEGIN
 IF NEW.email_confirmed_at IS NOT NULL THEN PERFORM public.provision_workspace_for_user(NEW.id); END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION public.provision_verified_user() FROM PUBLIC,anon,authenticated;
CREATE TRIGGER zz_provision_verified_workspace AFTER INSERT OR UPDATE OF email_confirmed_at ON auth.users
 FOR EACH ROW WHEN (NEW.email_confirmed_at IS NOT NULL) EXECUTE FUNCTION public.provision_verified_user();

CREATE FUNCTION public.ensure_workspace() RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE uid uuid:=auth.uid(); org uuid; result jsonb;
BEGIN
 IF uid IS NULL OR NOT public.is_active_user() THEN RAISE EXCEPTION 'Verified sign-in required' USING ERRCODE='42501'; END IF;
 org:=public.provision_workspace_for_user(uid);
 SELECT jsonb_build_object('workspaceId',o.id,'name',o.name,'description',o.description,'status',o.status,
  'workspaceRole',m.workspace_role,'onboardingStep',p.onboarding_step,'onboardingCompleted',p.onboarding_completed_at IS NOT NULL,
  'displayName',p.display_name,'firstSource',coalesce(p.onboarding_first_source,o.onboarding_settings->>'first_source')) INTO result
 FROM public.organizations o JOIN public.org_members m ON m.org_id=o.id AND m.user_id=uid JOIN public.profiles p ON p.id=uid WHERE o.id=org;
 IF result IS NULL THEN RAISE EXCEPTION 'Workspace access is unavailable' USING ERRCODE='42501'; END IF;
 RETURN result;
END $$;
REVOKE ALL ON FUNCTION public.ensure_workspace() FROM PUBLIC,anon;
GRANT EXECUTE ON FUNCTION public.ensure_workspace() TO authenticated;

CREATE FUNCTION public.select_workspace(_workspace uuid) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
BEGIN
 IF NOT public.is_active_user() OR NOT EXISTS(SELECT 1 FROM public.org_members m JOIN public.organizations o ON o.id=m.org_id
  WHERE m.user_id=auth.uid() AND m.org_id=_workspace AND o.status='active') THEN RAISE EXCEPTION 'Workspace unavailable' USING ERRCODE='42501'; END IF;
 UPDATE public.profiles SET active_organization_id=_workspace WHERE id=auth.uid();
 RETURN public.ensure_workspace();
END $$;
REVOKE ALL ON FUNCTION public.select_workspace(uuid) FROM PUBLIC,anon;
GRANT EXECUTE ON FUNCTION public.select_workspace(uuid) TO authenticated;

CREATE FUNCTION public.update_onboarding(_step integer,_name text DEFAULT NULL,_description text DEFAULT NULL,_first_source text DEFAULT NULL)
 RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE uid uuid:=auth.uid(); org uuid; current_step smallint; member_role text;
BEGIN
 PERFORM public.ensure_workspace();
 SELECT active_organization_id,onboarding_step INTO org,current_step FROM public.profiles WHERE id=uid FOR UPDATE;
 SELECT workspace_role INTO member_role FROM public.org_members WHERE org_id=org AND user_id=uid FOR SHARE;
 IF member_role IS NULL THEN RAISE EXCEPTION 'Workspace membership required' USING ERRCODE='42501'; END IF;
 IF _step IS NULL OR _step NOT BETWEEN 1 AND 3 OR _step>current_step+1 THEN RAISE EXCEPTION 'Complete the current onboarding step first'; END IF;
 IF _step<=current_step THEN RETURN public.ensure_workspace(); END IF;
 IF _step=1 THEN
  IF member_role NOT IN ('owner','admin') THEN
   IF _name IS NOT NULL OR _description IS NOT NULL THEN RAISE EXCEPTION 'Workspace administrator required' USING ERRCODE='42501'; END IF;
  ELSE
  IF _name IS NULL OR length(btrim(_name)) NOT BETWEEN 2 AND 80 OR length(coalesce(_description,''))>500 THEN RAISE EXCEPTION 'Enter a workspace name of 2 to 80 characters and a description up to 500 characters'; END IF;
  UPDATE public.organizations SET name=btrim(_name),description=btrim(coalesce(_description,'')) WHERE id=org;
  END IF;
 ELSIF _step=2 THEN
  IF coalesce(_first_source,'skip') NOT IN ('postgres','mysql','kafka','skip') THEN RAISE EXCEPTION 'Choose a supported source or skip'; END IF;
  UPDATE public.profiles SET onboarding_first_source=coalesce(_first_source,'skip') WHERE id=uid;
  IF member_role IN ('owner','admin') THEN
   UPDATE public.organizations SET onboarding_settings=onboarding_settings || jsonb_build_object('first_source',coalesce(_first_source,'skip')) WHERE id=org;
  END IF;
 END IF;
 UPDATE public.profiles SET onboarding_step=_step,onboarding_completed_at=CASE WHEN _step=3 THEN now() ELSE NULL END WHERE id=uid;
 RETURN public.ensure_workspace();
END $$;
REVOKE ALL ON FUNCTION public.update_onboarding(integer,text,text,text) FROM PUBLIC,anon;
GRANT EXECUTE ON FUNCTION public.update_onboarding(integer,text,text,text) TO authenticated;

CREATE FUNCTION public.update_account_profile(_name text) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
BEGIN
 IF NOT public.is_active_user() THEN RAISE EXCEPTION 'Verified sign-in required' USING ERRCODE='42501'; END IF;
 IF _name IS NULL OR length(btrim(_name)) NOT BETWEEN 2 AND 80 THEN RAISE EXCEPTION 'Enter a name of 2 to 80 characters'; END IF;
 UPDATE public.profiles SET display_name=btrim(_name) WHERE id=auth.uid();
 RETURN public.ensure_workspace();
END $$;
REVOKE ALL ON FUNCTION public.update_account_profile(text) FROM PUBLIC,anon;
GRANT EXECUTE ON FUNCTION public.update_account_profile(text) TO authenticated;

-- New context and progress columns are writable only through the checked functions above.
REVOKE UPDATE ON public.profiles FROM authenticated;
GRANT UPDATE(display_name) ON public.profiles TO authenticated;

-- A signed JWT is insufficient for an unverified or deactivated account, including direct API/storage access.
DO $$ DECLARE t record; BEGIN
 FOR t IN SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='public' AND c.relkind='r' AND c.relrowsecurity LOOP
  EXECUTE format('CREATE POLICY "Verified active accounts" ON public.%I AS RESTRICTIVE FOR ALL TO authenticated USING (public.is_active_user()) WITH CHECK (public.is_active_user())',t.relname);
 END LOOP;
END $$;
CREATE POLICY "Verified active accounts" ON storage.objects AS RESTRICTIVE FOR ALL TO authenticated
 USING(public.is_active_user()) WITH CHECK(public.is_active_user());

-- Normal source registration inherits the verified actor's selected workspace. Legacy personal scope remains valid.
CREATE OR REPLACE FUNCTION public.create_ingest_source_verified(_actor uuid,_name text,_kind text,_target text,_config jsonb,_headers jsonb,_hash text)
 RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE sid uuid; org uuid;
BEGIN
 IF _actor IS NULL OR _kind NOT IN ('rest','postgres','mysql','kafka_cdc','kafka','s3','adls') THEN RAISE EXCEPTION 'Invalid source'; END IF;
 SELECT p.active_organization_id INTO org FROM public.profiles p WHERE p.id=_actor;
 IF org IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members m JOIN public.organizations o ON o.id=m.org_id WHERE m.user_id=_actor AND m.org_id=org AND o.status='active') THEN RAISE EXCEPTION 'Workspace access required' USING ERRCODE='42501'; END IF;
 INSERT INTO public.ingest_sources(owner_id,organization_id,name,kind,target_table,config,token_hash) VALUES(_actor,org,_name,_kind,_target,_config,_hash) RETURNING id INTO sid;
 IF _kind='rest' THEN INSERT INTO public.ingest_source_secrets(source_id,headers) VALUES(sid,coalesce(_headers,'{}')); END IF;
 RETURN sid;
END $$;
REVOKE ALL ON FUNCTION public.create_ingest_source_verified(uuid,text,text,text,jsonb,jsonb,text) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.create_ingest_source_verified(uuid,text,text,text,jsonb,jsonb,text) TO service_role;
