-- 0108: workspace worker pools (Community ingestion Phase 1, milestone M2).
-- A pool is a named group of agents in one workspace with its own token (mp_...). Its agents claim
-- tasks of every enabled connection assigned to it, least recently served connection first, so one
-- large backfill cannot starve the other sources. A connection belongs to at most one pool; its own
-- source token keeps working. max_parallel_tasks (NULL = no limit) caps a connection's live task
-- leases on every claim path, to protect a source database. Agents report themselves on each claim
-- (connect_workers). Pool tokens are stored as SHA-256 hashes only, like source tokens.

CREATE TABLE public.connect_pools (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  owner_id uuid NOT NULL,
  organization_id uuid,
  name text NOT NULL CHECK (length(name) BETWEEN 1 AND 80),
  token_hash text NOT NULL UNIQUE CHECK (token_hash ~ '^[a-f0-9]{64}$'),
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX connect_pools_scope_name ON public.connect_pools(owner_id, coalesce(organization_id,'00000000-0000-0000-0000-000000000000'::uuid), lower(name));
ALTER TABLE public.connect_pools ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Verified active accounts" ON public.connect_pools AS RESTRICTIVE FOR ALL TO authenticated
  USING (public.is_active_user()) WITH CHECK (public.is_active_user());
CREATE POLICY connect_pool_read ON public.connect_pools FOR SELECT TO authenticated
  USING (public.can_read_scoped(owner_id, organization_id));
REVOKE ALL ON public.connect_pools FROM PUBLIC, anon, authenticated;
-- The token hash never reaches clients.
GRANT SELECT (id, owner_id, organization_id, name, enabled, created_at) ON public.connect_pools TO authenticated;
GRANT ALL ON public.connect_pools TO service_role;

CREATE TABLE public.connect_workers (
  pool_id uuid NOT NULL REFERENCES public.connect_pools(id) ON DELETE CASCADE,
  agent text NOT NULL CHECK (agent ~ '^[\w.:-]{1,200}$'),
  version text CHECK (version IS NULL OR version ~ '^[\w.+-]{1,40}$'),
  slots integer CHECK (slots IS NULL OR slots BETWEEN 1 AND 64),
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (pool_id, agent)
);
ALTER TABLE public.connect_workers ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Verified active accounts" ON public.connect_workers AS RESTRICTIVE FOR ALL TO authenticated
  USING (public.is_active_user()) WITH CHECK (public.is_active_user());
CREATE POLICY connect_worker_read ON public.connect_workers FOR SELECT TO authenticated
  USING (EXISTS (SELECT 1 FROM public.connect_pools p WHERE p.id=pool_id AND public.can_read_scoped(p.owner_id,p.organization_id)));
REVOKE ALL ON public.connect_workers FROM PUBLIC, anon, authenticated;
GRANT SELECT ON public.connect_workers TO authenticated;
GRANT ALL ON public.connect_workers TO service_role;

ALTER TABLE public.connect_connections
  ADD COLUMN pool_id uuid REFERENCES public.connect_pools(id) ON DELETE SET NULL,
  ADD COLUMN max_parallel_tasks integer CHECK (max_parallel_tasks IS NULL OR max_parallel_tasks BETWEEN 1 AND 64),
  ADD COLUMN last_claimed_at timestamptz;
CREATE INDEX connect_connections_pool ON public.connect_connections(pool_id, last_claimed_at) WHERE pool_id IS NOT NULL;
GRANT SELECT (pool_id, max_parallel_tasks, last_claimed_at) ON public.connect_connections TO authenticated;

-- A connection joins only a pool of its own workspace: the same organization, or, for personal
-- connections, the same owner. Pool agents find credential files by credential_ref, so it must be
-- unique within a pool.
CREATE FUNCTION public.connect_pool_assignment_check() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE p public.connect_pools;
BEGIN
 IF NEW.pool_id IS NULL THEN RETURN NEW; END IF;
 SELECT * INTO p FROM public.connect_pools WHERE id=NEW.pool_id;
 IF NOT FOUND OR p.organization_id IS DISTINCT FROM NEW.organization_id
   OR (p.organization_id IS NULL AND p.owner_id<>NEW.owner_id) THEN
  RAISE EXCEPTION 'Pool outside the connection workspace';
 END IF;
 IF EXISTS (SELECT 1 FROM public.connect_connections o WHERE o.pool_id=NEW.pool_id AND o.id<>NEW.id AND o.credential_ref=NEW.credential_ref) THEN
  RAISE EXCEPTION 'Another connection in this pool uses credential file %', NEW.credential_ref;
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION public.connect_pool_assignment_check() FROM PUBLIC, anon, authenticated;
CREATE TRIGGER connect_pool_assignment BEFORE INSERT OR UPDATE OF pool_id, credential_ref, owner_id, organization_id
  ON public.connect_connections FOR EACH ROW EXECUTE FUNCTION public.connect_pool_assignment_check();

-- Live leases of a connection: running tasks with an unexpired lease, plus whole-run leases.
CREATE FUNCTION public.connect_running_tasks(_connection uuid) RETURNS integer
 LANGUAGE sql STABLE SECURITY DEFINER SET search_path=public AS $$
 SELECT (SELECT count(*) FROM public.connect_run_streams x JOIN public.connect_runs r ON r.id=x.run_id
          WHERE r.connection_id=_connection AND r.status='running' AND r.lease_mode='task'
            AND x.task_status='running' AND x.lease_until>now())::integer
      + (SELECT count(*) FROM public.connect_runs r WHERE r.connection_id=_connection AND r.status='running'
          AND r.lease_mode='run' AND r.lease_until>now())::integer
$$;
REVOKE ALL ON FUNCTION public.connect_running_tasks(uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.connect_running_tasks(uuid) TO service_role;

-- Every claim path respects the connection's limit.
CREATE OR REPLACE FUNCTION public.connect_claim_task(_connection uuid,_worker text DEFAULT NULL) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE c public.connect_connections; r public.connect_runs; rs public.connect_run_streams; token uuid; payload jsonb;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Agent service only'; END IF;
 SELECT * INTO c FROM public.connect_connections WHERE id=_connection FOR UPDATE;
 IF NOT FOUND OR NOT c.enabled OR NOT EXISTS(SELECT 1 FROM public.ingest_sources s WHERE s.id=c.source_id AND s.enabled AND s.owner_id=c.owner_id AND s.organization_id IS NOT DISTINCT FROM c.organization_id) OR (c.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members WHERE org_id=c.organization_id AND user_id=c.owner_id)) THEN RETURN NULL; END IF;
 -- 0108: a connection's limit counts its live task leases and live whole-run leases.
 IF c.max_parallel_tasks IS NOT NULL AND public.connect_running_tasks(c.id)>=c.max_parallel_tasks THEN RETURN NULL; END IF;
 SELECT * INTO r FROM public.connect_runs WHERE connection_id=c.id AND status IN ('queued','retry_wait','running') ORDER BY created_at LIMIT 1 FOR UPDATE;
 IF NOT FOUND AND c.interval_minutes>0 AND coalesce(c.next_run_at,now())<=now() AND EXISTS(SELECT 1 FROM public.connect_streams WHERE connection_id=c.id AND enabled) THEN
 INSERT INTO public.connect_runs(connection_id,request_key,operation) VALUES(c.id,gen_random_uuid(),'sync') RETURNING * INTO r;
 INSERT INTO public.connect_run_streams(run_id,stream_id,initial_state,checkpoint,schema_json,generation) SELECT r.id,s.id,CASE WHEN s.sync_mode='full_refresh' THEN '{}'::jsonb ELSE s.state END,CASE WHEN s.sync_mode='full_refresh' THEN '{}'::jsonb ELSE s.state END,s.schema_json,s.state_generation FROM public.connect_streams s WHERE s.connection_id=c.id AND s.enabled;
 UPDATE public.connect_connections SET next_run_at=now()+make_interval(mins=>interval_minutes) WHERE id=c.id; END IF;
 IF r.id IS NULL THEN RETURN NULL; END IF;
 IF r.lease_mode='run' OR r.operation IN ('check','discover') THEN RETURN public.connect_claim(_connection); END IF;
 IF r.status<>'running' AND r.next_attempt_at>now() THEN RETURN NULL; END IF;
 IF r.lease_mode IS NULL OR r.status<>'running' THEN
  UPDATE public.connect_runs SET status='running',lease_mode='task',attempts=attempts+1,lease_token=NULL,lease_until=NULL WHERE id=r.id RETURNING * INTO r;
  INSERT INTO public.connect_attempts(run_id,attempt,token) VALUES(r.id,r.attempts,gen_random_uuid());
  INSERT INTO public.connect_events(connection_id,run_id,code) VALUES(c.id,r.id,'attempt_started');
 END IF;
 SELECT * INTO rs FROM public.connect_run_streams WHERE run_id=r.id
   AND (task_status='queued' OR (task_status='retry_wait' AND next_attempt_at<=now()) OR (task_status='running' AND lease_until<=now()))
   ORDER BY stream_id,shard='' DESC,length(shard),shard LIMIT 1 FOR UPDATE SKIP LOCKED;
 IF NOT FOUND THEN RETURN NULL; END IF;
 IF rs.task_status='running' THEN
  INSERT INTO public.connect_events(connection_id,run_id,level,code) VALUES(c.id,r.id,'WARN','task_lease_expired');
 END IF;
 IF rs.attempts>=r.max_attempts THEN
  PERFORM public.connect_fail_task_run(r.id,'attempts_exhausted');
  RETURN NULL;
 END IF;
 token:=gen_random_uuid();
 UPDATE public.connect_run_streams SET task_status='running',lease_token=token,lease_until=now()+interval '90 seconds',
   attempts=attempts+1,worker=left(_worker,200),started_at=coalesce(started_at,now()),error_code=NULL
  WHERE run_id=rs.run_id AND stream_id=rs.stream_id AND shard=rs.shard RETURNING * INTO rs;
 SELECT coalesce(jsonb_agg(jsonb_build_object('id',s.id,'namespace',s.namespace,'name',s.name,'primary_key',s.primary_key,'primary_keys',to_jsonb(s.key_columns),'cursor_field',s.cursor_field,'sync_mode',CASE WHEN r.operation IN ('reset','backfill') THEN 'full_refresh' ELSE s.sync_mode END,'schema',x.schema_json,'state',x.checkpoint,'sequence',x.sequence,'completed',x.completed,'generation',x.generation,'deferred',x.replay_deferred,
   'shard',x.shard,
   'splittable',(x.shard='' AND x.sequence=0 AND r.operation IN ('sync','reset','backfill') AND c.source_version LIKE 'kafka-%'),
   'dead_letters',CASE WHEN r.operation='replay' THEN (SELECT coalesce(jsonb_agg(jsonb_build_object('id',d.id,'position',d.position,'error_code',d.error_code,'object_key',d.object_key,'line',d.line) ORDER BY d.created_at),'[]') FROM public.connect_dead_letters d WHERE d.replay_run_id=r.id AND d.stream_id=s.id AND d.status='open') END)),'[]')
  INTO payload FROM public.connect_run_streams x JOIN public.connect_streams s ON s.id=x.stream_id WHERE x.run_id=r.id AND x.stream_id=rs.stream_id AND x.shard=rs.shard;
 RETURN jsonb_build_object('run_id',r.id,'lease_token',token,'attempt',rs.attempts,'task',true,'operation',r.operation,'streams',payload,'credential_ref',c.credential_ref,'source_version',c.source_version,'destination_version',c.destination_version,'operation_timeout_seconds',c.operation_timeout_seconds);
END $$;

-- A pool agent's claim: one task from the pool's connections. Connections are tried least recently
-- served first; a connection another agent is claiming from right now is skipped (SKIP LOCKED), so
-- concurrent agents never wait on one another. Each try is the connection's own claim
-- (connect_claim_task), with every check it makes. The claim names its connection.
CREATE FUNCTION public.connect_claim_pool_task(_pool uuid,_worker text DEFAULT NULL,_agent text DEFAULT NULL,_slots integer DEFAULT NULL,_version text DEFAULT NULL) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE p public.connect_pools; cid uuid; claim jsonb;
BEGIN
 IF auth.role()<>'service_role' THEN RAISE EXCEPTION 'Agent service only'; END IF;
 SELECT * INTO p FROM public.connect_pools WHERE id=_pool;
 IF NOT FOUND OR NOT p.enabled OR (p.organization_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.org_members WHERE org_id=p.organization_id AND user_id=p.owner_id)) THEN RETURN NULL; END IF;
 IF _agent IS NOT NULL THEN
  INSERT INTO public.connect_workers(pool_id,agent,version,slots) VALUES(p.id,left(_agent,200),_version,_slots)
   ON CONFLICT (pool_id,agent) DO UPDATE SET last_seen_at=now(),version=EXCLUDED.version,slots=EXCLUDED.slots;
 END IF;
 FOR cid IN SELECT c.id FROM public.connect_connections c
   WHERE c.pool_id=p.id AND c.enabled AND c.organization_id IS NOT DISTINCT FROM p.organization_id
     AND (p.organization_id IS NOT NULL OR c.owner_id=p.owner_id)
   ORDER BY c.last_claimed_at NULLS FIRST, c.id LIMIT 1000
 LOOP
  PERFORM 1 FROM public.connect_connections WHERE id=cid FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN CONTINUE; END IF;
  claim:=public.connect_claim_task(cid,_worker);
  IF claim IS NOT NULL THEN
   UPDATE public.connect_connections SET last_claimed_at=now() WHERE id=cid;
   RETURN claim||jsonb_build_object('connection_id',cid);
  END IF;
 END LOOP;
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION public.connect_claim_pool_task(uuid,text,text,integer,text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.connect_claim_pool_task(uuid,text,text,integer,text) TO service_role;
