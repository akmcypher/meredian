-- Writer exclusion for ingestion storage reconciliation.
--
-- Every writer uploads an object first and registers it second, and an identical retry may
-- register an object that already exists: a push worker that lost its response, a retried
-- Connect commit, a Spark part uploaded through a signed URL. A scan that checks references and
-- then decides can therefore report, or delete, an object that a writer registers in between.
--
-- The protocol:
--   * Registration of an ingest-files key (the four tables that reference objects) takes a
--     transaction-level advisory lock on the key and is refused while the key is fenced.
--   * storage_reconcile_check takes the same lock, then decides from committed state: the key is
--     unreferenced, not fenced, and no writer run that could still register it is active.
--   * storage_fence_object makes that decision and records a fence in the same transaction.
-- Under the lock a registration either committed before the check (the check sees its row) or
-- runs after the fence (and is refused), so a fenced key is never registered again. Deleting a
-- fenced object, then releasing the fence, can never remove a referenced object. Destructive
-- cleanup remains disabled in src/lib/storage-reconcile.server.ts; the dry run uses the check.
CREATE TABLE public.storage_deletion_fences (
  bucket text NOT NULL CHECK (bucket = 'ingest-files'),
  object_key text NOT NULL,
  reconcile_run uuid NOT NULL,
  fenced_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (bucket, object_key));
ALTER TABLE public.storage_deletion_fences ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.storage_deletion_fences FROM PUBLIC, anon, authenticated;
GRANT SELECT, INSERT, DELETE ON public.storage_deletion_fences TO service_role;

CREATE FUNCTION public.storage_key_lock(_key text) RETURNS void
LANGUAGE sql VOLATILE SET search_path = public AS $$
  SELECT pg_advisory_xact_lock(hashtextextended('ingest-files:' || _key, 0));
$$;

CREATE FUNCTION public.storage_refuse_fenced_key() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE k text := to_jsonb(NEW) ->> TG_ARGV[0];
BEGIN
  PERFORM public.storage_key_lock(k);
  IF EXISTS (SELECT 1 FROM public.storage_deletion_fences f WHERE f.bucket = 'ingest-files' AND f.object_key = k) THEN
    RAISE EXCEPTION 'storage_object_fenced' USING ERRCODE = '55006';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER storage_fence_landing BEFORE INSERT OR UPDATE OF object_key ON public.ingest_landing_batches
  FOR EACH ROW EXECUTE FUNCTION public.storage_refuse_fenced_key('object_key');
CREATE TRIGGER storage_fence_commit_manifest BEFORE INSERT OR UPDATE OF manifest_key ON public.connect_commits
  FOR EACH ROW EXECUTE FUNCTION public.storage_refuse_fenced_key('manifest_key');
CREATE TRIGGER storage_fence_connect_dead_letter BEFORE INSERT OR UPDATE OF object_key ON public.connect_dead_letters
  FOR EACH ROW EXECUTE FUNCTION public.storage_refuse_fenced_key('object_key');
CREATE TRIGGER storage_fence_ingest_dead_letter BEFORE INSERT OR UPDATE OF object_key ON public.ingest_dead_letters
  FOR EACH ROW EXECUTE FUNCTION public.storage_refuse_fenced_key('object_key');

-- Why a key must be kept, or NULL when it may be fenced. The caller holds the key lock.
--   _kind        dead_letter | landing | manifest
--   _writer_run  the Connect run named by a manifest's content (manifests only)
-- Dead letters name their run in the key (<owner>/dead-letters/<run>/...); landing objects are
-- kept while their owner has any active Connect run, because a Spark part is uploaded before its
-- run registers it.
CREATE FUNCTION public.storage_keep_reason(_key text, _kind text, _writer_run uuid) RETURNS text
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public AS $$
DECLARE parts text[] := string_to_array(_key, '/'); owner uuid; run uuid;
BEGIN
  IF _kind NOT IN ('dead_letter', 'landing', 'manifest') THEN RAISE EXCEPTION 'unknown object kind'; END IF;
  IF EXISTS (SELECT 1 FROM public.storage_deletion_fences WHERE bucket = 'ingest-files' AND object_key = _key) THEN RETURN 'fenced'; END IF;
  IF EXISTS (SELECT 1 FROM public.ingest_landing_batches WHERE object_key = _key)
     OR EXISTS (SELECT 1 FROM public.connect_commits WHERE manifest_key = _key)
     OR EXISTS (SELECT 1 FROM public.connect_dead_letters WHERE object_key = _key)
     OR EXISTS (SELECT 1 FROM public.ingest_dead_letters WHERE object_key = _key) THEN
    RETURN 'referenced';
  END IF;
  IF parts[1] !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN RETURN 'foreign'; END IF;
  owner := parts[1]::uuid;
  IF _kind = 'dead_letter' AND parts[3] ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    run := parts[3]::uuid;
  ELSIF _kind = 'manifest' THEN
    IF _writer_run IS NULL THEN RETURN 'foreign'; END IF;
    run := _writer_run;
  END IF;
  IF run IS NOT NULL AND EXISTS (SELECT 1 FROM public.connect_runs WHERE id = run AND status IN ('queued', 'running', 'retry_wait')) THEN
    RETURN 'active';
  END IF;
  IF _kind = 'landing' AND EXISTS (
      SELECT 1 FROM public.connect_runs r JOIN public.connect_connections c ON c.id = r.connection_id
      WHERE c.owner_id = owner AND r.status IN ('queued', 'running', 'retry_wait')) THEN
    RETURN 'active';
  END IF;
  RETURN NULL;
END $$;

-- Dry run: the decision under the key lock, without a fence. Returns the keep reason, or NULL.
CREATE FUNCTION public.storage_reconcile_check(_key text, _kind text, _writer_run uuid DEFAULT NULL) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
  IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'service only'; END IF;
  PERFORM public.storage_key_lock(_key);
  RETURN public.storage_keep_reason(_key, _kind, _writer_run);
END $$;

-- Destructive cleanup (not enabled): fence the key when it may go. Returns the keep reason, or
-- NULL after fencing. Delete the object only after this returned NULL, then release the fence.
CREATE FUNCTION public.storage_fence_object(_key text, _kind text, _reconcile_run uuid, _writer_run uuid DEFAULT NULL) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE reason text;
BEGIN
  IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'service only'; END IF;
  PERFORM public.storage_key_lock(_key);
  reason := public.storage_keep_reason(_key, _kind, _writer_run);
  IF reason IS NULL THEN
    INSERT INTO public.storage_deletion_fences(bucket, object_key, reconcile_run) VALUES ('ingest-files', _key, _reconcile_run);
  END IF;
  RETURN reason;
END $$;

-- Releases a fence once its object is deleted (or the deletion was abandoned).
CREATE FUNCTION public.storage_release_fence(_key text, _reconcile_run uuid) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
  IF auth.role() IS DISTINCT FROM 'service_role' THEN RAISE EXCEPTION 'service only'; END IF;
  PERFORM public.storage_key_lock(_key);
  DELETE FROM public.storage_deletion_fences WHERE bucket = 'ingest-files' AND object_key = _key AND reconcile_run = _reconcile_run;
  RETURN FOUND;
END $$;

REVOKE ALL ON FUNCTION public.storage_key_lock(text), public.storage_refuse_fenced_key(),
  public.storage_keep_reason(text, text, uuid), public.storage_reconcile_check(text, text, uuid),
  public.storage_fence_object(text, text, uuid, uuid), public.storage_release_fence(text, uuid)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.storage_reconcile_check(text, text, uuid),
  public.storage_fence_object(text, text, uuid, uuid), public.storage_release_fence(text, uuid) TO service_role;
