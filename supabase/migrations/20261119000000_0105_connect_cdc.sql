-- CDC streams (deploy/connect/README.md, "Change data capture"). A stream in sync mode `cdc` reads the database's
-- change log through a `-cdc-v1` connection; such a connection carries only `cdc` streams. The
-- checkpoint keeps its replication position like any incremental stream; reset and backfill clear
-- it, which makes the actor start a new slot and copy the table again.
ALTER TABLE public.connect_streams DROP CONSTRAINT connect_streams_sync_mode_check;
ALTER TABLE public.connect_streams ADD CONSTRAINT connect_streams_sync_mode_check
  CHECK (sync_mode IN ('full_refresh', 'incremental', 'cdc'));

CREATE OR REPLACE FUNCTION public.connect_stream_cdc_mode() RETURNS trigger
LANGUAGE plpgsql SET search_path = public AS $$
DECLARE cdc boolean;
BEGIN
  SELECT c.source_version ~ '-cdc-v[0-9]+$' INTO cdc FROM public.connect_connections c WHERE c.id = NEW.connection_id;
  IF cdc IS DISTINCT FROM (NEW.sync_mode = 'cdc') THEN
    RAISE EXCEPTION 'CDC connections take cdc streams only, and cdc streams need a CDC connection'
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION public.connect_stream_cdc_mode() FROM PUBLIC, anon, authenticated;

CREATE TRIGGER connect_stream_cdc_mode BEFORE INSERT OR UPDATE OF sync_mode, connection_id
  ON public.connect_streams FOR EACH ROW EXECUTE FUNCTION public.connect_stream_cdc_mode();
