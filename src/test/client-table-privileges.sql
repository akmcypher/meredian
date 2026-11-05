-- Run with psql -v ON_ERROR_STOP=1 as the local migration owner. Always rolls back.
BEGIN;
CREATE TABLE public.meridian_acl_probe (id integer);
CREATE TABLE core.meridian_acl_probe (id integer);
DO $$
DECLARE client text; namespace text; privilege text; relation regclass;
BEGIN
  FOREACH client IN ARRAY ARRAY['anon', 'authenticated'] LOOP
    FOREACH namespace IN ARRAY ARRAY['public', 'core'] LOOP
      FOREACH privilege IN ARRAY ARRAY['TRUNCATE', 'TRIGGER', 'REFERENCES'] LOOP
        IF has_table_privilege(client, namespace || '.meridian_acl_probe', privilege) THEN
          RAISE EXCEPTION '% inherits % on new %.meridian_acl_probe', client, privilege, namespace;
        END IF;
      END LOOP;
    END LOOP;
  END LOOP;
  FOR relation IN SELECT c.oid::regclass FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('public','core') AND c.relkind IN ('r','v','p','m') LOOP
    FOREACH privilege IN ARRAY ARRAY['SELECT','INSERT','UPDATE','DELETE'] LOOP
      IF has_table_privilege('anon',relation,privilege) THEN RAISE EXCEPTION 'Anonymous % on %',privilege,relation; END IF;
    END LOOP;
  END LOOP;
  IF NOT has_table_privilege('authenticated', 'public.meridian_acl_probe', 'SELECT') THEN
    RAISE EXCEPTION 'Ordinary application SELECT grant was removed';
  END IF;
END $$;
ROLLBACK;
