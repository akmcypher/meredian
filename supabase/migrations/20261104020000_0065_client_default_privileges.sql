-- Schema-specific defaults cannot subtract a grant inherited from global defaults.
-- Preserve CRUD and service-role grants while removing client DDL capabilities.
ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE TRUNCATE, TRIGGER, REFERENCES ON TABLES FROM anon, authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public REVOKE TRUNCATE, TRIGGER, REFERENCES ON TABLES FROM anon, authenticated;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA core REVOKE TRUNCATE, TRIGGER, REFERENCES ON TABLES FROM anon, authenticated;
DO $$
DECLARE r record;
BEGIN
  FOR r IN SELECT schemaname, tablename FROM pg_tables WHERE schemaname IN ('public', 'core') LOOP
    EXECUTE format('REVOKE TRUNCATE, TRIGGER, REFERENCES ON TABLE %I.%I FROM anon, authenticated', r.schemaname, r.tablename);
  END LOOP;
END $$;
