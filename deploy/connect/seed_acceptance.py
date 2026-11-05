"""Seeds the isolated source fixture used by src/test/connect-live.e2e.test.ts.

Creates database CONNECT_TEST_DB (default connect_source) beside, never inside, the app database,
and resets public.connect_acceptance to 1200 rows. Local test fixture only.
"""
import os
from psycopg import connect, sql

HOST = os.environ.get('CONNECT_TEST_HOST', '127.0.0.1')
PORT = int(os.environ.get('CONNECT_TEST_PORT', '55439'))
DB = os.environ.get('CONNECT_TEST_DB', 'connect_source')
if HOST not in ('127.0.0.1', 'localhost', '::1'):
    raise SystemExit('Refusing to seed a non-local database')
if DB in ('postgres', 'template0', 'template1'):
    raise SystemExit('CONNECT_TEST_DB must be a dedicated fixture database')

with connect(host=HOST, port=PORT, dbname='postgres', user='postgres', sslmode='disable', autocommit=True) as admin:
    if not admin.execute('SELECT 1 FROM pg_database WHERE datname=%s', (DB,)).fetchone():
        admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(DB)))
with connect(host=HOST, port=PORT, dbname=DB, user='postgres', sslmode='disable') as conn:
    conn.execute('DROP TABLE IF EXISTS public.connect_acceptance')
    conn.execute('CREATE TABLE public.connect_acceptance(id bigint PRIMARY KEY,updated_at bigint NOT NULL,name text,email text,source text)')
    conn.execute("INSERT INTO public.connect_acceptance SELECT n,n,'customer-'||n,'customer-'||n||'@test','postgres' FROM generate_series(1,1200)n")
print(f'seeded {DB}.public.connect_acceptance with 1200 rows')
