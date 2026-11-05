"""Certifies source_postgres.py with the conformance kit against the local Supabase Postgres."""
import os, unittest
from pathlib import Path
from psycopg import connect
from conformance import RelationalConformance, ROWS
class PostgresConformance(RelationalConformance):
    actor=Path(__file__).parent/'source_postgres.py'
    namespace='connect_certify'
    typed_expected={'big':'9007199254740993','small':42,'amount':'12345678901234567890.123456789','ok':True,'note':'名前 ✓','missing':None,'at':'2026-11-02T10:30:00+00:00','day':'2026-11-02','raw':'3q2+7w==','doc':{'a':[1,'x']}}
    @classmethod
    def config(cls):
        return {'host':os.environ.get('CONNECT_TEST_HOST','127.0.0.1'),'port':int(os.environ.get('CONNECT_TEST_PORT','54322')),'dbname':os.environ.get('CONNECT_TEST_DB','postgres'),'user':'postgres','password':os.environ.get('PGPASSWORD','postgres'),'sslmode':'disable','options':'-c client_encoding=UTF8 -c TimeZone=UTC'}
    @classmethod
    def bad_config(cls): return {**cls.config(),'password':'wrong-password'}
    @classmethod
    def sql(cls, *statements):
        with connect(**cls.config()) as conn:
            for s in statements: conn.execute(s)
    @classmethod
    def create_fixtures(cls):
        cls.sql('DROP SCHEMA IF EXISTS connect_certify CASCADE','CREATE SCHEMA connect_certify',
          'CREATE TABLE connect_certify.customers(id bigint PRIMARY KEY,updated_at bigint NOT NULL,name text,email text)',
          f"INSERT INTO connect_certify.customers SELECT n,n,'customer-'||n,'c'||n||'@test' FROM generate_series(1,{ROWS})n",
          'CREATE TABLE connect_certify.pairs(a int,b int,updated_at bigint NOT NULL,v text,PRIMARY KEY(a,b))',
          "INSERT INTO connect_certify.pairs SELECT n/10,n%10,1,'v'||n FROM generate_series(0,299)n",
          'CREATE TABLE connect_certify.typed(id int PRIMARY KEY,updated_at bigint NOT NULL,big bigint,small int,amount numeric(40,9),ok boolean,note text,missing text,at timestamptz,day date,raw bytea,doc jsonb)',
          "INSERT INTO connect_certify.typed VALUES(1,1,9007199254740993,42,12345678901234567890.123456789,true,'名前 ✓',NULL,'2026-11-02 10:30:00+00','2026-11-02','\\xdeadbeef','{\"a\":[1,\"x\"]}')")
    @classmethod
    def touch(cls, ids, cursor): cls.sql(f"UPDATE connect_certify.customers SET updated_at={int(cursor)} WHERE id IN ({','.join(str(int(i)) for i in ids)})")
    @classmethod
    def drop_fixtures(cls): cls.sql('DROP SCHEMA IF EXISTS connect_certify CASCADE')
if __name__=='__main__': unittest.main()
