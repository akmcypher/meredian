"""Real PostgreSQL connector contract/recovery harness. No mocked SQL engine."""
import unittest,os,json
from psycopg import connect
from psycopg.rows import dict_row
from source_postgres import discover,read
class Contract(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.config={'host':os.environ.get('CONNECT_TEST_HOST','127.0.0.1'),'port':int(os.environ.get('CONNECT_TEST_PORT','55439')),'dbname':'postgres','user':'postgres','sslmode':'disable'}
  cls.conn=connect(**cls.config,row_factory=dict_row);cls.conn.execute("SET client_encoding='UTF8'")
  cls.conn.execute('CREATE SCHEMA IF NOT EXISTS connect_test');cls.conn.execute('DROP TABLE IF EXISTS connect_test.customers');cls.conn.execute('CREATE TABLE connect_test.customers(id bigint PRIMARY KEY,updated_at bigint NOT NULL,name text,email text,source text)')
  cls.conn.execute("INSERT INTO connect_test.customers SELECT n,n,'customer-'||n,'customer-'||n||'@test','postgres' FROM generate_series(1,1200)n");cls.conn.commit()
  cls.schema=next(s for s in discover(cls.conn) if s['namespace']=='connect_test' and s['name']=='customers')['json_schema']
  cls.stream={'namespace':'connect_test','name':'customers','primary_key':'id','cursor_field':'updated_at','sync_mode':'incremental','schema':cls.schema}
 @classmethod
 def tearDownClass(cls): cls.conn.execute('DROP SCHEMA connect_test CASCADE');cls.conn.execute('DROP ROLE IF EXISTS connect_test_reader');cls.conn.commit();cls.conn.close()
 def test_select_only_role_discovers_keys(self):
  # information_schema hides key constraints from roles holding only SELECT; discovery must not.
  c=self.conn;c.execute('DROP TABLE IF EXISTS connect_test.order_lines');c.execute('CREATE TABLE connect_test.order_lines(order_id int,line int,qty int NOT NULL,PRIMARY KEY(line,order_id))')
  c.execute("DO $$BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='connect_test_reader') THEN CREATE ROLE connect_test_reader NOLOGIN; END IF; END$$")
  # The test user may hold CREATEROLE without superuser (Supabase): it needs membership to SET ROLE.
  c.execute('GRANT connect_test_reader TO current_user')
  c.execute('GRANT USAGE ON SCHEMA connect_test TO connect_test_reader');c.execute('GRANT SELECT ON ALL TABLES IN SCHEMA connect_test TO connect_test_reader');c.commit()
  try:
   c.execute('SET ROLE connect_test_reader')
   self.assertFalse(c.execute("SELECT has_table_privilege('connect_test.customers','REFERENCES') AS r").fetchone()['r'])
   streams={s['name']:s for s in discover(c) if s['namespace']=='connect_test'}
  finally: c.rollback();c.execute('RESET ROLE');c.execute('DROP TABLE connect_test.order_lines');c.commit()
  self.assertEqual(streams['customers']['primary_key'],'id');self.assertEqual(streams['customers']['json_schema'],self.schema)
  self.assertEqual(streams['order_lines']['primary_key'],['line','order_id'])
 def test_check_discover(self):
  self.assertEqual(self.conn.execute('SELECT 1 as n').fetchone()['n'],1)
  self.assertEqual(next(s for s in discover(self.conn) if s['name']=='customers')['primary_key'],'id')
 def test_full_refresh(self):
  messages=list(read(self.conn,{**self.stream,'sync_mode':'full_refresh'},{},200));self.assertEqual(sum(m['type']=='RECORD' for m in messages),1200);self.assertTrue(messages[-1]['state']['completed'])
 def test_checkpoint_restart(self):
  messages=read(self.conn,self.stream,{},200);first=[]
  for m in messages:
   first.append(m)
   if m['type']=='STATE':break
  checkpoint=first[-1]['state'];messages.close()
  rest=list(read(self.conn,self.stream,checkpoint,200));ids=[m['record']['primary_key'] for m in first+rest if m['type']=='RECORD'];self.assertEqual(len(ids),1200);self.assertEqual(len(set(ids)),1200)
 def test_incremental_and_ties(self):
  checkpoint={'cursor':'1199','primary_key':1199};rows=[m['record']['primary_key'] for m in read(self.conn,self.stream,checkpoint) if m['type']=='RECORD'];self.assertEqual(rows,[1200])
 def test_schema_drift_fails(self):
  with self.assertRaisesRegex(ValueError,'schema_drift'):list(read(self.conn,{**self.stream,'schema':{}},{}))
 def test_auth_failure(self):
  with self.assertRaises(Exception):connect(**{**self.config,'user':'nonexistent_connector_user'},connect_timeout=3)
if __name__=='__main__':unittest.main()
