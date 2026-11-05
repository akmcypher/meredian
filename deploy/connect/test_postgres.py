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
 def tearDownClass(cls): cls.conn.execute('DROP SCHEMA connect_test CASCADE');cls.conn.commit();cls.conn.close()
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
