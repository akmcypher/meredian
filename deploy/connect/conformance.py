"""Connector conformance kit for meridian-connect/1 relational actors.

Drives the actor as a separate process over STDIO (exactly as agent.py does) against a live
database the subclass prepares. A connector is certified when its subclass passes every test.
Covers: spec, check, auth failure, discover (single and composite keys), full refresh,
checkpoint resume without loss or duplicates, incremental pick-up, reset, type round-trip and
schema drift. Deletes are declared in spec; relational actors without CDC declare none.
"""
import json, os, subprocess, sys, tempfile, unittest
from pathlib import Path
from sdk import validate, PROTOCOL
ROWS=1200
class RelationalConformance(unittest.TestCase):
    actor=None          # path to the actor script
    namespace=None      # schema (Postgres) or database (MySQL) holding the fixtures
    typed_expected={}   # column -> value the typed fixture row must arrive as
    @classmethod
    def config(cls): raise NotImplementedError
    @classmethod
    def bad_config(cls): raise NotImplementedError
    @classmethod
    def create_fixtures(cls):
        """Create customers(id bigint pk, updated_at bigint, name, email), pairs(a,b composite pk,
        updated_at, v) and typed(id pk, updated_at, ...typed_expected columns) with ROWS customers,
        300 pairs and one typed row."""
        raise NotImplementedError
    @classmethod
    def touch(cls, ids, cursor): raise NotImplementedError
    @classmethod
    def drop_fixtures(cls): raise NotImplementedError
    @classmethod
    def setUpClass(cls):
        if cls is RelationalConformance: raise unittest.SkipTest('abstract kit')
        cls.create_fixtures()
        cls.catalog=cls.discover()
    @classmethod
    def tearDownClass(cls):
        if cls is not RelationalConformance: cls.drop_fixtures()
    @classmethod
    def run_actor(cls, command, config=None, catalog=None, state=None):
        with tempfile.TemporaryDirectory() as tmp:
            args=[sys.executable,str(cls.actor),command]
            for name,value in [('config',config if config is not None else cls.config()),('catalog',catalog),('state',state)]:
                if value is None: continue
                p=Path(tmp)/f'{name}.json'; p.write_text(json.dumps(value)); args+=['--'+name,str(p)]
            proc=subprocess.run(args,capture_output=True,text=True,timeout=300,env={**os.environ,'PYTHONPATH':str(Path(cls.actor).parent)})
        messages=[validate(json.loads(line)) for line in proc.stdout.splitlines() if line.strip()]
        return messages,proc.returncode
    @classmethod
    def discover(cls):
        messages,code=cls.run_actor('discover')
        assert code==0, 'discover failed'
        return {(s['namespace'],s['name']):s for s in next(m for m in messages if m['type']=='CATALOG')['catalog']['streams']}
    def stream(self, name, mode='incremental'):
        s=self.catalog[(self.namespace,name)]; keys=s['primary_key'] if isinstance(s['primary_key'],list) else [s['primary_key']]
        return {'namespace':self.namespace,'name':name,'primary_key':','.join(keys),'primary_keys':keys,'cursor_field':'updated_at','sync_mode':mode,'schema':s['json_schema']}
    def read(self, stream, state=None):
        messages,code=self.run_actor('read',catalog=stream,state=state or {})
        self.assertEqual(code,0,messages[-1:])
        return messages
    @staticmethod
    def records(messages): return [m['record'] for m in messages if m['type']=='RECORD']
    @staticmethod
    def states(messages): return [m['state'] for m in messages if m['type']=='STATE']
    @staticmethod
    def key(r): return json.dumps(r['primary_key'])
    def test_spec(self):
        messages,code=self.run_actor('spec',config={})
        spec=messages[0]['spec']
        self.assertEqual(code,0); self.assertEqual(spec['protocol'],PROTOCOL)
        self.assertIn('full_refresh',spec['modes']); self.assertIn('incremental',spec['modes'])
        self.assertIn('deletes',spec)
    def test_check(self):
        messages,code=self.run_actor('check')
        self.assertEqual(code,0); self.assertEqual(messages[-1]['connectionStatus']['status'],'SUCCEEDED')
    def test_auth_failure_is_a_safe_code(self):
        messages,code=self.run_actor('check',config=self.bad_config())
        self.assertNotEqual(code,0)
        self.assertEqual(messages[-1]['trace']['code'],'auth_failed')
        self.assertNotIn('password',json.dumps(messages).lower())
    def test_discover_keys(self):
        self.assertEqual(self.catalog[(self.namespace,'customers')]['primary_key'],'id')
        self.assertEqual(self.catalog[(self.namespace,'pairs')]['primary_key'],['a','b'])
    def test_full_refresh(self):
        messages=self.read(self.stream('customers','full_refresh'))
        keys=[self.key(r) for r in self.records(messages)]
        self.assertEqual(len(keys),ROWS); self.assertEqual(len(set(keys)),ROWS)
        self.assertTrue(self.states(messages)[-1]['completed'])
        self.assertTrue(all(m['type']!='STATE' or not m['state']['completed'] for m in messages[:-1]))
    def test_checkpoint_resume_without_loss_or_duplicates(self):
        for name,total,mode in [('customers',ROWS,'incremental'),('customers',ROWS,'full_refresh'),('pairs',300,'incremental')]:
            stream=self.stream(name,mode); first=self.read(stream)
            # Resume from the first committed checkpoint, as a new attempt would after a crash.
            cut=next(i for i,m in enumerate(first) if m['type']=='STATE')
            before=[self.key(r) for r in self.records(first[:cut])]
            after=[self.key(r) for r in self.records(self.read(stream,first[cut]['state']))]
            self.assertEqual(len(before)+len(after),total,(name,mode))
            self.assertEqual(len(set(before)|set(after)),total,(name,mode))
    def test_incremental_picks_up_changes_and_reset_reads_all(self):
        stream=self.stream('customers'); done=self.states(self.read(stream))[-1]
        self.assertEqual(self.records(self.read(stream,done)),[])
        self.touch([7,700],10**6)
        changed=self.records(self.read(stream,done))
        self.assertEqual(sorted(r['primary_key'] for r in changed),[7,700])
        self.assertEqual(len(self.records(self.read(stream,{}))),ROWS)
    def test_type_round_trip(self):
        data=self.records(self.read(self.stream('typed','full_refresh')))[0]['data']
        for column,expected in self.typed_expected.items(): self.assertEqual(data[column],expected,column)
    def test_schema_drift_fails_with_safe_code(self):
        stream={**self.stream('customers'),'schema':{'type':'object','properties':{}}}
        messages,code=self.run_actor('read',catalog=stream,state={})
        self.assertNotEqual(code,0); self.assertEqual(messages[-1]['trace']['code'],'schema_drift')
        self.assertEqual(self.records(messages),[])
