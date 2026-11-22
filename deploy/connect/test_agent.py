"""Agent process control with real child processes: a blocked, silent actor that has started a
grandchild is killed as a whole group when the run is cancelled (lease lost) or its deadline
passes. No control plane is needed: agent.source() is driven directly."""
import os, subprocess, sys, tempfile, threading, time, unittest
from unittest.mock import patch
from pathlib import Path
os.environ.setdefault('MERIDIAN_URL','http://127.0.0.1:9')
os.environ.setdefault('INGEST_TOKEN_FILE',os.devnull)
import agent
BLOCKED='''import subprocess,sys,time
child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(600)'])
open(sys.argv[-1] if sys.argv[-1].endswith('.pid') else 'x.pid','w').write(str(child.pid))
time.sleep(600)
'''
def alive(pid):
    if os.name=='nt':
        out=subprocess.run(['tasklist','/FI',f'PID eq {pid}','/NH'],capture_output=True,text=True).stdout
        return str(pid) in out
    try: os.kill(pid,0); return True
    except ProcessLookupError: return False
class ActorProcessControl(unittest.TestCase):
    def test_timeout_removes_child_snapshot_files(self):
        import shutil
        messages=[]
        with tempfile.TemporaryDirectory() as tmp:
            actor=Path(tmp)/'snapshot.py'
            actor.write_text('import tempfile,json,time\nfrom pathlib import Path\np=tempfile.mkdtemp(prefix="meridian-files-")\nPath(p,"object").write_bytes(b"source snapshot")\nprint(json.dumps({"type":"LOG","log":{"path":p}}),flush=True)\ntime.sleep(600)\n')
            try:
                with self.assertRaisesRegex(ValueError,'timeout'):
                    for message in agent.source(actor,'read',os.devnull,deadline=time.time()+2): messages.append(message)
                path=Path(messages[0]['log']['path'])
                self.assertFalse(path.exists(),'killed actor left its source snapshot behind')
            finally:
                if messages: shutil.rmtree(messages[0]['log']['path'],ignore_errors=True)
    def run_blocked(self, **limits):
        with tempfile.TemporaryDirectory() as tmp:
            actor=Path(tmp)/'blocked.py'; actor.write_text(BLOCKED)
            pidfile=Path(tmp)/'grandchild.pid'
            # The actor treats its last argument as the pid file when it ends in .pid.
            gen=agent.source(actor,'read',str(pidfile),**limits)
            started=time.time()
            with self.assertRaisesRegex(ValueError,'timeout'): list(gen)
            elapsed=time.time()-started
            for _ in range(40):
                if pidfile.exists() and pidfile.read_text(): break
                time.sleep(0.25)
            grandchild=int(pidfile.read_text())
            for _ in range(20):
                if not alive(grandchild): break
                time.sleep(0.25)
            self.assertFalse(alive(grandchild),'grandchild survived')
            return elapsed
    def test_cancel_kills_the_whole_group(self):
        stop=threading.Event()
        threading.Timer(2.0,stop.set).start()
        self.assertLess(self.run_blocked(stop=stop),15)
    def test_deadline_kills_the_whole_group(self):
        self.assertLess(self.run_blocked(deadline=time.time()+2.0),15)
    def test_no_limit_means_no_kill(self):
        # A normal actor that exits on its own is unaffected by the watchdog.
        with tempfile.TemporaryDirectory() as tmp:
            actor=Path(tmp)/'ok.py'; actor.write_text('print(\'{"type":"LOG","log":{"level":"INFO","code":"x"}}\')')
            out=list(agent.source(actor,'read',os.devnull,stop=threading.Event(),deadline=time.time()+30))
            self.assertEqual(out,[{'type':'LOG','log':{'level':'INFO','code':'x'}}])
class TaskClaims(unittest.TestCase):
    """Claiming without a control plane: agent.post and agent.execute are replaced."""
    def setUp(self):
        self.saved=(agent.post,agent.execute,dict(agent.TASKS),os.environ.get('CONNECT_ONCE'),os.environ.get('CONNECT_SLOTS'))
        os.environ['CONNECT_ONCE']='1'
    def tearDown(self):
        agent.post,agent.execute=self.saved[0],self.saved[1]; agent.TASKS.clear(); agent.TASKS.update(self.saved[2])
        for key,value in (('CONNECT_ONCE',self.saved[3]),('CONNECT_SLOTS',self.saved[4])):
            if value is None: os.environ.pop(key,None)
            else: os.environ[key]=value
    def test_claims_tasks_with_a_worker_id_per_slot(self):
        seen=[]; lock=threading.Lock(); ran=[]
        def post(base,payload):
            # One claim per worker, then nothing: CONNECT_ONCE drains until no work is left.
            with lock:
                first=payload.get('worker') not in {p.get('worker') for p in seen}
                seen.append(payload)
            return {'claim':{'run_id':'r','worker':payload.get('worker')} if first else None}
        agent.post=post; agent.execute=lambda claim: ran.append(claim['worker'])
        os.environ['CONNECT_SLOTS']='3'
        agent.main()
        self.assertEqual({p['action'] for p in seen},{'claim_task'})
        self.assertEqual(len({p['worker'] for p in seen}),3)
        self.assertEqual(sorted(ran),sorted({p['worker'] for p in seen}))
    def test_falls_back_to_run_claims_when_the_server_has_no_task_claims(self):
        import io, urllib.error
        actions=[]
        def post(base,payload):
            actions.append(payload['action'])
            if payload['action']=='claim_task': raise urllib.error.HTTPError('u',400,'bad',{},io.BytesIO(b'{}'))
            return {'claim':None}
        agent.post=post; agent.execute=lambda claim: None
        os.environ['CONNECT_SLOTS']='1'
        agent.main(); agent.main()
        # One rejected task claim, then whole-run claims only.
        self.assertEqual(actions,['claim_task','claim','claim'])
    def test_a_splittable_task_plans_and_splits_instead_of_reading(self):
        posted=[]; commands=[]
        saved=agent.source; saved_group=agent.group_mode
        def source(module,command,config,stream=None,state=None,stop=None,deadline=None):
            commands.append((command,stream['shard']))
            yield {'type':'PLAN','plan':{'shards':['0','1','2']}}
        def post(base,payload): posted.append(payload); return {'ok':True}
        agent.source=source; agent.post=post; agent.group_mode=lambda claim,config: False
        try:
            agent.execute({'run_id':'r','lease_token':'t','task':True,'operation':'sync','credential_ref':'k','source_version':'kafka-v1',
                           'streams':[{'id':'s','shard':'','splittable':True,'completed':False,'sequence':0,'state':{}}]})
        finally: agent.source=saved; agent.group_mode=saved_group
        self.assertEqual(commands,[('plan','')])
        # Split ends the planning task: no commit and no finish.
        self.assertEqual([p['action'] for p in posted if p['action']!='heartbeat'],['split'])
        self.assertEqual(posted[-1]['shards'],['0','1','2'])
    def test_file_reset_and_backfill_build_the_ledger_for_the_next_incremental_sync(self):
        from unittest.mock import patch
        for operation in ('reset','backfill'):
            seen=[]
            def source(module,command,config,stream=None,state=None,**kw):
                seen.append((stream['sync_mode'],state))
                yield {'type':'STATE','state':{'version':1,'completed':True}}
            def post(base,payload): return {'sequence':payload.get('sequence',0)+1}
            with patch.object(agent,'source',source),patch.object(agent,'post',post),patch('destination_bronze.post',post):
                agent.execute({'run_id':'r','lease_token':'t','operation':operation,'credential_ref':'k','source_version':'files-v1',
                               'streams':[{'id':'s','sync_mode':'full_refresh','completed':False,'sequence':0,'state':{}}]})
            self.assertEqual(seen,[('incremental',{})])
    def test_retirement_is_cleanup_and_commits_its_completion(self):
        commands=[]; posted=[]
        def source(module,command,config,stream=None,state=None,**kw):
            commands.append(command)
            yield {'type':'STATE','state':{'version':1,'completed':True,'retirement':{'status':'done'}}}
        def post(base,payload): posted.append(payload); return {'sequence':payload.get('sequence',0)+1}
        with patch.object(agent,'source',source),patch.object(agent,'post',post),patch('destination_bronze.post',post):
            agent.execute({'run_id':'r','lease_token':'t','operation':'sync','credential_ref':'k','source_version':'postgres-cdc-v1',
                           'streams':[{'id':'s','completed':False,'sequence':0,'state':{'retirement':{'status':'pending'}}}]})
        self.assertEqual(commands,['cleanup'])
        commit=next(p for p in posted if p['action']=='commit')
        self.assertEqual(commit['state']['state']['retirement'],{'status':'done'})
        self.assertEqual(commit['records'],[])
    def test_group_config_selects_whole_topic_task(self):
        import json
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'config.json'; path.write_text(json.dumps({'consumer_group':True,'group_id':'workers'}))
            self.assertTrue(agent.group_mode({'source_version':'kafka-v1'},str(path)))
            self.assertFalse(agent.group_mode({'source_version':'postgres-v1'},str(path)))
    def test_spark_kafka_replay_uses_bounded_kafka_actor_and_fenced_commit(self):
        commands=[]; posted=[]
        def source(module,command,config,stream=None,state=None,**kw):
            commands.append((module.name,command))
            yield {'type':'STATE','state':{'version':1,'completed':True,
                'replayed':['11111111-1111-4111-8111-111111111111']}}
        def post(base,payload):
            posted.append(payload)
            return {'sequence':payload.get('sequence',0)+1}
        with patch.object(agent,'source',source),patch.object(agent,'post',post),patch('destination_bronze.post',post),patch.object(agent,'spark',side_effect=AssertionError('replay must not submit a Spark application')):
            agent.execute({'run_id':'r','lease_token':'t','task':True,'operation':'replay',
                'credential_ref':'k','source_version':'spark-kafka-v1',
                'streams':[{'id':'s','completed':False,'sequence':0,'state':{},'dead_letters':[]}]})
        self.assertEqual(commands,[('source_kafka.py','replay')])
        commit=next(p for p in posted if p['action']=='commit')
        self.assertEqual(commit['state']['state']['replayed'],['11111111-1111-4111-8111-111111111111'])
        self.assertTrue(next(p for p in posted if p['action']=='finish')['ok'])
if __name__=='__main__': unittest.main()
