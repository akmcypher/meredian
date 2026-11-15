"""Agent process control with real child processes: a blocked, silent actor that has started a
grandchild is killed as a whole group when the run is cancelled (lease lost) or its deadline
passes. No control plane is needed: agent.source() is driven directly."""
import os, subprocess, sys, tempfile, threading, time, unittest
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
        saved=agent.source
        def source(module,command,config,stream=None,state=None,stop=None,deadline=None):
            commands.append((command,stream['shard']))
            yield {'type':'PLAN','plan':{'shards':['0','1','2']}}
        def post(base,payload): posted.append(payload); return {'ok':True}
        agent.source=source; agent.post=post
        try:
            agent.execute({'run_id':'r','lease_token':'t','task':True,'operation':'sync','credential_ref':'k','source_version':'kafka-v1',
                           'streams':[{'id':'s','shard':'','splittable':True,'completed':False,'sequence':0,'state':{}}]})
        finally: agent.source=saved
        self.assertEqual(commands,[('plan','')])
        # Split ends the planning task: no commit and no finish.
        self.assertEqual([p['action'] for p in posted if p['action']!='heartbeat'],['split'])
        self.assertEqual(posted[-1]['shards'],['0','1','2'])
if __name__=='__main__': unittest.main()
