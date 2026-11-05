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
if __name__=='__main__': unittest.main()
