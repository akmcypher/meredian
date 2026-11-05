"""Outbound-only agent. Source/destination children remain outside the control plane."""
import os,sys,json,time,tempfile,subprocess,threading,urllib.error
from pathlib import Path
from destination_bronze import post,write
from sdk import validate
ROOT=Path(__file__).parent
HEARTBEAT=float(os.environ.get('CONNECT_HEARTBEAT_SECONDS','20'))
# The control plane's lease is 90 s. Transient heartbeat failures are retried; the run is treated
# as lost when the control plane says the attempt is stale, or when no heartbeat has succeeded
# for this long (the lease is then about to lapse anyway).
LEASE_GRACE=float(os.environ.get('CONNECT_LEASE_GRACE_SECONDS','60'))
BASE={'url':os.environ['MERIDIAN_URL'],'token_file':os.environ['INGEST_TOKEN_FILE']}
# source_version 'postgres-v1' runs source_postgres.py. Only certified actors are listed.
SOURCES={'postgres':'source_postgres.py','mysql':'source_mysql.py','kafka':'source_kafka.py'}
SAFE={'auth_failed','source_unavailable','schema_drift','connector_failed','destination_failed','timeout','dead_letter_budget','offset_out_of_range','source_changed','record_too_large','replay_incomplete'}
# Codes whose actor detail (positions, identities, counts; never record data) is reported.
REPORTED={'offset_out_of_range','source_changed','record_too_large','replay_incomplete'}
class ActorError(ValueError):
    # detail: the actor's structured reason (offset gaps: positions only), reported before finish.
    def __init__(self, code, detail=None): super().__init__(code); self.detail=detail
def actor(version):
    name=SOURCES.get(version.rsplit('-v',1)[0])
    if not name: raise ValueError('connector_failed')
    return ROOT/name
def kill_group(proc):
    """Ends the actor and everything it started, not just the direct child. On POSIX the group is
    signalled even after the actor itself exited, so helpers it left behind go too. Windows can
    only walk the tree while the actor is alive."""
    if os.name=='nt':
        if proc.poll() is None: subprocess.run(['taskkill','/F','/T','/PID',str(proc.pid)],capture_output=True)
    else:
        import signal
        try: os.killpg(proc.pid,signal.SIGKILL)
        except (ProcessLookupError,PermissionError): pass
    if proc.poll() is not None: return
    try: proc.wait(timeout=10)
    except subprocess.TimeoutExpired: proc.kill(); proc.wait()
def source(module,command,config,stream=None,state=None,stop=None,deadline=None):
    """Runs one actor command. A watchdog kills the actor's process group as soon as `stop` is set
    (lease lost or run cancelled) or `deadline` passes, even if the actor is blocked and silent;
    the run then ends with timeout."""
    with tempfile.TemporaryDirectory() as tmp:
        args=[sys.executable,str(module),command,'--config',config]
        for name,value in [('catalog',stream),('state',state)]:
            if value is not None:
                p=Path(tmp)/f'{name}.json'; p.write_text(json.dumps(value)); args+=['--'+name,str(p)]
        group={'creationflags':subprocess.CREATE_NEW_PROCESS_GROUP} if os.name=='nt' else {'start_new_session':True}
        proc=subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True,**group)
        fired=threading.Event(); done=threading.Event()
        def watchdog():
            while not done.wait(0.5):
                if (stop is not None and stop.is_set()) or (deadline is not None and time.time()>=deadline):
                    fired.set(); kill_group(proc); return
        threading.Thread(target=watchdog,daemon=True).start()
        code='connector_failed'; detail=None
        try:
            for line in proc.stdout:
                message=validate(json.loads(line))
                # The actor names a safe error code in its TRACE; anything else stays generic.
                if message['type']=='TRACE':
                    trace=message.get('trace',{}); code=trace.get('code',code); detail=trace.get('detail'); continue
                if message['type']=='LOG' and message.get('log',{}).get('level') in ('WARN','ERROR'): print(json.dumps(message),flush=True)
                yield message
            if fired.is_set(): raise ActorError('timeout')
            if proc.wait()!=0: raise ActorError(code if code in SAFE else 'connector_failed',detail if code in REPORTED else None)
        finally:
            done.set()
            kill_group(proc)
            proc.stdout.close()
def execute(claim):
    config=str(Path(os.environ.get('CONNECT_SECRET_DIR','/run/secrets'))/(claim['credential_ref']+'.json'))
    run=claim['run_id']; token=claim['lease_token']; stop=threading.Event(); lost=threading.Event()
    def heartbeat():
        ok_at=time.time(); wait=HEARTBEAT
        while not stop.wait(wait):
            try:
                post(BASE,{'action':'heartbeat','runId':run,'token':token}); ok_at=time.time(); wait=HEARTBEAT
            except urllib.error.HTTPError as e:
                # 409: the attempt is stale (cancelled, fenced or expired). Stop at once.
                if e.code==409: lost.set(); return
                wait=min(HEARTBEAT,5)
            except Exception: wait=min(HEARTBEAT,5)
            if time.time()-ok_at>LEASE_GRACE: lost.set(); return
    thread=threading.Thread(target=heartbeat,daemon=True);thread.start()
    deadline=time.time()+int(claim.get('operation_timeout_seconds') or 3600)
    current=None
    try:
        module=actor(claim.get('source_version','postgres-v1'))
        operation=claim['operation']; catalog=None
        if operation in ['check','discover']:
            messages=list(source(module,operation,config,stop=lost,deadline=deadline))
            if operation=='check' and not any(m.get('connectionStatus',{}).get('status')=='SUCCEEDED' for m in messages): raise ValueError('connector_failed')
            if operation=='discover': catalog=next(m['catalog']['streams'] for m in messages if m['type']=='CATALOG')
        else:
            command='replay' if operation=='replay' else 'read'
            for stream in claim['streams']:
                if stream['completed']: continue
                current=stream['id']
                dest={**BASE,'run_id':run,'lease_token':token,'stream_id':stream['id'],'sequence':stream['sequence'],'state':stream['state'],'schema':stream.get('schema')}
                for _ in write(dest,source(module,command,config,stream,stream['state'],stop=lost,deadline=deadline)):
                    if lost.is_set(): raise ValueError('timeout')
        post(BASE,{'action':'finish','runId':run,'token':token,'ok':True,'code':'ok',**({'catalog':catalog} if catalog is not None else {})})
    except Exception as e:
        code=str(e) if str(e) in SAFE else 'connector_failed'
        if getattr(e,'detail',None):
            try: post(BASE,{'action':'report','runId':run,'token':token,'code':code,'detail':e.detail,**({'streamId':current} if current else {})})
            except Exception: pass
        try: post(BASE,{'action':'finish','runId':run,'token':token,'ok':False,'code':code})
        except Exception: pass
    finally: stop.set();thread.join(timeout=2)
def main():
    while True:
        try:
            claim=post(BASE,{'action':'claim'}).get('claim')
            if claim: execute(claim)
        except Exception: print(json.dumps({'type':'LOG','log':{'level':'ERROR','code':'agent_poll_failed'}}),flush=True)
        if os.environ.get('CONNECT_ONCE')=='1': break
        time.sleep(10)
if __name__=='__main__':main()
