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
SOURCES={'postgres':'source_postgres.py','mysql':'source_mysql.py','kafka':'source_kafka.py','files':'source_files.py',
  # CDC (README, "Change data capture"): the database's change log, streams in sync mode `cdc`.
  'postgres-cdc':'source_postgres_cdc.py','mysql-cdc':'source_mysql_cdc.py',
  # Spark ingestion (deploy/connect/README.md, "Spark ingestion"): check and discover use the source's
  # own actor; sync, reset and backfill run as a Spark application (spark()).
  'spark-postgres':'source_postgres.py','spark-mysql':'source_mysql.py','spark-files':'source_files.py','spark-kafka':'source_kafka.py'}
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
def lagged(messages, run, token, stream):
    """Forwards the actor's partition_lag progress (sent after the checkpoint it follows has been
    committed). Lag is a metric: a failed report never fails the run."""
    for m in messages:
        if m['type']=='LOG' and m.get('log',{}).get('code')=='partition_lag':
            try: post(BASE,{'action':'lag','runId':run,'token':token,'streamId':stream,'partitions':m['log']['detail']})
            except Exception: pass
            continue
        yield m
def spark(claim, config_path, stop, deadline):
    """Runs spark_ingest.py through CONNECT_SPARK_SUBMIT (a spark-submit command line, client mode).
    The claim, the source config and the ingest token go on stdin. Like an actor, the application is
    killed as a process group when the lease is lost or the deadline passes."""
    import shlex
    command=os.environ.get('CONNECT_SPARK_SUBMIT')
    if not command: raise ActorError('connector_failed')
    with open(config_path) as f: config=json.load(f)
    if claim.get('source_version','').startswith('spark-kafka-'):
        # Executors cannot open this host's certificate files: they travel inline as PEM.
        import source_kafka
        config=source_kafka.portable(source_kafka.resolve_files(config,os.path.dirname(os.path.abspath(config_path))))
    # Local development: the source as Spark containers see it (e.g. host.docker.internal).
    config.update(json.loads(os.environ.get('CONNECT_SPARK_CONFIG_OVERRIDES') or '{}'))
    with open(BASE['token_file']) as f: ingest=f.read().strip()
    job={'meridian_url':os.environ.get('CONNECT_SPARK_MERIDIAN_URL') or BASE['url'],'ingest_token':ingest,'claim':claim,'config':config,
         'options':json.loads(os.environ.get('CONNECT_SPARK_OPTIONS') or '{}')}
    group={'creationflags':subprocess.CREATE_NEW_PROCESS_GROUP} if os.name=='nt' else {'start_new_session':True}
    proc=subprocess.Popen(shlex.split(command),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True,**group)
    fired=threading.Event(); done=threading.Event()
    def watchdog():
        while not done.wait(0.5):
            if stop.is_set() or time.time()>=deadline: fired.set(); kill_group(proc); return
    threading.Thread(target=watchdog,daemon=True).start()
    code='connector_failed'; detail=None
    try:
        proc.stdin.write(json.dumps(job)); proc.stdin.close()
        for line in proc.stdout:
            try: message=json.loads(line)
            except ValueError: continue
            if isinstance(message,dict) and message.get('type')=='TRACE':
                trace=message.get('trace',{}); code=trace.get('code',code); detail=trace.get('detail')
            # Operator warnings and errors (e.g. spark_insecure), as for actors.
            elif isinstance(message,dict) and message.get('type')=='LOG' and message.get('log',{}).get('level') in ('WARN','ERROR'):
                print(json.dumps(message),flush=True)
        if fired.is_set(): raise ActorError('timeout')
        if proc.wait()!=0: raise ActorError(code if code in SAFE else 'connector_failed',detail if code in REPORTED else None)
    finally:
        done.set(); kill_group(proc)
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
        elif claim.get('task') and len(claim['streams'])==1 and claim['streams'][0].get('splittable'):
            # The first task of a partitioned stream plans: one task per partition replaces it, and
            # the agent (and others) then claim those. The split ends this task's lease.
            stream=claim['streams'][0]
            messages=list(source(module,'plan',config,stream,stop=lost,deadline=deadline))
            shards=next(m['plan']['shards'] for m in messages if m['type']=='PLAN')
            post(BASE,{'action':'split','runId':run,'token':token,'shards':shards})
            return
        elif claim.get('source_version','').startswith('spark-'):
            if operation not in ('sync','reset','backfill'): raise ValueError('connector_failed')
            spark(claim,config,lost,deadline)
        else:
            command='replay' if operation=='replay' else 'read'
            for stream in claim['streams']:
                if stream['completed']: continue
                current=stream['id']
                dest={**BASE,'run_id':run,'lease_token':token,'stream_id':stream['id'],'sequence':stream['sequence'],'state':stream['state'],'schema':stream.get('schema')}
                for _ in write(dest,lagged(source(module,command,config,stream,stream['state'],stop=lost,deadline=deadline),run,token,stream['id'])):
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
# Task claims hand out one stream of a run at a time, so slots (and other agents) work on a run's
# streams in parallel. A server without task claims answers 400; the agent then claims whole runs.
TASKS={'enabled':os.environ.get('CONNECT_TASKS','1')!='0'}
def claim_once(worker):
    if TASKS['enabled']:
        try: return post(BASE,{'action':'claim_task','worker':worker}).get('claim')
        except urllib.error.HTTPError as e:
            if e.code!=400: raise
            TASKS['enabled']=False
    return post(BASE,{'action':'claim'}).get('claim')
def loop(worker):
    while True:
        claimed=False
        try:
            claim=claim_once(worker)
            if claim: claimed=True; execute(claim)
        except Exception: print(json.dumps({'type':'LOG','log':{'level':'ERROR','code':'agent_poll_failed'}}),flush=True)
        # CONNECT_ONCE: drain the work that is claimable now (a split run needs several claims), then exit.
        if os.environ.get('CONNECT_ONCE')=='1':
            if not claimed: break
            continue
        # Claim again at once after finishing work; wait only when there was nothing to do.
        if not claimed: time.sleep(10)
def main():
    import socket,re
    slots=max(1,min(64,int(os.environ.get('CONNECT_SLOTS','1'))))
    base=re.sub(r'[^\w.:-]','-',os.environ.get('CONNECT_WORKER_ID') or f'{socket.gethostname()}-{os.getpid()}')[:180]
    threads=[threading.Thread(target=loop,args=(f'{base}:{i}',),daemon=True) for i in range(slots)]
    for t in threads: t.start()
    for t in threads: t.join()
if __name__=='__main__':main()
