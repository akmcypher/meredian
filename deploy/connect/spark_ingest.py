"""Ingestion on Spark with direct writes to Bronze (deploy/connect/README.md, "Spark ingestion").

Started by the agent as a Spark application (client mode), with the claim, the source config and the
agent's ingest token handed over privately (spark-submit.sh), never in arguments. The driver holds the
task's lease (the agent keeps heartbeating it) and runs one Spark job per hash partition of the primary
key. Executors read their partition, encode rows exactly as the source's actor does, and upload Bronze
parts (at most 8 MiB / 10,000 rows) through signed URLs bound to this attempt. The driver commits each
part through the fenced agent API, in order, with a checkpoint that lets a new attempt resume every
partition by position.

Object-store files (S3) run one task per planned range of source_files.plan; see main_files.

Sources: PostgreSQL (one exported snapshot shared by every partition) and MySQL (each partition reads
in its own consistent snapshot; MySQL cannot share one across connections). Sync modes:
- full_refresh: every row, partitions ordered by primary key.
- incremental: the window after the stream's committed (cursor, key) position, up to the highest
  (cursor, key) at the start of the run. The window is fixed in the checkpoint, so a resumed attempt
  reads the same window; completion moves the stream's position to the window's end, the same
  position a single-worker run reading the same rows would reach.

Exit status: 0 when every partition is committed; otherwise non-zero with a safe code on the last
line (the agent reports it). Nothing is retried here: attempts and backoff belong to the control plane.
"""
import hashlib, json, os, sys, threading, time, urllib.error, urllib.request
from concurrent.futures import ThreadPoolExecutor
PART_BYTES=7*1024*1024  # landing batches are capped at 8 MiB; keep headroom for the last line
PART_ROWS=10000
FETCH=1000
class Stop(Exception):
    # detail: positions, identities and counts only (never record data), reported by the agent.
    def __init__(self, code, detail=None): super().__init__(code); self.code=code; self.detail=detail
# Source credentials, the ingest token and the lease token travel to executors in task closures
# over Spark RPC, and record data comes back the same way. Spark leaves RPC unauthenticated and in
# clear text by default, so the driver refuses to run a task unless the cluster requires the
# shared secret and encrypts both the network and local spill files (S5).
SECURE={'spark.authenticate':'true','spark.network.crypto.enabled':'true','spark.io.encryption.enabled':'true'}
def insecure_settings(get):
    """The required settings that are off, given get(key) -> configured value or None."""
    return sorted(k for k,v in SECURE.items() if str(get(k) or 'false').strip().lower()!=v)
def log(level, code, **fields):
    print(json.dumps({'type':'LOG','log':{'level':level,'code':code,**fields}}),flush=True)
def require_secure(conf, options):
    missing=insecure_settings(lambda k: conf.get(k,None))
    if not missing: return
    # Local development only: an explicit option, and a warning the agent passes on.
    if options.get('allow_insecure_spark') is True:
        log('WARN','spark_insecure_allowed',settings=missing); return
    log('ERROR','spark_insecure',settings=missing)
    raise Stop('connector_failed')
class Postgres:
    consistency='single'  # every partition reads the driver's exported snapshot
    shared_snapshot=True
    def __init__(self, config): self.config=config
    def connect(self):
        import psycopg
        from psycopg.rows import dict_row
        return psycopg.connect(**self.config,row_factory=dict_row,connect_timeout=10,autocommit=True)
    @staticmethod
    def ident(name): return '"'+name.replace('"','""')+'"'
    def bucket(self, keys, n):
        # bigint before abs(): abs() of the smallest int4 hash would overflow.
        return f'mod(abs(hashtext(row({",".join(map(self.ident,keys))})::text)::bigint),{int(n)})'
    def discover(self, conn, stream):
        import source_postgres
        conn.execute("SET statement_timeout='60s'")
        found=next((s for s in source_postgres.discover(conn) if s['namespace']==stream['namespace'] and s['name']==stream['name']),None)
        conn.execute('SET statement_timeout=0')
        return found
    def schema_hash(self, found):
        import source_postgres
        return source_postgres.schema_hash(found)
    def begin(self, conn, snapshot):
        from psycopg import sql
        conn.execute('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
        if snapshot: conn.execute(sql.SQL('SET TRANSACTION SNAPSHOT {}').format(sql.Literal(snapshot)))
    def export(self, conn): return conn.execute('SELECT pg_export_snapshot() AS s').fetchone()['s']
    def rows(self, conn, query, args, name):
        with conn.cursor(name=name) as cur:
            cur.itersize=FETCH; cur.execute(query,args)
            yield from cur
    def end(self, conn): conn.execute('ROLLBACK')
    def encode(self, row, columns):
        from sdk import to_json
        return to_json(row)
class MySQL:
    consistency='partition'  # each partition is consistent in itself
    shared_snapshot=False
    def __init__(self, config): self.config=config
    def connect(self):
        import source_mysql
        conn=source_mysql.connection(self.config)
        # One query streams a whole partition: no statement limit, and room for part uploads
        # between reads.
        with conn.cursor() as c: c.execute('SET SESSION max_execution_time=0, SESSION net_write_timeout=600')
        return conn
    @staticmethod
    def ident(name):
        import source_mysql
        return source_mysql.quote(name)
    def bucket(self, keys, n): return f'MOD(CRC32(CONCAT_WS(CHAR(31),{",".join(map(self.ident,keys))})),{int(n)})'
    def discover(self, conn, stream):
        import source_mysql
        return next((s for s in source_mysql.discover(conn,stream['namespace']) if s['name']==stream['name']),None)
    def schema_hash(self, found):
        import source_mysql
        return source_mysql.schema_hash(found)
    def begin(self, conn, snapshot):
        with conn.cursor() as c: c.execute('START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY')
    def export(self, conn): return None
    def rows(self, conn, query, args, name):
        import pymysql.cursors
        cur=conn.cursor(pymysql.cursors.SSDictCursor)
        try:
            cur.execute(query,args)
            yield from cur
        finally: cur.close()
    def end(self, conn): conn.rollback()
    def encode(self, row, columns):
        import source_mysql
        return source_mysql.typed(row,columns)
SOURCES={'postgres':Postgres,'mysql':MySQL}
def api(job, payload):
    """POST to the agent API. A 409 means the lease or the request was refused: stop, never retry."""
    req=urllib.request.Request(job['meridian_url'].rstrip('/')+'/api/public/connect-agent',data=json.dumps(payload).encode(),
        headers={'Content-Type':'application/json','x-ingest-token':job['ingest_token']})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req,timeout=120) as r: return json.load(r)
        except urllib.error.HTTPError as e:
            # The control plane names refusals a retry cannot change (dead_letter_budget).
            try: refused=json.load(e).get('error')
            except Exception: refused=None
            if refused=='dead_letter_budget': raise Stop('dead_letter_budget') from None
            if e.code in (400,401,403,409,413): raise Stop('destination_failed') from None
            if attempt==2: raise Stop('destination_failed') from None
        except (urllib.error.URLError,TimeoutError,ConnectionError):
            if attempt==2: raise Stop('destination_failed') from None
        time.sleep(2**attempt)
def rewrite(job, url):
    """Local development only: a container reaches the host's services through another name."""
    for old,new in job.get('options',{}).get('rewrite',[]): url=url.replace(old,new,1)
    return url
def shape(job):
    """Key columns and sort order. Full refresh orders by key; incremental by (cursor, key), so a
    position is the key (full) or [cursor, key] (incremental), as the actors checkpoint them."""
    from sdk import key_columns
    stream=job['claim']['streams'][0]; keys=key_columns(stream)
    return keys,(keys if job['full'] else [stream['cursor_field'],*keys])
def position(job, row):
    from sdk import to_json, key_value
    stream=job['claim']['streams'][0]; keys,_=shape(job); pk=to_json(key_value(row,keys))
    return pk if job['full'] else [str(row[stream['cursor_field']]),pk]
def values(job, pos):
    """A position as the values compared against the sort order."""
    keys,_=shape(job)
    pk=pos if job['full'] else pos[1]
    pk=list(pk) if len(keys)>1 else [pk]
    return pk if job['full'] else [pos[0],*pk]
def select(src, job, bounds, partition=None, desc=False, limit=None):
    stream=job['claim']['streams'][0]; keys,order=shape(job)
    cols=','.join(map(src.ident,order)); where=[]; args=[]
    if partition is not None: where.append(f'{src.bucket(keys,job["partitions"])}={int(partition)}')
    for op,pos in bounds:
        vals=values(job,pos); where.append(f'({cols}){op}({",".join(["%s"]*len(vals))})'); args+=vals
    q=f'SELECT * FROM {src.ident(stream["namespace"])}.{src.ident(stream["name"])}'
    if where: q+=' WHERE '+' AND '.join(where)
    q+=' ORDER BY '+','.join(src.ident(c)+(' DESC' if desc else '') for c in order)
    if limit: q+=f' LIMIT {int(limit)}'
    return q,args
def window_bounds(job):
    w=job['window']; b=[]
    if w['after'] is not None: b.append(('>',w['after']))
    if w['until'] is not None: b.append(('<=',w['until']))
    return b
class Failed(Exception):
    """An executor failure with a safe code. Spark wraps executor exceptions, so the code travels in
    the message as `meridian:<code>` and the driver reads it back (code_of)."""
    def __init__(self, code): super().__init__(f'meridian:{code}')
SAFE_CODES=('schema_drift','source_changed','connector_failed','auth_failed','destination_failed','record_too_large','offset_out_of_range')
def code_of(e):
    text=str(e)
    return next((c for c in SAFE_CODES if f'meridian:{c}' in text),None)
DEAD_ITEMS=1000
DEAD_BYTES=4*1024*1024
def upload(job, i, items):
    """Executor: uploads `items` in order as Bronze parts of task i, at most PART_BYTES / PART_ROWS
    each, through one signed URL per part. An item is ('record', data, cursor, primary_key,
    position, operation) or ('dead', DEAD_LETTER message, position); dead letters travel with the
    part committed after them (at most DEAD_ITEMS / DEAD_BYTES per part)."""
    import socket
    claim=job['claim']; stream=claim['streams'][0]
    executor=f"{socket.gethostname()}:{os.getpid()}"
    out=[]; buf=[]; size=0; last=None; run={}; urls={}; dead=[]; dead_bytes=0; envs=[]
    # Parquet parts when the connection opts in (S6); a part whose records can't be laid out as
    # columns falls back to NDJSON (bronze_parquet.encode). Size limits apply to the NDJSON size.
    want=(job.get('options') or {}).get('bronze_format') or 'ndjson'
    def fetch(k,fmt):
        # One signed URL per part, for the part's format (the object key depends on it). The first
        # response also carries the run's connection id and extraction time, which every envelope needs.
        r=api(job,{'action':'upload_url','runId':claim['run_id'],'token':claim['lease_token'],'streamId':stream['id'],'part':f'{i}-{k}','format':fmt})
        run.update(connection_id=r['connection_id'],extracted_at=r['extracted_at']); urls[k]=(fmt,r['url'])
    def flush():
        nonlocal buf,size,dead,dead_bytes,envs
        k=len(out)
        body=None; fmt='ndjson'
        if want=='parquet' and envs:
            import bronze_parquet
            body=bronze_parquet.encode(envs)
            if body is not None: fmt='parquet'
        if body is None: body=''.join(buf).encode()
        if urls.get(k,(None,))[0]!=fmt: fetch(k,fmt)
        kind='application/vnd.apache.parquet' if fmt=='parquet' else 'application/x-ndjson'
        req=urllib.request.Request(rewrite(job,urls.pop(k)[1]),data=body,method='PUT',headers={'Content-Type':kind,'x-upsert':'false'})
        with urllib.request.urlopen(req,timeout=300) as r: r.read()
        part={'part':f'{i}-{k}','format':fmt,'rows':len(buf),'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'after':last,'executor':executor}
        if dead: part['dead_letters']=dead
        out.append(part)
        buf=[]; size=0; dead=[]; dead_bytes=0; envs=[]
    for item in items:
        if not run: fetch(0,want)
        if item[0]=='dead':
            n=len(json.dumps(item[1]))
            if dead and (len(dead)>=DEAD_ITEMS or dead_bytes+n>DEAD_BYTES): flush()
            dead.append(item[1]); dead_bytes+=n; last=item[2]
            continue
        _,data,cursor,pk,pos,operation=item
        env={'protocol':'meridian-connect/1','connection_id':run['connection_id'],'run_id':claim['run_id'],'stream_id':stream['id'],
            'schema':stream['schema'],'extracted_at':run['extracted_at'],'cursor':cursor,'primary_key':pk,'operation':operation,
            'source_namespace':stream['namespace'],'source_stream':stream['name'],'data':data}
        line=json.dumps(env,separators=(',',':'),ensure_ascii=False)+'\n'
        n=len(line.encode())
        if (buf or dead) and (size+n>PART_BYTES or len(buf)>=PART_ROWS): flush()
        buf.append(line); size+=n; last=pos
        if want=='parquet': envs.append(env)
    if buf or dead: flush()
    return out
def load_partition(job, i, after, snapshot):
    """Executor: reads hash partition i of the window after position `after`; uploads parts."""
    from sdk import to_json, key_value
    stream=job['claim']['streams'][0]; keys,_=shape(job); cursor=stream['cursor_field']
    src=SOURCES[job['source']](job['config'])
    q,args=select(src,job,window_bounds(job)+([('>',after)] if after is not None else []),partition=i)
    conn=src.connect()
    try:
        src.begin(conn,snapshot)
        out=upload(job,i,(('record',src.encode(row,job['columns']),str(row[cursor]),to_json(key_value(row,keys)),position(job,row),'upsert')
                          for row in src.rows(conn,q,args,f'meridian_part_{i}')))
        src.end(conn)
    finally: conn.close()
    return out
def load_range(job, i, after):
    """Executor: reads planned file range i after record position `after`; uploads parts."""
    import source_files as sf
    stream=job['claim']['streams'][0]
    try:
        fs_=sf.filesystem(job['config'])
        def records(snapshot=None):
            for data,pos in sf.read_range(fs_,job['config'],stream['schema'],job['plan'][i],snapshot=snapshot):
                if after is None or pos>after: yield 'record',data,data['_modified'],[data['_file'],pos],pos,'upsert'
        if 'file_digests' in job:
            rng=job['plan'][i]; _,bucket,_=sf.settings(job['config'])
            path=f'{bucket}/{rng[0]}'; info=fs_.get_file_info(path)
            with sf.object_snapshot(fs_,path,rng[1],sf.modified(info.mtime)) as (snapshot,digest):
                if digest!=job['file_digests'][rng[0]]: raise sf.Changed()
                return upload(job,i,records(snapshot))
        return upload(job,i,records())
    except sf.Changed: raise Failed('source_changed') from None
    except ValueError as e:
        if str(e) in ('schema_drift','connector_failed'): raise Failed(str(e)) from None
        raise
def drive(job, done, checkpoint, load):
    """Driver: runs one Spark job per unfinished task (`done` maps task -> {after, done}), commits each
    returned part in order through the fenced agent API, and commits completion. `load(i, after)` runs
    on an executor; `checkpoint(completed)` builds the STATE committed with each part."""
    claim=job['claim']; stream=claim['streams'][0]
    lock=threading.Lock(); seq={'n':int(stream['sequence'])}; committed={'n':0,'complete':False}
    # The first failure stops every further commit; tasks in flight finish reading but commit
    # nothing, and the next attempt resumes them by position.
    aborted=threading.Event()
    fail_after=job.get('options',{}).get('fail_after_commits')
    def commit(i, part, last_of_task):
        with lock:
            if aborted.is_set(): raise Stop('connector_failed')
            previous=done[str(i)]
            try:
                done[str(i)]={'after':part['after'],'done':last_of_task}
                complete=all(v['done'] for v in done.values())
                r=api(job,{'action':'commit_file','runId':claim['run_id'],'token':claim['lease_token'],'streamId':stream['id'],'sequence':seq['n'],
                    'part':part['part'],'format':part.get('format','ndjson'),'rows':part['rows'],'bytes':part['bytes'],'sha256':part['sha256'],'executor':part['executor'],'state':checkpoint(complete),
                    **({'dead_letters':part['dead_letters']} if part.get('dead_letters') else {})})
                seq['n']=r['sequence']; committed['n']+=1; committed['complete']=complete
            except Exception:
                done[str(i)]=previous
                aborted.set()
                raise
            if fail_after is not None and committed['n']>=int(fail_after): aborted.set(); raise Stop('connector_failed')
    todo=[int(i) for i,v in sorted(done.items(),key=lambda kv:int(kv[0])) if not v['done']]
    if todo:
        from pyspark.sql import SparkSession
        spark=SparkSession.builder.appName(f"meridian-ingest-{claim['run_id'][:8]}").getOrCreate()
        sc=spark.sparkContext
        # Before any closure (and the credentials in it) leaves the driver.
        require_secure(sc.getConf(),job.get('options') or {})
        def run_task(i):
            if aborted.is_set(): return
            after=done[str(i)]['after']
            try: result=sc.parallelize([i],1).map(lambda x: load(x,after)).collect()[0]
            except Exception as e:
                code=code_of(e)
                if code: raise Stop(code) from None
                raise
            # A loader returns its parts, or {'parts', 'after'} when the task's final position can
            # lie past its last part (Kafka: compacted or aborted offsets at the end of a chunk).
            parts,final=(result['parts'],result['after']) if isinstance(result,dict) else (result,None)
            for k,part in enumerate(parts): commit(i,part,k==len(parts)-1)
            with lock:
                if not parts: done[str(i)]={'after':final if final is not None else after,'done':True}
                elif final is not None: done[str(i)]={'after':final,'done':True}
        try:
            with ThreadPoolExecutor(max_workers=max(1,min(len(todo),sc.defaultParallelism))) as pool:
                futures=[pool.submit(run_task,i) for i in todo]
                errors=[]
                for f in futures:
                    try: f.result()
                    except Exception as e: aborted.set(); errors.append(e)
                if errors: raise next((e for e in errors if isinstance(e,Stop) and e.code!='connector_failed'),errors[0])
        finally: spark.stop()
    # Every task is done; if the last ones were empty, nothing has committed completion yet.
    with lock:
        if not all(v['done'] for v in done.values()): raise Stop('connector_failed')
        last_state=checkpoint(True)
    if not committed['complete']:
        body=b''; name=f'{len(done)}-0'
        meta=api(job,{'action':'upload_url','runId':claim['run_id'],'token':claim['lease_token'],'streamId':stream['id'],'part':name})
        req=urllib.request.Request(rewrite(job,meta['url']),data=body,method='PUT',headers={'Content-Type':'application/x-ndjson','x-upsert':'false'})
        with urllib.request.urlopen(req,timeout=60) as r: r.read()
        api(job,{'action':'commit_file','runId':claim['run_id'],'token':claim['lease_token'],'streamId':stream['id'],'sequence':seq['n'],
            'part':name,'rows':0,'bytes':0,'sha256':hashlib.sha256(body).hexdigest(),'executor':'driver','state':last_state})
def main_database(job, kind, stream, st, progress):
    from sdk import key_columns
    claim=job['claim']
    job['source']=kind; job['full']=stream['sync_mode']=='full_refresh'
    keys=key_columns(stream)
    n=progress['partitions'] if progress else int(job.get('options',{}).get('partitions') or 8)
    if not 1<=n<=4096: raise Stop('connector_failed')
    job['partitions']=n
    src=SOURCES[kind](job['config'])
    conn=src.connect()
    try:
        found=src.discover(conn,stream)
        if not found or found['json_schema']!=stream['schema']: raise Stop('schema_drift')
        cols=found['json_schema']['properties']
        if stream['cursor_field'] not in cols or cols[stream['cursor_field']]['nullable'] or any(k not in cols or cols[k]['nullable'] for k in keys): raise Stop('schema_drift')
        job['columns']=cols
        src.begin(conn,None)
        snapshot=src.export(conn)
        if progress: window=progress['window']
        else:
            lower=None if job['full'] or st.get('primary_key') is None else [st.get('cursor'),st['primary_key']]
            until=None
            if not job['full']:
                job['window']={'after':lower,'until':None}
                q,args=select(src,job,window_bounds(job),desc=True,limit=1)
                top=list(src.rows(conn,q,args,'meridian_window'))
                until=position(job,top[0]) if top else None
            window={'after':lower,'until':until}
        job['window']=window
        # Without a shared snapshot the driver needs no open transaction (it would only hold back
        # the source's purge for the whole run).
        if not src.shared_snapshot: src.end(conn)
        empty=not job['full'] and window['until'] is None
        done={str(i):{'after':None,'done':empty} for i in range(n)}
        if progress: done.update(progress['progress'])
        consistency='mixed' if progress else src.consistency
        def checkpoint(completed):
            end=window['until'] if completed and window['until'] is not None else window['after']
            cursor,pk=(end[0],end[1]) if end is not None and not job['full'] else (None,None)
            return {'type':'STATE','state':{'version':1,'cursor':cursor,'primary_key':pk,'completed':completed,'schema_hash':src.schema_hash(found),
                'spark':{'run':claim['run_id'],'partitions':n,'consistency':consistency,'window':window,'progress':done}}}
        drive(job,done,checkpoint,lambda i,after: load_partition(job,i,after,snapshot))
    finally: conn.close()
def main_files(job, stream, st, progress):
    """Object-store files: one task per planned range (source_files.plan). The plan is stored in the
    checkpoint, so a resumed attempt reads the same ranges of the same object versions (every range
    re-checks its object's size and modification time: `source_changed` otherwise)."""
    import source_files as sf
    claim=job['claim']
    if stream['sync_mode']=='incremental':
        from spark_files_incremental import prepare
        try:
            work=prepare(sf.filesystem(job['config']),job['config'],stream,st,claim['run_id'],
                         int(job.get('options',{}).get('range_bytes') or job['config'].get('range_bytes') or sf.RANGE_BYTES))
            job['plan']=work.plan; job['file_digests']=work.digests
            drive(job,work.done,work.checkpoint,lambda i,after: load_range(job,i,after))
        except sf.Changed: raise Stop('source_changed') from None
        except ValueError as e:
            if str(e) in ('schema_drift','connector_failed','record_too_large'): raise Stop(str(e)) from None
            raise
        return
    if stream['sync_mode']!='full_refresh': raise Stop('connector_failed')
    source=sf.source_identity(job['config'],stream['schema'])
    if progress:
        saved=progress.get('source')
        # Historical S3 checkpoints predate source binding. ADLS never had that contract.
        if saved!=source and (saved is not None or job['config'].get('provider','s3')!='s3'):
            raise Stop('source_changed')
    fs_=sf.filesystem(job['config'])
    found=sf.discover(fs_,job['config'])[0]
    if found['json_schema']!=stream['schema']: raise Stop('schema_drift')
    if progress: plan=progress['plan']
    else: plan=sf.plan(fs_,job['config'],int(job.get('options',{}).get('range_bytes') or job['config'].get('range_bytes') or sf.RANGE_BYTES))
    if len(plan)>4096: raise Stop('connector_failed')
    job['plan']=plan
    done={str(i):{'after':None,'done':False} for i in range(len(plan))} or {'0':{'after':None,'done':True}}
    if progress: done.update(progress['progress'])
    def checkpoint(completed):
        return {'type':'STATE','state':{'version':1,'cursor':None,'primary_key':None,'completed':completed,
            'spark':{'source':source,'run':claim['run_id'],'partitions':len(done),'consistency':'single','window':{'after':None,'until':None},'plan':plan,'progress':done}}}
    drive(job,done,checkpoint,lambda i,after: load_range(job,i,after))
CHUNK_RECORDS=100000
def load_chunk(job, i, after):
    """Executor: reads planned Kafka chunk i ([partition, first, end) offsets) from `after` (the next
    offset to read) as the Kafka actor converts records; uploads parts with their dead letters."""
    import source_kafka as sk
    stream=job['claim']['streams'][0]
    p,a,b=job['plan'][i]
    start=int(after) if after is not None else int(a)
    reached={'cluster':job['kafka']['cluster'],'topic_id':job['kafka']['topic_id']}
    def items():
        for message,o in sk.chunk(job['config'],stream,int(p),start,int(b),reached):
            if message['type']=='RECORD':
                r=message['record']
                yield ('record',r['data'],r['cursor'],r['primary_key'],str(o+1),r.get('operation') or 'upsert')
            else: yield ('dead',message,str(o+1))
    try: parts=upload(job,i,items())
    except sk.Stop as e: raise Failed(e.code) from None
    except ValueError as e:
        if str(e) in ('schema_drift','connector_failed'): raise Failed(str(e)) from None
        raise
    # The chunk's position may move past its last record (compacted or aborted offsets), never
    # past what the log returned.
    return {'parts':parts,'after':str(reached['offset'])}
def kafka_plan(starts, highs, size, max_records):
    """[partition, first, end] chunks of at most `size` offsets, interleaved across partitions
    (fair under max_run_records, which cuts the window), at most 4,096 chunks."""
    total=sum(max(0,highs[p]-starts[p]) for p in starts)
    if total and -(-total//size)>4096: size=-(-total//4096)
    per={p:[[p,a,min(a+size,highs[p])] for a in range(starts[p],highs[p],size)] for p in sorted(starts)}
    out=[]; k=0; budget=max_records
    while any(k<len(v) for v in per.values()):
        for p in sorted(per):
            if k<len(per[p]):
                c=per[p][k]
                if budget is not None:
                    if budget<=0: return out
                    if c[2]-c[1]>budget: c=[c[0],c[1],c[1]+budget]
                    budget-=c[2]-c[1]
                out.append(c)
        k+=1
    return out
def main_kafka(job, stream, st, progress):
    """Kafka: the run reads, per partition, from the committed position up to the high watermark
    captured when the run first started, as offset chunks stored in the checkpoint. Positions are
    committed per partition only across contiguous finished chunks, so a stream is never ahead of
    what has landed; identity, positions and retention gaps are validated by the actor's own
    prepare() on every attempt."""
    import source_kafka as sk
    claim=job['claim']
    topic=stream['schema'].get('x-kafka-topic')
    if not topic or sk.stream_name(topic)!=stream['name'] or stream.get('shard'): raise Stop('connector_failed')
    config=sk.apply_profile(job['config']); job['config']=config
    c=sk.consumer(config)
    try: cluster,topic_id,positions,low,high,accepted=sk.prepare(config,stream,st,c)
    except sk.Stop as e: raise Stop(e.code,e.detail) from None
    finally: c.close()
    job['kafka']={'cluster':cluster,'topic_id':topic_id}
    if progress:
        plan=progress['plan']; starts={int(p):int(o) for p,o in progress['window']['after'].items()}; highs={int(p):int(o) for p,o in progress['window']['until'].items()}
    else:
        starts={p:max(positions.get(p,low[p]),low[p]) for p in low}; highs=dict(high)
        plan=kafka_plan(starts,highs,int(job.get('options',{}).get('chunk_records') or CHUNK_RECORDS),sk.limits(config)['max_run_records'])
        # A window cut by max_run_records ends where its last chunk ends.
        for p in highs:
            ends=[ch[2] for ch in plan if ch[0]==p]
            highs[p]=max(ends) if ends else starts[p]
    job['plan']=plan
    done={str(i):{'after':None,'done':False} for i in range(len(plan))} or {'0':{'after':None,'done':True}}
    if progress: done.update(progress['progress'])
    window={'after':{str(p):str(o) for p,o in starts.items()},'until':{str(p):str(o) for p,o in highs.items()}}
    def positions_now():
        out={}
        for p in starts:
            cur=starts[p]
            for i,(q,a,b) in enumerate(plan):
                if q!=p: continue
                d=done.get(str(i))
                if d['after'] is not None: cur=max(cur,int(d['after']))
                if not d['done'] or cur<b: break
            out[p]=cur
        return out
    def checkpoint(completed):
        s=sk.state(positions=positions_now(),completed=completed,gaps=accepted or None,source={k:v for k,v in (('cluster_id',cluster),('topic_id',topic_id)) if v})['state']
        s['spark']={'run':claim['run_id'],'partitions':len(done),'consistency':'single','window':window,'plan':plan,'progress':done}
        return {'type':'STATE','state':s}
    drive(job,done,checkpoint,lambda i,after: load_chunk(job,i,after))
    # Lag metric (best effort): committed position and this run's end per partition.
    try:
        final=positions_now()
        api(job,{'action':'lag','runId':claim['run_id'],'token':claim['lease_token'],'streamId':stream['id'],
                 'partitions':[{'partition':str(p),'position':str(final[p]),'end':str(high.get(p,highs[p]))} for p in sorted(final)][:1024]})
    except Stop: pass
def main():
    # The launcher (spark-submit.sh) hands the job over as a private file; read it and delete it.
    if '--job-file' in sys.argv:
        path=sys.argv[sys.argv.index('--job-file')+1]
        with open(path) as f: job=json.load(f)
        os.unlink(path)
    else: job=json.loads(sys.stdin.read())
    claim=job['claim']
    # The Spark image runs Python 3.8 (no str.removeprefix).
    kind=claim.get('source_version','').rsplit('-v',1)[0]
    kind=kind[len('spark-'):] if kind.startswith('spark-') else ''
    if kind not in SOURCES and kind not in ('files','kafka'): raise Stop('connector_failed')
    if len(claim.get('streams') or [])!=1: raise Stop('connector_failed')
    stream=claim['streams'][0]
    if kind=='files' and claim.get('operation') in ('reset','backfill'):
        stream={**stream,'sync_mode':stream.get('configured_sync_mode',stream['sync_mode'])}
        claim['streams']=[stream]
    if stream.get('sync_mode') not in ('full_refresh','incremental'): raise Stop('connector_failed')
    if kind=='kafka' and stream['sync_mode']!='incremental': raise Stop('connector_failed')
    if (job.get('options') or {}).get('bronze_format',None) not in (None,'ndjson','parquet'): raise Stop('connector_failed')
    # Already committed in full (an earlier attempt failed after its last commit): nothing to load.
    if stream.get('completed'): return
    st=stream.get('state') or {}
    # Progress of this run's earlier attempt; an incremental stream also carries the previous
    # run's progress, which is not a resume.
    progress=st.get('spark') if (st.get('spark') or {}).get('run')==claim['run_id'] else None
    if kind=='files': main_files(job,stream,st,progress)
    elif kind=='kafka': main_kafka(job,stream,st,progress)
    else: main_database(job,kind,stream,st,progress)
if __name__=='__main__':
    try: main()
    except Stop as e:
        print(json.dumps({'type':'TRACE','trace':{'type':'ERROR','code':e.code,**({'detail':e.detail} if e.detail else {})}}),flush=True); sys.exit(1)
    except Exception as e:
        code=code_of(e) or ('schema_drift' if str(e)=='schema_drift' else 'source_unavailable')
        print(json.dumps({'type':'TRACE','trace':{'type':'ERROR','code':code}}),flush=True); sys.exit(1)
