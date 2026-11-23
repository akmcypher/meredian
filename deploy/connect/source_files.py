"""Meridian Connect object-store files actor (deploy/connect/README.md, "Spark ingestion").

Reads CSV, TSV, JSON Lines, Parquet and Avro OCF objects in ADLS Gen2 or an S3-compatible bucket (AWS S3,
GCS through its S3 interoperability endpoint, MinIO, R2, Supabase Storage). Credentials come from the
connection's credential file on the worker host, never from the control plane.

A file is planned into ranges that can be read independently:
- CSV/TSV/JSONL: byte ranges. A range owns the records that *start* inside it (the reader skips to
  the first record start after the range's first byte and finishes the record that crosses its end),
  so the ranges of a file read every record exactly once. CSV whose quoted fields may contain newlines
  (`csv_quoted_newlines: true`) cannot be split this way: each such file is one range.
- Parquet: groups of row groups. Avro: groups of object-container blocks.

Each record carries `_file`, `_position` (byte offset of the record, or the global row index for
Parquet, Avro and for unsplittable CSV) and `_modified` (the object's modification time). (`_file`,
`_position`) is the stream key, identical however the file is split; `_modified` is the cursor.

The single-worker `read` below and the Spark executors (spark_ingest.py) use the same planning and
parsing functions, so both land the same records.

Single-worker incremental reads keep a bounded SHA-256 object ledger in the fenced checkpoint.
Known keys must retain their bytes; a replacement requires an explicit reset. Missing completed
objects retain their identities and Bronze history. See README.md, "Incremental object-store files".
"""
import csv, datetime, hashlib, io, json, sys, tempfile, time
from contextlib import contextmanager
from sdk import arguments, load, emit, record, state, to_json
VERSION='files-v1'
FORMATS=('csv','tsv','jsonl','parquet','avro')
EXTENSIONS={'avro':('.avro',),'csv':('.csv',),'tsv':('.tsv','.tab'),'jsonl':('.jsonl','.ndjson','.json'),'parquet':('.parquet',)}
META={'_file':{'source_type':'string','nullable':False},'_position':{'source_type':'bigint','nullable':False},
      '_modified':{'source_type':'timestamp with time zone','nullable':False}}
KEYS=['_file','_position']; CURSOR='_modified'
RANGE_BYTES=128*1024*1024
READ=1024*1024
SAMPLE=100
LEDGER_BYTES=48*1024
class Changed(Exception):
    """The source identity or an object's committed/planned content changed."""
@contextmanager
def object_snapshot(fs_, path, size, mod):
    """Hash and parse the same bounded-disk bytes; discard them on every exit."""
    import pyarrow as pa
    with tempfile.TemporaryDirectory(prefix='meridian-files-') as tmp:
        local=f'{tmp}/object'; digest=hashlib.sha256(); copied=0
        with fs_.open_input_file(path) as src, open(local,'wb') as dest:
            while True:
                chunk=src.read(READ)
                if not chunk: break
                dest.write(chunk); digest.update(chunk); copied+=len(chunk)
        info=fs_.get_file_info(path)
        if copied!=size or info.size!=size or modified(info.mtime)!=mod: raise Changed()
        with pa.OSFile(local,'rb') as snapshot:
            yield snapshot,digest.hexdigest()

def filesystem(config):
    from pyarrow import fs
    provider=config.get('provider','s3')
    if provider not in ('s3','adls'): raise ValueError('connector_failed')
    if provider=='adls':
        account=config.get('account_name') or ''
        if not account or not all(c.isascii() and (c.isalnum() or c=='-') for c in account): raise ValueError('connector_failed')
        # Use Azure's HTTPS endpoints. Never reinterpret an S3 endpoint as an Azure authority.
        if config.get('endpoint'): raise ValueError('connector_failed')
        keys=('account_key','sas_token','tenant_id','client_id','client_secret')
        return fs.AzureFileSystem(account_name=account,**{k:config[k] for k in keys if config.get(k) is not None})
    endpoint=config.get('endpoint')
    kw={'access_key':config.get('access_key_id'),'secret_key':config.get('secret_access_key'),'session_token':config.get('session_token'),
        'region':config.get('region') or 'us-east-1'}
    if endpoint:
        scheme,_,host=endpoint.partition('://')
        if scheme not in ('http','https') or not host: raise ValueError('connector_failed')
        # Plain HTTP would send the signed requests in clear text: only for local development.
        if scheme=='http' and not config.get('allow_insecure_endpoint'): raise ValueError('connector_failed')
        kw.update(endpoint_override=host,scheme=scheme)
    return fs.S3FileSystem(**{k:v for k,v in kw.items() if v is not None},force_virtual_addressing=bool(config.get('virtual_addressing')))
def settings(config):
    fmt=config.get('format')
    if fmt not in FORMATS: raise ValueError('connector_failed')
    bucket=config.get('bucket') or ''; prefix=(config.get('prefix') or '').strip('/')
    if not bucket or '/' in bucket or '..' in prefix.split('/'): raise ValueError('connector_failed')
    return fmt,bucket,prefix
def listing(fs_, config):
    """Every object of the configured format under the prefix, in key order: (key, size, mtime)."""
    from pyarrow import fs
    fmt,bucket,prefix=settings(config)
    base=f'{bucket}/{prefix}' if prefix else bucket
    infos=fs_.get_file_info(fs.FileSelector(base,recursive=True,allow_not_found=True))
    out=[(i.path[len(bucket)+1:],i.size,i.mtime) for i in infos if i.type==fs.FileType.File and i.path.lower().endswith(EXTENSIONS[fmt])]
    return sorted(out)
def modified(mtime):
    return mtime.astimezone(datetime.timezone.utc).isoformat().replace('+00:00','Z') if mtime else None
def identifier(text):
    """Stream names are identifiers ([A-Za-z_][A-Za-z0-9_]{0,62}); bucket and prefix stay in the config."""
    out=''.join(c if c.isascii() and (c.isalnum() or c=='_') else '_' for c in text)[:63] or '_'
    return out if not out[0].isdigit() else ('_'+out)[:63]
def stream_of(config):
    fmt,bucket,prefix=settings(config)
    return identifier(bucket),identifier(prefix.rsplit('/',1)[-1] if prefix else bucket)
def discover(fs_, config):
    fmt,bucket,_=settings(config)
    files=listing(fs_,config)
    namespace,name=stream_of(config)
    schema=schema_of(None,fmt)
    if files:
        key=files[0][0]
        with fs_.open_input_file(f'{bucket}/{key}') as f:
            schema=schema_of(f,fmt)
    return [{'namespace':namespace,'name':name,'json_schema':schema,'primary_key':KEYS,'cursor_field':CURSOR}]

def schema_of(f, fmt):
    """One schema inference path for discovery and immutable incremental snapshots."""
    props={}
    if f is not None:
        if fmt=='avro':
            from fastavro import reader
            return schema_of_avro(reader(f).writer_schema)
        elif fmt=='parquet':
            import pyarrow.parquet as pq
            for field in pq.ParquetFile(f).schema_arrow: props[field.name]={'source_type':str(field.type),'nullable':field.nullable}
        elif fmt in ('csv','tsv'):
            for col in header(f,fmt): props[col]={'source_type':'string','nullable':True}
        else:
            for _,line in lines(f,0,min(f.size(),READ*4),False):
                for k,v in json.loads(line).items(): props.setdefault(k,{'source_type':type(v).__name__ if v is not None else 'null','nullable':True})
                if len(props)>2000: break
    if any(k in META for k in props): raise ValueError('schema_drift')
    return {'type':'object','properties':{**props,**META},'x-files':{'format':fmt,'columns':list(props)}}
def schema_of_avro(avro):
    if not isinstance(avro,dict) or avro.get('type')!='record': raise ValueError('schema_drift')
    props={}
    for field in avro['fields']:
        typ=field['type']
        props[field['name']]={'source_type':json.dumps(typ,sort_keys=True,separators=(',',':')),
                              'nullable':typ=='null' or isinstance(typ,list) and 'null' in typ}
    if any(k in META for k in props): raise ValueError('schema_drift')
    return {'type':'object','properties':{**props,**META},
            'x-files':{'format':'avro','columns':list(props),'avro_schema':avro}}

def columns_of(schema):
    """Column order lives in x-files.columns: the schema is stored as jsonb, which keeps no key order."""
    return list(schema['x-files']['columns'])
def _strip_bom(b): return b[3:] if b.startswith(b'\xef\xbb\xbf') else b
def _line_end(f, start):
    """Offset just after the first newline at or after `start` (or the file size)."""
    f.seek(start); pos=start
    while True:
        chunk=f.read(READ)
        if not chunk: return pos
        i=chunk.find(b'\n')
        if i>=0: return pos+i+1
        pos+=len(chunk)
def header(f, fmt):
    end=_line_end(f,0)
    if end>16*1024*1024: raise ValueError('schema_drift')
    f.seek(0); first=_strip_bom(f.read(end)).rstrip(b'\n').rstrip(b'\r')
    return next(csv.reader([first.decode('utf-8')],delimiter=',' if fmt=='csv' else '\t'))
def lines(f, start, end, skip_header):
    """Yields (offset, line) for every record whose first byte lies in [start, end). A record that
    starts before `start` belongs to the previous range; the last record may run past `end`."""
    size=f.size()
    # The record containing byte start-1 started earlier: skip to the first newline at or after it
    # (a newline at start-1 means a record starts exactly at start).
    offset=(_line_end(f,0) if skip_header else 0) if start==0 else _line_end(f,start-1)
    # `base` is the file offset of buf[0]; every record starts at base + its index in buf.
    f.seek(offset); buf=b''; base=offset
    while base<end and base<size:
        chunk=f.read(READ)
        buf+=chunk; p=0
        while base+p<end:
            i=buf.find(b'\n',p)
            if i<0: break
            line=buf[p:i]
            if base+p==0: line=_strip_bom(line)
            line=line.rstrip(b'\r')
            if line: yield base+p,line
            p=i+1
        buf=buf[p:]; base+=p
        if base>=end: return
        if not chunk:
            line=(_strip_bom(buf) if base==0 else buf).rstrip(b'\r')
            if line: yield base,line
            return
def plan_object(config, key, size, mod, range_bytes, f=None, limit=None):
    """Plan one immutable file, optionally from its already verified disk snapshot."""
    fmt,_,_=settings(config); out=[]
    if range_bytes<1: raise ValueError('connector_failed')
    def add(rng):
        if limit is not None and len(out)>=limit: raise ValueError('record_too_large')
        out.append(rng)
    if fmt=='avro':
        from fastavro import block_reader
        f.seek(0); first=None; end=0; rows=0; base=0
        for block in block_reader(f):
            if first is not None and block.offset+block.size-first>range_bytes:
                add([key,size,mod,'avroblocks',first,end,base])
                base+=rows; first=None; rows=0
            if first is None: first=block.offset
            end=block.offset+block.size; rows+=block.num_records
        add([key,size,mod,'avroblocks',first if first is not None else 0,end,base])
    elif fmt=='parquet':
        import pyarrow.parquet as pq
        f.seek(0); meta=pq.ParquetFile(f).metadata; g=0; base=0
        while g<meta.num_row_groups:
            first=g; rows=0; nbytes=0
            while g<meta.num_row_groups and (g==first or nbytes+meta.row_group(g).total_byte_size<=range_bytes):
                nbytes+=meta.row_group(g).total_byte_size; rows+=meta.row_group(g).num_rows; g+=1
            add([key,size,mod,'rowgroups',first,g,base]); base+=rows
        if meta.num_row_groups==0: add([key,size,mod,'rowgroups',0,0,0])
    elif fmt in ('csv','tsv') and config.get('csv_quoted_newlines'):
        add([key,size,mod,'records',0,size,0])
    else:
        a=0
        while True:
            b=min(size,a+range_bytes); add([key,size,mod,'bytes',a,b,0]); a=b
            if a>=size: break
    return out

def plan(fs_, config, range_bytes=RANGE_BYTES):
    """Ranges in key order: [key, size, modified, kind, a, b, global row base]."""
    fmt,bucket,_=settings(config); out=[]
    for key,size,mtime in listing(fs_,config):
        mod=modified(mtime)
        if fmt in ('avro','parquet'):
            with fs_.open_input_file(f'{bucket}/{key}') as f:
                out.extend(plan_object(config,key,size,mod,range_bytes,f))
        else: out.extend(plan_object(config,key,size,mod,range_bytes))
    return out

def read_range(fs_, config, schema, rng, snapshot=None):
    """Yields (data, position) for one planned range. An optional disk snapshot supplies
    already-hashed immutable bytes; otherwise verify the object's planned metadata."""
    fmt,bucket,_=settings(config)
    key,size,mod,kind,a,b,base=rng
    path=f'{bucket}/{key}'
    if snapshot is None:
        info=fs_.get_file_info(path)
        if info.size!=size or modified(info.mtime)!=mod: raise Changed()
    cols=columns_of(schema)
    meta=lambda pos: {'_file':key,'_position':pos,'_modified':mod}
    with (snapshot if snapshot is not None else fs_.open_input_file(path)) as f:
        if fmt=='avro':
            from fastavro import block_reader
            blocks=block_reader(f)
            if schema_of_avro(blocks.writer_schema)!=schema: raise ValueError('schema_drift')
            if a: f.seek(a)
            pos=base
            for block in blocks:
                if block.offset>=b: break
                for row in block:
                    yield {**{k:to_json(v) for k,v in row.items()},**meta(pos)},pos; pos+=1
        elif fmt=='parquet':
            import pyarrow.parquet as pq
            pf=pq.ParquetFile(f)
            if [x.name for x in pf.schema_arrow]!=cols or any({'source_type':str(x.type),'nullable':x.nullable}!=schema['properties'][x.name] for x in pf.schema_arrow): raise ValueError('schema_drift')
            pos=base
            for g in range(a,b):
                for row in pf.read_row_group(g).to_pylist():
                    yield {**{k:to_json(v) for k,v in row.items()},**meta(pos)},pos; pos+=1
        elif kind=='records':
            text=io.TextIOWrapper(f,encoding='utf-8-sig',newline='')
            reader=csv.reader(text,delimiter=',' if fmt=='csv' else '\t')
            if next(reader,None)!=cols: raise ValueError('schema_drift')
            for pos,row in enumerate(reader):
                if not row: continue
                if len(row)!=len(cols): raise ValueError('connector_failed')
                yield {**dict(zip(cols,row)),**meta(pos)},pos
        elif fmt in ('csv','tsv'):
            if a==0 and header(f,fmt)!=cols: raise ValueError('schema_drift')
            delimiter=',' if fmt=='csv' else '\t'
            for pos,line in lines(f,a,b,True):
                row=next(csv.reader([line.decode('utf-8')],delimiter=delimiter))
                if len(row)!=len(cols): raise ValueError('connector_failed')
                yield {**dict(zip(cols,row)),**meta(pos)},pos
        else:
            for pos,line in lines(f,a,b,False):
                value=json.loads(line)
                if not isinstance(value,dict) or any(k in META for k in value): raise ValueError('connector_failed')
                yield {**{k:to_json(v) for k,v in value.items()},**meta(pos)},pos
def files_state(ledger, completed=False):
    # Leave room under the database rewind limit (64 KiB) for the STATE envelope.
    if len(json.dumps(ledger,ensure_ascii=True).encode())>LEDGER_BYTES: raise ValueError('record_too_large')
    return state(completed=completed,source={'files':json.loads(json.dumps(ledger))})

def source_identity(config, schema):
    """Secret-free source location and parsing identity, shared by ledger and Spark resumes."""
    fmt,bucket,prefix=settings(config)
    binding={k:config.get(k) for k in ('endpoint','region','virtual_addressing')}
    binding.update(bucket=bucket,prefix=prefix,format=fmt,
                   csv_quoted_newlines=bool(config.get('csv_quoted_newlines')),schema=schema)
    if config.get('provider','s3')!='s3': binding.update(provider=config['provider'],account_name=config.get('account_name'))
    return hashlib.sha256(json.dumps(binding,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def read_incremental(fs_, config, stream, checkpoint):
    """Content-addressed, append-only object ingestion. Known keys may never change bytes.
    Each row and its position land together, including when destination budgets are small."""
    fmt,bucket,prefix=settings(config)
    source=source_identity(config,stream['schema'])
    ledger=json.loads(json.dumps(checkpoint.get('files'))) if checkpoint.get('files') is not None else None
    if ledger is None:
        if checkpoint: raise Changed()
        ledger={'version':1,'source':source,'objects':{}}
    if ledger.get('version')!=1 or ledger.get('source')!=source or not isinstance(ledger.get('objects'),dict): raise Changed()
    files_state(ledger)
    objects=ledger['objects']; files=listing(fs_,config)
    present={key for key,_,_ in files}
    if any(not obj['done'] and key not in present for key,obj in objects.items()): raise Changed()
    for key,size,mtime in files:
        path=f'{bucket}/{key}'; mod=modified(mtime)
        with object_snapshot(fs_,path,size,mod) as (f,sha):
            old=objects.get(key)
            if old is not None:
                if old['sha256']!=sha or old['size']!=size: raise Changed()
                if old['done']: continue
                obj=old
            else:
                obj={'sha256':sha,'size':size,'modified':mod,'after':None,'done':False}
                objects[key]=obj
            files_state(ledger)  # Refuse an unrepresentable new key before emitting rows.
            if not size and fmt not in ('parquet','avro'):
                obj['done']=True
                yield files_state(ledger)
                continue
            found=schema_of(f,fmt)
            # JSON members are named, unlike positional CSV/TSV and Parquet columns.
            if fmt=='jsonl': found['x-files']['columns']=columns_of(stream['schema'])
            if found!=stream['schema']: raise ValueError('schema_drift')
            f.seek(0)
            if fmt=='avro':
                kind='avroblocks'; end=size
            elif fmt=='parquet':
                import pyarrow.parquet as pq
                kind='rowgroups'; end=pq.ParquetFile(f).metadata.num_row_groups
            else:
                kind='records' if fmt in ('csv','tsv') and config.get('csv_quoted_newlines') else 'bytes'
                end=size
            rng=[key,size,obj['modified'],kind,0,end,0]
            for data,pos in read_range(fs_,config,stream['schema'],rng,snapshot=f):
                if obj['after'] is not None and pos<=obj['after']: continue
                obj['after']=pos
                checkpoint_message=files_state(ledger)
                yield record(stream,data,data['_modified'],[key,pos],int(time.time()*1000))
                yield checkpoint_message
            obj['done']=True
            yield files_state(ledger)
    yield files_state(ledger,True)

def read(fs_, config, stream, checkpoint):
    """Single worker: content ledger for incremental; row-key resume for full refresh."""
    if stream.get('sync_mode')=='incremental':
        yield from read_incremental(fs_,config,stream,checkpoint)
        return
    found=discover(fs_,config)[0]
    if found['json_schema']!=stream['schema']: raise ValueError('schema_drift')
    after=checkpoint.get('primary_key')
    after=tuple(after) if isinstance(after,list) else None
    last=after
    for rng in plan(fs_,config,int(config.get('range_bytes') or RANGE_BYTES)):
        n=0
        for data,pos in read_range(fs_,config,stream['schema'],rng):
            if after is not None and (data['_file'],pos)<=after: continue
            yield record(stream,data,data['_modified'],[data['_file'],pos],int(time.time()*1000)); last=(data['_file'],pos); n+=1
            if n%500==0: yield state(None,list(last),False)
        if n: yield state(None,list(last),False)
    yield state(None,list(last) if last else None,True)
def main():
    a=arguments()
    if a.command=='spec': emit('SPEC',spec={'protocol':'meridian-connect/1','version':VERSION,'config':['provider','account_name','account_key','sas_token','tenant_id','client_id','client_secret','endpoint','region','bucket','prefix','format','access_key_id','secret_access_key','session_token','csv_quoted_newlines','range_bytes'],'modes':['full_refresh','incremental'],'deletes':False,'composite_keys':True}); return
    if a.command=='health': emit('LOG',log={'level':'INFO','code':'healthy','version':VERSION}); return
    try:
        config=load(a.config); fs_=filesystem(config)
        if a.command=='check': listing(fs_,config); emit('CONNECTION_STATUS',connectionStatus={'status':'SUCCEEDED'})
        elif a.command=='discover': emit('CATALOG',catalog={'streams':discover(fs_,config)})
        elif a.command=='read':
            stream=load(a.catalog)
            if stream.get('sync_mode','full_refresh') not in ('full_refresh','incremental'): raise ValueError('connector_failed')
            for m in read(fs_,config,stream,load(a.state)): print(json.dumps(m),flush=True)
        else: raise ValueError('connector_failed')
    except Changed:
        emit('TRACE',trace={'type':'ERROR','code':'source_changed'}); raise SystemExit(1)
    except Exception as e:
        text=str(e)
        code=text if text in ('schema_drift','connector_failed','record_too_large') else 'auth_failed' if any(s in text for s in ('AccessDenied','InvalidAccessKeyId','SignatureDoesNotMatch','403')) else 'source_unavailable'
        emit('TRACE',trace={'type':'ERROR','code':code}); raise SystemExit(1)
if __name__=='__main__': main()
