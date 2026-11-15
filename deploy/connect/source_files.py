"""Meridian Connect object-store files actor (deploy/connect/README.md, "Spark ingestion").

Reads CSV, TSV, JSON Lines and Parquet objects under one prefix of an S3-compatible bucket (AWS S3,
GCS through its S3 interoperability endpoint, MinIO, R2, Supabase Storage). Credentials come from the
connection's credential file on the worker host, never from the control plane.

A file is planned into ranges that can be read independently:
- CSV/TSV/JSONL: byte ranges. A range owns the records that *start* inside it (the reader skips to
  the first record start after the range's first byte and finishes the record that crosses its end),
  so the ranges of a file read every record exactly once. CSV whose quoted fields may contain newlines
  (`csv_quoted_newlines: true`) cannot be split this way: each such file is one range.
- Parquet: groups of row groups.

Each record carries `_file`, `_position` (byte offset of the record, or the global row index for
Parquet and for unsplittable CSV) and `_modified` (the object's modification time). (`_file`,
`_position`) is the stream key, identical however the file is split; `_modified` is the cursor.

The single-worker `read` below and the Spark executors (spark_ingest.py) use the same planning and
parsing functions, so both land the same records.
"""
import csv, datetime, io, json, sys, time
from sdk import arguments, load, emit, record, state, to_json
VERSION='files-v1'
FORMATS=('csv','tsv','jsonl','parquet')
EXTENSIONS={'csv':('.csv',),'tsv':('.tsv','.tab'),'jsonl':('.jsonl','.ndjson','.json'),'parquet':('.parquet',)}
META={'_file':{'source_type':'string','nullable':False},'_position':{'source_type':'bigint','nullable':False},
      '_modified':{'source_type':'timestamp with time zone','nullable':False}}
KEYS=['_file','_position']; CURSOR='_modified'
RANGE_BYTES=128*1024*1024
READ=1024*1024
SAMPLE=100
class Changed(Exception):
    """The object changed between planning and reading."""
def filesystem(config):
    from pyarrow import fs
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
    props={}
    if files:
        key=files[0][0]
        with fs_.open_input_file(f'{bucket}/{key}') as f:
            if fmt=='parquet':
                import pyarrow.parquet as pq
                for field in pq.ParquetFile(f).schema_arrow: props[field.name]={'source_type':str(field.type),'nullable':field.nullable}
            elif fmt in ('csv','tsv'):
                for col in header(f,fmt): props[col]={'source_type':'string','nullable':True}
            else:
                for _,line in lines(f,0,min(f.size(),READ*4),False):
                    for k,v in json.loads(line).items(): props.setdefault(k,{'source_type':type(v).__name__ if v is not None else 'null','nullable':True})
                    if len(props)>2000: break
    if any(k in META for k in props): raise ValueError('schema_drift')
    return [{'namespace':namespace,'name':name,'json_schema':{'type':'object','properties':{**props,**META},'x-files':{'format':fmt,'columns':list(props)}},
             'primary_key':KEYS,'cursor_field':CURSOR}]
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
def plan(fs_, config, range_bytes=RANGE_BYTES):
    """The ranges to read, in key order: [key, size, modified, kind, a, b, base]. Byte ranges are
    [a, b); row-group ranges are row groups a..b-1 starting at global row `base`."""
    fmt,bucket,_=settings(config); out=[]
    for key,size,mtime in listing(fs_,config):
        mod=modified(mtime)
        if fmt=='parquet':
            import pyarrow.parquet as pq
            with fs_.open_input_file(f'{bucket}/{key}') as f: meta=pq.ParquetFile(f).metadata
            g=0; base=0
            while g<meta.num_row_groups:
                first=g; rows=0; nbytes=0
                while g<meta.num_row_groups and (g==first or nbytes+meta.row_group(g).total_byte_size<=range_bytes):
                    nbytes+=meta.row_group(g).total_byte_size; rows+=meta.row_group(g).num_rows; g+=1
                out.append([key,size,mod,'rowgroups',first,g,base]); base+=rows
            if meta.num_row_groups==0: out.append([key,size,mod,'rowgroups',0,0,0])
        elif fmt in ('csv','tsv') and config.get('csv_quoted_newlines'):
            out.append([key,size,mod,'records',0,size,0])
        else:
            a=0
            while True:
                b=min(size,a+range_bytes); out.append([key,size,mod,'bytes',a,b,0]); a=b
                if a>=size: break
    return out
def read_range(fs_, config, schema, rng):
    """Yields (data, position) for one planned range, using the schema's columns."""
    fmt,bucket,_=settings(config)
    key,size,mod,kind,a,b,base=rng
    path=f'{bucket}/{key}'
    info=fs_.get_file_info(path)
    if info.size!=size or modified(info.mtime)!=mod: raise Changed()
    cols=columns_of(schema)
    meta=lambda pos: {'_file':key,'_position':pos,'_modified':mod}
    with fs_.open_input_file(path) as f:
        if fmt=='parquet':
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
def read(fs_, config, stream, checkpoint):
    """Single worker: every range in order. The checkpoint is the last committed (_file, _position)."""
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
    if a.command=='spec': emit('SPEC',spec={'protocol':'meridian-connect/1','version':VERSION,'config':['endpoint','region','bucket','prefix','format','access_key_id','secret_access_key','session_token','csv_quoted_newlines','range_bytes'],'modes':['full_refresh'],'deletes':False,'composite_keys':True}); return
    if a.command=='health': emit('LOG',log={'level':'INFO','code':'healthy','version':VERSION}); return
    try:
        config=load(a.config); fs_=filesystem(config)
        if a.command=='check': listing(fs_,config); emit('CONNECTION_STATUS',connectionStatus={'status':'SUCCEEDED'})
        elif a.command=='discover': emit('CATALOG',catalog={'streams':discover(fs_,config)})
        elif a.command=='read':
            stream=load(a.catalog)
            if stream.get('sync_mode','full_refresh')!='full_refresh': raise ValueError('connector_failed')
            for m in read(fs_,config,stream,load(a.state)): print(json.dumps(m),flush=True)
        else: raise ValueError('connector_failed')
    except Changed:
        emit('TRACE',trace={'type':'ERROR','code':'source_changed'}); raise SystemExit(1)
    except Exception as e:
        text=str(e)
        code=text if text in ('schema_drift','connector_failed') else 'auth_failed' if any(s in text for s in ('AccessDenied','InvalidAccessKeyId','SignatureDoesNotMatch','403')) else 'source_unavailable'
        emit('TRACE',trace={'type':'ERROR','code':code}); raise SystemExit(1)
if __name__=='__main__': main()
