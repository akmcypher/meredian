"""MySQL CDC actor (deploy/connect/README.md, "Change data capture"): reads the binlog as a replica with
python-mysql-replication. Each stream has its own position (binlog file and offset after the last
landed transaction) and its own replica server_id; changes land as upsert and delete records in
commit order, with the MySQL actor's own value rules."""
import hashlib, json, re, time
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent, RotateEvent, XidEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
from sdk import arguments, load, emit, record, state, key_columns, key_value, to_json
import source_mysql
VERSION='mysql-cdc-v1'
BATCH=500

class Stop(Exception):
    """A fail-closed stop: a safe error code plus a log line naming the reason."""
    def __init__(self, code, reason, **detail): super().__init__(code); self.reason=reason; self.detail=detail

def server_id(stream):
    """A replica id per stream, in a range real replicas rarely use (2^30 and up)."""
    return 0x40000000+int(hashlib.sha256(stream['id'].encode()).hexdigest()[:8],16)%0x3FFFFFFF

def variables(conn):
    names=('log_bin','binlog_format','binlog_row_image','binlog_row_metadata','binlog_row_value_options')
    rows=source_mysql.query(conn,'SHOW GLOBAL VARIABLES WHERE Variable_name IN %s',(names,))
    return {r['Variable_name']:r['Value'] for r in rows}

def prerequisites(conn):
    v=variables(conn)
    want={'log_bin':'ON','binlog_format':'ROW','binlog_row_image':'FULL','binlog_row_metadata':'FULL'}
    wrong={k:v.get(k) for k,x in want.items() if str(v.get(k,'')).upper()!=x}
    # Partial JSON updates send only the changed paths of a JSON value.
    if 'PARTIAL_JSON' in str(v.get('binlog_row_value_options','')).upper(): wrong['binlog_row_value_options']=v['binlog_row_value_options']
    if wrong: raise Stop('connector_failed','cdc_binlog_settings',settings=wrong)
    grants=' '.join(str(list(r.values())[0]) for r in source_mysql.query(conn,'SHOW GRANTS FOR CURRENT_USER()')).upper()
    if 'ALL PRIVILEGES ON *.*' not in grants and not ('REPLICATION SLAVE' in grants and 'REPLICATION CLIENT' in grants):
        raise Stop('connector_failed','cdc_replication_privilege')

def binlog_end(conn):
    for statement in ('SHOW BINARY LOG STATUS','SHOW MASTER STATUS'):
        try:
            r=source_mysql.query(conn,statement)
            if r: return {'file':r[0]['File'],'position':int(r[0]['Position'])}
        except pymysql.err.MySQLError: continue
    raise Stop('connector_failed','cdc_binlog_status')

def order(p): return (p['file'],p['position'])

def normalize(value):
    """Binary JSON from the binlog can carry bytes; the copy reads JSON as text."""
    if isinstance(value,bytes): return value.decode('utf-8','surrogateescape')
    if isinstance(value,dict): return {normalize(k):normalize(v) for k,v in value.items()}
    if isinstance(value,list): return [normalize(v) for v in value]
    return value

def typed(row, columns):
    """The MySQL actor's value rules for a binlog row. JSON arrives parsed, not as text."""
    out={}
    for k,v in row.items():
        t=columns.get(k,{}).get('source_type')
        if v is not None and t=='json': out[k]=to_json(normalize(v)); continue
        if t=='json': out[k]=None; continue
        out.update(source_mysql.typed({k:v},columns))
    return out

def read(conn, settings, stream, checkpoint, max_changes=100000):
    if stream['sync_mode'] not in ('cdc','full_refresh'): raise Stop('connector_failed','cdc_stream_mode',sync_mode=stream['sync_mode'])
    found=next((s for s in source_mysql.discover(conn,stream['namespace']) if s['name']==stream['name']),None)
    if not found or found['json_schema']!=stream['schema']: raise ValueError('schema_drift')
    prerequisites(conn)
    keys=key_columns(stream); columns=found['json_schema']['properties']; schema=source_mysql.schema_hash(found)
    cdc=checkpoint.get('cdc')
    if not cdc:
        # First run or reset: take the log position first, then copy; changes made during the
        # copy are read from the log afterwards.
        cdc={'phase':'snapshot','binlog':binlog_end(conn),'server_id':server_id(stream)}; checkpoint={}
    if cdc['phase']=='snapshot':
        yield from copy(conn,stream,keys,columns,cdc,checkpoint.get('primary_key'),schema)
        cdc={**cdc,'phase':'stream'}
        yield state(None,None,False,schema,source={'cdc':cdc})
    yield from changes(conn,settings,stream,keys,columns,cdc,schema,max_changes)

def copy(conn, stream, keys, columns, cdc, after, schema, batch_size=BATCH):
    table=f'{source_mysql.quote(stream["namespace"])}.{source_mysql.quote(stream["name"])}'
    cols=','.join(map(source_mysql.quote,keys))
    current=None if after is None else (after if isinstance(after,list) else [after])
    while True:
        where=f' WHERE ({cols}) > ({",".join(["%s"]*len(keys))})' if current is not None else ''
        rows=source_mysql.query(conn,f'SELECT * FROM {table}{where} ORDER BY {cols} LIMIT %s',(*(current or []),batch_size))
        if not rows: return
        for row in rows: yield record(stream,source_mysql.typed(row,columns),None,to_json(key_value(row,keys)),int(time.time()*1000),'upsert')
        pk=to_json(key_value(rows[-1],keys)); current=pk if isinstance(pk,list) else [pk]
        yield state(None,pk,False,schema,source={'cdc':cdc})

def changes(conn, settings, stream, keys, columns, cdc, schema, max_changes):
    start=cdc['binlog']
    files={r['Log_name'] for r in source_mysql.query(conn,'SHOW BINARY LOGS')}
    if start['file'] not in files: raise Stop('offset_out_of_range','cdc_binlog_purged',file=start['file'],position=start['position'])
    end=binlog_end(conn)
    ns,name=stream['namespace'],stream['name']
    truncate=re.compile(r'^\s*TRUNCATE\s+(?:TABLE\s+)?(?:`?(\w+)`?\.)?`?(\w+)`?\s*;?\s*$',re.I)
    alter=re.compile(r'^\s*(?:ALTER|DROP|RENAME)\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:`?(\w+)`?\.)?`?(\w+)`?',re.I)
    reader=BinLogStreamReader(connection_settings=settings,server_id=cdc['server_id'],log_file=start['file'],log_pos=start['position'],
        resume_stream=True,blocking=False,only_events=[WriteRowsEvent,UpdateRowsEvent,DeleteRowsEvent,XidEvent,QueryEvent,RotateEvent],
        only_schemas=[ns],only_tables=[name],enable_logging=False)
    position=dict(start); file=start['file']; pending=0; read_=0; open_=False
    def mine(match, default): return match and (match.group(1) or default)==ns and match.group(2)==name
    try:
        for event in reader:
            if isinstance(event,RotateEvent): file=event.next_binlog; continue
            at={'file':file,'position':event.packet.log_pos}
            # The window ends at the log position captured at the start, never inside a transaction.
            if order(at)>order(end) and not open_: break
            if isinstance(event,QueryEvent):
                q=event.query if isinstance(event.query,str) else event.query.decode('utf-8','replace')
                default=event.schema.decode() if isinstance(event.schema,bytes) else event.schema
                if mine(truncate.match(q),default): raise Stop('source_changed','cdc_truncate')
                if mine(alter.match(q),default): raise Stop('schema_drift','cdc_columns_changed')
                if q.strip().upper()=='COMMIT': event=None
                else: continue
            if event is None or isinstance(event,XidEvent):
                # A transaction ended: the next one starts here.
                position=at; open_=False
                if pending>=BATCH: yield state(None,None,False,schema,source={'cdc':{**cdc,'binlog':position}}); pending=0
                if read_>=max_changes: break
                continue
            open_=True
            if sorted(c.name for c in event.columns)!=sorted(columns): raise Stop('schema_drift','cdc_columns_changed')
            for row in event.rows:
                if isinstance(event,UpdateRowsEvent):
                    old=typed(row['before_values'],columns); new=typed(row['after_values'],columns)
                    before=to_json(key_value(old,keys)); pk=to_json(key_value(new,keys))
                    if before!=pk: yield record(stream,old,None,before,int(time.time()*1000),'delete'); pending+=1
                    yield record(stream,new,None,pk,int(time.time()*1000),'upsert')
                elif isinstance(event,DeleteRowsEvent):
                    old=typed(row['values'],columns)
                    yield record(stream,old,None,to_json(key_value(old,keys)),int(time.time()*1000),'delete')
                else:
                    new=typed(row['values'],columns)
                    yield record(stream,new,None,to_json(key_value(new,keys)),int(time.time()*1000),'upsert')
                pending+=1; read_+=1
    finally: reader.close()
    yield state(None,None,True,schema,source={'cdc':{**cdc,'binlog':position}})
    yield lag(conn,position)

def lag(conn, position):
    """Bytes of binlog after the landed position: the rest of its file plus every later file.
    Positions are expressed as offsets into the concatenated binlog files still on the server."""
    files=[(r['Log_name'],int(r['File_size'])) for r in source_mysql.query(conn,'SHOW BINARY LOGS')]
    total=0; at=None
    for name,size in files:
        if name==position['file']: at=total+position['position']
        total+=size
    at=total if at is None else min(at,total)
    return {'type':'LOG','log':{'level':'INFO','code':'partition_lag','detail':[{'partition':'0','position':str(at),'end':str(total)}]}}

def main():
    a=arguments()
    if a.command=='spec': emit('SPEC',spec={'protocol':'meridian-connect/1','version':VERSION,'config':['host','port','user','password','database','ssl_ca'],'modes':['cdc'],'deletes':True,'composite_keys':True}); return
    if a.command=='health': emit('LOG',log={'level':'INFO','code':'healthy','version':VERSION}); return
    try:
        config=load(a.config)
        settings={'host':config['host'],'port':int(config.get('port',3306)),'user':config['user'],'passwd':config.get('password',''),'charset':'utf8mb4'}
        if config.get('ssl_ca'): settings['ssl']={'ca':config['ssl_ca']}
        conn=source_mysql.connection(config)
        try:
            if a.command=='check': prerequisites(conn); emit('CONNECTION_STATUS',connectionStatus={'status':'SUCCEEDED'})
            elif a.command=='discover': emit('CATALOG',catalog={'streams':source_mysql.discover(conn,config.get('database'))})
            elif a.command=='read':
                for m in read(conn,settings,load(a.catalog),load(a.state)): print(json.dumps(m),flush=True)
            else: raise ValueError('connector_failed')
        finally: conn.close()
    except Stop as e:
        emit('LOG',log={'level':'ERROR','code':e.reason,**e.detail})
        emit('TRACE',trace={'type':'ERROR','code':str(e)}); raise SystemExit(1)
    except Exception as e:
        auth=isinstance(e,pymysql.err.OperationalError) and e.args and e.args[0] in (1044,1045)
        code=str(e) if str(e) in ('schema_drift','connector_failed') else 'auth_failed' if auth else 'source_unavailable'
        emit('TRACE',trace={'type':'ERROR','code':code}); raise SystemExit(1)
if __name__=='__main__': main()
