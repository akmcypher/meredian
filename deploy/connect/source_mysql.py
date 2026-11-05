import json, hashlib, re, time, datetime
import pymysql
from pymysql.cursors import DictCursor
from sdk import arguments, load, emit, record, state, key_columns, key_value, to_json
VERSION='mysql-v1'
SYSTEM=('mysql','information_schema','performance_schema','sys')
IDENT=re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]{0,62}$')
def connection(config):
    ssl={'ca':config['ssl_ca']} if config.get('ssl_ca') else None
    conn=pymysql.connect(host=config['host'],port=int(config.get('port',3306)),user=config['user'],password=config.get('password',''),database=config.get('database'),ssl=ssl,connect_timeout=10,read_timeout=120,charset='utf8mb4',cursorclass=DictCursor,autocommit=True)
    with conn.cursor() as c: c.execute("SET SESSION time_zone='+00:00', SESSION max_execution_time=60000, SESSION transaction_isolation='REPEATABLE-READ'")
    return conn
def query(conn, sql, args=()):
    with conn.cursor() as c: c.execute(sql,args); return c.fetchall()
def quote(name):
    # Identifiers come from discovery and are re-validated; MySQL has no bound identifiers.
    if not IDENT.match(name): raise ValueError('schema_drift')
    return '`'+name+'`'
def discover(conn, database=None):
    rows=query(conn,"""SELECT c.TABLE_SCHEMA s,c.TABLE_NAME t,c.COLUMN_NAME col,c.DATA_TYPE dt,c.COLUMN_TYPE ct,c.IS_NULLABLE nul,k.ORDINAL_POSITION pk
      FROM information_schema.COLUMNS c JOIN information_schema.TABLES t ON t.TABLE_SCHEMA=c.TABLE_SCHEMA AND t.TABLE_NAME=c.TABLE_NAME AND t.TABLE_TYPE='BASE TABLE'
      LEFT JOIN information_schema.KEY_COLUMN_USAGE k ON k.TABLE_SCHEMA=c.TABLE_SCHEMA AND k.TABLE_NAME=c.TABLE_NAME AND k.COLUMN_NAME=c.COLUMN_NAME AND k.CONSTRAINT_NAME='PRIMARY'
      WHERE c.TABLE_SCHEMA NOT IN %s AND (%s IS NULL OR c.TABLE_SCHEMA=%s) ORDER BY c.TABLE_SCHEMA,c.TABLE_NAME,c.ORDINAL_POSITION LIMIT 10000""",(SYSTEM,database,database))
    grouped={}
    for r in rows:
        if not (IDENT.match(r['s']) and IDENT.match(r['t']) and IDENT.match(r['col'])): continue
        s=grouped.setdefault((r['s'],r['t']),{'namespace':r['s'],'name':r['t'],'json_schema':{'type':'object','properties':{}},'keys':[]})
        s['json_schema']['properties'][r['col']]={'source_type':r['ct'] if r['dt']=='tinyint' else r['dt'],'nullable':r['nul']=='YES'}
        if r['pk']: s['keys'].append((r['pk'],r['col']))
    def pk(keys): keys=[c for _,c in sorted(keys)]; return keys[0] if len(keys)==1 else keys
    return [{**{k:v for k,v in s.items() if k!='keys'},'primary_key':pk(s['keys'])} for s in grouped.values() if s['keys']]
def schema_hash(s): return hashlib.sha256(json.dumps(s['json_schema'],sort_keys=True).encode()).hexdigest()
def typed(row, columns):
    """MySQL wire values to their logical JSON: tinyint(1) is boolean, JSON columns arrive as
    text, TIMESTAMP is UTC in this session (DATETIME stays zone-less)."""
    out={}
    for k,v in row.items():
        t=columns.get(k,{}).get('source_type')
        if v is not None and t=='tinyint(1)': v=bool(v)
        elif v is not None and t=='json': v=json.loads(v)
        elif isinstance(v,datetime.datetime) and t=='timestamp': v=v.replace(tzinfo=datetime.timezone.utc)
        out[k]=to_json(v)
    return out
def read(conn, stream, checkpoint, batch_size=500):
    found=next((s for s in discover(conn,stream['namespace']) if s['name']==stream['name']),None)
    if not found or found['json_schema']!=stream['schema']: raise ValueError('schema_drift')
    keys=key_columns(stream); cursor=stream['cursor_field']; columns=found['json_schema']['properties']
    if cursor not in columns or columns[cursor]['nullable'] or any(k not in columns or columns[k]['nullable'] for k in keys): raise ValueError('schema_drift')
    full=stream['sync_mode']=='full_refresh'; order=keys if full else [cursor,*keys]
    def position(cp):
        if cp.get('primary_key') is None: return None
        pk=cp['primary_key'] if isinstance(cp['primary_key'],list) else [cp['primary_key']]
        return pk if full else [cp.get('cursor'),*pk]
    current=position(checkpoint); cols=','.join(map(quote,order))
    while True:
        where=f' WHERE ({cols}) > ({",".join(["%s"]*len(order))})' if current is not None else ''
        rows=query(conn,f'SELECT * FROM {quote(stream["namespace"])}.{quote(stream["name"])}{where} ORDER BY {cols} LIMIT %s',(*(current or []),batch_size))
        if not rows:
            yield state(checkpoint.get('cursor'),checkpoint.get('primary_key'),True,schema_hash(found)); break
        for row in rows:
            yield record(stream,typed(row,columns),str(row[cursor]),to_json(key_value(row,keys)),int(time.time()*1000))
        last=rows[-1]
        checkpoint={'cursor':str(last[cursor]),'primary_key':to_json(key_value(last,keys))}; current=position(checkpoint)
        yield state(checkpoint['cursor'],checkpoint['primary_key'],False,schema_hash(found))
def main():
    a=arguments()
    if a.command=='spec': emit('SPEC',spec={'protocol':'meridian-connect/1','version':VERSION,'config':['host','port','user','password','database','ssl_ca'],'modes':['full_refresh','incremental'],'deletes':False,'composite_keys':True}); return
    if a.command=='health': emit('LOG',log={'level':'INFO','code':'healthy','version':VERSION}); return
    try:
        config=load(a.config)
        conn=connection(config)
        try:
            if a.command=='check': query(conn,'SELECT 1'); emit('CONNECTION_STATUS',connectionStatus={'status':'SUCCEEDED'})
            elif a.command=='discover': emit('CATALOG',catalog={'streams':discover(conn,config.get('database'))})
            elif a.command=='read':
                for m in read(conn,load(a.catalog),load(a.state)): print(json.dumps(m),flush=True)
            else: raise ValueError('connector_failed')
        finally: conn.close()
    except Exception as e:
        auth=isinstance(e,pymysql.err.OperationalError) and e.args and e.args[0] in (1044,1045)
        code=str(e) if str(e) in ('schema_drift','connector_failed') else 'auth_failed' if auth else 'source_unavailable'
        emit('TRACE',trace={'type':'ERROR','code':code}); raise SystemExit(1)
if __name__=='__main__': main()
