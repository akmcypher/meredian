import os, json, hashlib, time
from psycopg import connect, sql
from psycopg.rows import dict_row
from sdk import arguments, load, emit, record, state, key_columns, key_value, to_json
VERSION='postgres-v1'
def connection(config):
    return connect(**config, row_factory=dict_row, connect_timeout=10)
def discover(conn):
    rows=conn.execute("""SELECT c.table_schema,c.table_name,c.column_name,c.data_type,c.is_nullable,
      (SELECT k.ordinal_position FROM information_schema.table_constraints t JOIN information_schema.key_column_usage k ON k.constraint_name=t.constraint_name AND k.constraint_schema=t.constraint_schema WHERE t.constraint_type='PRIMARY KEY' AND t.table_schema=c.table_schema AND t.table_name=c.table_name AND k.column_name=c.column_name) AS pk
      FROM information_schema.columns c JOIN information_schema.tables t USING(table_schema,table_name) WHERE t.table_type='BASE TABLE' AND c.table_schema NOT IN ('pg_catalog','information_schema') ORDER BY c.table_schema,c.table_name,c.ordinal_position LIMIT 10000""").fetchall()
    grouped={}
    for r in rows:
        key=(r['table_schema'],r['table_name']); s=grouped.setdefault(key,{'namespace':key[0],'name':key[1],'json_schema':{'type':'object','properties':{}},'keys':[]})
        s['json_schema']['properties'][r['column_name']]={'source_type':r['data_type'],'nullable':r['is_nullable']=='YES'}
        if r['pk']: s['keys'].append((r['pk'],r['column_name']))
    # Single-column keys keep the original v1 string shape; composite keys are a list in key order.
    def pk(keys): keys=[c for _,c in sorted(keys)]; return keys[0] if len(keys)==1 else keys
    return [{**{k:v for k,v in s.items() if k!='keys'},'primary_key':pk(s['keys'])} for s in grouped.values() if s['keys']]
def schema_hash(s): return hashlib.sha256(json.dumps(s['json_schema'],sort_keys=True).encode()).hexdigest()
def read(conn, stream, checkpoint, batch_size=500):
    found=next((s for s in discover(conn) if s['namespace']==stream['namespace'] and s['name']==stream['name']),None)
    if not found or found['json_schema']!=stream['schema']: raise ValueError('schema_drift')
    keys=key_columns(stream); cursor=stream['cursor_field']; columns=found['json_schema']['properties']
    if cursor not in columns or columns[cursor]['nullable'] or any(k not in columns or columns[k]['nullable'] for k in keys): raise ValueError('schema_drift')
    # Full refresh resumes by primary key; incremental uses cursor + unique PK tie breaker.
    full=stream['sync_mode']=='full_refresh'; order=keys if full else [cursor,*keys]
    def position(cp):
        if cp.get('primary_key') is None: return None
        pk=cp['primary_key'] if isinstance(cp['primary_key'],list) else [cp['primary_key']]
        return pk if full else [cp.get('cursor'),*pk]
    current=position(checkpoint)
    while True:
        where=sql.SQL(''); args=[]
        if current is not None:
            where=sql.SQL(' WHERE ({}) > ({})').format(sql.SQL(',').join(map(sql.Identifier,order)),sql.SQL(',').join(sql.Placeholder()*len(order))); args=list(current)
        query=sql.SQL('SELECT * FROM {}.{}{} ORDER BY {} LIMIT %s').format(sql.Identifier(stream['namespace']),sql.Identifier(stream['name']),where,sql.SQL(',').join(map(sql.Identifier,order)))
        rows=conn.execute(query, args+[batch_size]).fetchall()
        if not rows:
            yield state(checkpoint.get('cursor'),checkpoint.get('primary_key'),True,schema_hash(found)); break
        for row in rows:
            yield record(stream,to_json(row),str(row[cursor]),to_json(key_value(row,keys)),int(time.time()*1000))
        last=rows[-1]; pk=to_json(key_value(last,keys))
        checkpoint={'cursor':str(last[cursor]),'primary_key':pk}; current=position(checkpoint)
        yield state(checkpoint['cursor'],checkpoint['primary_key'],False,schema_hash(found))
def main():
    a=arguments()
    if a.command=='spec': emit('SPEC',spec={'protocol':'meridian-connect/1','version':VERSION,'config':['host','port','dbname','user','password','sslmode'],'modes':['full_refresh','incremental'],'deletes':False,'composite_keys':True}); return
    if a.command=='health': emit('LOG',log={'level':'INFO','code':'healthy','version':VERSION}); return
    try:
        with connection(load(a.config)) as conn:
            conn.execute("SET statement_timeout='60s'")
            if a.command=='check': conn.execute('SELECT 1'); emit('CONNECTION_STATUS',connectionStatus={'status':'SUCCEEDED'})
            elif a.command=='discover': emit('CATALOG',catalog={'streams':discover(conn)})
            elif a.command=='read':
                for m in read(conn,load(a.catalog),load(a.state)): print(json.dumps(m),flush=True)
            else: raise ValueError('connector_failed')
    except Exception as e:
        code=str(e) if str(e) in ('schema_drift','connector_failed') else 'auth_failed' if 'password authentication' in str(e) else 'source_unavailable'
        emit('TRACE',trace={'type':'ERROR','code':code}); raise SystemExit(1)
if __name__=='__main__': main()
