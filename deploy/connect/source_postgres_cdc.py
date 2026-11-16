"""PostgreSQL CDC actor (deploy/connect/README.md, "Change data capture"): logical replication through core SQL
functions. Each stream has its own slot; changes are decoded from `pgoutput` and land as upsert and
delete records in commit order. The checkpoint is a transaction's end LSN, stored with the fenced
commit; the next run first moves the slot to it."""
import hashlib, json, struct, time
from psycopg import connect, sql
from psycopg.pq import Format
from psycopg.rows import dict_row
from sdk import arguments, load, emit, record, state, key_columns, key_value, to_json
import source_postgres
VERSION='postgres-cdc-v1'
BATCH=500
NULL=object(); UNCHANGED=object()

class Stop(Exception):
    """A fail-closed stop: a safe error code plus a log line naming the reason."""
    def __init__(self, code, reason, **detail): super().__init__(code); self.reason=reason; self.detail=detail

def lsn_int(text):
    hi,lo=text.split('/'); return (int(hi,16)<<32)|int(lo,16)
def lsn_text(n): return f'{n>>32:X}/{n&0xFFFFFFFF:X}'
def slot_name(stream):
    """Recognisable and stable per stream; slot names allow [a-z0-9_], at most 63 bytes."""
    return 'meridian_'+hashlib.sha256(stream['id'].encode()).hexdigest()[:24]

class Reader:
    def __init__(self, data): self.b=bytes(data); self.i=0
    def take(self, fmt):
        v=struct.unpack_from(fmt,self.b,self.i); self.i+=struct.calcsize(fmt); return v[0]
    def string(self):
        j=self.b.index(0,self.i); v=self.b[self.i:j].decode(); self.i=j+1; return v
    def tuple(self):
        values=[]
        for _ in range(self.take('>h')):
            kind=chr(self.take('B'))
            if kind=='n': values.append(NULL)
            elif kind=='u': values.append(UNCHANGED)
            elif kind=='t': n=self.take('>i'); values.append(self.b[self.i:self.i+n]); self.i+=n
            else: raise Stop('connector_failed','cdc_unsupported_tuple',kind=kind)
        return values

def decode(data):
    """One pgoutput (protocol 1) message as (kind, fields). Unused kinds return their tag only."""
    r=Reader(data); kind=chr(r.take('B'))
    if kind=='B': return kind,{'final':r.take('>Q')}
    if kind=='C': r.take('B'); r.take('>Q'); return kind,{'end':r.take('>Q')}
    if kind=='R':
        oid=r.take('>I'); ns=r.string(); name=r.string(); r.take('B'); cols=[]
        for _ in range(r.take('>h')):
            r.take('B'); col=r.string(); typ=r.take('>I'); r.take('>i'); cols.append((col,typ))
        return kind,{'oid':oid,'namespace':ns,'name':name,'columns':cols}
    if kind=='I':
        oid=r.take('>I'); r.take('B'); return kind,{'oid':oid,'new':r.tuple()}
    if kind=='U':
        oid=r.take('>I'); tag=chr(r.take('B')); old=None
        full=tag=='O'
        if tag in 'KO': old=r.tuple(); tag=chr(r.take('B'))
        return kind,{'oid':oid,'full':full,'old':old,'new':r.tuple()}
    if kind=='D':
        oid=r.take('>I'); tag=chr(r.take('B')); return kind,{'oid':oid,'full':tag=='O','old':r.tuple()}
    if kind=='T':
        n=r.take('>i'); r.take('B'); return kind,{'oids':[r.take('>I') for _ in range(n)]}
    return kind,{}

class Rows:
    """Turns pgoutput text values into the Python values a SELECT returns, with psycopg's own loader
    per column type (domains through their base type), so a changed row lands with the same data
    as the snapshot copy."""
    def __init__(self, conn, columns):
        found=conn.execute("SELECT oid,typbasetype FROM pg_type WHERE typtype='d' AND oid=ANY(%s)",[[t for _,t in columns]]).fetchall()
        bases={r['oid']:r['typbasetype'] for r in found}
        self.names=[c for c,_ in columns]; self.loaders=[]
        for _,typ in columns:
            typ=bases.get(typ,typ); cls=conn.adapters.get_loader(typ,Format.TEXT)
            self.loaders.append(cls(typ,conn) if cls else None)
    def row(self, values, old=None):
        out={}; missing=[]
        for i,(name,value) in enumerate(zip(self.names,values)):
            if value is UNCHANGED:
                # An unchanged TOASTed value: known only from the old row (REPLICA IDENTITY FULL).
                value=old[i] if old is not None and old[i] is not UNCHANGED else UNCHANGED
                if value is UNCHANGED: missing.append(name); continue
            if value is NULL: out[name]=None; continue
            loader=self.loaders[i]
            out[name]=loader.load(value) if loader else bytes(value).decode()
        if missing: raise Stop('schema_drift','cdc_unchanged_value',columns=missing,remedy='ALTER TABLE ... REPLICA IDENTITY FULL')
        return out

def prerequisites(conn, publication):
    level=conn.execute('SHOW wal_level').fetchone()['wal_level']
    if level!='logical': raise Stop('connector_failed','cdc_wal_level',wal_level=level)
    role=conn.execute("""SELECT r.rolreplication OR r.rolsuper OR pg_has_role(r.oid,'rds_replication','member') AS ok
      FROM pg_roles r WHERE r.rolname=current_user""" if conn.execute("SELECT 1 FROM pg_roles WHERE rolname='rds_replication'").fetchone()
      else "SELECT rolreplication OR rolsuper AS ok FROM pg_roles WHERE rolname=current_user").fetchone()
    if not role['ok']: raise Stop('connector_failed','cdc_replication_privilege')
    if not conn.execute('SELECT 1 FROM pg_publication WHERE pubname=%s',[publication]).fetchone():
        raise Stop('connector_failed','cdc_publication_missing',publication=publication)

def read(conn, admin, stream, checkpoint, publication, max_changes=100000):
    if stream['sync_mode'] not in ('cdc','full_refresh'): raise Stop('connector_failed','cdc_stream_mode',sync_mode=stream['sync_mode'])
    found=next((s for s in source_postgres.discover(conn) if s['namespace']==stream['namespace'] and s['name']==stream['name']),None)
    if not found or found['json_schema']!=stream['schema']: raise ValueError('schema_drift')
    prerequisites(conn,publication)
    if not conn.execute('SELECT 1 FROM pg_publication_tables WHERE pubname=%s AND schemaname=%s AND tablename=%s',[publication,stream['namespace'],stream['name']]).fetchone():
        raise Stop('connector_failed','cdc_table_not_published',publication=publication)
    conn.commit()
    keys=key_columns(stream); schema=source_postgres.schema_hash(found)
    slot=slot_name(stream); cdc=checkpoint.get('cdc')
    def slot_lsn():
        r=admin.execute('SELECT confirmed_flush_lsn::text AS lsn FROM pg_replication_slots WHERE slot_name=%s',[slot]).fetchone()
        return r and r['lsn']
    if not cdc:
        # A first run, or a reset: nothing of this stream's log is committed. Start a new slot,
        # then copy the table; changes made during the copy are read from the slot afterwards.
        if slot_lsn() is not None: admin.execute('SELECT pg_drop_replication_slot(%s)',[slot])
        start=admin.execute("SELECT lsn::text AS lsn FROM pg_create_logical_replication_slot(%s,'pgoutput')",[slot]).fetchone()['lsn']
        cdc={'slot':slot,'phase':'snapshot','lsn':start}; checkpoint={}
    elif cdc.get('slot')!=slot or slot_lsn() is None:
        raise Stop('offset_out_of_range','cdc_slot_missing',slot=slot,lsn=cdc.get('lsn'))
    if cdc['phase']=='snapshot':
        yield from copy(conn,stream,keys,cdc,checkpoint.get('primary_key'),schema)
        cdc={**cdc,'phase':'stream'}
        yield state(None,None,False,schema,source={'cdc':cdc})
    yield from changes(conn,admin,stream,keys,cdc,publication,schema,max_changes)

def copy(conn, stream, keys, cdc, after, schema, batch_size=BATCH):
    table=sql.SQL('{}.{}').format(sql.Identifier(stream['namespace']),sql.Identifier(stream['name']))
    order=sql.SQL(',').join(map(sql.Identifier,keys))
    current=None if after is None else (after if isinstance(after,list) else [after])
    while True:
        where=sql.SQL('') if current is None else sql.SQL(' WHERE ({}) > ({})').format(order,sql.SQL(',').join(sql.Placeholder()*len(keys)))
        rows=conn.execute(sql.SQL('SELECT * FROM {}{} ORDER BY {} LIMIT %s').format(table,where,order),(current or [])+[batch_size]).fetchall()
        conn.commit()
        if not rows: return
        for row in rows: yield record(stream,to_json(row),None,to_json(key_value(row,keys)),int(time.time()*1000),'upsert')
        pk=to_json(key_value(rows[-1],keys)); current=pk if isinstance(pk,list) else [pk]
        yield state(None,pk,False,schema,source={'cdc':cdc})

def changes(conn, admin, stream, keys, cdc, publication, schema, max_changes):
    done=lsn_int(cdc['lsn'])
    # Move the slot to the committed position: what this stream has landed is never read again,
    # and the source can recycle that WAL. A slot is never moved backwards.
    current=admin.execute('SELECT confirmed_flush_lsn::text AS lsn FROM pg_replication_slots WHERE slot_name=%s',[cdc['slot']]).fetchone()['lsn']
    if lsn_int(current)<done: admin.execute('SELECT pg_replication_slot_advance(%s,%s::pg_lsn)',[cdc['slot'],cdc['lsn']])
    end=admin.execute('SELECT pg_current_wal_lsn()::text AS lsn').fetchone()['lsn']
    oid=conn.execute('SELECT to_regclass(%s)::oid AS oid',[sql.SQL('{}.{}').format(sql.Identifier(stream['namespace']),sql.Identifier(stream['name'])).as_string(conn)]).fetchone()['oid']
    expected=list(stream['schema']['properties'])
    rows=None; pending=0; last=done; skip=False
    # Reading many pending changes can take longer than the session's 60 s statement timeout.
    conn.execute("SET LOCAL statement_timeout='15min'")
    with conn.cursor(name='meridian_cdc') as cur:
        cur.itersize=1000
        cur.execute("SELECT data FROM pg_logical_slot_peek_binary_changes(%s,%s::pg_lsn,%s,'proto_version','1','publication_names',%s)",[cdc['slot'],end,max_changes,publication])
        for row in cur:
            kind,m=decode(row['data'])
            # A transaction that committed at or before the checkpoint has already landed.
            if kind=='B': skip=m['final']<done; continue
            if kind=='C':
                if skip: continue
                last=m['end']
                if pending>=BATCH:
                    yield state(None,None,False,schema,source={'cdc':{**cdc,'lsn':lsn_text(last)}}); pending=0
                continue
            if kind=='R':
                if m['oid']==oid:
                    if sorted(c for c,_ in m['columns'])!=sorted(expected): raise Stop('schema_drift','cdc_columns_changed')
                    rows=Rows(admin,m['columns'])
                continue
            if kind=='T' and oid in m['oids']: raise Stop('source_changed','cdc_truncate')
            if kind not in 'IUD' or m['oid']!=oid or skip: continue
            if rows is None: raise Stop('connector_failed','cdc_relation_unknown')
            if kind=='D':
                # The old row: every column with REPLICA IDENTITY FULL, otherwise the key only.
                old=rows.row(m['old'])
                data=old if m['full'] else {k:old[k] for k in keys}
                yield record(stream,to_json(data),None,to_json(key_value(old,keys)),int(time.time()*1000),'delete')
            else:
                new=rows.row(m['new'],m.get('old')); pk=to_json(key_value(new,keys))
                if kind=='U' and m['old'] is not None:
                    # The old row arrives when the key changed (or with REPLICA IDENTITY FULL); a
                    # changed key is a delete of the old key followed by the new row.
                    old=rows.row(m['old']); before=to_json(key_value(old,keys))
                    if before!=pk:
                        yield record(stream,to_json(old if m['full'] else {k:old[k] for k in keys}),None,before,int(time.time()*1000),'delete'); pending+=1
                yield record(stream,to_json(new),None,pk,int(time.time()*1000),'upsert')
            pending+=1
    conn.commit()
    yield state(None,None,True,schema,source={'cdc':{**cdc,'lsn':lsn_text(last)}})
    # Lag, after the checkpoint it follows: how many bytes of WAL the landed position is behind,
    # which is also roughly what this stream's slot keeps on the source.
    now=admin.execute('SELECT pg_current_wal_lsn()::text AS lsn').fetchone()['lsn']
    yield {'type':'LOG','log':{'level':'INFO','code':'partition_lag','detail':[{'partition':'0','position':str(last),'end':str(max(lsn_int(now),last))}]}}

def main():
    a=arguments()
    if a.command=='spec': emit('SPEC',spec={'protocol':'meridian-connect/1','version':VERSION,'config':['host','port','dbname','user','password','sslmode','publication'],'modes':['cdc'],'deletes':True,'composite_keys':True}); return
    if a.command=='health': emit('LOG',log={'level':'INFO','code':'healthy','version':VERSION}); return
    try:
        config=load(a.config); publication=config.pop('publication',None) or 'meridian'
        with source_postgres.connection(config) as conn, connect(**config,row_factory=dict_row,connect_timeout=10,autocommit=True) as admin:
            conn.execute("SET statement_timeout='60s'"); conn.commit()
            if a.command=='check': prerequisites(conn,publication); emit('CONNECTION_STATUS',connectionStatus={'status':'SUCCEEDED'})
            elif a.command=='discover': emit('CATALOG',catalog={'streams':source_postgres.discover(conn)})
            elif a.command=='read':
                for m in read(conn,admin,load(a.catalog),load(a.state),publication): print(json.dumps(m),flush=True)
            else: raise ValueError('connector_failed')
    except Stop as e:
        emit('LOG',log={'level':'ERROR','code':e.reason,**e.detail})
        emit('TRACE',trace={'type':'ERROR','code':str(e)}); raise SystemExit(1)
    except Exception as e:
        code=str(e) if str(e) in ('schema_drift','connector_failed') else 'auth_failed' if 'password authentication' in str(e) else 'source_unavailable'
        emit('TRACE',trace={'type':'ERROR','code':code}); raise SystemExit(1)
if __name__=='__main__': main()
