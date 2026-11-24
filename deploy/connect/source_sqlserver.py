"""SQL Server batch/incremental actor; checkpoints acknowledge landed Bronze only."""
import base64
import hashlib
import json
import os
import re
import ssl
import tempfile
import time

import pytds
from pytds.extensions import ISOLATION_LEVEL_SNAPSHOT
from sdk import arguments, emit, key_columns, key_value, load, record, state, to_json

VERSION = 'sqlserver-v1'
IDENT = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]{0,62}$')
MAX_ROW_BYTES = 512*1024
TEMPORAL = {'date', 'time', 'datetime', 'smalldatetime', 'datetime2', 'datetimeoffset'}
BINARY = {'binary', 'varbinary', 'image', 'rowversion', 'timestamp'}
SUPPORTED = TEMPORAL | BINARY | {'tinyint', 'smallint', 'int', 'bigint', 'bit', 'decimal',
    'numeric', 'money', 'smallmoney', 'float', 'real', 'uniqueidentifier', 'char', 'varchar',
    'nchar', 'nvarchar', 'text', 'ntext', 'xml'}
CATALOG = """SELECT TOP (10001) s.name AS namespace,t.name AS table_name,c.name AS column_name,
 COALESCE(b.name,d.name) AS data_type,c.max_length,c.precision,c.scale,c.is_nullable,
 c.collation_name,ic.key_ordinal
 FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id
 JOIN sys.columns c ON c.object_id=t.object_id
 JOIN sys.types d ON d.user_type_id=c.user_type_id
 LEFT JOIN sys.types b ON b.user_type_id=c.system_type_id AND b.user_type_id=b.system_type_id
 LEFT JOIN sys.indexes i ON i.object_id=t.object_id AND i.is_primary_key=1
 LEFT JOIN sys.index_columns ic ON ic.object_id=t.object_id AND ic.index_id=i.index_id AND ic.column_id=c.column_id AND ic.key_ordinal>0
 WHERE t.is_ms_shipped=0 AND s.name NOT IN ('sys','INFORMATION_SCHEMA')
 AND (%s IS NULL OR s.name=%s) ORDER BY s.name,t.name,c.column_id"""


def _dns_matches(pattern, host):
    pattern = pattern.rstrip('.').lower()
    if pattern == host: return True
    # One wildcard, as the whole left-most label, never matching across dots or IPs.
    if not pattern.startswith('*.') or '*' in pattern[2:]: return False
    suffix = pattern[2:]
    label, _, rest = host.partition('.')
    return bool(label) and rest == suffix and suffix.count('.') >= 1


def validate_host(cert, name):
    """RFC 6125 host check. pytds 1.17.1's own check misparses SAN entries (lstrip of
    'DNS:'), never matches IP SANs and calls X509.get_extension, which pyOpenSSL 26
    removed. SANs, when present, replace the CN; an IP must match an IP SAN."""
    import ipaddress
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    host = (name.decode('ascii') if isinstance(name, bytes) else name).strip('[]').rstrip('.').lower()
    parsed = cert.to_cryptography()
    try: ip = ipaddress.ip_address(host)
    except ValueError: ip = None
    try: san = parsed.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    except x509.ExtensionNotFound: san = None
    if ip is not None:
        return san is not None and ip in san.get_values_for_type(x509.IPAddress)
    if san is not None:
        return any(_dns_matches(p, host) for p in san.get_values_for_type(x509.DNSName))
    return any(_dns_matches(a.value, host) for a in parsed.subject.get_attributes_for_oid(NameOID.COMMON_NAME))


# establish_channel looks this name up at call time.
pytds.tls.validate_host = validate_host


def connection(config):
    if any(not isinstance(config.get(k), str) or not config[k].strip() for k in ('host','database','user')):
        raise ValueError('connector_failed')
    port = config.get('port', 1433)
    if isinstance(port, bool) or not str(port).isdigit() or not 1 <= int(port) <= 65535:
        raise ValueError('connector_failed')
    ca = config.get('ssl_ca') or ssl.get_default_verify_paths().cafile
    temporary = None
    try:
        if not ca:
            # Windows system trust is a certificate store, not a PEM file. The driver
            # requires a file during its TLS handshake; these are public CA certificates.
            roots = ssl.create_default_context().get_ca_certs(binary_form=True)
            if not roots: raise ValueError('auth_failed')
            with tempfile.NamedTemporaryFile('w', suffix='.pem', delete=False) as bundle:
                bundle.write(''.join(ssl.DER_cert_to_PEM_cert(cert) for cert in roots))
                temporary = bundle.name
            ca = temporary
        return pytds.connect(dsn=config['host'],port=int(port),database=config['database'],
            user=config['user'],password=config.get('password',''),cafile=ca,
            validate_host=True,enc_login_only=False,login_timeout=10,timeout=60,
            as_dict=True,autocommit=True,readonly=True,disable_connect_retry=True,
            appname='Meridian Connect')
    finally:
        if temporary: os.unlink(temporary)


def quote(name):
    if not isinstance(name, str) or not IDENT.fullmatch(name): raise ValueError('schema_drift')
    return '['+name+']'


def query(conn, sql, args=()):
    with conn.cursor() as cursor:
        cursor.execute(sql, args)
        return cursor.fetchmany(10001)


def rows(conn, sql, args=()):
    with conn.cursor() as cursor:
        cursor.execute(sql, args)
        while True:
            row = cursor.fetchone()
            if row is None: break
            yield row


def discover(conn, namespace=None):
    catalog = query(conn, CATALOG, (namespace, namespace))
    if len(catalog)>10000: raise ValueError('record_too_large')
    grouped = {}
    for row in catalog:
        if not all(IDENT.fullmatch(row[k]) for k in ('namespace','table_name','column_name')): continue
        item = grouped.setdefault((row['namespace'],row['table_name']), {
            'namespace': row['namespace'], 'name': row['table_name'],
            'json_schema': {'type':'object','properties':{}}, 'keys': []})
        kind = row['data_type']
        source_type = kind
        if kind in ('numeric','decimal'): source_type += f"({row['precision']},{row['scale']})"
        elif kind in ('datetime2','datetimeoffset','time'): source_type += f"({row['scale']})"
        elif kind in ('char','varchar','nchar','nvarchar','binary','varbinary'):
            length = row['max_length']
            if length>0 and kind in ('nchar','nvarchar'): length//=2
            source_type += '('+('max' if length<0 else str(length))+')'
        item['json_schema']['properties'][row['column_name']] = {'source_type':source_type,
            'sql_type':kind,'nullable':bool(row['is_nullable']),'max_length':row['max_length'],
            'collation':row['collation_name']}
        if row['key_ordinal']: item['keys'].append((row['key_ordinal'],row['column_name']))
    output = []
    for item in grouped.values():
        keys = [name for _,name in sorted(item.pop('keys'))]
        if keys: output.append({**item,'primary_key':keys[0] if len(keys)==1 else keys})
    return output


def schema_hash(found, config, stream):
    # Password/CA rotation does not change the source. Changing the endpoint,
    # database or login principal can change the visible rows and requires reset.
    binding = {k:config.get(k) for k in ('host','database','user')}
    binding['port'] = int(config.get('port',1433))
    # sync_mode stays out: a reset or backfill runs an incremental stream as full_refresh,
    # and the incremental runs after it continue from that checkpoint.
    binding.update({k:stream[k] for k in ('namespace','name','cursor_field')})
    binding['keys'] = key_columns(stream)
    return hashlib.sha256(json.dumps({'source':binding,'schema':found['json_schema']},sort_keys=True).encode()).hexdigest()


def project(name, column):
    q = quote(name)
    # Driver datetime values have microsecond precision; server ISO conversion keeps
    # SQL Server's seventh fractional digit intact for both data and seek positions.
    # Style 126 keeps a datetimeoffset's own offset; 127 would rewrite it to UTC (Z).
    return f'CONVERT(nvarchar(48),{q},126) AS {q}' if column['sql_type'] in TEMPORAL else q


COLLATION = re.compile(r'^[A-Za-z0-9_]{1,128}$')


def parameter(column):
    """A seek parameter compared under the column's own rules. Strings travel as nvarchar;
    against a char/varchar column that would convert the column to nvarchar, and under a
    SQL_* collation Unicode comparison orders differently from the column's ORDER BY
    (for example 'con' and 'co-op'), so seeks could skip rows."""
    if column.get('sql_type') not in ('char', 'varchar'): return '%s'
    collation = column.get('collation') or ''
    if not COLLATION.fullmatch(collation): raise ValueError('schema_drift')
    return f'CAST(%s COLLATE {collation} AS varchar(8000))'


def seek(order, values, columns):
    if len(order)!=len(values): raise ValueError('source_changed')
    terms, args = [], []
    for index, name in enumerate(order):
        terms.append('('+' AND '.join([f'{quote(k)} = {parameter(columns[k])}' for k in order[:index]]
                                      +[f'{quote(name)} > {parameter(columns[name])}'])+')')
        args.extend(values[:index+1])
    # The driver sends plain bytes as nvarchar; binary keys and rowversion cursors must
    # travel as varbinary to compare correctly.
    return ' OR '.join(terms), [pytds.Binary(v) if isinstance(v,(bytes,bytearray)) else v for v in args]


def restore(value, column):
    if column['sql_type'] in BINARY:
        try: return base64.b64decode(value, validate=True)
        except (ValueError,TypeError): raise ValueError('source_changed') from None
    return value


def size_column(columns):
    name = '__meridian_row_size'
    while name in columns: name+='x'
    return name


def read(conn, stream, checkpoint, config, batch_size=500):
    if type(batch_size) is not int or not 1<=batch_size<=1000: raise ValueError('connector_failed')
    old_auto, old_isolation = conn.autocommit, conn.isolation_level
    conn.isolation_level = ISOLATION_LEVEL_SNAPSHOT
    conn.autocommit = False
    try:
        found = next((s for s in discover(conn,stream['namespace']) if s['name']==stream['name']),None)
        if not found or found['json_schema']!=stream['schema']: raise ValueError('schema_drift')
        columns = found['json_schema']['properties']; keys = key_columns(stream); cursor = stream['cursor_field']
        if (cursor not in columns or columns[cursor]['nullable'] or
                any(k not in columns or columns[k]['nullable'] for k in keys) or
                any(c['sql_type'] not in SUPPORTED for c in columns.values()) or
                columns[cursor].get('max_length',0)<0 or columns[cursor].get('max_length',0)>4096):
            raise ValueError('schema_drift')
        full = stream['sync_mode']=='full_refresh'
        if stream['sync_mode'] not in ('full_refresh','incremental'): raise ValueError('connector_failed')
        order = keys if full else [cursor,*[k for k in keys if k!=cursor]]
        digest = schema_hash(found,config,stream)
        if checkpoint.get('schema_hash') not in (None,digest): raise ValueError('source_changed')
        if checkpoint.get('primary_key') is not None and checkpoint.get('schema_hash')!=digest:
            raise ValueError('source_changed')
        current = None
        if checkpoint.get('primary_key') is not None:
            pk = checkpoint['primary_key']
            pk = pk if isinstance(pk,list) else [pk]
            if len(pk)!=len(keys) or (not full and checkpoint.get('cursor') is None): raise ValueError('source_changed')
            position = dict(zip(keys,pk))
            if not full: position[cursor]=checkpoint['cursor']
            current = [restore(position[k],columns[k]) for k in order]
        table = quote(stream['namespace'])+'.'+quote(stream['name'])
        ordering = ','.join(map(quote,order)); marker = size_column(columns)
        sizes = '+'.join(f'CONVERT(bigint,COALESCE(DATALENGTH({quote(k)}),0))' for k in columns)
        projected = ','.join(project(k,v) for k,v in columns.items())
        while True:
            predicate, values = seek(order,current,columns) if current is not None else ('',[])
            where = ' WHERE ('+predicate+')' if predicate else ''
            suffix = f' FROM {table}{where} ORDER BY {ordering}'
            # Read bounded key/size metadata first under the SAME snapshot. Large
            # values never reach the TDS decoder before their row budget is checked.
            header = ','.join(project(k,columns[k]) for k in order)
            metadata = query(conn,f'SELECT TOP (%s) {header},({sizes}) AS {quote(marker)}'+suffix,[batch_size,*values])
            if any(row[marker]>MAX_ROW_BYTES for row in metadata): raise ValueError('record_too_large')
            if not metadata: break
            count = 0
            for row in rows(conn,f'SELECT TOP (%s) {projected}'+suffix,[batch_size,*values]):
                if count>=len(metadata) or any(to_json(row[k])!=to_json(metadata[count][k]) for k in order):
                    raise ValueError('source_changed')
                count+=1
                cursor_value, pk_value = to_json(row[cursor]), to_json(key_value(row,keys))
                yield record(stream,to_json(row),cursor_value,pk_value,int(time.time()*1000))
                yield state(cursor_value,pk_value,False,digest)
                checkpoint = {'cursor':cursor_value,'primary_key':pk_value}
                current = [row[k] for k in order]
            if count!=len(metadata): raise ValueError('source_changed')
            if count<batch_size: break
        yield state(checkpoint.get('cursor'),checkpoint.get('primary_key'),True,digest)
    finally:
        try: conn.rollback()
        finally:
            conn.autocommit = old_auto
            conn.isolation_level = old_isolation


def main():
    args = arguments()
    if args.command=='spec':
        emit('SPEC',spec={'protocol':'meridian-connect/1','version':VERSION,
            'config':['host','port','database','user','password','ssl_ca'],
            'modes':['full_refresh','incremental'],'deletes':False,'composite_keys':True,
            'snapshot_isolation_required':True}); return
    if args.command=='health': emit('LOG',log={'level':'INFO','code':'healthy','version':VERSION}); return
    conn = None
    try:
        config = load(args.config)
        if config.get('ssl_ca') and not os.path.isabs(config['ssl_ca']):
            config['ssl_ca'] = str(os.path.join(os.path.dirname(os.path.abspath(args.config)),config['ssl_ca']))
        conn = connection(config)
        if args.command=='check': query(conn,'SELECT 1 AS healthy'); emit('CONNECTION_STATUS',connectionStatus={'status':'SUCCEEDED'})
        elif args.command=='discover': emit('CATALOG',catalog={'streams':discover(conn)})
        elif args.command=='read':
            for message in read(conn,load(args.catalog),load(args.state),config): print(json.dumps(message),flush=True)
        else: raise ValueError('connector_failed')
    except Exception as error:
        number = getattr(error,'number',None) or getattr(error,'msg_no',None)
        auth = number in (18456,18452,4060,229) or isinstance(error,(ssl.SSLError,FileNotFoundError))
        if 'certificate' in str(error).lower() or 'tls' in str(error).lower(): auth=True
        code = str(error) if str(error) in ('schema_drift','source_changed','record_too_large','connector_failed','auth_failed') else 'auth_failed' if auth else 'source_unavailable'
        emit('TRACE',trace={'type':'ERROR','code':code}); raise SystemExit(1)
    finally:
        if conn: conn.close()


if __name__=='__main__': main()
