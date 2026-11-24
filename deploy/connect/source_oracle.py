"""Oracle Database batch/incremental actor; checkpoints acknowledge landed Bronze only.

Uses python-oracledb in thin mode (no Oracle Client libraries). Every session runs over TLS
(TCPS) with certificate and hostname verification; thin mode has no native network
encryption, so plain TCP is refused. Each read is one read-only transaction (a consistent
snapshot); pages seek on (cursor, key) with expanded composite predicates. See
deploy/connect/README.md, "Oracle".
"""
import decimal
import hashlib
import json
import os
import re
import ssl
import time

import oracledb
from sdk import arguments, emit, key_columns, key_value, load, record, state, to_json

VERSION = 'oracle-v1'
IDENT = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]{0,62}$')
SERVICE = re.compile(r'^[A-Za-z0-9_.-]{1,128}$')
MAX_ROW_BYTES = 512*1024
DATE = 'YYYY-MM-DD"T"HH24:MI:SS'
STAMP = 'YYYY-MM-DD"T"HH24:MI:SS.FF9'
STAMP_TZ = 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM'
# Base type -> (projection format, bind conversion) for temporal columns.
TEMPORAL = {'DATE': DATE, 'TIMESTAMP': STAMP, 'TIMESTAMP WITH TIME ZONE': STAMP_TZ,
            'TIMESTAMP WITH LOCAL TIME ZONE': STAMP}
LOBS = {'CLOB': 4, 'NCLOB': 4, 'BLOB': 1}
CODES = {'schema_drift', 'source_changed', 'record_too_large', 'connector_failed'}
AUTH_ERRORS = ('ORA-01017', 'ORA-28000', 'ORA-01045', 'DPY-6006', 'CERTIFICATE_VERIFY_FAILED')
SUPPORTED = set(TEMPORAL) | set(LOBS) | {'NUMBER', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE', 'VARCHAR2',
                                         'NVARCHAR2', 'CHAR', 'NCHAR', 'RAW'}
COMPARABLE = set(TEMPORAL) | {'NUMBER', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE', 'VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR', 'RAW'}
CATALOG = """SELECT c.owner, c.table_name, c.column_name, c.data_type, c.data_precision, c.data_scale,
 c.char_length, c.data_length, c.nullable, k.position AS key_position
 FROM all_tab_columns c JOIN all_tables t ON t.owner = c.owner AND t.table_name = c.table_name
 LEFT JOIN all_constraints p ON p.owner = c.owner AND p.table_name = c.table_name AND p.constraint_type = 'P'
 LEFT JOIN all_cons_columns k ON k.owner = p.owner AND k.constraint_name = p.constraint_name AND k.column_name = c.column_name
 WHERE c.owner = :owner AND t.temporary = 'N' AND t.nested = 'NO' AND t.secondary = 'N'
 ORDER BY c.table_name, c.column_id FETCH FIRST 10001 ROWS ONLY"""


def base_type(data_type):
    t = re.sub(r'\(\d+\)', '', data_type).strip()
    return 'TIMESTAMP WITH TIME ZONE' if t.startswith('TIMESTAMP') and 'WITH TIME ZONE' in t and 'LOCAL' not in t else \
        'TIMESTAMP WITH LOCAL TIME ZONE' if t.startswith('TIMESTAMP') and 'LOCAL' in t else \
        'TIMESTAMP' if t.startswith('TIMESTAMP') else t


def connection(config):
    for k in ('host', 'service_name', 'user'):
        if not isinstance(config.get(k), str) or not config[k].strip(): raise ValueError('connector_failed')
    if not SERVICE.fullmatch(config['service_name']) or any(c in config['host'] for c in '@/()= '):
        raise ValueError('connector_failed')
    port = config.get('port', 2484)
    if isinstance(port, bool) or not str(port).isdigit() or not 1 <= int(port) <= 65535: raise ValueError('connector_failed')
    context = ssl.create_default_context(cafile=config.get('ssl_ca') or None)
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    conn = oracledb.connect(user=config['user'], password=config.get('password', ''), host=config['host'].strip(),
                            port=int(port), service_name=config['service_name'], protocol='tcps',
                            ssl_context=context, ssl_server_dn_match=True, tcp_connect_timeout=10,
                            retry_count=0, expire_time=1)
    conn.call_timeout = 60000
    # Local-time-zone timestamps are read and compared in UTC on every machine. Seek
    # predicates and ORDER BY must agree: binary comparison and sorting, whatever the
    # instance's NLS defaults, and '.' as the decimal separator for checkpointed decimals.
    with conn.cursor() as c:
        c.execute("ALTER SESSION SET TIME_ZONE = '+00:00' NLS_COMP = 'BINARY' NLS_SORT = 'BINARY'"
                  " NLS_NUMERIC_CHARACTERS = '.,'")
    return conn


def query(conn, sql, args=None, limit=10001):
    with conn.cursor() as c:
        c.execute(sql, args or {})
        return [dict(zip([d[0].lower() for d in c.description], row)) for row in c.fetchmany(limit)]


def discover(conn, owner):
    if not IDENT.fullmatch(owner): raise ValueError('schema_drift')
    rows = query(conn, CATALOG, {'owner': owner.upper()})
    if len(rows) > 10000: raise ValueError('record_too_large')
    grouped, skipped = {}, set()
    for r in rows:
        if not IDENT.fullmatch(r['table_name']) or not IDENT.fullmatch(r['column_name']):
            skipped.add(r['table_name']); continue
        item = grouped.setdefault(r['table_name'], {'namespace': r['owner'], 'name': r['table_name'],
                                                     'json_schema': {'type': 'object', 'properties': {}}, 'keys': []})
        kind = base_type(r['data_type'])
        source = r['data_type']
        if kind == 'NUMBER' and r['data_precision'] is not None: source = f"NUMBER({r['data_precision']},{r['data_scale'] or 0})"
        elif kind in ('VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR'): source = f"{kind}({r['char_length']})"
        elif kind == 'RAW': source = f"RAW({r['data_length']})"
        item['json_schema']['properties'][r['column_name']] = {'source_type': source, 'sql_type': kind,
                                                               'nullable': r['nullable'] == 'Y'}
        if r['key_position']: item['keys'].append((r['key_position'], r['column_name']))
    out = []
    for name, item in grouped.items():
        if name in skipped: continue
        keys = [k for _, k in sorted(item.pop('keys'))]
        if keys: out.append({**item, 'primary_key': keys[0] if len(keys) == 1 else keys})
    return out, len(skipped)


def quote(name):
    if not isinstance(name, str) or not IDENT.fullmatch(name): raise ValueError('schema_drift')
    return '"'+name+'"'


def project(name, column):
    fmt = TEMPORAL.get(column['sql_type'])
    q = quote(name)
    return f"TO_CHAR({q}, '{fmt}') AS {q}" if fmt else q


def bind(name, column, index):
    """A seek parameter converted back to the column's own type on the server."""
    fmt = TEMPORAL.get(column['sql_type'])
    p = f':p{index}'
    if column['sql_type'] == 'DATE': return f"TO_DATE({p}, '{fmt}')"
    if column['sql_type'] == 'TIMESTAMP WITH TIME ZONE': return f"TO_TIMESTAMP_TZ({p}, '{fmt}')"
    if fmt: return f"TO_TIMESTAMP({p}, '{fmt}')"
    if column['sql_type'] == 'RAW': return f'HEXTORAW({p})'
    return p


def seek(order, values, columns):
    if len(order) != len(values): raise ValueError('source_changed')
    terms, args, n = [], {}, 0
    for index, name in enumerate(order):
        parts = []
        for k in order[:index]+[name]:
            args[f'p{n}'] = values[order.index(k)]
            parts.append(f'{quote(k)} {"=" if k != name else ">"} {bind(k, columns[k], n)}'); n += 1
        terms.append('('+' AND '.join(parts)+')')
    return ' OR '.join(terms), args


def schema_hash(found, config, stream):
    binding = {k: config.get(k) for k in ('host', 'service_name', 'user')}
    binding['port'] = int(config.get('port', 2484))
    # sync_mode stays out: a reset or backfill runs an incremental stream as full_refresh,
    # and the incremental runs after it continue from that checkpoint.
    binding.update({k: stream[k] for k in ('namespace', 'name', 'cursor_field')})
    binding['keys'] = key_columns(stream)
    return hashlib.sha256(json.dumps({'source': binding, 'schema': found['json_schema']}, sort_keys=True).encode()).hexdigest()


def value(v, column):
    """NUMBER arrives as Decimal: whole numbers land as integers (strings past 2^53),
    everything else as its exact decimal text."""
    if v is None: return None
    if isinstance(v, oracledb.LOB): v = v.read()
    if isinstance(v, decimal.Decimal) and v.is_finite() and v == v.to_integral_value(): v = int(v)
    return to_json(v)


def position(v, column):
    """Seek parameter for a landed value: RAW keys travel as hex, everything else as landed text/numbers."""
    if column['sql_type'] == 'RAW':
        import base64
        try: return base64.b64decode(v, validate=True).hex()
        except (ValueError, TypeError): raise ValueError('source_changed') from None
    return v


def read(conn, stream, checkpoint, config, batch_size=500):
    if type(batch_size) is not int or not 1 <= batch_size <= 1000: raise ValueError('connector_failed')
    if stream['sync_mode'] not in ('full_refresh', 'incremental'): raise ValueError('connector_failed')
    oracledb.defaults.fetch_decimals = True
    found = next((s for s in discover(conn, stream['namespace'])[0] if s['name'] == stream['name']), None)
    if not found or found['json_schema'] != stream['schema']: raise ValueError('schema_drift')
    columns = found['json_schema']['properties']; keys = key_columns(stream); cursor = stream['cursor_field']
    full = stream['sync_mode'] == 'full_refresh'
    order = keys if full else [cursor, *[k for k in keys if k != cursor]]
    # The cursor is checked and recorded in both modes, so a reset's checkpoint leaves the
    # next incremental run a usable position.
    if (any(k not in columns or columns[k]['nullable'] or columns[k]['sql_type'] not in COMPARABLE for k in {*order, cursor})
            or any(c['sql_type'] not in SUPPORTED for c in columns.values())):
        raise ValueError('schema_drift')
    digest = schema_hash(found, config, stream)
    if checkpoint.get('schema_hash') not in (None, digest): raise ValueError('source_changed')
    current = None
    if checkpoint.get('primary_key') is not None:
        if checkpoint.get('schema_hash') != digest: raise ValueError('source_changed')
        pk = checkpoint['primary_key']; pk = pk if isinstance(pk, list) else [pk]
        if len(pk) != len(keys) or (not full and checkpoint.get('cursor') is None): raise ValueError('source_changed')
        at = dict(zip(keys, pk))
        if not full: at[cursor] = checkpoint['cursor']
        current = [position(at[k], columns[k]) for k in order]
    table = quote(stream['namespace'])+'.'+quote(stream['name'])
    ordering = ','.join(map(quote, order))
    sizes = '+'.join(f"NVL(DBMS_LOB.GETLENGTH({quote(k)}),0)*{LOBS[c['sql_type']]}" if c['sql_type'] in LOBS
                     else f'NVL(VSIZE({quote(k)}),0)' for k, c in columns.items())
    projected = ','.join(project(k, v) for k, v in columns.items())
    header = ','.join(project(k, columns[k]) for k in order)
    with conn.cursor() as c: c.execute('SET TRANSACTION READ ONLY')
    try:
        last = {'cursor': checkpoint.get('cursor'), 'primary_key': checkpoint.get('primary_key')}
        while True:
            predicate, args = seek(order, current, columns) if current is not None else ('', {})
            suffix = f' FROM {table}' + (f' WHERE ({predicate})' if predicate else '') + f' ORDER BY {ordering} FETCH FIRST {batch_size} ROWS ONLY'
            # Sizes first, under the same read-only snapshot: large values are never fetched
            # before their row budget is checked.
            metadata = query(conn, f'SELECT {header},({sizes}) AS meridian_row_size' + suffix, args)
            if any(int(r['meridian_row_size']) > MAX_ROW_BYTES for r in metadata): raise ValueError('record_too_large')
            if not metadata: break
            count = 0
            with conn.cursor() as cur:
                cur.execute(f'SELECT {projected}' + suffix, args)
                names = [d[0] for d in cur.description]
                for row in cur:
                    data = {n: value(v, columns[n]) for n, v in zip(names, row)}
                    meta = metadata[count] if count < len(metadata) else None
                    if meta is None or any(value(meta[k.lower()], columns[k]) != data[k] for k in order):
                        raise ValueError('source_changed')
                    count += 1
                    cursor_value = data[cursor]
                    pk = key_value(data, keys)
                    yield record(stream, data, cursor_value, pk, int(time.time()*1000))
                    yield state(cursor_value, pk, False, digest)
                    last = {'cursor': cursor_value, 'primary_key': pk}
                    current = [position(data[k], columns[k]) for k in order]
            if count != len(metadata): raise ValueError('source_changed')
            if count < batch_size: break
        yield state(last['cursor'], last['primary_key'], True, digest)
    finally:
        conn.rollback()


def main():
    args = arguments()
    if args.command == 'spec':
        emit('SPEC', spec={'protocol': 'meridian-connect/1', 'version': VERSION,
             'config': ['host', 'port', 'service_name', 'user', 'password', 'schema', 'ssl_ca'],
             'modes': ['full_refresh', 'incremental'], 'deletes': False, 'composite_keys': True}); return
    if args.command == 'health': emit('LOG', log={'level': 'INFO', 'code': 'healthy', 'version': VERSION}); return
    conn = None
    try:
        config = load(args.config)
        if config.get('ssl_ca') and not os.path.isabs(config['ssl_ca']):
            config['ssl_ca'] = os.path.join(os.path.dirname(os.path.abspath(args.config)), config['ssl_ca'])
        conn = connection(config)
        if args.command == 'check':
            query(conn, 'SELECT 1 AS healthy FROM dual')
            emit('CONNECTION_STATUS', connectionStatus={'status': 'SUCCEEDED'})
        elif args.command == 'discover':
            streams, skipped = discover(conn, config.get('schema') or config['user'])
            if skipped: emit('LOG', log={'level': 'WARN', 'code': 'tables_skipped', 'detail': [{'unsupported_names': str(skipped)}]})
            emit('CATALOG', catalog={'streams': streams})
        elif args.command == 'read':
            for m in read(conn, load(args.catalog), load(args.state), config): print(json.dumps(m), flush=True)
        else: raise ValueError('connector_failed')
    except Exception as error:
        text = str(error)
        # Bad credentials, a locked account, an untrusted certificate or a name the
        # certificate does not carry are configuration failures, not outages.
        auth = isinstance(error, ssl.SSLError) or any(c in text for c in AUTH_ERRORS)
        code = text if text in CODES else 'auth_failed' if auth else 'source_unavailable'
        emit('TRACE', trace={'type': 'ERROR', 'code': code}); raise SystemExit(1)
    finally:
        if conn: conn.close()


if __name__ == '__main__': main()
