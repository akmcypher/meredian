"""MongoDB batch/incremental actor; checkpoints acknowledge landed Bronze only.

Documents land whole as MongoDB Extended JSON v2 (relaxed), except that integers outside
the JavaScript-safe range keep their canonical {"$numberLong": "..."} form, so no value
is rounded by the control plane. Collections are schemaless: discovery samples top-level
fields only so an owner can choose a cursor; new fields never fail a run. See
deploy/connect/README.md, "MongoDB".
"""
import datetime
import hashlib
import json
import math
import re
import time

import bson
from bson import json_util
from bson.raw_bson import RawBSONDocument
from sdk import arguments, emit, load, record, state

VERSION = 'mongodb-v1'
IDENT = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]{0,62}$')
SAFE_INT = 2**53-1
MAX_DOC_BYTES = 512*1024
SAMPLE = 100
MAX_COLLECTIONS = 1000
MAX_FIELDS = 500
# BSON type names as reported by $type, grouped into the families $gt/$lt compare within.
FAMILY = {'double': 'number', 'int': 'number', 'long': 'number', 'decimal': 'number',
          'string': 'string', 'objectId': 'objectId', 'date': 'date', 'timestamp': 'timestamp',
          'bool': 'bool', 'binData': 'binData'}
CURSOR_FAMILIES = {'number', 'string', 'objectId', 'date', 'timestamp'}
RELAXED = json_util.JSONOptions(json_mode=json_util.JSONMode.RELAXED, tz_aware=True,
                                tzinfo=datetime.timezone.utc,
                                datetime_conversion=bson.DatetimeConversion.DATETIME_AUTO)


def client(config):
    from pymongo import MongoClient
    for k in ('database', 'user'):
        if not isinstance(config.get(k), str) or not config[k].strip(): raise ValueError('connector_failed')
    hosts = config.get('hosts') or config.get('host')
    if not isinstance(hosts, str) or not hosts.strip() or any(c in hosts for c in '@/?#'):
        raise ValueError('connector_failed')
    kwargs = {}
    if config.get('srv'):
        if ',' in hosts or ':' in hosts: raise ValueError('connector_failed')
        target = 'mongodb+srv://'+hosts.strip()
    else:
        target = [h.strip() for h in hosts.split(',') if h.strip()]
        port = config.get('port', 27017)
        if isinstance(port, bool) or not str(port).isdigit() or not 1 <= int(port) <= 65535:
            raise ValueError('connector_failed')
        kwargs['port'] = int(port)
    if config.get('replica_set'): kwargs['replicaSet'] = str(config['replica_set'])
    if config.get('direct_connection') is not None: kwargs['directConnection'] = bool(config['direct_connection'])
    if config.get('ssl_ca'): kwargs['tlsCAFile'] = config['ssl_ca']
    # TLS with certificate and hostname verification is not optional; no "allow invalid" knobs
    # are read from the config.
    return MongoClient(target, username=config['user'], password=config.get('password', ''),
                       authSource=config.get('auth_source') or 'admin', tls=True,
                       serverSelectionTimeoutMS=10000, connectTimeoutMS=10000, socketTimeoutMS=60000,
                       appname='Meridian Connect', retryReads=True, readConcernLevel='majority',
                       tz_aware=True, datetime_conversion=bson.DatetimeConversion.DATETIME_AUTO,
                       document_class=dict, **kwargs)


def value(v):
    """Lossless JSON: relaxed Extended JSON v2 with safe integers kept as numbers."""
    if v is None or isinstance(v, (bool, str)): return v
    if isinstance(v, bson.int64.Int64) or isinstance(v, int):
        return int(v) if -SAFE_INT <= v <= SAFE_INT else {'$numberLong': str(int(v))}
    if isinstance(v, float):
        if math.isfinite(v): return v
        return {'$numberDouble': 'NaN' if math.isnan(v) else ('Infinity' if v > 0 else '-Infinity')}
    if isinstance(v, dict): return {str(k): value(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)): return [value(x) for x in v]
    # ObjectId, dates, Decimal128, binary/UUID, timestamps, regexes, MinKey/MaxKey, code:
    # json_util's relaxed form, which keeps the BSON type.
    return json.loads(json_util.dumps(v, json_options=RELAXED))


def restore(v):
    return json_util.loads(json.dumps(v), json_options=RELAXED)


def bson_type(v):
    if isinstance(v, bool): return 'bool'
    if isinstance(v, bson.int64.Int64): return 'long'
    if isinstance(v, int): return 'int' if -2**31 <= v < 2**31 else 'long'
    if isinstance(v, float): return 'double'
    if isinstance(v, str): return 'string'
    if isinstance(v, bson.ObjectId): return 'objectId'
    if isinstance(v, (datetime.datetime, bson.datetime_ms.DatetimeMS)): return 'date'
    if isinstance(v, bson.decimal128.Decimal128): return 'decimal'
    if isinstance(v, bson.timestamp.Timestamp): return 'timestamp'
    if isinstance(v, (bytes, bson.binary.Binary)): return 'binData'
    if v is None: return 'null'
    if isinstance(v, dict): return 'object'
    if isinstance(v, list): return 'array'
    return type(v).__name__


def discover(mongo, config):
    db = mongo[config['database']]
    names = sorted(n for n in db.list_collection_names(filter={'type': 'collection'}) if not n.startswith('system.'))
    if len(names) > MAX_COLLECTIONS: raise ValueError('record_too_large')
    if not IDENT.fullmatch(config['database']): raise ValueError('schema_drift')
    streams, skipped = [], 0
    for name in names:
        if not IDENT.fullmatch(name): skipped += 1; continue
        seen = {}
        sampled = 0
        for doc in db[name].aggregate([{'$sample': {'size': SAMPLE}}]):
            sampled += 1
            for k, v in doc.items():
                if IDENT.fullmatch(k) and (k in seen or len(seen) < MAX_FIELDS): seen.setdefault(k, set()).add(bson_type(v))
        props = {k: {'source_type': 'mongodb:'+'|'.join(sorted(t-{'null'})) if t-{'null'} else 'mongodb:null',
                     'nullable': 'null' in t or k != '_id'} for k, t in sorted(seen.items())}
        props.setdefault('_id', {'source_type': 'mongodb:objectId', 'nullable': False})
        props['_id']['nullable'] = False
        streams.append({'namespace': config['database'], 'name': name, 'primary_key': '_id',
                        'json_schema': {'type': 'object', 'properties': props,
                                        'x-mongodb': {'collection': name, 'sampled': sampled}}})
    return streams, skipped


def family(stream, field):
    found = stream['schema'].get('properties', {}).get(field, {}).get('source_type', '')
    types = set(found.removeprefix('mongodb:').split('|')) if found.startswith('mongodb:') else set()
    families = {FAMILY.get(t) for t in types}
    if len(families) != 1 or None in families: raise ValueError('schema_drift')
    return families.pop()


def binding(config, stream):
    source = {k: config.get(k) for k in ('hosts', 'host', 'port', 'srv', 'database', 'user', 'auth_source', 'replica_set')}
    # sync_mode stays out: a reset or backfill runs an incremental stream as full_refresh,
    # and the incremental runs after it continue from that checkpoint.
    source.update({k: stream[k] for k in ('namespace', 'name', 'cursor_field')})
    return hashlib.sha256(json.dumps(source, sort_keys=True, default=str).encode()).hexdigest()


def read(mongo, stream, checkpoint, config, batch_size=500):
    if type(batch_size) is not int or not 1 <= batch_size <= 1000: raise ValueError('connector_failed')
    if stream['namespace'] != config['database'] or not IDENT.fullmatch(stream['name']): raise ValueError('schema_drift')
    keys = stream.get('primary_keys') or [stream.get('primary_key')]
    if list(keys) != ['_id']: raise ValueError('schema_drift')
    full = stream['sync_mode'] == 'full_refresh'
    if stream['sync_mode'] not in ('full_refresh', 'incremental'): raise ValueError('connector_failed')
    # The cursor is validated and emitted in both modes, so a reset's checkpoint (a full
    # refresh ordered by _id) leaves the next incremental run a usable position.
    cursor = stream['cursor_field']
    if not isinstance(cursor, str) or not IDENT.fullmatch(cursor): raise ValueError('schema_drift')
    digest = binding(config, stream)
    if checkpoint.get('schema_hash') not in (None, digest): raise ValueError('source_changed')
    coll = mongo[config['database']].get_collection(stream['name'], codec_options=bson.CodecOptions(
        document_class=RawBSONDocument, tz_aware=True, datetime_conversion=bson.DatetimeConversion.DATETIME_AUTO))
    id_family = family(stream, '_id')
    order = ['_id'] if full or cursor == '_id' else [cursor, '_id']
    families = {cursor: family(stream, cursor), '_id': id_family}
    if families[cursor] not in CURSOR_FAMILIES or id_family not in CURSOR_FAMILIES: raise ValueError('schema_drift')
    # $gt/$lt only match values of the same BSON type family, so a document whose cursor or
    # _id is missing or of another type would never be read. Refuse instead of skipping it.
    alias = {'number': 'number', 'string': 'string', 'objectId': 'objectId', 'date': 'date', 'timestamp': 'timestamp'}
    stray = {'$or': [{k: {'$not': {'$type': alias[families[k]]}}} for k in families]}
    if coll.count_documents(stray, limit=1): raise ValueError('schema_drift')
    current = None
    if checkpoint.get('primary_key') is not None:
        if checkpoint.get('schema_hash') != digest: raise ValueError('source_changed')
        position = {'_id': checkpoint['primary_key']}
        if not full:
            if checkpoint.get('cursor') is None: raise ValueError('source_changed')
            position[cursor] = checkpoint['cursor']
        current = [restore(position[k]) for k in order]
    last = {'cursor': checkpoint.get('cursor'), 'primary_key': checkpoint.get('primary_key')}
    while True:
        if current is None: query = {}
        elif len(order) == 1: query = {order[0]: {'$gt': current[0]}}
        else: query = {'$or': [{order[0]: {'$gt': current[0]}}, {order[0]: current[0], '_id': {'$gt': current[1]}}]}
        count = 0
        for raw in coll.find(query, sort=[(k, 1) for k in order], limit=batch_size, batch_size=batch_size):
            if len(raw.raw) > MAX_DOC_BYTES: raise ValueError('record_too_large')
            doc = bson.decode(raw.raw, codec_options=RELAXED)
            count += 1
            data = value(doc)
            cursor_value = data[cursor]
            yield record(stream, data, cursor_value, data['_id'], int(time.time()*1000))
            yield state(cursor_value, data['_id'], False, digest)
            last = {'cursor': cursor_value, 'primary_key': data['_id']}
            current = [doc[k] for k in order]
        if count < batch_size: break
    yield state(last['cursor'], last['primary_key'], True, digest)


def main():
    args = arguments()
    if args.command == 'spec':
        emit('SPEC', spec={'protocol': 'meridian-connect/1', 'version': VERSION,
             'config': ['hosts', 'port', 'srv', 'database', 'user', 'password', 'auth_source',
                        'replica_set', 'direct_connection', 'ssl_ca'],
             'modes': ['full_refresh', 'incremental'], 'deletes': False, 'composite_keys': False}); return
    if args.command == 'health': emit('LOG', log={'level': 'INFO', 'code': 'healthy', 'version': VERSION}); return
    mongo = None
    try:
        import os
        config = load(args.config)
        if config.get('ssl_ca') and not os.path.isabs(config['ssl_ca']):
            config['ssl_ca'] = os.path.join(os.path.dirname(os.path.abspath(args.config)), config['ssl_ca'])
        mongo = client(config)
        if args.command == 'check':
            mongo[config['database']].command('ping')
            emit('CONNECTION_STATUS', connectionStatus={'status': 'SUCCEEDED'})
        elif args.command == 'discover':
            streams, skipped = discover(mongo, config)
            if skipped: emit('LOG', log={'level': 'WARN', 'code': 'collections_skipped', 'detail': [{'unsupported_names': str(skipped)}]})
            emit('CATALOG', catalog={'streams': streams})
        elif args.command == 'read':
            for message in read(mongo, load(args.catalog), load(args.state), config): print(json.dumps(message), flush=True)
        else: raise ValueError('connector_failed')
    except Exception as error:
        from pymongo import errors
        text = str(error)
        auth = (isinstance(error, errors.OperationFailure) and error.code in (13, 18)) or \
            any(s in text for s in ('CERTIFICATE_VERIFY_FAILED', "doesn't match", 'hostname', 'Authentication failed'))
        code = text if text in ('schema_drift', 'source_changed', 'record_too_large', 'connector_failed', 'auth_failed') \
            else 'connector_failed' if isinstance(error, errors.ConfigurationError) \
            else 'auth_failed' if auth else 'source_unavailable'
        emit('TRACE', trace={'type': 'ERROR', 'code': code}); raise SystemExit(1)
    finally:
        if mongo: mongo.close()


if __name__ == '__main__': main()
