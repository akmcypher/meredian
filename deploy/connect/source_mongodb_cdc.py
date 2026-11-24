"""MongoDB change-stream CDC actor (deploy/connect/README.md, "MongoDB change streams").

The first run (or a reset) records a change-stream resume token, copies the collection by _id,
then reads changes from that token. Each later run reads changes up to the cluster time captured
when it starts and stops there, so a run always ends. Inserts, updates and replaces land as upsert
records with the current document (updateLookup); deletes land as delete records keyed by _id.
Positions are change-stream resume tokens stored with each landed commit, never in MongoDB.
"""
import json
import time

import bson
from sdk import arguments, emit, load, record, state
import source_mongodb as mongodb

VERSION = 'mongodb-cdc-v1'
BATCH = 500
IDLE_POLLS = 3
AWAIT_MS = 500


class Stop(Exception):
    """A fail-closed stop: a safe error code plus a log line naming the reason."""
    def __init__(self, code, reason, **detail): super().__init__(code); self.reason = reason; self.detail = detail


def prerequisites(mongo):
    hello = mongo.admin.command('hello')
    # Change streams need a replica set or a sharded cluster (mongos).
    if not hello.get('setName') and hello.get('msg') != 'isdbgrid':
        raise Stop('connector_failed', 'cdc_replica_set_required')
    return hello


def token_time(token):
    """Cluster time encoded at the start of a resume token (KeyString type 130, Timestamp)."""
    data = (token or {}).get('_data')
    if not isinstance(data, str) or len(data) < 18 or data[:2] != '82': return None
    try: return bson.Timestamp(int(data[2:10], 16), int(data[10:18], 16))
    except ValueError: return None


def cluster_time(mongo, database):
    """The window's end: the deployment's latest write when the run starts."""
    hello = mongo.admin.command('hello')
    t = ((hello.get('lastWrite') or {}).get('opTime') or {}).get('ts') or hello.get('operationTime') \
        or (hello.get('$clusterTime') or {}).get('clusterTime')
    if not isinstance(t, bson.Timestamp): raise Stop('connector_failed', 'cdc_cluster_time')
    return t


def watch(collection, **kwargs):
    return collection.watch(full_document='updateLookup', max_await_time_ms=AWAIT_MS, batch_size=BATCH, **kwargs)


def read(mongo, stream, checkpoint, config, max_changes=100000):
    if stream['sync_mode'] not in ('cdc', 'full_refresh'):
        raise Stop('connector_failed', 'cdc_stream_mode', sync_mode=stream['sync_mode'])
    if stream['namespace'] != config['database'] or not mongodb.IDENT.fullmatch(stream['name']): raise ValueError('schema_drift')
    keys = stream.get('primary_keys') or [stream.get('primary_key')]
    if list(keys) != ['_id']: raise ValueError('schema_drift')
    prerequisites(mongo)
    digest = mongodb.binding(config, {**stream, 'cursor_field': '_id', 'sync_mode': 'cdc'})
    coll = mongo[config['database']].get_collection(stream['name'])
    cdc = (checkpoint.get('source') or {}).get('cdc') or checkpoint.get('cdc')
    if cdc and checkpoint.get('schema_hash') not in (None, digest): raise ValueError('source_changed')
    if not cdc:
        # First run or reset: take the change-stream position first, then copy; changes made
        # during the copy are read from the stream afterwards (as upserts of the current document).
        with watch(coll) as s:
            s.try_next()
            token = s.resume_token
        if not token: raise Stop('connector_failed', 'cdc_resume_token')
        cdc = {'phase': 'snapshot', 'resume': mongodb.value(dict(token))}
        checkpoint = {}
    if not isinstance(cdc.get('resume'), dict) or cdc.get('phase') not in ('snapshot', 'stream'):
        raise Stop('source_changed', 'cdc_position_missing')
    if cdc['phase'] == 'snapshot':
        yield from copy(coll, stream, cdc, checkpoint.get('primary_key'), digest)
        cdc = {**cdc, 'phase': 'stream'}
        yield state(None, None, False, digest, source={'cdc': cdc})
    yield from changes(mongo, coll, stream, cdc, digest, config, max_changes)


def copy(coll, stream, cdc, after, digest, batch_size=BATCH):
    family = mongodb.family(stream, '_id')
    if coll.count_documents({'_id': {'$not': {'$type': family}}}, limit=1): raise ValueError('schema_drift')
    raw_coll = coll.with_options(codec_options=bson.CodecOptions(document_class=bson.raw_bson.RawBSONDocument, tz_aware=True,
                                 datetime_conversion=bson.DatetimeConversion.DATETIME_AUTO))
    current = None if after is None else mongodb.restore(after)
    while True:
        count = 0
        for raw in raw_coll.find({} if current is None else {'_id': {'$gt': current}}, sort=[('_id', 1)], limit=batch_size, batch_size=batch_size):
            if len(raw.raw) > mongodb.MAX_DOC_BYTES: raise ValueError('record_too_large')
            doc = bson.decode(raw.raw, codec_options=mongodb.RELAXED)
            data = mongodb.value(doc)
            yield record(stream, data, None, data['_id'], int(time.time()*1000), 'upsert')
            count += 1; current = doc['_id']
            if count % 100 == 0: yield state(None, data['_id'], False, digest, source={'cdc': cdc})
        if count: yield state(None, mongodb.value(current), False, digest, source={'cdc': cdc})
        if count < batch_size: return


def changes(mongo, coll, stream, cdc, digest, config, max_changes):
    from pymongo import errors
    end = cluster_time(mongo, config['database'])
    position = cdc['resume']; pending = 0; read_ = 0; idle = 0; skipped_updates = 0
    resume = mongodb.restore(position)
    try:
        with watch(coll, resume_after=resume) as s:
            while read_ < max_changes:
                event = s.try_next()
                if event is None:
                    at = token_time(s.resume_token)
                    # Caught up once the stream's own position has passed the window's end time.
                    if at is not None and at >= end: position = mongodb.value(dict(s.resume_token)); break
                    idle += 1
                    if at is None and idle >= IDLE_POLLS: break
                    continue
                idle = 0
                if event['clusterTime'] > end: break
                kind = event['operationType']
                if kind in ('drop', 'rename', 'dropDatabase', 'invalidate'):
                    raise Stop('source_changed', 'cdc_collection_'+kind)
                token = mongodb.value(dict(event['_id']))
                if kind in ('insert', 'update', 'replace'):
                    doc = event.get('fullDocument')
                    if doc is None:
                        # Deleted before the lookup: its delete event follows in this stream.
                        skipped_updates += 1
                    else:
                        raw = bson.encode(doc)
                        if len(raw) > mongodb.MAX_DOC_BYTES: raise ValueError('record_too_large')
                        data = mongodb.value(bson.decode(raw, codec_options=mongodb.RELAXED))
                        yield record(stream, data, None, data['_id'], int(time.time()*1000), 'upsert')
                        pending += 1; read_ += 1
                elif kind == 'delete':
                    key = mongodb.value(bson.decode(bson.encode(event['documentKey']), codec_options=mongodb.RELAXED))
                    yield record(stream, key, None, key['_id'], int(time.time()*1000), 'delete')
                    pending += 1; read_ += 1
                position = token
                if pending >= BATCH:
                    yield state(None, None, False, digest, source={'cdc': {**cdc, 'resume': position}}); pending = 0
    except errors.OperationFailure as error:
        if error.code in (286, 280) or 'ChangeStreamHistoryLost' in str(error) or 'resume' in str(error).lower():
            raise Stop('offset_out_of_range', 'cdc_history_lost') from None
        raise
    if skipped_updates:
        yield {'type': 'LOG', 'log': {'level': 'INFO', 'code': 'cdc_updates_superseded_by_delete', 'detail': [{'count': str(skipped_updates)}]}}
    yield state(None, None, True, digest, source={'cdc': {**cdc, 'resume': position}})


def main():
    args = arguments()
    if args.command == 'spec':
        emit('SPEC', spec={'protocol': 'meridian-connect/1', 'version': VERSION,
             'config': ['hosts', 'port', 'srv', 'database', 'user', 'password', 'auth_source',
                        'replica_set', 'direct_connection', 'ssl_ca'],
             'modes': ['cdc'], 'deletes': True, 'composite_keys': False}); return
    if args.command == 'health': emit('LOG', log={'level': 'INFO', 'code': 'healthy', 'version': VERSION}); return
    mongo = None
    try:
        import os
        config = load(args.config)
        if config.get('ssl_ca') and not os.path.isabs(config['ssl_ca']):
            config['ssl_ca'] = os.path.join(os.path.dirname(os.path.abspath(args.config)), config['ssl_ca'])
        mongo = mongodb.client(config)
        if args.command == 'check':
            prerequisites(mongo); mongo[config['database']].command('ping')
            emit('CONNECTION_STATUS', connectionStatus={'status': 'SUCCEEDED'})
        elif args.command == 'discover':
            streams, skipped = mongodb.discover(mongo, config)
            if skipped: emit('LOG', log={'level': 'WARN', 'code': 'collections_skipped', 'detail': [{'unsupported_names': str(skipped)}]})
            emit('CATALOG', catalog={'streams': streams})
        elif args.command == 'read':
            for message in read(mongo, load(args.catalog), load(args.state), config): print(json.dumps(message), flush=True)
        else: raise ValueError('connector_failed')
    except Stop as error:
        emit('LOG', log={'level': 'ERROR', 'code': error.reason, **error.detail})
        emit('TRACE', trace={'type': 'ERROR', 'code': str(error)}); raise SystemExit(1)
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
