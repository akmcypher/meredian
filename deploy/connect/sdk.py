"""Meridian Connect v1 STDIO SDK. Airbyte-shaped subset; no Airbyte implementation code."""
import json, sys, argparse
PROTOCOL = 'meridian-connect/1'
DEAD_LETTER_CODES = {'deserialization_failed','schema_not_found','schema_incompatible','record_too_large','invalid_record'}
def emit(kind, **payload):
    print(json.dumps({'type': kind, **payload}, separators=(',', ':'), default=str), flush=True)
def arguments():
    p=argparse.ArgumentParser(); p.add_argument('command',choices=['spec','check','discover','read','replay','write','health']); p.add_argument('--config'); p.add_argument('--catalog'); p.add_argument('--state'); return p.parse_args()
def load(path):
    if not path: return {}
    with open(path) as f: return json.load(f)
def key_columns(stream):
    """Composite keys arrive as primary_keys; single-column v1 streams only as primary_key."""
    keys=stream.get('primary_keys') or stream['primary_key']
    return list(keys) if isinstance(keys,(list,tuple)) else [keys]
def key_value(row, keys):
    return row[keys[0]] if len(keys)==1 else [row[k] for k in keys]
SAFE_INT=2**53-1
def to_json(value):
    """Lossless JSON for source values: the control plane parses with JavaScript numbers, so
    integers past 2^53, decimals and non-finite floats travel as strings."""
    import base64, datetime, decimal, math, uuid
    if isinstance(value,bool) or value is None or isinstance(value,str): return value
    if isinstance(value,int): return value if -SAFE_INT<=value<=SAFE_INT else str(value)
    if isinstance(value,float): return value if math.isfinite(value) else str(value)
    if isinstance(value,decimal.Decimal): return str(value)
    if isinstance(value,(datetime.datetime,datetime.date,datetime.time)): return value.isoformat()
    if isinstance(value,datetime.timedelta): return str(value.total_seconds())
    if isinstance(value,(bytes,bytearray,memoryview)): return base64.b64encode(bytes(value)).decode()
    if isinstance(value,uuid.UUID): return str(value)
    if isinstance(value,dict): return {str(k):to_json(v) for k,v in value.items()}
    if isinstance(value,(list,tuple,set)): return [to_json(v) for v in value]
    return str(value)
def record(stream, data, cursor, pk, emitted_at, operation=None):
    r={'stream':stream['name'],'namespace':stream['namespace'],'data':data,'cursor':cursor,'primary_key':pk,'emitted_at':emitted_at}
    if operation: r['operation']=operation
    return {'type':'RECORD','record':r}
def state(cursor=None, pk=None, completed=False, schema_hash=None, positions=None, replayed=None, gaps=None, source=None):
    s={'version':1,'cursor':cursor,'primary_key':pk,'completed':completed}
    # The control plane accepts an absent hash, not null (actors without a schema, e.g. Kafka).
    if schema_hash is not None: s['schema_hash']=schema_hash
    # Offsets as decimal strings: the control plane would round integers past 2^53.
    if positions is not None: s['positions']={str(k):str(v) for k,v in positions.items()}
    if replayed is not None: s['replayed']=replayed
    # Offset ranges retention deleted before they were read, accepted by the owner for this run.
    if gaps: s['gaps']=gaps
    # Identity of the data the positions belong to (e.g. Kafka cluster and topic id).
    if source: s.update(source)
    return {'type':'STATE','state':s}
def dead_letter(stream, position, code, payload):
    if code not in DEAD_LETTER_CODES: raise ValueError('invalid_protocol')
    return {'type':'DEAD_LETTER','dead_letter':{'stream':stream['name'],'namespace':stream['namespace'],'position':str(position),'error_code':code,'payload':payload}}
def validate(message):
    if message.get('type') not in ['RECORD','STATE','DEAD_LETTER','SPEC','CONNECTION_STATUS','CATALOG','LOG','TRACE']: raise ValueError('invalid_protocol')
    if message['type']=='RECORD' and (not isinstance(message.get('record',{}).get('data'),dict) or not message['record'].get('stream')): raise ValueError('invalid_record')
    if message['type']=='RECORD' and message['record'].get('operation') not in (None,'upsert','delete'): raise ValueError('invalid_record')
    if message['type']=='DEAD_LETTER' and message.get('dead_letter',{}).get('error_code') not in DEAD_LETTER_CODES: raise ValueError('invalid_protocol')
    return message
