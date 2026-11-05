import json, os, urllib.request, urllib.error, time
from sdk import arguments, load, emit, validate
VERSION='bronze-v1'
# Rejections the control plane names; retrying the identical payload cannot change them.
FINAL={'dead_letter_budget','record_too_large'}
def post(config,payload):
    token=open(config['token_file']).read().strip()
    req=urllib.request.Request(config['url'].rstrip('/')+'/api/public/connect-agent',data=json.dumps(payload).encode(),headers={'Content-Type':'application/json','x-ingest-token':token})
    try:
        with urllib.request.urlopen(req,timeout=60) as response: return json.load(response)
    except urllib.error.HTTPError as e:
        try: code=json.load(e).get('error')
        except Exception: code=None
        if code in FINAL: raise ValueError(code) from None
        raise
# Commits stay well under the control plane's 8 MiB request and landing-object limits. Requests
# carry records and base64 dead letters; the landed object repeats the stream schema and envelope
# fields for every record, so both are budgeted. 1000 is the protocol's per-commit count limit.
BUDGET=6*1024*1024
MAX_ITEMS=1000
ENVELOPE=512
def commit(config, seq, records, dead, state):
    payload={'action':'commit','runId':config['run_id'],'token':config['lease_token'],'streamId':config['stream_id'],'sequence':seq,'records':records,'state':state}
    # Dead letters ride in the same commit as the position that skips them.
    if dead: payload['dead_letters']=dead
    # Replay the identical payload on network/response loss. State emitted only after ACK.
    for attempt in range(3):
        try: return post(config,payload)['sequence']
        except ValueError: raise
        except Exception:
            if attempt==2: raise
            time.sleep(2**attempt)
def write(config, messages):
    """Commits RECORD/DEAD_LETTER messages at each STATE. When the messages before a STATE exceed
    the byte or count budget they are committed in segments that carry the last committed
    checkpoint, so the position moves only with the final segment, after every earlier segment of
    that checkpoint has landed. A crash between segments re-reads them (at-least-once). A single
    message larger than the budget fails with record_too_large instead of being split or cut."""
    seq=config['sequence']
    schema_bytes=len(json.dumps(config.get('schema') or {}))
    last=dict(config.get('state') or {}); last['version']=1; last['completed']=False
    held={'type':'STATE','state':last}
    records=[]; dead=[]; request=0; landed=0
    for message in messages:
        validate(message)
        if message['type'] in ('RECORD','DEAD_LETTER'):
            size=len(json.dumps(message,separators=(',',':')))
            land=size+schema_bytes+ENVELOPE if message['type']=='RECORD' else 0
            if size>BUDGET or land>BUDGET: raise ValueError('record_too_large')
            if request+size>BUDGET or landed+land>BUDGET or len(records)+len(dead)>=MAX_ITEMS:
                seq=commit(config,seq,records,dead,held); records=[]; dead=[]; request=0; landed=0
            (records if message['type']=='RECORD' else dead).append(message)
            request+=size; landed+=land
        elif message['type']=='STATE':
            seq=commit(config,seq,records,dead,message); records=[]; dead=[]; request=0; landed=0
            held={'type':'STATE','state':{**message['state'],'completed':False}}
            yield message
    if records or dead: raise ValueError('missing_state')
def main():
    a=arguments()
    if a.command=='spec': emit('SPEC',spec={'protocol':'meridian-connect/1','version':VERSION,'modes':['append'],'durability':'at-least-once extraction; idempotent committed batch sequence'}); return
    config=load(a.config)
    try:
        if a.command=='write':
            import sys
            for message in write(config,(json.loads(line) for line in sys.stdin if line.strip())): print(json.dumps(message),flush=True)
        elif a.command=='check': post(config,{'action':'heartbeat','runId':config['run_id'],'token':config['lease_token']}); emit('CONNECTION_STATUS',connectionStatus={'status':'SUCCEEDED'})
    except Exception: emit('TRACE',trace={'type':'ERROR','code':'destination_failed'}); raise SystemExit(1)
if __name__=='__main__':main()
