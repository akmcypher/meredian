"""Kafka source actor (confluent-kafka, Apache-2.0).

Positions are the per-partition next offsets in STATE, which the control plane stores with each
landed commit; the actor assigns partitions explicitly and never commits to the consumer group,
so a crash, a rebalance elsewhere or a reset group cannot move Meridian's position. A run reads
up to the high watermarks captured at its start, then completes. Messages that cannot be decoded
become DEAD_LETTER messages and their position still advances; replay re-decodes them later.

Offsets leave the actor as decimal strings (positions, keys, gaps, dead letters), because the
control plane parses JSON with 64-bit floats; they are integers only inside this process.
Checkpoints carry the cluster and topic identity; a different topic (deleted and recreated), a
different cluster, a missing partition or a position above the high watermark fails closed with
source_changed until the owner runs an explicit reset.
"""
import base64, io, json, re, time, urllib.request, urllib.error
from confluent_kafka import Consumer, TopicPartition, KafkaError, KafkaException, TopicCollection
from confluent_kafka.admin import AdminClient
from sdk import arguments, load, emit, record, state, dead_letter, to_json
VERSION='kafka-v1'
NAMESPACE='kafka'
# Values up to MAX_VALUE become records; up to MAX_DEAD_LETTER they become dead letters with the
# full payload; anything larger fails the run (record_too_large), since it can be neither landed
# nor kept whole.
MAX_VALUE=1_000_000
MAX_DEAD_LETTER=1_100_000
KEY=['_partition','_offset']
PROPERTIES={'_partition':{'source_type':'kafka_partition','nullable':False},'_offset':{'source_type':'kafka_offset','nullable':False}}
def client_config(config):
    c={'bootstrap.servers':config['bootstrap_servers'],'client.id':'meridian-connect','socket.timeout.ms':10000}
    for ours,theirs in [('security_protocol','security.protocol'),('sasl_mechanism','sasl.mechanism'),('sasl_username','sasl.username'),('sasl_password','sasl.password'),('ssl_ca_location','ssl.ca.location')]:
        if config.get(ours): c[theirs]=config[ours]
    return c
def consumer(config):
    # group.id is required by librdkafka; offsets are never committed to it.
    return Consumer({**client_config(config),'group.id':config.get('group_id','meridian-connect'),'enable.auto.commit':False,'enable.auto.offset.store':False,'auto.offset.reset':'earliest','enable.partition.eof':True,'isolation.level':'read_committed'})
def stream_name(topic):
    name=re.sub(r'[^a-zA-Z0-9_]','_',topic)
    return ('t_'+name if not re.match(r'[a-zA-Z_]',name) else name)[:63]
class Registry:
    """Confluent-compatible schema registry client (Karapace, Apicurio ccompat, Confluent)."""
    def __init__(self, config):
        self.url=(config.get('schema_registry_url') or '').rstrip('/'); self.cache={}
        self.auth=None
        if config.get('schema_registry_username'):
            self.auth='Basic '+base64.b64encode(f"{config['schema_registry_username']}:{config.get('schema_registry_password','')}".encode()).decode()
    def get(self, path):
        req=urllib.request.Request(self.url+path,headers={'Accept':'application/vnd.schemaregistry.v1+json',**({'Authorization':self.auth} if self.auth else {})})
        with urllib.request.urlopen(req,timeout=10) as r: return json.load(r)
    def schema(self, schema_id):
        if schema_id not in self.cache:
            import fastavro
            try: found=self.get(f'/schemas/ids/{schema_id}')
            except urllib.error.HTTPError as e:
                if e.code==404: raise Decode('schema_not_found')
                raise
            if found.get('schemaType','AVRO')!='AVRO': raise Decode('deserialization_failed')
            self.cache[schema_id]=fastavro.parse_schema(json.loads(found['schema']))
        return self.cache[schema_id]
class Stop(Exception):
    """Fails the run with a safe code and a positions-only detail the agent reports."""
    def __init__(self, code, detail): super().__init__(code); self.code=code; self.detail=detail
class Decode(Exception):
    def __init__(self, code): super().__init__(code); self.code=code
def decoder(config, stream):
    registry=Registry(config) if config.get('schema_registry_url') else None
    reader=stream.get('schema',{}).get('x-avro-reader-schema')
    if reader:
        import fastavro; reader=fastavro.parse_schema(reader)
    def avro(raw, use_reader):
        import fastavro
        if registry is None: raise Decode('schema_not_found')
        writer=registry.schema(int.from_bytes(raw[1:5],'big'))
        try: return fastavro.schemaless_reader(io.BytesIO(raw[5:]),writer,reader if use_reader else None)
        except fastavro.read.SchemaResolutionError: raise Decode('schema_incompatible')
        except Exception: raise Decode('deserialization_failed')
    def decode(raw, use_reader=True):
        if raw is None: return None
        # Confluent wire format: magic byte 0, 4-byte schema id, Avro body.
        if len(raw)>5 and raw[0]==0: return avro(raw,use_reader)
        try: return json.loads(raw.decode('utf-8'))
        except Exception: raise Decode('deserialization_failed')
    def key(raw):
        # Keys stay text unless Avro-encoded; JSON-parsing b'1' would turn the key into a number.
        if raw is None: return None
        try: return to_json(avro(raw,False)) if len(raw)>5 and raw[0]==0 else raw.decode('utf-8')
        except (Decode,UnicodeDecodeError): return base64.b64encode(raw).decode()
    return decode,key
def debezium(value):
    """Unwraps Debezium change events (with or without the schema envelope)."""
    if isinstance(value,dict) and isinstance(value.get('payload'),dict) and 'op' in value['payload']: value=value['payload']
    if isinstance(value,dict) and value.get('op') in ('c','u','r','d') and ('after' in value or 'before' in value):
        return (value.get('before') or {}, 'delete') if value['op']=='d' else (value.get('after') or {}, 'upsert')
    return None
def size(raw_key, raw_value): return len(raw_key or b'')+len(raw_value or b'')
def to_record(stream, decode, key, raw_key, raw_value, partition, offset, timestamp):
    if size(raw_key,raw_value)>MAX_VALUE: raise Decode('record_too_large')
    value=decode(raw_value)
    operation='delete' if value is None else 'upsert'
    data=value if isinstance(value,dict) else {} if value is None else {'value':value}
    unwrapped=debezium(value)
    if unwrapped: data,operation=unwrapped
    data={**to_json(data),'_kafka':{'topic':stream['schema']['x-kafka-topic'],'partition':partition,'offset':str(offset),'timestamp':timestamp,'key':key(raw_key)}}
    return record(stream,data,f'{partition}:{offset}',[partition,str(offset)],int(time.time()*1000),operation)
def raw_payload(raw_key, raw_value, partition, offset, timestamp):
    # Never truncated: a dead letter keeps the whole record or the run stops (see MAX_DEAD_LETTER).
    b64=lambda b: None if b is None else base64.b64encode(b).decode()
    return {'key_b64':b64(raw_key),'value_b64':b64(raw_value),'timestamp':timestamp,'partition':partition,'offset':str(offset)}
def identity(config, c, topic):
    """Cluster id and topic id (a recreated topic gets a new id). Principals without Describe on
    the topic still read: the topic id is then unknown, and recreation is caught only through
    missing partitions or positions past the end."""
    cluster=c.list_topics(timeout=10).cluster_id
    # Keep the client referenced until the future resolves, or its handle is destroyed.
    admin=AdminClient(client_config(config))
    for attempt in range(4):
        try:
            described=admin.describe_topics(TopicCollection([topic]),request_timeout=10)[topic].result()
            return cluster,str(described.topic_id)
        except KafkaException as e:
            err=e.args[0] if e.args else None
            if isinstance(err,KafkaError) and err.code() in (KafkaError.TOPIC_AUTHORIZATION_FAILED,KafkaError.CLUSTER_AUTHORIZATION_FAILED):
                emit('LOG',log={'level':'WARN','code':'topic_identity_unavailable'})
                return cluster,None
            # A broker that just went away can fail the first request; others answer on retry.
            if attempt==3: raise
            time.sleep(1+attempt)
def discover(config):
    c=consumer(config)
    try: topics=c.list_topics(timeout=10).topics
    finally: c.close()
    registry=Registry(config) if config.get('schema_registry_url') else None
    subjects=set(registry.get('/subjects')) if registry else set()
    streams=[]
    for topic in sorted(topics):
        if topic.startswith('_') or topic.startswith('__'): continue
        schema={'type':'object','x-kafka-topic':topic,'x-value-format':'avro' if f'{topic}-value' in subjects else 'json','properties':PROPERTIES}
        streams.append({'namespace':NAMESPACE,'name':stream_name(topic),'json_schema':schema,'primary_key':KEY})
    return streams
def read(config, stream, checkpoint, batch_size=500, deadline_seconds=600):
    topic=stream['schema'].get('x-kafka-topic')
    if not topic or stream_name(topic)!=stream['name']: raise ValueError('schema_drift')
    decode,key=decoder(config,stream)
    c=consumer(config)
    try:
        meta=c.list_topics(topic,timeout=10).topics.get(topic)
        if meta is None or meta.error is not None: raise ValueError('schema_drift')
        cluster,topic_id=identity(config,c,topic)
        positions={int(p):int(o) for p,o in (checkpoint.get('positions') or {}).items()}
        high={}; low={}
        for p in sorted(meta.partitions): low[p],high[p]=c.get_watermark_offsets(TopicPartition(topic,p),timeout=10)
        # Same topic, same cluster, every saved partition present and no position past the end;
        # otherwise the saved positions describe some other data and must not be applied.
        changed=[]
        for field,current in (('cluster_id',cluster),('topic_id',topic_id)):
            if checkpoint.get(field) and current and checkpoint[field]!=current: changed.append({'reason':field,'saved':checkpoint[field],'current':current})
        for p in sorted(positions):
            if p not in high: changed.append({'reason':'partition_missing','partition':p,'offset':str(positions[p])})
            elif positions[p]>high[p]: changed.append({'reason':'offset_above_high','partition':p,'offset':str(positions[p]),'high':str(high[p])})
        if changed: raise Stop('source_changed',changed)
        # A saved position below the low watermark means retention deleted records this stream
        # never read. Fail closed unless the owner approved exactly this gap: same topic, same
        # stream generation, same partitions, saved offsets and low watermarks. If retention moved
        # on after the approval, the gap differs and a fresh approval is required.
        gaps=[{'partition':p,'offset':str(positions[p]),'low':str(low[p]),'gap':str(low[p]-positions[p])} for p in sorted(low) if p in positions and positions[p]<low[p]]
        if gaps:
            approval=checkpoint.get('gap_approval') or {}
            wanted=sorted((g['partition'],g['offset'],g['low']) for g in gaps)
            approved=sorted((int(g['partition']),str(g['offset']),str(g['low'])) for g in approval.get('partitions',[]))
            if not (approval.get('topic')==topic and str(approval.get('generation'))==str(stream.get('generation')) and approval.get('topic_id') in (None,topic_id) and wanted==approved):
                raise Stop('offset_out_of_range',gaps)
        # Accepted gaps travel in every checkpoint of this run, so commit history keeps them.
        accepted=[{'partition':g['partition'],'from':g['offset'],'to':g['low']} for g in gaps]
        def checkpoint_state(completed=False):
            return state(positions=positions,completed=completed,gaps=accepted or None,source={k:v for k,v in (('cluster_id',cluster),('topic_id',topic_id)) if v})
        assign=[]
        for p in sorted(low):
            start=max(positions.get(p,low[p]),low[p])
            positions[p]=start
            if start<high[p]: assign.append(TopicPartition(topic,p,start))
        pending={tp.partition for tp in assign}
        if assign: c.assign(assign)
        out=[]; deadline=time.time()+deadline_seconds
        while pending and time.time()<deadline:
            msg=c.poll(1.0)
            if msg is None: continue
            p=msg.partition()
            if msg.error():
                if msg.error().code()==KafkaError._PARTITION_EOF:
                    # End of the log: the position may move up to it, but never past the high
                    # watermark of this run (later messages were not read and are not skipped).
                    if p in pending: positions[p]=max(positions[p],min(msg.offset(),high[p])); pending.discard(p)
                    continue
                raise KafkaException(msg.error())
            if p not in pending: continue
            o=msg.offset()
            if o>=high[p]: pending.discard(p); continue
            ts=msg.timestamp()[1]
            if size(msg.key(),msg.value())>MAX_DEAD_LETTER: raise Stop('record_too_large',[{'partition':p,'offset':str(o),'bytes':str(size(msg.key(),msg.value()))}])
            try: out.append(to_record(stream,decode,key,msg.key(),msg.value(),p,o,ts))
            except Decode as e: out.append(dead_letter(stream,f'{topic_id or cluster}:{p}:{o}',e.code,raw_payload(msg.key(),msg.value(),p,o,ts)))
            positions[p]=o+1
            if positions[p]>=high[p]: pending.discard(p)
            if len(out)>=batch_size:
                yield from out; out=[]; yield checkpoint_state()
        yield from out
        yield checkpoint_state(True)
    finally: c.close()
def replay(config, stream):
    """Re-decodes stored dead letters with the stream's current schema settings. Resolved ones are
    committed; if any still fail, the run ends with replay_incomplete and the counts, after the
    resolved ones have landed, so they stay open for another replay instead of being dropped."""
    decode,key=decoder(config,stream); replayed=[]; unresolved=[]
    letters=stream.get('dead_letters',[])
    for d in letters:
        pl=d['payload']; raw=lambda f: None if pl.get(f) is None else base64.b64decode(pl[f])
        try: m=to_record(stream,decode,key,raw('key_b64'),raw('value_b64'),pl['partition'],int(pl['offset']),pl.get('timestamp'))
        except Decode: unresolved.append(d['id']); continue
        replayed.append(d['id']); yield m
    yield state(replayed=replayed,completed=True)
    # Deferred: selected letters the control plane did not inline (size budget); still open.
    deferred=int(stream.get('deferred') or 0)
    if unresolved or deferred: raise Stop('replay_incomplete',[{'selected':str(len(letters)+deferred),'replayed':str(len(replayed)),'still_failing':str(len(unresolved)),'deferred':str(deferred)}])
def main():
    a=arguments()
    if a.command=='spec': emit('SPEC',spec={'protocol':'meridian-connect/1','version':VERSION,'config':['bootstrap_servers','security_protocol','sasl_mechanism','sasl_username','sasl_password','ssl_ca_location','schema_registry_url','schema_registry_username','schema_registry_password','group_id'],'modes':['incremental'],'deletes':True,'positions':'per-partition offsets, stored by the control plane','formats':['json','debezium-json','confluent-avro']}); return
    if a.command=='health': emit('LOG',log={'level':'INFO','code':'healthy','version':VERSION}); return
    try:
        config=load(a.config)
        if a.command=='check':
            c=consumer(config)
            try: c.list_topics(timeout=10)
            finally: c.close()
            if config.get('schema_registry_url'): Registry(config).get('/subjects')
            emit('CONNECTION_STATUS',connectionStatus={'status':'SUCCEEDED'})
        elif a.command=='discover': emit('CATALOG',catalog={'streams':discover(config)})
        elif a.command in ('read','replay'):
            stream=load(a.catalog)
            messages=read(config,stream,load(a.state)) if a.command=='read' else replay(config,stream)
            for m in messages: print(json.dumps(m),flush=True)
        else: raise ValueError('connector_failed')
    except Stop as e:
        # Positions, identities and counts only: never record data.
        emit('LOG',log={'level':'ERROR','code':e.code,'detail':e.detail})
        emit('TRACE',trace={'type':'ERROR','code':e.code,'detail':e.detail}); raise SystemExit(1)
    except Exception as e:
        err=e.args[0] if isinstance(e,KafkaException) and e.args else None
        auth=(isinstance(err,KafkaError) and err.code() in (KafkaError._AUTHENTICATION,KafkaError.SASL_AUTHENTICATION_FAILED,KafkaError.TOPIC_AUTHORIZATION_FAILED)) or (isinstance(e,urllib.error.HTTPError) and e.code in (401,403))
        code=str(e) if str(e) in ('schema_drift','connector_failed') else 'auth_failed' if auth else 'source_unavailable'
        emit('TRACE',trace={'type':'ERROR','code':code}); raise SystemExit(1)
if __name__=='__main__': main()
