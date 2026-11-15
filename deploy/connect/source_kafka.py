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
import base64, io, json, os, re, ssl, time, urllib.request, urllib.error
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
# Spark chunk reads (chunk()): seconds without any message before the read fails.
STALL=60
KEY=['_partition','_offset']
PROPERTIES={'_partition':{'source_type':'kafka_partition','nullable':False},'_offset':{'source_type':'kafka_offset','nullable':False}}
# Connection security settings the actor accepts (config key -> librdkafka property). Nothing else
# from the config reaches the client, so a config cannot switch on unrelated client behaviour.
SECURITY={'security_protocol':'security.protocol','sasl_mechanism':'sasl.mechanism','sasl_username':'sasl.username','sasl_password':'sasl.password',
  'ssl_ca_location':'ssl.ca.location','ssl_ca_pem':'ssl.ca.pem','ssl_certificate_location':'ssl.certificate.location','ssl_certificate_pem':'ssl.certificate.pem',
  'ssl_key_location':'ssl.key.location','ssl_key_pem':'ssl.key.pem','ssl_key_password':'ssl.key.password','ssl_keystore_location':'ssl.keystore.location',
  'ssl_keystore_password':'ssl.keystore.password','ssl_endpoint_identification_algorithm':'ssl.endpoint.identification.algorithm'}
PROTOCOLS={'PLAINTEXT','SSL','SASL_PLAINTEXT','SASL_SSL'}
MECHANISMS={'PLAIN','SCRAM-SHA-256','SCRAM-SHA-512'}
# File settings may name a file next to the config (in the private secrets directory).
FILES={'ssl_ca_location','ssl_certificate_location','ssl_key_location','ssl_keystore_location','schema_registry_ca_location'}
def resolve_files(config, base):
    return {k:(os.path.join(base,v) if k in FILES and isinstance(v,str) and v and not os.path.isabs(v) else v) for k,v in config.items()}
# Managed Kafka services (Phase 3 K4). A profile fills in the security settings the service needs
# and refuses ones that contradict it, so an owner supplies only the bootstrap address and the
# service's own credential. Managed services always use TLS with hostname verification.
PROFILES={
  # API key / secret over SASL_SSL PLAIN.
  'confluent_cloud':{'fixed':{'security_protocol':'SASL_SSL','sasl_mechanism':'PLAIN'},'aliases':{'api_key':'sasl_username','api_secret':'sasl_password'}},
  # Event Hubs' Kafka endpoint (port 9093): the user name is literally $ConnectionString and the
  # password is the namespace or hub connection string.
  'azure_event_hubs':{'fixed':{'security_protocol':'SASL_SSL','sasl_mechanism':'PLAIN','sasl_username':'$ConnectionString'},'aliases':{'connection_string':'sasl_password'}},
  'aws_msk_scram':{'fixed':{'security_protocol':'SASL_SSL','sasl_mechanism':'SCRAM-SHA-512'},'aliases':{}},
  # IAM access control: short-lived SigV4 tokens over SASL OAUTHBEARER, signed with the AWS
  # default credential chain (environment, web identity / IRSA, container or instance role), an
  # optional role to assume, or explicit keys from the private config.
  'aws_msk_iam':{'fixed':{'security_protocol':'SASL_SSL','sasl_mechanism':'OAUTHBEARER'},'aliases':{}},
  'redpanda_cloud':{'fixed':{'security_protocol':'SASL_SSL'},'choices':{'sasl_mechanism':('SCRAM-SHA-256','SCRAM-SHA-512')},'aliases':{}},
  # Aiven: client certificates (SSL) or SCRAM over SASL_SSL.
  'aiven':{'fixed':{},'choices':{'security_protocol':('SSL','SASL_SSL')},'aliases':{}},
}
AWS_REGION=re.compile(r'^[a-z]{2}(-gov)?-[a-z]+-\d$')
def apply_profile(config):
    config={k:v for k,v in config.items() if not k.startswith('_')}
    name=config.get('profile')
    if name is None: return config
    profile=PROFILES.get(name)
    if profile is None: raise ValueError('connector_failed')
    for alias,target in profile['aliases'].items():
        if alias in config:
            if config.get(target) not in (None,config[alias]): raise ValueError('connector_failed')
            config[target]=config.pop(alias)
    for key,value in profile['fixed'].items():
        if config.get(key) not in (None,value): raise ValueError('connector_failed')
        config[key]=value
    for key,allowed in profile.get('choices',{}).items():
        config.setdefault(key,allowed[0])
        if config[key] not in allowed: raise ValueError('connector_failed')
    if config.get('ssl_endpoint_identification_algorithm')=='none' or config.get('allow_cleartext_password'): raise ValueError('connector_failed')
    if name=='azure_event_hubs' and not str(config.get('sasl_password') or '').startswith('Endpoint=sb://'): raise ValueError('connector_failed')
    if name=='aws_msk_iam':
        if not AWS_REGION.match(str(config.get('aws_region') or '')): raise ValueError('connector_failed')
        if bool(config.get('aws_access_key_id'))!=bool(config.get('aws_secret_access_key')): raise ValueError('connector_failed')
        config['_msk_iam']=True
    return config
def msk_iam_token(config):
    """oauth_cb for MSK IAM: returns (token, expiry in epoch seconds)."""
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    region=config['aws_region']
    def callback(_oauth_config):
        if config.get('aws_role_arn'):
            token,expiry_ms=MSKAuthTokenProvider.generate_auth_token_from_role_arn(region,config['aws_role_arn'])
        elif config.get('aws_access_key_id'):
            from botocore.credentials import CredentialProvider, Credentials
            class Fixed(CredentialProvider):
                METHOD='meridian-config'
                def load(self): return Credentials(config['aws_access_key_id'],config['aws_secret_access_key'],config.get('aws_session_token'))
            token,expiry_ms=MSKAuthTokenProvider.generate_auth_token_from_credentials_provider(region,Fixed())
        else:
            token,expiry_ms=MSKAuthTokenProvider.generate_auth_token(region)
        return token,expiry_ms/1000
    return callback
def client_config(config):
    protocol=(config.get('security_protocol') or 'PLAINTEXT').upper()
    mechanism=(config.get('sasl_mechanism') or '').upper()
    if protocol not in PROTOCOLS: raise ValueError('connector_failed')
    if mechanism=='OAUTHBEARER':
        # Only through the aws_msk_iam profile, which supplies the token callback.
        if not config.get('_msk_iam') or protocol!='SASL_SSL': raise ValueError('connector_failed')
    elif protocol.startswith('SASL'):
        if mechanism not in MECHANISMS or not config.get('sasl_username') or not config.get('sasl_password'): raise ValueError('connector_failed')
        # PLAIN without TLS sends the password in clear text: only when the owner says so explicitly.
        if protocol=='SASL_PLAINTEXT' and mechanism=='PLAIN' and config.get('allow_cleartext_password') is not True: raise ValueError('connector_failed')
    if config.get('ssl_endpoint_identification_algorithm') not in (None,'https','none'): raise ValueError('connector_failed')
    if protocol in ('SSL','SASL_SSL') and config.get('ssl_endpoint_identification_algorithm')=='none':
        emit('LOG',log={'level':'WARN','code':'tls_hostname_verification_disabled'})
    c={'bootstrap.servers':config['bootstrap_servers'],'client.id':'meridian-connect','socket.timeout.ms':10000}
    for ours,theirs in SECURITY.items():
        if config.get(ours): c[theirs]=config[ours]
    c['security.protocol']=protocol
    if mechanism: c['sasl.mechanism']=mechanism
    if protocol in ('SSL','SASL_SSL'):
        c.setdefault('ssl.endpoint.identification.algorithm','https')
        # Public CAs (managed services): use the system trust store when no CA is given. Windows
        # builds read the Windows store by default.
        if os.name!='nt' and not (config.get('ssl_ca_location') or config.get('ssl_ca_pem')): c['ssl.ca.location']='probe'
    if mechanism=='OAUTHBEARER': c['oauth_cb']=msk_iam_token(config)
    return c
class Errors:
    """Client-level errors (authentication, TLS) that librdkafka reports through error_cb while a
    metadata request only times out; they are raised in its place so the run says why."""
    def __init__(self): self.seen=None
    def __call__(self, err):
        if err.code() in (KafkaError._AUTHENTICATION,KafkaError.SASL_AUTHENTICATION_FAILED,KafkaError._SSL): self.seen=err
    def check(self, error):
        if self.seen is not None: raise KafkaException(self.seen) from error
        raise error
class Guarded:
    """A consumer whose failed metadata calls surface the authentication/TLS error behind them."""
    def __init__(self, c, errors): self.c=c; self.errors=errors
    def list_topics(self, *a, **k):
        try: return self.c.list_topics(*a,**k)
        except KafkaException as e:
            # error_cb runs only inside poll(); nothing is assigned yet, so poll reads no records.
            self.c.poll(0.5)
            self.errors.check(e)
    def __getattr__(self, name): return getattr(self.c,name)
def consumer(config):
    # group.id is required by librdkafka; offsets are never committed to it.
    errors=Errors()
    # Prefetch is capped (total, not per partition) so a fast topic cannot fill memory while the
    # destination is slow; a blocked stdout pipe then stops polling, which is the backpressure.
    prefetch=limits(config)['max_prefetch_kbytes']
    c=Consumer({**client_config(config),'error_cb':errors,'queued.max.messages.kbytes':prefetch,'fetch.max.bytes':8*1024*1024,'group.id':config.get('group_id','meridian-connect'),'enable.auto.commit':False,'enable.auto.offset.store':False,'auto.offset.reset':'earliest','enable.partition.eof':True,'isolation.level':'read_committed'})
    # OAUTHBEARER: the first token is fetched from poll(), before any metadata request.
    if config.get('_msk_iam'): c.poll(0)
    return Guarded(c,errors)
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
        # A private CA for the registry's TLS certificate; hostname verification always stays on.
        # Basic credentials are never sent over plain HTTP.
        self.tls=None
        if self.url.startswith('https://'):
            self.tls=ssl.create_default_context(cafile=config.get('schema_registry_ca_location') or None,cadata=config.get('schema_registry_ca_pem') or None)
        elif self.auth: raise ValueError('connector_failed')
    def get(self, path):
        req=urllib.request.Request(self.url+path,headers={'Accept':'application/vnd.schemaregistry.v1+json',**({'Authorization':self.auth} if self.auth else {})})
        with urllib.request.urlopen(req,timeout=10,context=self.tls) as r: return json.load(r)
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
    if config.get('_msk_iam'): admin.poll(0)
    for attempt in range(4):
        try:
            described=admin.describe_topics(TopicCollection([topic]),request_timeout=10)[topic].result()
            return cluster,str(described.topic_id)
        except KafkaException as e:
            err=e.args[0] if e.args else None
            # Managed endpoints (e.g. Event Hubs) may not implement DescribeTopics: same as no access.
            if isinstance(err,KafkaError) and err.code() in (KafkaError.TOPIC_AUTHORIZATION_FAILED,KafkaError.CLUSTER_AUTHORIZATION_FAILED,KafkaError.UNSUPPORTED_VERSION,KafkaError._UNSUPPORTED_FEATURE,KafkaError.INVALID_REQUEST):
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
def plan(config, stream):
    """Lists the topic's partitions: the control plane makes one task per partition."""
    topic=stream['schema'].get('x-kafka-topic')
    if not topic or stream_name(topic)!=stream['name']: raise ValueError('schema_drift')
    c=consumer(config)
    try: meta=c.list_topics(topic,timeout=10).topics.get(topic)
    finally: c.close()
    if meta is None or meta.error is not None or not meta.partitions: raise ValueError('schema_drift')
    return [str(p) for p in sorted(meta.partitions)]
# Backpressure (Phase 3 K3). Records stream straight through to the destination, so the actor holds
# none of them; a checkpoint is taken every max_batch_records records or max_batch_bytes of record
# payload, whichever comes first, and librdkafka's prefetch queue is capped. A run stops after
# max_run_records records or max_run_seconds and completes at the position it reached: the next run
# continues from there (the lag metric shows what is left). Values are bounded so a config cannot
# turn the limits off.
LIMITS={'max_batch_records':(500,1,10000),'max_batch_bytes':(4*1024*1024,64*1024,8*1024*1024),
        'max_run_records':(None,1,10**9),'max_run_seconds':(600,10,3600),'max_prefetch_kbytes':(16384,1024,65536)}
def limits(config):
    out={}
    for name,(default,low,high) in LIMITS.items():
        value=config.get(name,default)
        if value is not None and (isinstance(value,bool) or not isinstance(value,int) or not low<=value<=high): raise ValueError('connector_failed')
        out[name]=value
    return out
def prepare(config, stream, checkpoint, c, shard=''):
    """Reads the topic's identity and watermarks and validates the saved positions against them:
    same cluster and topic, every saved partition present, nothing past the end, and any gap that
    retention opened approved by the owner. Returns (cluster, topic_id, positions, low, high,
    accepted gaps). Used by read() and by the Spark driver (spark_ingest.py)."""
    topic=stream['schema'].get('x-kafka-topic')
    only={int(shard)} if shard else None
    meta=c.list_topics(topic,timeout=10).topics.get(topic)
    if meta is None or meta.error is not None: raise ValueError('schema_drift')
    cluster,topic_id=identity(config,c,topic)
    positions={int(p):int(o) for p,o in (checkpoint.get('positions') or {}).items() if only is None or int(p) in only}
    high={}; low={}
    for p in sorted(meta.partitions):
        if only is None or p in only: low[p],high[p]=c.get_watermark_offsets(TopicPartition(topic,p),timeout=10)
    if only and not (only & set(high)): raise Stop('source_changed',[{'reason':'partition_missing','partition':int(shard),'offset':str(positions.get(int(shard),'0'))}])
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
        approved=sorted((int(g['partition']),str(g['offset']),str(g['low'])) for g in approval.get('partitions',[]) if only is None or int(g['partition']) in only)
        if not (approval.get('topic')==topic and str(approval.get('generation'))==str(stream.get('generation')) and approval.get('topic_id') in (None,topic_id) and wanted==approved):
            raise Stop('offset_out_of_range',gaps)
    # Accepted gaps travel in every checkpoint of this run, so commit history keeps them.
    accepted=[{'partition':g['partition'],'from':g['offset'],'to':g['low']} for g in gaps]
    return cluster,topic_id,positions,low,high,accepted
def read(config, stream, checkpoint):
    topic=stream['schema'].get('x-kafka-topic')
    if not topic or stream_name(topic)!=stream['name']: raise ValueError('schema_drift')
    # A partition task (shard) reads and checkpoints only its own partition.
    shard=stream.get('shard') or ''
    only={int(shard)} if shard else None
    decode,key=decoder(config,stream)
    bounds=limits(config)
    c=consumer(config)
    try:
        cluster,topic_id,positions,low,high,accepted=prepare(config,stream,checkpoint,c,shard)
        def checkpoint_state(completed=False):
            return state(positions=positions,completed=completed,gaps=accepted or None,source={k:v for k,v in (('cluster_id',cluster),('topic_id',topic_id)) if v})
        assign=[]
        for p in sorted(low):
            start=max(positions.get(p,low[p]),low[p])
            positions[p]=start
            if start<high[p]: assign.append(TopicPartition(topic,p,start))
        def lag(fresh=False):
            # Progress for the lag metric: committed position and end offset per partition. The
            # final report re-reads end offsets, so lag counts records produced during the run.
            out=[]
            for p in sorted(low):
                end=high[p]
                if fresh:
                    try: end=max(end,c.get_watermark_offsets(TopicPartition(topic,p),timeout=5)[1])
                    except KafkaException: pass
                out.append({'partition':str(p),'position':str(positions[p]),'end':str(end)})
            for i in range(0,len(out),1024): yield {'type':'LOG','log':{'level':'INFO','code':'partition_lag','detail':out[i:i+1024]}}
        pending={tp.partition for tp in assign}
        if assign: c.assign(assign)
        deadline=time.time()+bounds['max_run_seconds']
        batch=0; batch_bytes=0; total=0; stopped=None
        while pending:
            if time.time()>=deadline: stopped='max_run_seconds'; break
            if bounds['max_run_records'] is not None and total>=bounds['max_run_records']: stopped='max_run_records'; break
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
            try: message=to_record(stream,decode,key,msg.key(),msg.value(),p,o,ts)
            except Decode as e: message=dead_letter(stream,f'{topic_id or cluster}:{p}:{o}',e.code,raw_payload(msg.key(),msg.value(),p,o,ts))
            yield message
            positions[p]=o+1; total+=1; batch+=1; batch_bytes+=size(msg.key(),msg.value())
            if positions[p]>=high[p]: pending.discard(p)
            if batch>=bounds['max_batch_records'] or batch_bytes>=bounds['max_batch_bytes']:
                batch=0; batch_bytes=0
                yield checkpoint_state(); yield from lag()
        if stopped:
            # Not an error: the run ends at a committed position and the next run continues.
            yield {'type':'LOG','log':{'level':'INFO','code':'run_limit_reached','detail':[{'limit':stopped,'records':str(total),'partitions_left':str(len(pending))}]}}
        yield checkpoint_state(True)
        yield from lag(True)
    finally: c.close()
def portable(config):
    """The config with its certificate files inlined as PEM, for a reader that cannot open the
    agent host's files (Spark executors). PKCS#12 keystores have no PEM form in librdkafka: use the
    certificate and key PEM files instead."""
    out=dict(config)
    for location,pem in (('ssl_ca_location','ssl_ca_pem'),('ssl_certificate_location','ssl_certificate_pem'),('ssl_key_location','ssl_key_pem'),('schema_registry_ca_location','schema_registry_ca_pem')):
        path=out.pop(location,None)
        if path:
            with open(path,encoding='utf-8') as f: out[pem]=f.read()
    if out.get('ssl_keystore_location'): raise ValueError('connector_failed')
    return out
def chunk(config, stream, partition, start, end, reached):
    """Reads offsets [start, end) of one partition, as read() converts them: RECORD messages, or
    DEAD_LETTER messages for records that cannot be decoded. reached['offset'] ends as the next
    offset to read, which stays below `end` when the log ends first (compaction, aborted
    transactions or records not yet committed): a position never moves past what was read."""
    topic=stream['schema']['x-kafka-topic']
    decode,key=decoder(config,stream)
    reached['offset']=start
    if start>=end: return
    c=consumer(config)
    try:
        topic_id=reached.get('topic_id'); cluster=reached.get('cluster')
        c.assign([TopicPartition(topic,partition,start)])
        # The chunk's offsets exist (they are below the high watermark the run captured), so a
        # partition that returns nothing for STALL seconds means the brokers are unreachable.
        quiet=time.time()
        while reached['offset']<end:
            msg=c.poll(1.0)
            if msg is None:
                if time.time()-quiet>STALL: raise KafkaException(KafkaError(KafkaError._TIMED_OUT))
                continue
            quiet=time.time()
            if msg.error():
                if msg.error().code()==KafkaError._PARTITION_EOF:
                    reached['offset']=max(reached['offset'],min(msg.offset(),end)); return
                raise KafkaException(msg.error())
            o=msg.offset()
            if o>=end: reached['offset']=end; return
            ts=msg.timestamp()[1]
            if size(msg.key(),msg.value())>MAX_DEAD_LETTER: raise Stop('record_too_large',[{'partition':partition,'offset':str(o),'bytes':str(size(msg.key(),msg.value()))}])
            try: message=to_record(stream,decode,key,msg.key(),msg.value(),partition,o,ts)
            except Decode as e: message=dead_letter(stream,f'{topic_id or cluster}:{partition}:{o}',e.code,raw_payload(msg.key(),msg.value(),partition,o,ts))
            yield message,o
            reached['offset']=o+1
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
    if a.command=='spec': emit('SPEC',spec={'protocol':'meridian-connect/1','version':VERSION,'partitioned':True,'config':['bootstrap_servers',*SECURITY,'allow_cleartext_password','schema_registry_url','schema_registry_username','schema_registry_password','schema_registry_ca_location','group_id',*LIMITS,'profile','api_key','api_secret','connection_string','aws_region','aws_role_arn','aws_access_key_id','aws_secret_access_key','aws_session_token'],'profiles':sorted(PROFILES),'security':{'protocols':sorted(PROTOCOLS),'sasl_mechanisms':sorted(MECHANISMS),'mtls':True},'modes':['incremental'],'deletes':True,'positions':'per-partition offsets, stored by the control plane','formats':['json','debezium-json','confluent-avro']}); return
    if a.command=='health': emit('LOG',log={'level':'INFO','code':'healthy','version':VERSION}); return
    try:
        config=apply_profile(resolve_files(load(a.config),os.path.dirname(os.path.abspath(a.config)) if a.config else ''))
        if a.command=='check':
            c=consumer(config)
            try: c.list_topics(timeout=10)
            finally: c.close()
            if config.get('schema_registry_url'): Registry(config).get('/subjects')
            emit('CONNECTION_STATUS',connectionStatus={'status':'SUCCEEDED'})
        elif a.command=='discover': emit('CATALOG',catalog={'streams':discover(config)})
        elif a.command=='plan': emit('PLAN',plan={'shards':plan(config,load(a.catalog))})
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
        # Credentials, authorization and TLS trust failures are terminal: retrying cannot fix them.
        auth=(isinstance(err,KafkaError) and err.code() in (KafkaError._AUTHENTICATION,KafkaError.SASL_AUTHENTICATION_FAILED,KafkaError.TOPIC_AUTHORIZATION_FAILED,KafkaError.GROUP_AUTHORIZATION_FAILED,KafkaError.CLUSTER_AUTHORIZATION_FAILED,KafkaError._SSL)) or (isinstance(e,urllib.error.HTTPError) and e.code in (401,403)) or isinstance(getattr(e,'reason',None),ssl.SSLCertVerificationError)
        code=str(e) if str(e) in ('schema_drift','connector_failed') else 'auth_failed' if auth else 'source_unavailable'
        emit('TRACE',trace={'type':'ERROR','code':code}); raise SystemExit(1)
if __name__=='__main__': main()
