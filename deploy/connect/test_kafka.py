"""Live Kafka actor tests against the 3-broker cluster in deploy/connect/certify.compose.yml.
Skips when the cluster is not running. Stops and restarts one broker (docker) to prove reads and
acks=all writes survive the loss of a replica."""
import base64, io, json, os, shutil, socket, struct, subprocess, sys, tempfile, time, unittest, urllib.request, uuid
import fastavro
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from sdk import validate
BROKERS=os.environ.get('KAFKA_CERTIFY_BROKERS','127.0.0.1:39191,127.0.0.1:39192,127.0.0.1:39193')
REGISTRY=os.environ.get('KAFKA_CERTIFY_REGISTRY','http://127.0.0.1:38081')
PROJECT=os.environ.get('KAFKA_CERTIFY_PROJECT','meridian-certify')
ACTOR=Path(__file__).parent/'source_kafka.py'
DOCKER=shutil.which('docker') or r'C:\Program Files\Docker\Docker\resources\bin\docker.exe'
def reachable():
    host,port=BROKERS.split(',')[0].split(':')
    try: socket.create_connection((host,int(port)),2).close(); return True
    except OSError: return False
def run_actor(command, config, catalog=None, state=None):
    with tempfile.TemporaryDirectory() as tmp:
        args=[sys.executable,str(ACTOR),command]
        for name,value in [('config',config),('catalog',catalog),('state',state)]:
            if value is None: continue
            p=Path(tmp)/f'{name}.json'; p.write_text(json.dumps(value)); args+=['--'+name,str(p)]
        proc=subprocess.run(args,capture_output=True,text=True,timeout=300,env={**os.environ,'PYTHONPATH':str(ACTOR.parent)})
    return [validate(json.loads(l)) for l in proc.stdout.splitlines() if l.strip()],proc.returncode
def records(ms): return [m['record'] for m in ms if m['type']=='RECORD']
def dead(ms): return [m['dead_letter'] for m in ms if m['type']=='DEAD_LETTER']
def states(ms): return [m['state'] for m in ms if m['type']=='STATE']
def docker(*args): subprocess.run([DOCKER,*args],check=True,capture_output=True,timeout=180)
def wait_healthy(container, seconds=180):
    end=time.time()+seconds
    while time.time()<end:
        out=subprocess.run([DOCKER,'inspect','-f','{{.State.Health.Status}}',container],capture_output=True,text=True).stdout.strip()
        if out=='healthy': return
        time.sleep(2)
    raise AssertionError(f'{container} not healthy')
class KafkaCluster(unittest.TestCase):
    """Shared helpers: topics are unique per test so runs never see each other's data."""
    config={'bootstrap_servers':BROKERS}
    @classmethod
    def setUpClass(cls):
        cls.admin=AdminClient({'bootstrap.servers':BROKERS})
    def topic(self, partitions=3):
        name=f'certify.{self.id().rsplit(".",1)[-1]}.{uuid.uuid4().hex[:8]}'
        f=self.admin.create_topics([NewTopic(name,partitions,3,config={'min.insync.replicas':'2'})])[name]; f.result(30)
        # Discovery may bootstrap through any broker before metadata has propagated.
        for broker in BROKERS.split(','):
            client=AdminClient({'bootstrap.servers':broker})
            for _ in range(30):
                metadata=client.list_topics(name,timeout=10).topics.get(name)
                parts=metadata.partitions if metadata else {}
                if len(parts)==partitions and all(p.leader>=0 for p in parts.values()): break
                time.sleep(1)
            else: raise AssertionError(f'{name}: partitions not ready on {broker}')
        return name
    def produce(self, topic, messages):
        # Fixture calls may follow deletion/recreation under the same topic name. A fresh
        # producer cannot retain the old topic metadata/PID or pending deliveries from another test.
        producer=Producer({'bootstrap.servers':BROKERS,'acks':'all','enable.idempotence':True,'linger.ms':5})
        # Right after creation a lagging broker can briefly report fewer partitions; librdkafka
        # then fails the queued messages locally (_UNKNOWN_PARTITION) without sending them, so
        # only those are re-sent. Any other delivery error fails the test.
        from confluent_kafka import KafkaError, KafkaException
        unknown=(KafkaError._UNKNOWN_PARTITION,KafkaError._UNKNOWN_TOPIC)
        pending=list(messages)
        for _ in range(10):
            errors=[]; retry=[]
            def done(e,_msg,m): (retry if e and e.code() in unknown else errors).append(m if e and e.code() in unknown else e) if e else None
            for m in pending:
                try: producer.produce(topic,callback=lambda e,msg,m=m: done(e,msg,m),**m)
                except KafkaException as e:
                    if e.args[0].code() not in unknown: raise
                    retry.append(m)
            self.assertEqual(producer.flush(60),0,f'{topic}: fixture deliveries did not drain; errors={errors}'); self.assertEqual(errors,[])
            if not retry: return
            pending=retry; time.sleep(1); producer.list_topics(topic,timeout=10)
        self.fail(f'{topic}: partitions stayed unknown for {len(pending)} messages')
    def stream(self, topic, **schema):
        ms,code=run_actor('discover',self.config); self.assertEqual(code,0)
        s=next(s for s in ms[-1]['catalog']['streams'] if s['json_schema']['x-kafka-topic']==topic)
        return {'namespace':s['namespace'],'name':s['name'],'primary_key':'_partition,_offset','primary_keys':s['primary_key'],'cursor_field':'_offset','sync_mode':'incremental','generation':0,'schema':{**s['json_schema'],**schema}}
    def read(self, stream, state=None):
        ms,code=run_actor('read',self.config,stream,state or {}); self.assertEqual(code,0,ms[-1:]); return ms
@unittest.skipUnless(reachable(),'Kafka certification cluster not running (deploy/connect/certify.compose.yml)')
class KafkaActor(KafkaCluster):
    def test_spec_and_check(self):
        ms,code=run_actor('spec',{}); self.assertEqual(code,0); self.assertTrue(ms[0]['spec']['deletes'])
        ms,code=run_actor('check',self.config); self.assertEqual(code,0); self.assertEqual(ms[-1]['connectionStatus']['status'],'SUCCEEDED')
        ms,code=run_actor('check',{'bootstrap_servers':'127.0.0.1:1'}); self.assertNotEqual(code,0); self.assertEqual(ms[-1]['trace']['code'],'source_unavailable')
    def test_multi_partition_read_resume_and_incremental(self):
        topic=self.topic(3)
        self.produce(topic,[{'key':f'k{i%40}'.encode(),'value':json.dumps({'id':i,'n':'名'+str(i)}).encode()} for i in range(1200)])
        stream=self.stream(topic); first=self.read(stream)
        got=records(first); self.assertEqual(len(got),1200)
        self.assertEqual(sorted(r['data']['id'] for r in got),list(range(1200)))
        self.assertEqual(len({tuple(r['primary_key']) for r in got}),1200)
        self.assertEqual(len({r['primary_key'][0] for r in got}),3)
        # Offsets are strictly increasing within each partition.
        for p in range(3):
            offs=[int(r['primary_key'][1]) for r in got if r['primary_key'][0]==p]; self.assertEqual(offs,sorted(offs))
        final=states(first)[-1]; self.assertTrue(final['completed'])
        # Offsets travel as decimal strings; the checkpoint names the topic and cluster it is for.
        self.assertTrue(all(isinstance(v,str) for v in final['positions'].values()))
        self.assertEqual(sum(int(v) for v in final['positions'].values()),1200)
        self.assertTrue(final['topic_id'] and final['cluster_id'])
        # A new attempt resuming from the first committed checkpoint loses and repeats nothing.
        cut=next(i for i,m in enumerate(first) if m['type']=='STATE')
        before={tuple(r['primary_key']) for r in records(first[:cut])}
        after={tuple(r['primary_key']) for r in records(self.read(stream,first[cut]['state']))}
        self.assertEqual(len(before)+len(after),1200); self.assertEqual(len(before|after),1200)
        self.assertEqual(records(self.read(stream,final)),[])
        self.produce(topic,[{'key':b'new','value':json.dumps({'id':5000+i}).encode()} for i in range(10)])
        self.assertEqual(sorted(r['data']['id'] for r in records(self.read(stream,final))),list(range(5000,5010)))
    def test_deletes_debezium_and_poison_messages(self):
        topic=self.topic(1)
        self.produce(topic,[
          {'key':b'1','value':json.dumps({'id':1,'name':'a'}).encode()},
          {'key':b'1','value':None},
          {'key':b'2','value':json.dumps({'payload':{'op':'u','before':{'id':2,'v':0},'after':{'id':2,'v':1}}}).encode()},
          {'key':b'3','value':json.dumps({'op':'d','before':{'id':3},'after':None}).encode()},
          {'key':b'4','value':b'\xff not json'},
          {'key':b'5','value':json.dumps({'id':5}).encode()}])
        ms=self.read(self.stream(topic)); rs=records(ms)
        self.assertEqual([(r['operation'],r['data'].get('id')) for r in rs],[('upsert',1),('delete',None),('upsert',2),('delete',3),('upsert',5)])
        self.assertEqual(rs[2]['data']['v'],1); self.assertEqual(rs[1]['data']['_kafka']['key'],'1')
        d=dead(ms); self.assertEqual(len(d),1); self.assertEqual(d[0]['error_code'],'deserialization_failed'); self.assertEqual(d[0]['position'],f"{states(ms)[-1]['topic_id']}:0:4")
        self.assertEqual(base64.b64decode(d[0]['payload']['value_b64']),b'\xff not json')
        # The poison message does not block the partition: the position moves past it.
        self.assertEqual(states(ms)[-1]['positions'],{'0':'6'})
    def test_offsets_below_retention_fail_closed_until_the_exact_gap_is_approved(self):
        from confluent_kafka import TopicPartition
        topic=self.topic(2)
        self.produce(topic,[{'value':json.dumps({'id':i}).encode(),'partition':i%2} for i in range(20)])
        stream=self.stream(topic)
        saved={'version':1,'positions':{'0':'3','1':'4'}}
        def expire(before):
            for fut in self.admin.delete_records([TopicPartition(topic,0,before)]).values(): fut.result(30)
        # Retention removes partition 0 below offset 6; partition 1 keeps everything.
        expire(6)
        ms,code=run_actor('read',self.config,stream,saved)
        self.assertNotEqual(code,0)
        self.assertEqual(ms[-1]['trace']['code'],'offset_out_of_range')
        self.assertEqual(ms[-1]['trace']['detail'],[{'partition':0,'offset':'3','low':'6','gap':'3'}])
        # Nothing read, no checkpoint: the saved position stays where it was.
        self.assertEqual(records(ms),[]); self.assertEqual(states(ms),[])
        approval={'topic':topic,'generation':0,'partitions':[{'partition':0,'offset':'3','low':'6'}]}
        # Retention moves on after the approval: the gap is now larger, so the run fails again.
        expire(8)
        ms,code=run_actor('read',self.config,stream,{**saved,'gap_approval':approval})
        self.assertNotEqual(code,0)
        self.assertEqual(ms[-1]['trace']['detail'],[{'partition':0,'offset':'3','low':'8','gap':'5'}])
        self.assertEqual(states(ms),[])
        # An approval for another generation or topic does not count either.
        exact={**approval,'partitions':[{'partition':0,'offset':'3','low':'8'}]}
        for wrong in ({**exact,'generation':1},{**exact,'topic':'other'}):
            self.assertNotEqual(run_actor('read',self.config,stream,{**saved,'gap_approval':wrong})[1],0)
        accepted=self.read(stream,{**saved,'gap_approval':exact})
        self.assertEqual(sorted(r['data']['id'] for r in records(accepted)),sorted([i for i in range(16,20,2)]+[i for i in range(9,20,2)]))
        final=states(accepted)[-1]
        self.assertEqual(final['positions'],{'0':'10','1':'10'})
        self.assertEqual(final['gaps'],[{'partition':0,'from':'3','to':'8'}])
        self.assertNotIn('gap_approval',final)
    def test_recreated_topic_and_positions_past_the_end_fail_closed_until_reset(self):
        from confluent_kafka.admin import NewTopic
        topic=self.topic(2)
        self.produce(topic,[{'value':json.dumps({'id':i}).encode(),'partition':i%2} for i in range(10)])
        stream=self.stream(topic); done=states(self.read(stream))[-1]
        # A position beyond the high watermark cannot belong to this topic's data.
        ms,code=run_actor('read',self.config,stream,{**done,'positions':{'0':'99','1':'5'}})
        self.assertNotEqual(code,0)
        self.assertEqual(ms[-1]['trace']['code'],'source_changed')
        self.assertEqual(ms[-1]['trace']['detail'],[{'reason':'offset_above_high','partition':0,'offset':'99','high':'5'}])
        # Delete and recreate the topic under the same name with fewer partitions.
        for fut in self.admin.delete_topics([topic]).values(): fut.result(30)
        for _ in range(30):
            try:
                for fut in self.admin.create_topics([NewTopic(topic,1,3)]).values(): fut.result(30)
                break
            except Exception: time.sleep(1)
        self.produce(topic,[{'value':json.dumps({'id':100}).encode()}])
        ms,code=run_actor('read',self.config,stream,done)
        self.assertNotEqual(code,0); self.assertEqual(ms[-1]['trace']['code'],'source_changed')
        reasons=[d['reason'] for d in ms[-1]['trace']['detail']]
        self.assertIn('topic_id',reasons); self.assertIn('partition_missing',reasons)
        self.assertEqual(states(ms),[])
        # An explicit reset (empty state) starts over on the new topic.
        fresh=self.read(stream,{})
        self.assertEqual([r['data']['id'] for r in records(fresh)],[100])
        self.assertNotEqual(states(fresh)[-1]['topic_id'],done['topic_id'])
    def test_record_size_limits(self):
        from confluent_kafka.admin import NewTopic
        name=f'certify.size.{uuid.uuid4().hex[:8]}'
        for fut in self.admin.create_topics([NewTopic(name,1,3,config={'max.message.bytes':'3000000','min.insync.replicas':'2'})]).values(): fut.result(30)
        time.sleep(2)
        big=Producer({'bootstrap.servers':BROKERS,'acks':'all','message.max.bytes':3000000})
        values=[json.dumps({'id':1,'blob':'a'*990_000}).encode(), os.urandom(1_050_000), json.dumps({'id':3}).encode()]
        for v in values: big.produce(name,value=v)
        self.assertEqual(big.flush(60),0)
        stream=self.stream(name)
        # A ~1 MB record lands; a 1.05 MB binary value is dead-lettered whole, never truncated.
        ms=self.read(stream)
        self.assertEqual([r['data']['id'] for r in records(ms)],[1,3])
        self.assertEqual(len(records(ms)[0]['data']['blob']),990_000)
        d=dead(ms); self.assertEqual([x['error_code'] for x in d],['record_too_large'])
        self.assertEqual(base64.b64decode(d[0]['payload']['value_b64']),values[1])
        done=states(ms)[-1]
        # A 1.5 MB value can be neither landed nor kept whole: the run stops before it.
        big.produce(name,value=os.urandom(1_500_000)); self.assertEqual(big.flush(60),0)
        ms,code=run_actor('read',self.config,stream,done)
        self.assertNotEqual(code,0)
        self.assertEqual(ms[-1]['trace']['code'],'record_too_large')
        self.assertEqual(ms[-1]['trace']['detail'],[{'partition':0,'offset':'3','bytes':'1500000'}])
        self.assertEqual(states(ms),[])
    def test_messages_produced_during_a_run_are_not_skipped(self):
        import importlib, sys as _sys
        _sys.path.insert(0,str(ACTOR.parent)); source_kafka=importlib.import_module('source_kafka')
        topic=self.topic(1)
        self.produce(topic,[{'value':json.dumps({'id':i}).encode()} for i in range(5)])
        stream=self.stream(topic)
        # Drive the actor in-process: the run's high watermark is taken before the first yield,
        # then more messages arrive while it is still reading.
        gen=source_kafka.read(self.config,stream,{},batch_size=1)
        first=next(gen)
        self.produce(topic,[{'value':json.dumps({'id':100+i}).encode()} for i in range(20)])
        rest=list(gen)
        final=[m['state'] for m in rest if m['type']=='STATE'][-1]
        self.assertEqual(final['positions'],{'0':'5'})
        got=[m['record']['data']['id'] for m in [first,*rest] if m['type']=='RECORD']
        self.assertEqual(got,list(range(5)))
        nxt=self.read(stream,final)
        self.assertEqual([r['data']['id'] for r in records(nxt)],list(range(100,120)))
    def test_replay_reports_what_still_fails(self):
        topic=self.topic(1); stream=self.stream(topic)
        good={'id':str(uuid.uuid4()),'position':'0:1','error_code':'deserialization_failed','payload':{'value_b64':base64.b64encode(json.dumps({'id':1}).encode()).decode(),'partition':0,'offset':'1'}}
        bad={'id':str(uuid.uuid4()),'position':'0:2','error_code':'deserialization_failed','payload':{'value_b64':base64.b64encode(b'still not json').decode(),'partition':0,'offset':'2'}}
        ms,code=run_actor('replay',self.config,{**stream,'dead_letters':[good,bad]},{})
        # The resolved one is committed (STATE before the failure); the run still fails, with counts.
        self.assertEqual([r['data']['id'] for r in records(ms)],[1])
        self.assertEqual(states(ms)[-1]['replayed'],[good['id']])
        self.assertNotEqual(code,0)
        self.assertEqual(ms[-1]['trace'],{'type':'ERROR','code':'replay_incomplete','detail':[{'selected':'2','replayed':'1','still_failing':'1','deferred':'0'}]})
    def test_survives_broker_loss(self):
        topic=self.topic(3)
        self.produce(topic,[{'value':json.dumps({'id':i}).encode()} for i in range(300)])
        stream=self.stream(topic); done=states(self.read(stream))[-1]
        container=f'{PROJECT}-kafka-2-1'
        docker('stop',container)
        try:
            # acks=all still succeeds with min.insync.replicas=2 of 3; leaders move to live brokers.
            self.produce(topic,[{'value':json.dumps({'id':300+i}).encode()} for i in range(300)])
            got=records(self.read(stream,done))
            self.assertEqual(sorted(r['data']['id'] for r in got),list(range(300,600)))
            self.assertEqual(len(records(self.read(stream,{}))),600)
        finally:
            docker('start',container); wait_healthy(container)
def register(subject, schema):
    req=urllib.request.Request(f'{REGISTRY}/subjects/{subject}/versions',data=json.dumps({'schema':json.dumps(schema)}).encode(),headers={'Content-Type':'application/vnd.schemaregistry.v1+json'})
    with urllib.request.urlopen(req,timeout=10) as r: return json.load(r)['id']
def wire(schema_id, schema, value):
    out=io.BytesIO(); out.write(b'\x00'+struct.pack('>I',schema_id)); fastavro.schemaless_writer(out,fastavro.parse_schema(schema),value); return out.getvalue()
V1={'type':'record','name':'Order','namespace':'certify','fields':[{'name':'id','type':'long'},{'name':'amount','type':{'type':'bytes','logicalType':'decimal','precision':12,'scale':2}}]}
V2={**V1,'fields':V1['fields']+[{'name':'region','type':'string','default':'eu'}]}
@unittest.skipUnless(reachable(),'Kafka certification cluster not running (deploy/connect/certify.compose.yml)')
class AvroRegistry(KafkaCluster):
    config={'bootstrap_servers':BROKERS,'schema_registry_url':REGISTRY}
    def test_confluent_avro_decode_and_evolution(self):
        import decimal
        topic=self.topic(1); v1=register(f'{topic}-value',V1); v2=register(f'{topic}-value',V2)
        self.assertNotEqual(v1,v2)
        self.produce(topic,[{'value':wire(v1,V1,{'id':1,'amount':decimal.Decimal('12.50')})},{'value':wire(v2,V2,{'id':2,'amount':decimal.Decimal('0.01'),'region':'us'})}])
        stream=self.stream(topic); self.assertEqual(stream['schema']['x-value-format'],'avro')
        rs=records(self.read(stream))
        self.assertEqual([(r['data']['id'],r['data']['amount'],r['data'].get('region')) for r in rs],[(1,'12.50',None),(2,'0.01','us')])
        # A reader schema pins the shape: v2 data resolves to v1 (extra field dropped).
        pinned=records(self.read({**stream,'schema':{**stream['schema'],'x-avro-reader-schema':V1}}))
        self.assertEqual([sorted(r['data']) for r in pinned],[['_kafka','amount','id']]*2)
        # A reader needing a field the writer lacks (and no default) is a dead letter, not a crash.
        strict={**V1,'fields':V1['fields']+[{'name':'channel','type':'string'}]}
        ms=self.read({**stream,'schema':{**stream['schema'],'x-avro-reader-schema':strict}})
        self.assertEqual(records(ms),[]); self.assertEqual([d['error_code'] for d in dead(ms)],['schema_incompatible']*2)
    def test_unknown_schema_id_dead_letters_then_replays(self):
        topic=self.topic(1)
        # Karapace hands out global ids in order and reuses them for identical schemas, so both
        # schemas here are unique to this run: the next id is not registered yet.
        unique=f'certify.r{uuid.uuid4().hex}'
        upcoming=register(f'{topic}-probe',{'type':'record','name':'Probe','namespace':unique,'fields':[{'name':'p','type':'int'}]})+1
        schema={**V1,'namespace':unique}
        self.produce(topic,[{'key':b'o-9','value':wire(upcoming,schema,{'id':9,'amount':b'\x01'})}])
        stream=self.stream(topic); ms=self.read(stream)
        letters=dead(ms); self.assertEqual([d['error_code'] for d in letters],['schema_not_found'])
        self.assertEqual(states(ms)[-1]['positions'],{'0':'1'})
        self.assertEqual(register(f'{topic}-value',schema),upcoming)
        stored=[{'id':str(uuid.uuid4()),'position':d['position'],'error_code':d['error_code'],'payload':d['payload']} for d in letters]
        out,code=run_actor('replay',self.config,{**stream,'dead_letters':stored},{})
        self.assertEqual(code,0)
        self.assertEqual([(r['data']['id'],r['data']['amount'],r['data']['_kafka']['key'],r['primary_key']) for r in records(out)],[(9,'0.01','o-9',[0,'0'])])
        self.assertEqual(states(out),[{'version':1,'cursor':None,'primary_key':None,'completed':True,'replayed':[stored[0]['id']]}])
if __name__=='__main__': unittest.main()
